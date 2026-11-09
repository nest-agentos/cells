//! The node owns execution recovery, including durable command deduplication.
use super::{channel::Channels, service};
use crate::{app::State, core::error::Error};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
pub(super) mod decision;
pub(super) mod encoding;
pub(super) mod startup;
pub(super) mod store;
pub(crate) use startup::maintain;
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Binding {
    pub(super) project_id: String,
    pub(super) owner: String,
    pub(super) namespace: String,
    pub(super) profile_id: String,
    pub(super) session_id: String,
    pub(super) revision: u64,
}
pub(super) fn schema(db: &rusqlite::Connection) -> Result<(), Error> {
    store::schema(db)
}
fn storage(_: impl std::fmt::Display) -> Error {
    Error::unavailable()
}
pub(super) async fn call(state: &Arc<State>, identity: usize, args: Value) -> Result<Value, Error> {
    let started = std::time::Instant::now();
    let result = execute(state, identity, &args).await;
    crate::core::events::emit(
        crate::core::events::Level::Info,
        "harness.recovery",
        json!({"operation":"harness.recovery","operationId":args["operationId"].as_str().filter(|s| valid_id(s)),"stage":args["action"].as_str().filter(|s| ["inspect","recover","register"].contains(s)),"result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}),
    );
    result
}
async fn execute(state: &Arc<State>, identity: usize, args: &Value) -> Result<Value, Error> {
    if !["register", "inspect", "recover"].contains(&args["action"].as_str().unwrap_or("")) {
        return Err(Error::invalid());
    }
    let binding = authorize(state, identity, args)?;
    let channels = &state.services.require::<super::ProjectService>()?.channels;
    let _guard = channels.3.lock().await;
    let (raw, record) = store::load(channels, &binding)?;
    store::register(channels, &binding)?;
    if args["action"] == "register" {
        if args["queueOwner"] == "cells" {
            super::harness_queue::store::take_ownership(channels, &binding)?;
            super::harness_queue::worker_start(state, &binding);
        }
        return Ok(json!({"registered":true}));
    }
    if args["action"] == "inspect" {
        return Ok(
            json!({"session":inspect(state,identity,&binding).await?,"queue":record["instructionQueue"]}),
        );
    }
    if args["action"] != "recover"
        || !args["operationId"].as_str().is_some_and(valid_id)
        || !args["instructionId"].as_str().is_some_and(valid_id)
        || args["expectedRevision"].as_u64().is_none()
    {
        return Err(Error::invalid());
    }
    if let Some(mut result) = store::prior(channels, &binding, args)? {
        result["duplicate"] = json!(true);
        result["session"] = inspect(state, identity, &binding).await?;
        result["queue"] = record["instructionQueue"].clone();
        return Ok(result);
    }
    store::reserve(channels, &binding, args)?;
    let _native_guard = state
        .services
        .require::<crate::harness::Harnesses>()?
        .recovery_guard(state, identity, args.clone())
        .await?;
    let history = history(state, identity, &binding).await?;
    let latest = inspect(state, identity, &binding).await?;
    let (record, outcome) = decision::recover(record, &latest, &history, args, false)?;
    let mut result = store::commit(channels, &binding, args, &raw, &record, outcome)?;
    result["duplicate"] = json!(false);
    result["session"] = latest;
    result["queue"] = record["instructionQueue"].clone();
    Ok(result)
}
pub(super) fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
}
fn field(args: &Value, name: &str) -> Result<String, Error> {
    args[name]
        .as_str()
        .filter(|s| valid_id(s))
        .map(str::to_owned)
        .ok_or_else(Error::invalid)
}
pub(super) fn authorize(
    state: &Arc<State>,
    identity: usize,
    args: &Value,
) -> Result<Binding, Error> {
    let project = state
        .services
        .require::<super::ProjectService>()?
        .get(identity, &field(args, "projectId")?)?;
    service::authorize(state, identity, &project)?;
    if args["revision"].as_u64() != Some(project.revision) {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }
    let project = service::effective(state, identity, &project);
    if args["action"] != "read"
        && (super::capabilities(state, identity, &project)["sessionControl"] != true
            || project.access != "rw"
            || args["readOnly"] == true)
    {
        return Err(Error::forbidden("HARNESS_RECOVERY_READ_ONLY"));
    }
    let profile_id = field(args, "profileId")?;
    if !state.auth.client(identity).may_harness(&profile_id) {
        return Err(Error::forbidden("EACCES"));
    }
    let namespace = field(args, "namespace")?;
    if namespace.len() != 64 || !namespace.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::invalid());
    }
    Ok(Binding {
        project_id: project.id,
        owner: project.owner,
        revision: project.revision,
        namespace,
        profile_id,
        session_id: field(args, "sessionId")?,
    })
}
async fn native(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    name: &str,
) -> Result<Value, Error> {
    state.services.require::<crate::harness::Harnesses>()?.project_call(state,identity,name,json!({"projectId":binding.project_id,"revision":binding.revision,"profileId":binding.profile_id,"sessionId":binding.session_id})).await
}
async fn inspect(state: &Arc<State>, identity: usize, binding: &Binding) -> Result<Value, Error> {
    let value = native(state, identity, binding, "harness_session_info").await?;
    if value["session"]["id"] != binding.session_id {
        return Err(Error::conflict("HARNESS_RECOVERY_SESSION_CHANGED"));
    }
    Ok(value["session"].clone())
}
pub(super) async fn history(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
) -> Result<Value, Error> {
    let mut cursor = Value::Null;
    let mut turns = Vec::new();
    for _ in 0..16 {
        let mut args = json!({"projectId":binding.project_id,"revision":binding.revision,"profileId":binding.profile_id,"sessionId":binding.session_id});
        if !cursor.is_null() {
            args["cursor"] = cursor.clone();
        }
        let page = state
            .services
            .require::<crate::harness::Harnesses>()?
            .project_call(state, identity, "harness_session_read", args)
            .await?;
        if page["session"]["id"] != binding.session_id || page["recovery"].is_object() {
            return Err(Error::conflict("HARNESS_RECOVERY_HISTORY_INCOMPLETE"));
        }
        for turn in page["turns"].as_array().into_iter().flatten() {
            turns.push(json!({"id":turn["id"],"status":turn["status"]}));
        }
        let next = page["nextCursor"].clone();
        if next.is_null() {
            return Ok(json!({"turns":turns}));
        }
        if next == cursor {
            return Err(Error::conflict("HARNESS_RECOVERY_HISTORY_INCOMPLETE"));
        }
        cursor = next;
    }
    Err(Error::too_large("HARNESS_RECOVERY_HISTORY_LIMIT"))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration;
