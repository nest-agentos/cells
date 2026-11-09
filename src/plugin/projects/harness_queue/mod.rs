//! Cells owns native queue acceptance, editing and execution across client lifetimes.
use super::{
    channel::Channels,
    harness_recovery::{self, Binding},
};
use crate::{app::State, core::error::Error};
use serde_json::{json, Value};
use std::sync::Arc;
mod commands;
pub(super) mod store;
mod worker;
pub(crate) async fn call(state: &Arc<State>, identity: usize, args: Value) -> Result<Value, Error> {
    let started = std::time::Instant::now();
    let result = execute(state, identity, &args).await;
    crate::core::events::emit(
        crate::core::events::Level::Info,
        "harness.queue.command",
        json!({"operation":"harness.queue","operationId":args["operationId"].as_str().filter(|s|harness_recovery::valid_id(s)),"stage":"command","result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}),
    );
    result
}
async fn execute(state: &Arc<State>, identity: usize, args: &Value) -> Result<Value, Error> {
    if !["read", "enqueue", "edit"].contains(&args["action"].as_str().unwrap_or("")) {
        return Err(Error::invalid());
    }
    let binding = harness_recovery::authorize(state, identity, args)?;
    if args["action"] != "read"
        && !args["operationId"]
            .as_str()
            .is_some_and(harness_recovery::valid_id)
    {
        return Err(Error::invalid());
    }
    let channels = &state.services.require::<super::ProjectService>()?.channels;
    let _gate = channels.3.lock().await;
    if args["action"] == "read" {
        return Ok(
            json!({"queue":harness_recovery::store::load(channels,&binding)?.1["instructionQueue"]}),
        );
    }
    store::take_ownership(channels, &binding)?;
    let (raw, mut record) = harness_recovery::store::load(channels, &binding)?;
    if store::receipt(channels, &binding, args)?.is_some() {
        return Ok(json!({"queue":record["instructionQueue"]}));
    }
    let before = record["instructionQueue"].clone();
    if before.is_null() {
        record["instructionQueue"] = json!({"revision":0,"paused":false,"items":[],"receipts":[]});
    }
    commands::apply(&mut record["instructionQueue"], args)?;
    store::commit(channels, &binding, args, &raw, &record)?;
    harness_recovery::store::register(channels, &binding)?;
    worker::ensure(state, &binding);
    Ok(json!({"queue":record["instructionQueue"]}))
}
pub(super) fn schema(db: &rusqlite::Connection) -> Result<(), Error> {
    store::schema(db)
}
pub(super) fn protect(
    db: &rusqlite::Connection,
    project: &str,
    owner: &str,
    key: &str,
    before: Option<&str>,
    after: Option<&str>,
) -> Result<(), Error> {
    store::protect(db, project, owner, key, before, after)
}
fn storage(_: impl std::fmt::Display) -> Error {
    Error::unavailable()
}

pub(super) fn worker_start(state: &Arc<State>, binding: &Binding) {
    worker::ensure(state, binding);
}

#[cfg(test)]
mod tests;
