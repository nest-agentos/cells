//! `PUT /v1/fs/:alias/content` and `POST /v1/fs/:alias/mutate`.
//!
//! Both endpoints share one shape: validate the precondition, claim the
//! operation id, hand the work to a task that owns it after the HTTP waiter
//! disconnects, and return the recorded receipt. The domain work itself lives
//! in [`crate::fs::upload`] and [`crate::fs::mutation`].

use crate::{
    app::State,
    core::error::Error,
    fs::{mutation::Change, path},
    http::{access, query::ListQuery},
};
use axum::{
    body::Body,
    extract::{Path, Query, State as AxumState},
    http::{HeaderMap, StatusCode},
    Json,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use super::super::writing::{Replacement, WriteCondition};

pub async fn replace(
    AxumState(state): AxumState<Arc<State>>,
    Path(alias): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
    body: Body,
) -> Result<(StatusCode, Json<Value>), Error> {
    let deadline = access::deadline(&headers)?;
    path::validate(&query.path)?;
    let _project_guard =
        access::authorize_path(&state, &headers, &alias, &query.path, true, false)?;
    let condition = native_condition(&headers)?;
    let gate = state.files.shared().map_err(|error| Error {outcome: Some("not-committed"), ..error})?;
    let (identity, export) = access::writable_export(&state, &headers, &alias)?;
    let operation = state
        .services
        .require::<crate::operations::Operations>()?
        .register(identity, &alias, access::operation_id(&headers)?)?;
    let task = operation.clone();
    let audit = json!({"identity": identity, "alias": alias, "operationId": access::operation_id(&headers)?,
        "action": "replace", "path": query.path});
    crate::core::events::emit(
        crate::core::events::Level::Debug,
        "mutation.accepted",
        audit.clone(),
    );
    let replacement = Replacement {
        state,
        export,
        path: query.path,
        condition,
        deadline,
        operation: task.clone(),
        operation_id: access::operation_id(&headers)?.to_owned(),
        gate,
        project_guards:_project_guard,
    };
    // The operation owns the work and its cleanup after the waiter disconnects.
    crate::core::trace::spawn(async move {
        let result = replacement.run(Box::pin(body.into_data_stream().map(|chunk| chunk.map(|bytes| bytes.to_vec()).map_err(|_| Error::new(StatusCode::BAD_REQUEST, "UPLOAD_DISCONNECTED"))))).await;
        mutation_finished(audit, &result);
        task.finish(result);
    });
    let (status, receipt) = operation.wait(deadline).await;
    Ok((status, Json(receipt)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    action: String,
    path: String,
    to: Option<String>,
    #[serde(default)]
    recursive: bool,
    metadata: Option<serde_json::Map<String, Value>>,
    #[serde(default, rename = "removeKeys")]
    remove_keys: Vec<String>,
}

pub async fn mutate(
    AxumState(state): AxumState<Arc<State>>,
    Path(alias): Path<String>,
    headers: HeaderMap,
    Json(command): Json<Command>,
) -> Result<(StatusCode, Json<Value>), Error> {
    let deadline = access::deadline(&headers)?;
    if command.recursive {
        return Err(Error::unsupported());
    }
    let audit_paths = json!({"action": command.action, "path": command.path, "to": command.to});
    let mut project_guards=access::authorize_path(&state,&headers,&alias,&command.path,true,false)?;
    if let Some(to)=&command.to {project_guards.extend(access::authorize_path(&state,&headers,&alias,to,true,false)?);}
    let change = if command.action == "metadata" {
        path::validate(&command.path)?;
        let patch = command.metadata.ok_or_else(Error::invalid)?;
        if command.to.is_some()
            || command.remove_keys.len() > 1024
            || serde_json::to_vec(&patch)
                .map_err(|_| Error::invalid())?
                .len()
                > 256 * 1024
        {
            return Err(Error::invalid());
        }
        Change::Metadata {
            path: command.path,
            patch,
            remove: command.remove_keys,
        }
    } else {
        if command.metadata.is_some() || !command.remove_keys.is_empty() {
            return Err(Error::invalid());
        }
        Change::parse(&command.action, command.path, command.to)?
    };
    let gate = state.files.shared().map_err(|error| Error {outcome: Some("not-committed"), ..error})?;
    let (identity, export) = access::writable_export(&state, &headers, &alias)?;
    let operation = state
        .services
        .require::<crate::operations::Operations>()?
        .register(identity, &alias, access::operation_id(&headers)?)?;
    let task = operation.clone();
    let audit = json!({"identity": identity, "alias": alias, "operationId": access::operation_id(&headers)?, "change": audit_paths});
    crate::core::events::emit(
        crate::core::events::Level::Debug,
        "mutation.accepted",
        audit.clone(),
    );
    crate::core::trace::spawn(async move {
        let result = match state.workers.acquire(deadline).await {
            Ok(permit) => {
                let tracked = task.clone();
                let context = crate::core::trace::current();
                match tokio::task::spawn_blocking(move || crate::core::trace::sync_scope(context,|| {
                    let _permit = permit;
                    let _gate = gate;
                    let _project_guards=project_guards;
                    export.apply(&change, || tracked.checkpoint(deadline))
                }))
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(Error::unknown("FILE_COMMIT_WORKER_LOST")),
                }
            }
            Err(error) => Err(error),
        };
        let result = result.map(|()| json!({}));
        mutation_finished(audit, &result);
        task.finish(result);
    });
    let (status, receipt) = operation.wait(deadline).await;
    Ok((status, Json(receipt)))
}

fn mutation_finished(operation: Value, result: &Result<Value, Error>) {
    crate::core::events::emit(
        if result.is_ok() {
            crate::core::events::Level::Info
        } else {
            crate::core::events::Level::Warn
        },
        "mutation.finished",
        json!({"operation": operation,
        "outcome": result.as_ref().err().and_then(|error| error.outcome).unwrap_or(if result.is_ok() {"committed"} else {"not-committed"}),
        "code": result.as_ref().err().map(|error| error.code)}),
    );
}

fn native_condition(headers: &HeaderMap) -> Result<WriteCondition, Error> {
        let expected = headers
            .get("if-match")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let create = headers
            .get("if-none-match")
            .is_some_and(|value| value == "*");
        match (create, expected) {
            (true, None) => Ok(WriteCondition::Create),
            (false, Some(revision)) => Ok(WriteCondition::Replace(revision)),
            // Neither, or both: the caller has not stated a usable precondition.
            _ => Err(Error::precondition_required("REVISION_REQUIRED")),
        }
    }
