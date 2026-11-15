//! `POST /v1/fs/:alias/stat` — batch metadata lookup.
//!
//! A batch is bounded on both ends: at most [`MAX_PATHS`] paths and
//! [`MAX_PATH_BYTES`] of raw path bytes, so one request cannot pin a worker
//! indefinitely. Per-path failures are reported inside the batch rather than
//! failing the whole request.

use crate::{app::State, core::error::Error, http::access};
use axum::{
    extract::{Path, State as AxumState},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

const MAX_PATHS: usize = 256;
const MAX_PATH_BYTES: usize = 64 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatRequest {
    paths: Vec<String>,
    #[serde(default)]
    fields: crate::fs::model::ReadFields,
}

pub async fn stat(
    AxumState(state): AxumState<Arc<State>>,
    Path(alias): Path<String>,
    headers: HeaderMap,
    Json(input): Json<StatRequest>,
) -> Result<Json<Value>, Error> {
    let deadline = access::deadline(&headers)?;
    crate::core::gate_read::retry(deadline, || stat_once(
        AxumState(state.clone()), Path(alias.clone()), headers.clone(), Json(input.clone()),
    )).await
}

async fn stat_once(
    AxumState(state): AxumState<Arc<State>>,
    Path(alias): Path<String>,
    headers: HeaderMap,
    Json(input): Json<StatRequest>,
) -> Result<Json<Value>, Error> {
    let identity = access::identity(&state, &headers)?;
    state.auth.authorize(identity, &alias)?;
    let deadline = access::deadline(&headers)?;
    if input.paths.len() > MAX_PATHS
        || input.paths.iter().map(String::len).sum::<usize>() > MAX_PATH_BYTES
    {
        return Err(Error::invalid());
    }
    let _project_guards = input
        .paths
        .iter()
        .map(|path| access::authorize_path(&state, &headers, &alias, path, false, true))
        .collect::<Result<Vec<_>, _>>()?.into_iter().flatten().collect();
    let reading = super::super::reading::Reading::authorize(&state, identity, &alias, _project_guards, deadline)?;
    let results = reading.stat(input.paths, input.fields).await?
        .into_iter().map(|result| match result {
            Ok(stat) => json!({"stat":stat}), Err(error) => json!({"error":error.code}),
        }).collect::<Vec<_>>();
    Ok(Json(json!({"results":results})))
}
