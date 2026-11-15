//! Fixed application API, independent of MCP discovery and transport.
use crate::{app::State, core::error::Error, http::access};
use axum::{extract::{Path, State as AxumState}, http::HeaderMap, Json};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn invoke(
    AxumState(state): AxumState<Arc<State>>, Path((domain, operation)): Path<(String, String)>,
    headers: HeaderMap, Json(args): Json<Value>,
) -> Result<Json<Value>, Error> {
    let identity = access::identity(&state, &headers)?;
    let result = crate::app::operations::invoke(&state, identity, &domain, &operation, args).await?;
    Ok(Json(match result {
        Ok(value) => json!({"ok":true,"value":value}),
        Err(error) => json!({"ok":false,"error":{"code":error.code,"outcome":
            error.outcome.unwrap_or("not-committed")}}),
    }))
}
