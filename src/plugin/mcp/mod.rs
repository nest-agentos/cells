//! Stateless MCP transport; contracts and directory projections have separate owners.
mod protocol;
mod requests;
mod resources;
mod tools;
use crate::{app::State, core::error::Error, http::access};
use axum::{
    extract::{rejection::JsonRejection, State as AxumState},
    http::{HeaderMap, StatusCode},
    Json,
};
use protocol::{Failure, Response};
use serde_json::Value;
use std::sync::Arc;
const VERSION: &str = "2026-07-28";

pub(crate) async fn endpoint(
    AxumState(state): AxumState<Arc<State>>,
    headers: HeaderMap,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, Error> {
    let identity = access::identity(&state, &headers)?;
    origin(&state, &headers)?;
    let body = match body {
        Ok(Json(body)) => body,
        Err(error) => return Ok(Failure::json(error).response(Value::Null)),
    };
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    if let Err(error) = protocol::validate(&state, identity, &headers, &body) {
        return Ok(error.response(id));
    }
    Ok(
        match requests::invoke(&state, identity, &headers, &body).await {
            Ok(result) => (
                StatusCode::OK,
                Json(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result})),
            ),
            Err(error) => error.response(id),
        },
    )
}

fn origin(state: &State, headers: &HeaderMap) -> Result<(), Error> {
    let Some(origin) = headers.get("origin") else {
        return Ok(());
    };
    let origin = origin.to_str().map_err(|_| Error::forbidden("EACCES"))?;
    if state
        .allowed_origins
        .iter()
        .any(|allowed| allowed == origin || allowed == "*")
    {
        Ok(())
    } else {
        Err(Error::forbidden("EACCES"))
    }
}

pub(crate) mod plugin;

mod contribution;

pub use plugin::Mcp as Plugin;
