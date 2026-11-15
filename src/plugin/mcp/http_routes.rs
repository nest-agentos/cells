use crate::app::State;
use axum::{extract::DefaultBodyLimit, routing::post, Router};
use std::sync::Arc;

pub(crate) fn routes() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/channel/mcp", post(crate::http::agent_mcp::endpoint))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
}
