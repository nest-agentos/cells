use crate::app::State;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

pub(crate) fn routes() -> Router<Arc<State>> {
    use crate::plugin::process::http as processes;
    Router::new()
        .route("/v1/processes", post(processes::start))
        .route("/v1/processes/:epoch/:id", get(processes::status))
        .route("/v1/processes/:epoch/:id/cancel", post(processes::cancel))
        .layer(DefaultBodyLimit::max(512 * 1024))
}
