use crate::app::State;
use axum::{extract::DefaultBodyLimit, routing::get, Router};
use std::sync::Arc;

pub(crate) fn routes() -> Router<Arc<State>> {
    use crate::sync::transport;
    Router::new()
        .route(
            "/v1/sync/*path",
            get(transport::get)
                .post(transport::post)
                .put(transport::put),
        )
        .layer(DefaultBodyLimit::disable())
}
