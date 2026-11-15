use super::http as handlers;
use crate::app::State;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

pub(crate) fn routes() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/exports", get(handlers::exports::exports))
        .route("/v1/fs/:alias/stat", post(handlers::stat::stat))
        .route("/v1/fs/:alias/entries", get(handlers::entries::entries))
        .route(
            "/v1/fs/:alias/content",
            get(handlers::content::content).put(handlers::mutations::replace),
        )
        .route("/v1/fs/:alias/mutate", post(handlers::mutations::mutate))
        .merge(super::webdav::routes())
        .merge(seq_routes())
        .merge(operation_routes())
        .layer(DefaultBodyLimit::max(512 * 1024))
}

fn seq_routes() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/fs/:alias/seq/snapshot", post(handlers::seq::snapshot))
        .route("/v1/fs/:alias/seq/query", post(handlers::seq::query))
        .route("/v1/fs/:alias/seq/body", post(handlers::seq::body))
        .route("/v1/fs/:alias/seq/body/put", post(handlers::seq::put_body))
        .route(
            "/v1/fs/:alias/seq/transaction",
            post(handlers::seq::transaction),
        )
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
}
fn operation_routes() -> Router<Arc<State>> {
    Router::new()
        .route(
            "/v1/fs/:alias/operations/:id",
            get(handlers::operations::status),
        )
        .route(
            "/v1/fs/:alias/operations/:id/cancel",
            post(handlers::operations::cancel),
        )
}
