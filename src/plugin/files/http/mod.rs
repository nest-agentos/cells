pub mod content;
pub mod entries;
pub mod exports;
pub mod mutations;
pub mod operations;
pub mod seq;
pub mod stat;

/// Reuse the file protocol adapter with an independently assembled host.
pub fn routes() -> axum::Router<std::sync::Arc<crate::app::State>> { super::http_routes::routes() }
