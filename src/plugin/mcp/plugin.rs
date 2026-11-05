use crate::{app::State, plugin::common::NodePlugin, plugin::common::PluginConfiguration};
use axum::{extract::DefaultBodyLimit, routing::post, Router};
use std::sync::Arc;
pub struct Mcp;
impl NodePlugin for Mcp {
    fn request_headers(&self) -> &'static [&'static str] {
        &["mcp-protocol-version", "mcp-method", "mcp-name"]
    }

    fn id(&self) -> &'static str {
        "mcp"
    }
    fn routes(&self) -> Router<Arc<State>> {
        Router::new()
            .route("/v1/channel/mcp", post(crate::http::agent_mcp::endpoint))
            .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
    }
    fn validate(&self, _config: &dyn PluginConfiguration) -> Result<(), String> {
        Ok(())
    }
}
