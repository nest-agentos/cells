use crate::{app::State, plugin::common::NodePlugin, plugin::common::PluginConfiguration};

use std::sync::Arc;
pub struct Mcp;
impl NodePlugin for Mcp {
    fn configure(&self,config:&dyn PluginConfiguration,state:&mut State)->Result<(),String> {
        state.services.insert(Arc::new(super::listener::Listener::new(super::listener::address(config)?)))
    }
    fn start<'a>(&'a self,state:&'a Arc<State>)->crate::plugins::PluginFuture<'a> {
        Box::pin(async move {state.services.require::<super::listener::Listener>()?.start(state).await})
    }
    fn stop<'a>(&'a self,state:&'a Arc<State>)->crate::plugins::PluginFuture<'a> {
        Box::pin(async move {state.services.require::<super::listener::Listener>()?.stop().await})
    }
    fn request_headers(&self) -> &'static [&'static str] {
        &["mcp-protocol-version", "mcp-method", "mcp-name"]
    }

    fn id(&self) -> &'static str {
        "mcp"
    }

    fn validate(&self, config: &dyn PluginConfiguration) -> Result<(), String> {
        super::listener::address(config).map(|_|())
    }
}
