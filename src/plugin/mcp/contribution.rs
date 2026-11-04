use crate::{
    app::State,
    plugin::common::{McpContribution, ToolFuture},
};
use serde_json::Value;
use std::sync::{Arc, LazyLock};
static TOOLS: LazyLock<Vec<Value>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("tools.json")).expect("Discovery tool contracts")
});
impl McpContribution for super::plugin::Mcp {
    fn tools(&self) -> &[Value] {
        &TOOLS
    }
    fn call<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        _name: &'a str,
        _args: Value,
    ) -> ToolFuture<'a> {
        Box::pin(async move { super::tools::descriptor(state, identity) })
    }
    fn resources(&self, state: &State, _identity: usize) -> Vec<Value> {
        super::resources::list(state)["resources"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }
    fn read<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        uri: &'a str,
        deadline: tokio::time::Instant,
    ) -> ToolFuture<'a> {
        Box::pin(super::resources::read(state, identity, uri, deadline))
    }
}
