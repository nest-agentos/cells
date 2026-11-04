use crate::{
    app::State,
    plugin::common::{object, Capabilities, McpContribution, ToolFuture},
};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};
static TOOLS: LazyLock<Vec<Value>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("tools.json")).expect("Directory sync tool contracts")
});
impl McpContribution for super::plugin::Sync {
    fn tools(&self) -> &[Value] {
        &TOOLS
    }
    fn available(&self, state: &State, identity: usize, _name: &str) -> bool {
        state
            .services
            .get::<crate::projects::ProjectService>()
            .is_some()
            && allowed(state, identity)
    }
    fn call<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        name: &'a str,
        args: Value,
    ) -> ToolFuture<'a> {
        Box::pin(crate::plugin::projects::directory_sync::call(
            state, identity, name, args,
        ))
    }
    fn error_outcome(&self, name: &str, error: crate::core::error::Error) -> &'static str {
        if name == "project_sync_execute" || error.status.is_server_error() {
            "unknown"
        } else {
            "not-committed"
        }
    }
}
fn allowed(state: &State, identity: usize) -> bool {
    let client = state.auth.client(identity);
    state
        .services
        .get::<super::SyncService>()
        .is_some_and(|s| client.legacy() || s.principal_id() == client.id())
}
pub(super) fn capabilities(state: &State, identity: usize, enabled: bool) -> Capabilities {
    let service = state.services.get::<super::SyncService>();
    let sync = enabled && allowed(state, identity);
    Capabilities {
        http: object(
            json!({"sync":{"push":sync && service.is_some_and(|s|s.healthy()),
        "protocolVersion":if enabled && service.is_some(){Some(1)}else{None},"discovery":if enabled && service.is_some(){Some("/v1/sync/capabilities")}else{None}}}),
        ),
        mcp: object(
            json!({"sync":sync,"directorySync":sync && state.services.get::<crate::projects::ProjectService>().is_some(),"directorySyncVersion":2}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sync_contracts_cover_required_fields_and_writes_preserve_uncertainty() {
        for tool in TOOLS.iter() {
            for field in tool["inputSchema"]["required"].as_array().unwrap() {
                assert!(tool["inputSchema"]["properties"]
                    .get(field.as_str().unwrap())
                    .is_some());
            }
            assert!(tool["inputSchema"]["properties"].get("prompt").is_none());
        }
        assert_eq!(
            super::super::plugin::Sync
                .error_outcome("project_sync_execute", crate::core::error::Error::invalid()),
            "unknown"
        );
    }
}
