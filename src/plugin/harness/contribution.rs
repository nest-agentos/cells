use crate::{
    app::State,
    plugin::common::{object, Capabilities, McpContribution, ToolFuture},
};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};
static TOOLS: LazyLock<Vec<Value>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("tools.json")).expect("Harness tool contracts")
});
impl McpContribution for super::plugin::Harness {
    fn tools(&self) -> &[Value] {
        &TOOLS
    }
    fn available(&self, state: &State, identity: usize, _name: &str) -> bool {
        state
            .services
            .get::<super::Harnesses>()
            .is_some_and(|s| s.enabled())
            && state.auth.client(identity).has_harness()
    }
    fn call<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        name: &'a str,
        args: Value,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let service = state.services.require::<super::Harnesses>()?;
            if args.get("projectId").is_some() {
                service.project_call(state, identity, name, args).await
            } else {
                service.authorized_call(state, identity, name, args).await
            }
        })
    }
    fn error_outcome(&self, _name: &str, error: crate::core::error::Error) -> &'static str {
        if error.code == "HARNESS_RECEIPT_STORAGE" {
            "unknown"
        } else {
            "not-committed"
        }
    }
}
pub(super) fn capabilities(state: &State, identity: usize, enabled: bool) -> Capabilities {
    let harness = enabled
        && state
            .services
            .get::<super::Harnesses>()
            .is_some_and(|s| s.enabled())
        && state.auth.client(identity).has_harness();
    Capabilities {
        http: object(
            json!({"harness":{"enabled":harness && state.plugins.enabled("mcp"),"protocolVersion":"2026-07-28","endpoint":if state.plugins.enabled("mcp"){Some("/mcp")}else{None}}}),
        ),
        mcp: object(json!({"harness":harness})),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tool(name: &str) -> &'static Value {
        TOOLS.iter().find(|tool| tool["name"] == name).unwrap()
    }
    #[test]
    fn native_contracts_preserve_search_scopes_and_project_revision_dependencies() {
        assert_eq!(
            tool("harness_session_search")["inputSchema"]["properties"]["mode"]["enum"],
            json!(["title", "content", "prompt"])
        );
        assert_eq!(
            tool("harness_turn")["inputSchema"]["dependentRequired"]["projectId"],
            json!(["revision"])
        );
        assert_eq!(
            tool("harness_respond")["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .len(),
            5
        );
        assert!(tool("harness_sessions")["inputSchema"]["properties"]
            .get("mounts")
            .is_none());
    }
    #[test]
    fn receipt_failures_preserve_uncertainty_without_exposing_native_errors() {
        let plugin = super::super::plugin::Harness(super::super::HarnessPlugins::empty());
        let error = crate::core::error::Error::new(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "HARNESS_RECEIPT_STORAGE",
        );
        assert_eq!(plugin.error_outcome("harness_turn", error), "unknown");
        assert_eq!(
            plugin.error_outcome("harness_turn", crate::core::error::Error::invalid()),
            "not-committed"
        );
    }
}
