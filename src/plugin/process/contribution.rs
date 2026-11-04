use crate::{
    app::State,
    plugin::common::{object, Capabilities},
};
use serde_json::json;
pub(super) fn capabilities(state: &State, identity: usize, enabled: bool) -> Capabilities {
    let execution = enabled
        && state
            .services
            .get::<super::Execution>()
            .is_some_and(|s| s.ready())
        && state.auth.client(identity).has_execution();
    Capabilities {
        http: object(
            json!({"process":{"exec":execution},"executionModel":if execution{"sandbox"}else{"none"},
        "workspaceConsistency":if execution{"isolated"}else{"none"},"readOnlyEnforcement":if execution{"kernel-enforced"}else{"none"},"pathModel":if execution{"virtual-root"}else{"none"}}),
        ),
        mcp: Default::default(),
    }
}
