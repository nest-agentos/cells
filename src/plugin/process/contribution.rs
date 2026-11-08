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
    let native=crate::projects::native::enabled(state);
    Capabilities {
        http: object(
            json!({"process":{"exec":execution},"executionModel":if !execution{"none"}else if native{"trusted-cooperative-host"}else{"sandbox"},
        "workspaceConsistency":if !execution{"none"}else if native{"cooperative"}else{"isolated"},"readOnlyEnforcement":if execution&&!native{"kernel-enforced"}else{"none"},"pathModel":if !execution{"none"}else if native{"host-mapped"}else{"virtual-root"}}),
        ),
        node: Default::default(),
    }
}
