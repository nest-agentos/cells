use crate::{
    app::State,
    plugin::common::{object, Capabilities},
};
use serde_json::json;
pub(super) fn capabilities(state: &State, identity: usize, enabled: bool) -> Capabilities {
    let harness = enabled
        && state
            .services
            .get::<super::Harnesses>()
            .is_some_and(|s| s.enabled())
        && state.auth.client(identity).has_harness();
    Capabilities {
        http: object(
            json!({"harness":{"enabled":harness,"protocolVersion":"cells-api/1","endpoint":"/v1/api/harness/"}}),
        ),
        node: object(json!({"harness":harness})),
    }
}
