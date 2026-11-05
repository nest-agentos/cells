use crate::{
    app::State,
    plugin::common::{object, Capabilities},
};
use serde_json::json;
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
        node: object(
            json!({"sync":sync,"directorySync":sync && state.services.get::<crate::projects::ProjectService>().is_some(),"directorySyncVersion":2}),
        ),
    }
}
