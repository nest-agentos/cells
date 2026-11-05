use crate::{
    app::State,
    core::error::Error,
    plugin::common::{object, Capabilities},
};
use serde_json::{json, Value};
pub(super) fn capabilities(state: &State, identity: usize, enabled: bool) -> Capabilities {
    let projects = enabled
        && state.services.get::<super::ProjectService>().is_some()
        && !state.auth.client(identity).anonymous();
    Capabilities {
        http: Default::default(),
        node: object(
            json!({"projectExecutionEnvironment":projects,"projects":projects,"directoryEvents":projects,"directoryScopes":projects,"fileSearchVersion":2,"searchKeywords":projects,
        "fileSearch":projects && state.services.get::<crate::process::Execution>().is_some_and(|s|s.ready()) && std::path::Path::new("/usr/bin/rg").is_file(),"projectProtocol":"fs-agent-project-v1"}),
        ),
    }
}
pub(super) fn directories(
    state: &State,
    identity: usize,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<serde_json::Map<String, Value>, Error> {
    let mut values = vec![];
    if let Some(service) = state.services.get::<super::ProjectService>() {
        for project in service.list(identity)? {
            if cancel.is_cancelled() {
                return Err(Error::cancelled());
            }
            if let Ok(value) = super::directories::project(state, identity, &project) {
                values.push(value);
            }
        }
    }
    Ok(object(json!({"projects":values})))
}
