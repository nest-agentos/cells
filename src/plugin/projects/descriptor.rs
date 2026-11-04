//! Per-caller capability projections; persisted project policy is never a grant by itself.
use super::{model::Project, service};
use crate::{app::State, core::error::Error};
use serde_json::{json, Value};

pub(super) fn project(state: &State, identity: usize, project: &Project) -> Result<Value, Error> {
    service::authorize(state, identity, project)?;
    let project = service::effective(state, identity, project);
    let mut value = serde_json::to_value(&project).map_err(|_| Error::internal())?;
    value["authorization"] = state.auth.client(identity).authorization();
    value["capabilities"] = capabilities(state, identity, &project);
    Ok(value)
}

pub(crate) fn capabilities(state: &State, identity: usize, project: &Project) -> Value {
    let client = state.auth.client(identity);
    let run = state
        .services
        .get::<crate::process::Execution>()
        .is_some_and(|service| service.ready())
        && service::mounts(project)
            .iter()
            .all(|m| client.may_execute(&m.alias));
    json!({"fileRead":true,"fileWrite":project.access == "rw" && client.may_write(&project.alias),
        "run":run,"sessionRead":client.has_harness(),"sessionControl":run && client.has_harness()})
}
