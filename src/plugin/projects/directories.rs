use crate::{app::State, core::error::Error, projects};
use serde_json::{json, Value};
pub(super) fn project(
    state: &State,
    identity: usize,
    project: &projects::model::Project,
) -> Result<Value, Error> {
    projects::service::authorize(state, identity, project)?;
    let project = projects::service::effective(state, identity, project);
    let capabilities = projects::capabilities(state, identity, &project);
    let mut mounts = projects::service::mounts(&project)
        .iter()
        .map(|mount| {
            let mut value = crate::plugin::files::directories::directory(
                state,
                state.auth.client(identity),
                mount,
            );
            value["capabilities"]["run"] = capabilities["run"].clone();
            value
        })
        .collect::<Vec<_>>()
        .into_iter();
    Ok(
        json!({"id":project.id,"name":project.name,"revision":project.revision,
        "root":mounts.next(),"mounts":mounts.collect::<Vec<_>>(),"capabilities":capabilities}),
    )
}
