use crate::{
    app::State,
    core::error::Error,
    plugin::common::{object, Capabilities, McpContribution, ToolFuture},
};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};
static TOOLS: LazyLock<Vec<Value>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("tools.json")).expect("Project tool contracts")
});
impl McpContribution for super::plugin::Projects {
    fn tools(&self) -> &[Value] {
        &TOOLS
    }
    fn available(&self, state: &State, identity: usize, name: &str) -> bool {
        if state.services.get::<super::ProjectService>().is_none()
            || state.auth.client(identity).anonymous()
        {
            return false;
        }
        match name {
            "project_exec" => {
                state
                    .services
                    .get::<crate::process::Execution>()
                    .is_some_and(|s| s.ready())
                    && state.auth.client(identity).has_execution()
            }
            "project_search" => {
                state
                    .services
                    .get::<crate::process::Execution>()
                    .is_some_and(|s| s.ready())
                    && state.auth.client(identity).has_execution()
                    && std::path::Path::new("/usr/bin/rg").is_file()
            }
            _ => true,
        }
    }
    fn call<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        name: &'a str,
        args: Value,
    ) -> ToolFuture<'a> {
        Box::pin(super::call(state, identity, name, args))
    }
}
pub(super) fn capabilities(state: &State, identity: usize, enabled: bool) -> Capabilities {
    let projects = enabled
        && state.services.get::<super::ProjectService>().is_some()
        && !state.auth.client(identity).anonymous();
    Capabilities {
        http: Default::default(),
        mcp: object(
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn directory_search_modes_remain_owned_by_the_project_contract() {
        let tool = TOOLS
            .iter()
            .find(|tool| tool["name"] == "project_search")
            .unwrap();
        assert_eq!(
            tool["inputSchema"]["properties"]["mode"]["enum"],
            json!(["name", "path", "content"])
        );
    }
}
