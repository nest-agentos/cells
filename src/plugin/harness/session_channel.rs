//! Shared native-session channel binding; adapters only translate configuration.
use crate::{core::error::Error, projects::runtime::ProjectRuntime};
use serde_json::{json, Value};

pub fn directory(
    runtime: Option<&ProjectRuntime>,
    fallback: &std::path::Path,
    args: &Value,
) -> Result<String, Error> {
    match args.get("directory") {
        Some(value) => runtime
            .ok_or_else(Error::unsupported)?
            .working_directory(value.as_str().ok_or_else(Error::invalid)?),
        None => Ok(fallback.to_string_lossy().into_owned()),
    }
}
pub fn configuration(
    runtime: &ProjectRuntime,
    cwd: &str,
    session: &str,
) -> Result<Option<Value>, Error> {
    let state = runtime.state()?;
    let service = state
        .services
        .require::<crate::projects::ProjectService>()?;
    let cwd = runtime.resolve_cwd(cwd)?;
    let config = Some(service
        .channels
        .session_config(&runtime.project.id, &cwd, &runtime.project.owner, session)?)
        .filter(|config| config.get("mcp_servers.cells_session").is_some());
    Ok(config)
}
pub fn http_servers(config: Option<&Value>) -> Value {
    match config.and_then(|value| value.get("mcp_servers.cells_session")) {
        Some(value) if value["enabled"]!=false => {
            json!({"cells_session":{"type":"http","url":value["url"],"headers":value.get("http_headers").cloned().unwrap_or(json!({}))}})
        }
        _ => json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn translates_only_the_channel_without_widening_other_server_tool_policies() {
        let config = json!({"features.mcp_2026_07_28":true,"mcp_servers.cells_session":{"url":"http://localhost/channel","http_headers":{"Authorization":"private-token"}},"mcp_servers.extra":{"url":"http://localhost/extra"}});
        let servers = http_servers(Some(&config));
        assert_eq!(
            servers["cells_session"],
            json!({"type":"http","url":"http://localhost/channel","headers":{"Authorization":"private-token"}})
        );
        assert!(servers.get("extra").is_none());
        assert_eq!(servers.as_object().unwrap().len(), 1);
        assert_eq!(http_servers(None), json!({}));
        assert_eq!(http_servers(Some(&json!({"mcp_servers.cells_session":{"enabled":false,"url":"http://localhost/retired"}}))),json!({}));
    }
}
