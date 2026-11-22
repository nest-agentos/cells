use crate::{app::State, plugin::common::PluginConfiguration, plugins::NodePlugin};
use std::sync::Arc;

pub struct Projects;
impl NodePlugin for Projects {
    fn operation_domains(&self) -> &'static [&'static str] { &["projects", "agents"] }
    fn call<'a>(&'a self, state: &'a Arc<State>, identity: usize, domain: &'a str, operation: &'a str, args: serde_json::Value) -> crate::plugins::OperationFuture<'a> {
        Box::pin(async move { if state.auth.client(identity).anonymous() { return Err(crate::core::error::Error::unsupported()); }
            if domain == "projects" { return super::call(state, identity, &format!("project_{operation}"), args).await; }
            let service = state.services.require::<super::ProjectService>()?;
            let owner = state.auth.client(identity).id();
            match operation { "mcp_list" => service.channels.server_list(owner), "mcp_save" => service.channels.server_save(owner, &args), "mcp_remove" => service.channels.server_remove(owner, &args), "mcp_revoke_site" => service.channels.revoke_site(owner, &args), _ => Err(crate::core::error::Error::unsupported()) } })
    }
    fn subscribe<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        args: serde_json::Value,
    ) -> crate::plugins::EventFuture<'a> {
        Box::pin(async move {
            if args.get("channelId").is_some() {super::channel_subscription::subscribe(state.clone(),identity,args).await}
            else {super::watch::subscribe(state.clone(), identity, args).await}
        })
    }
    fn path_headers(&self) -> &'static [&'static str] {
        &[
            "x-fsagent-project",
            "x-fsagent-project-revision",
            "x-fsagent-project-readonly",
        ]
    }
    fn authorize_path(
        &self,
        state: &State,
        access: &crate::plugin::common::PathAccess<'_>,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, crate::core::error::Error> {
        super::access::project_path(
            state,
            access.identity,
            access.claims,
            access.alias,
            access.path,
            access.write,
            access.metadata,
        )
    }

    fn capabilities(
        &self,
        state: &State,
        identity: usize,
        enabled: bool,
    ) -> crate::plugin::common::Capabilities {
        super::contribution::capabilities(state, identity, enabled)
    }
    fn directory_fields(&self) -> &'static [&'static str] {
        &["projects"]
    }
    fn directories(
        &self,
        state: &State,
        identity: usize,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<serde_json::Map<String, serde_json::Value>, crate::core::error::Error> {
        return super::contribution::directories(state, identity, cancel);
    }
    fn id(&self) -> &'static str {
        "projects"
    }
    fn optional_dependencies(&self) -> &'static [&'static str] {
        &["sync","mcp"]
    }
    fn default_enabled(&self, config: &dyn PluginConfiguration) -> bool {
        let Ok(config) = crate::config::plugin_adapter::deployment(config) else {
            return false;
        };
        config.projects.is_some()
    }
    fn settings(&self, config: &mut dyn PluginConfiguration, enabled: bool) {
        let Some(config) = crate::config::plugin_adapter::deployment_mut(config) else {
            return;
        };
        if !enabled {
            config.projects = None;
        }
    }
    fn validate(&self, config: &dyn PluginConfiguration) -> Result<(), String> {
        let config = crate::config::plugin_adapter::deployment(config)?;
        if config.projects.is_none() {
            return Err("Enabled projects plugin requires [projects] configuration".into());
        }
        if config.server_id.is_none() {
            return Err("Project management requires a stable server_id".into());
        }
        Ok(())
    }
    fn configure(&self, config: &dyn PluginConfiguration, state: &mut State) -> Result<(), String> {
        let config = crate::config::plugin_adapter::deployment(config)?;
        let projects = config
            .projects
            .as_ref()
            .ok_or("Missing project configuration")?;
        state
            .services
            .insert(Arc::new(crate::projects::ProjectService::open(
                projects, config,
            )?))?;
        state.services.get::<crate::projects::ProjectService>()
            .ok_or("Project service unavailable")?.restore_metadata(state)
            .map_err(|_| "Project metadata initialization failed")?;
        if projects.network {if let Some(transport)=state.services.get::<crate::plugin::mcp::InternalTransport>(){transport.require();}}
        Ok(())
    }
    fn start<'a>(&'a self,state:&'a Arc<State>)->crate::plugins::PluginFuture<'a> {
        Box::pin(async move {
            let service=state.services.require::<super::ProjectService>()?;
            if service.channels_enabled {if let Some(transport)=state.services.get::<crate::plugin::mcp::InternalTransport>() {
                service.channels.set_endpoint(Some(transport.endpoint()?))?;
            }}
            if state.plugins.enabled("harness") { super::harness_recovery::maintain(Arc::downgrade(state)); }
            Ok(())
        })
    }
}
