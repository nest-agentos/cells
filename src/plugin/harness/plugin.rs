use crate::{
    app::State,
    plugin::common::PluginConfiguration,
    plugins::{NodePlugin, PluginFuture},
};
use std::sync::Arc;

pub struct Harness(pub crate::harness::HarnessPlugins);
impl NodePlugin for Harness {
    fn subscribe<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        args: serde_json::Value,
    ) -> crate::plugins::EventFuture<'a> {
        Box::pin(async move {
            state
                .services
                .require::<super::Harnesses>()?
                .subscribe(state, identity, args)
                .await
        })
    }
    fn mcp(&self) -> Option<&dyn crate::plugin::common::McpContribution> {
        Some(self)
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
        &["workspaces"]
    }
    fn directories(
        &self,
        state: &State,
        identity: usize,
        _cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<serde_json::Map<String, serde_json::Value>, crate::core::error::Error> {
        Ok(crate::plugin::common::object(
            serde_json::json!({"workspaces":state.services.get::<super::Harnesses>().map(|service|service.workspaces(state.auth.client(identity))).unwrap_or_default()}),
        ))
    }
    fn id(&self) -> &'static str {
        "harness"
    }
    fn default_enabled(&self, config: &dyn PluginConfiguration) -> bool {
        let Ok(config) = crate::config::plugin_adapter::deployment(config) else {
            return false;
        };
        !config.harnesses.is_empty()
    }
    fn settings(&self, config: &mut dyn PluginConfiguration, enabled: bool) {
        let Some(config) = crate::config::plugin_adapter::deployment_mut(config) else {
            return;
        };
        if !enabled {
            config.harnesses.clear();
        }
    }
    fn validate(&self, config: &dyn PluginConfiguration) -> Result<(), String> {
        let config = crate::config::plugin_adapter::deployment(config)?;
        crate::harness::config::validate_with_plugins(config, &self.0).map(|_| ())
    }
    fn configure(&self, config: &dyn PluginConfiguration, state: &mut State) -> Result<(), String> {
        let config = crate::config::plugin_adapter::deployment(config)?;
        state
            .services
            .insert(Arc::new(crate::harness::Harnesses::with_plugins(
                crate::harness::config::validate_with_plugins(config, &self.0)?,
                crate::app::random_epoch()?,
                self.0.clone(),
            )?))?;
        Ok(())
    }
    fn start<'a>(&'a self, state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async move {
            super::Harnesses::maintain(Arc::downgrade(state));
            Ok(())
        })
    }
    fn stop<'a>(&'a self, state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async move {
            state
                .services
                .require::<crate::harness::Harnesses>()?
                .close()
                .await;
            Ok(())
        })
    }
}
