use crate::{
    app::State,
    plugin::common::PluginConfiguration,
    plugins::{NodePlugin, PluginFailure, PluginFuture},
};
use axum::{extract::DefaultBodyLimit, routing::get, Router};
use std::{sync::Arc, time::Duration};

pub struct Sync;
impl NodePlugin for Sync {
    fn request_headers(&self) -> &'static [&'static str] {
        &["x-sync-history-epoch"]
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
    fn id(&self) -> &'static str {
        "sync"
    }
    fn default_enabled(&self, config: &dyn PluginConfiguration) -> bool {
        let Ok(config) = crate::config::plugin_adapter::deployment(config) else {
            return false;
        };
        config.sync.as_ref().is_some_and(|s| s.enabled)
    }
    fn settings(&self, config: &mut dyn PluginConfiguration, enabled: bool) {
        let Some(config) = crate::config::plugin_adapter::deployment_mut(config) else {
            return;
        };
        if !enabled {
            config.sync = None;
        } else if let Some(sync) = &mut config.sync {
            sync.enabled = true;
        }
    }
    fn validate(&self, config: &dyn PluginConfiguration) -> Result<(), String> {
        let config = crate::config::plugin_adapter::deployment(config)?;
        let sync = config
            .sync
            .as_ref()
            .ok_or("Enabled sync plugin requires [sync] configuration")?;
        crate::sync::SyncService::validate_isolation(sync, config)
            .map_err(|error| error.to_string())
    }
    fn configure(&self, config: &dyn PluginConfiguration, state: &mut State) -> Result<(), String> {
        let config = crate::config::plugin_adapter::deployment(config)?;
        let sync = config.sync.as_ref().ok_or("Missing sync configuration")?;
        state
            .services
            .insert(crate::sync::SyncService::open(sync).map_err(|error| error.to_string())?)?;
        crate::sync::SyncService::validate_isolation(sync, config)
            .map_err(|error| error.to_string())
    }
    fn routes(&self) -> Router<Arc<State>> {
        use crate::sync::transport;
        Router::new()
            .route(
                "/v1/sync/*path",
                get(transport::get)
                    .post(transport::post)
                    .put(transport::put),
            )
            .layer(DefaultBodyLimit::disable())
    }
    fn start<'a>(&'a self, state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async move {
            if let Some(sync) = &state.services.get::<crate::sync::SyncService>() {
                sync.start_maintenance();
            }
            Ok(())
        })
    }
    fn stop<'a>(&'a self, state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async move {
            let Some(sync) = state.services.get::<crate::sync::SyncService>().cloned() else {
                return Ok(());
            };
            sync.stop();
            let timeout = Duration::from_secs(5);
            tokio::task::spawn_blocking(move || sync.drain_timeout(timeout))
                .await
                .map_err(|_| {
                    PluginFailure::new("SYNC_DRAIN_WORKER_FAILED", "Sync drain worker failed")
                })?
                .map_err(|error| PluginFailure::new("SYNC_DRAIN_FAILED", error.to_string()))
        })
    }
}
