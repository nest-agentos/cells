use crate::{
    app::State,
    plugin::common::PluginConfiguration,
    plugins::{NodePlugin, PluginFailure, PluginFuture},
};
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

pub struct Process;
impl NodePlugin for Process {
    fn subscribe<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        args: serde_json::Value,
    ) -> crate::plugins::EventFuture<'a> {
        Box::pin(async move { super::subscription::subscribe(state, identity, args) })
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
        "process"
    }
    fn default_enabled(&self, config: &dyn PluginConfiguration) -> bool {
        let Ok(config) = crate::config::plugin_adapter::deployment(config) else {
            return false;
        };
        config.execution
    }
    fn settings(&self, config: &mut dyn PluginConfiguration, enabled: bool) {
        let Some(config) = crate::config::plugin_adapter::deployment_mut(config) else {
            return;
        };
        config.execution = enabled;
    }
    fn validate(&self, config: &dyn PluginConfiguration) -> Result<(), String> {
        let config = crate::config::plugin_adapter::deployment(config)?;
        if config.exports.is_empty() {
            Err("Execution requires an export".into())
        } else {
            Ok(())
        }
    }
    fn configure(
        &self,
        _config: &dyn PluginConfiguration,
        state: &mut State,
    ) -> Result<(), String> {
        state
            .services
            .insert(Arc::new(super::Execution::new(state.epoch.clone())))
    }
    fn stop_admission(&self, state: &State) {
        if let Some(service) = state.services.get::<super::Execution>() {
            service.stop_accepting();
        }
    }
    fn routes(&self) -> Router<Arc<State>> {
        use crate::plugin::process::http as processes;
        Router::new()
            .route("/v1/processes", post(processes::start))
            .route("/v1/processes/:epoch/:id", get(processes::status))
            .route("/v1/processes/:epoch/:id/cancel", post(processes::cancel))
            .layer(DefaultBodyLimit::max(512 * 1024))
    }
    fn start<'a>(&'a self, state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async move {
            crate::process::enable(state)
                .await
                .map_err(|error| PluginFailure::new("PROCESS_SANDBOX_UNAVAILABLE", error))
        })
    }
    fn stop<'a>(&'a self, state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async move {
            state
                .services
                .require::<crate::process::Execution>()?
                .stop_accepting();
            state
                .services
                .require::<crate::process::Execution>()?
                .cancel_all();
            Ok(())
        })
    }
}
