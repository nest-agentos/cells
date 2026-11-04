use super::State;
use crate::{
    config::Config,
    plugins::{PluginRegistry, PluginRuntime},
};
use std::sync::Arc;

/// The application owns feature orchestration; transports consume the assembled State.
pub struct Application {
    state: Arc<State>,
    runtime: PluginRuntime,
}
impl Application {
    pub fn assemble(
        mut host: State,
        registry: PluginRegistry,
        configuration: &dyn crate::plugin::common::PluginConfiguration,
    ) -> Result<Self, String> {
        let (effective, plan) = registry.resolve(configuration)?;
        plan.configure(effective.as_ref(), &mut host)?;
        host.plugins = plan;
        Ok(Self {
            state: Arc::new(host),
            runtime: PluginRuntime::default(),
        })
    }
    pub fn from_config(config: &Config) -> Result<Self, String> {
        Self::with_registry(config, PluginRegistry::builtins())
    }
    pub fn with_registry(config: &Config, registry: PluginRegistry) -> Result<Self, String> {
        Ok(Self {
            state: State::from_config_with_registry(config, registry)?,
            runtime: PluginRuntime::default(),
        })
    }
    pub fn state(&self) -> &Arc<State> {
        &self.state
    }
    pub fn router(&self) -> Result<axum::Router, String> {
        crate::http::router(self.state.clone(), &self.state.allowed_origins)
    }
    pub async fn start(&self) -> Result<(), String> {
        let result = self.runtime.start(&self.state.plugins, &self.state).await;
        if let Err(error) = result {
            let cleanup = self.shutdown().await;
            return Err(format!(
                "{error}{}",
                cleanup
                    .err()
                    .map(|e| format!("; host cleanup: {e}"))
                    .unwrap_or_default()
            ));
        }
        Ok(())
    }
    pub async fn shutdown(&self) -> Result<(), String> {
        self.state.event_stop.cancel();
        self.state.plugins.stop_admission(&self.state);
        let stopped = self.runtime.stop(&self.state).await;
        let drained =
            tokio::time::timeout(std::time::Duration::from_secs(5), self.state.files.drain()).await;
        if drained.is_err() {
            crate::core::events::emit(
                crate::core::events::Level::Error,
                "server.drain_timeout",
                serde_json::json!({"code":"FILES_DRAIN_TIMEOUT","operationId":self.state.epoch.as_str(),"timeoutMs":5000}),
            );
            return Err(format!(
                "Host file drain deadline exceeded{}",
                stopped.err().map(|e| format!("; {e}")).unwrap_or_default()
            ));
        }
        stopped
    }
}
