//! Node feature contracts and orchestration; native harness drivers remain a separate extension point.
mod registry;
mod runtime;

pub use registry::{PluginPlan, PluginRegistry};
pub use runtime::PluginRuntime;

use crate::app::State;
use axum::Router;
use std::{future::Future, pin::Pin, sync::Arc};

pub type PluginFuture<'a> = Pin<Box<dyn Future<Output = Result<(), PluginFailure>> + Send + 'a>>;

/// A safe diagnostic category is logged separately from the caller-visible explanation.
#[derive(Debug)]
pub struct PluginFailure {
    pub code: &'static str,
    pub reason: String,
}
impl PluginFailure {
    pub fn new(code: &'static str, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }
}
impl std::fmt::Display for PluginFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.reason)
    }
}
impl std::error::Error for PluginFailure {}
impl From<String> for PluginFailure {
    fn from(reason: String) -> Self {
        Self::new("PLUGIN_HOOK_FAILED", reason)
    }
}
impl From<&str> for PluginFailure {
    fn from(reason: &str) -> Self {
        Self::new("PLUGIN_HOOK_FAILED", reason)
    }
}

/// Compiled-in features own settings, validation, service assembly, routes and cleanup.
/// Hooks must release their own partial work when construction fails; stop must be idempotent.
pub trait NodePlugin: Send + Sync {
    fn subscribe<'a>(
        &'a self,
        _state: &'a Arc<State>,
        _identity: usize,
        _args: serde_json::Value,
    ) -> EventFuture<'a> {
        Box::pin(async { Err(crate::core::error::Error::unsupported()) })
    }
    fn request_headers(&self) -> &'static [&'static str] {
        &[]
    }
    fn path_headers(&self) -> &'static [&'static str] {
        &[]
    }
    fn authorize_path(
        &self,
        _state: &State,
        _access: &PathAccess<'_>,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, crate::core::error::Error> {
        Ok(None)
    }
    fn mcp(&self) -> Option<&dyn McpContribution> {
        None
    }
    fn capabilities(&self, _state: &State, _identity: usize, _enabled: bool) -> Capabilities {
        Capabilities::default()
    }
    fn directory_fields(&self) -> &'static [&'static str] {
        &[]
    }
    fn directories(
        &self,
        _state: &State,
        _identity: usize,
        _cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<serde_json::Map<String, serde_json::Value>, crate::core::error::Error> {
        Ok(Default::default())
    }
    fn id(&self) -> &'static str;
    fn dependencies(&self) -> &'static [&'static str] {
        &[]
    }
    /// Order after these plugins only when both are enabled; never force them on.
    fn optional_dependencies(&self) -> &'static [&'static str] {
        &[]
    }
    fn default_enabled(&self, _config: &dyn PluginConfiguration) -> bool {
        true
    }
    fn settings(&self, _config: &mut dyn PluginConfiguration, _enabled: bool) {}
    fn validate(&self, _config: &dyn PluginConfiguration) -> Result<(), String> {
        Ok(())
    }
    fn configure(
        &self,
        _config: &dyn PluginConfiguration,
        _state: &mut State,
    ) -> Result<(), String> {
        Ok(())
    }
    fn stop_admission(&self, _state: &State) {}
    fn routes(&self) -> Router<Arc<State>> {
        Router::new()
    }
    fn start<'a>(&'a self, _state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
    fn stop<'a>(&'a self, _state: &'a Arc<State>) -> PluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

mod services;
pub use services::Services;
mod events;
pub use events::{EventFuture, EventStream};

impl From<crate::core::error::Error> for PluginFailure {
    fn from(error: crate::core::error::Error) -> Self {
        Self::new(error.code, error.code)
    }
}

mod extension;
pub use extension::{object, Capabilities, McpContribution, ToolFuture};

mod configuration;
pub use configuration::{PluginConfiguration, PluginSettings};

pub mod workspace;

mod contributions;

pub struct PathAccess<'a> {
    pub headers: &'a axum::http::HeaderMap,
    pub alias: &'a str,
    pub path: &'a str,
    pub write: bool,
    pub metadata: bool,
}
