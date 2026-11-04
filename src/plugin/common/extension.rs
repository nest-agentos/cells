//! Protocol contributions are declared by feature owners and collected without feature-name branches.
use crate::{app::State, core::error::Error};
use serde_json::{Map, Value};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio_util::sync::CancellationToken;
pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, Error>> + Send + 'a>>;
#[derive(Default)]
pub struct Capabilities {
    pub http: Map<String, Value>,
    pub mcp: Map<String, Value>,
}
pub fn object(value: Value) -> Map<String, Value> {
    value
        .as_object()
        .cloned()
        .expect("Plugin declaration is an object")
}
pub trait McpContribution: Send + Sync {
    fn tools(&self) -> &[Value] {
        &[]
    }
    fn available(&self, _state: &State, _identity: usize, _name: &str) -> bool {
        true
    }
    fn call<'a>(
        &'a self,
        _state: &'a Arc<State>,
        _identity: usize,
        _name: &'a str,
        _args: Value,
    ) -> ToolFuture<'a> {
        Box::pin(async { Err(Error::unsupported()) })
    }
    fn error_outcome(&self, _name: &str, _error: Error) -> &'static str {
        "not-committed"
    }
    fn resources(&self, _state: &State, _identity: usize) -> Vec<Value> {
        vec![]
    }
    fn read<'a>(
        &'a self,
        _state: &'a Arc<State>,
        _identity: usize,
        _uri: &'a str,
        _deadline: tokio::time::Instant,
    ) -> ToolFuture<'a> {
        Box::pin(async { Err(Error::not_found("RESOURCE_UNKNOWN")) })
    }
}
pub(crate) fn merge(
    target: &mut Map<String, Value>,
    values: Map<String, Value>,
) -> Result<(), Error> {
    for (key, value) in values {
        if target.contains_key(&key) {
            return Err(Error::conflict("PLUGIN_DECLARATION_CONFLICT"));
        }
        target.insert(key, value);
    }
    Ok(())
}
pub(crate) fn snapshot(
    plan: &super::PluginPlan,
    state: &State,
    identity: usize,
    cancel: &CancellationToken,
) -> Result<Value, Error> {
    let mut value = empty_snapshot(plan, state, identity);
    for plugin in &plan.ordered {
        if cancel.is_cancelled() {
            return Err(Error::cancelled());
        }
        for (key, contribution) in plugin.directories(state, identity, cancel)? {
            if !plugin.directory_fields().contains(&key.as_str()) {
                return Err(Error::conflict("PLUGIN_UNDECLARED_FIELD"));
            }
            value.insert(key, contribution);
        }
    }
    Ok(Value::Object(value))
}

fn empty_snapshot(plan: &super::PluginPlan, state: &State, identity: usize) -> Map<String, Value> {
    let mut value = object(
        serde_json::json!({"version":1,"serverId":state.auth.server_id(),"authorization":state.auth.client(identity).authorization()}),
    );
    for plugin in &plan.registered {
        for field in plugin.directory_fields() {
            value.insert((*field).into(), serde_json::json!([]));
        }
    }

    value
}
