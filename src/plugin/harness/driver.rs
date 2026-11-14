//! Harness-neutral control port. Native protocols stay in their own adapters.
use crate::core::error::Error;
use futures_util::future::BoxFuture;
use serde_json::Value;

/// Trusted adapter evidence about whether the requested mutation was dispatched.
pub enum NativeExecution {
    NotStarted(Error),
    Dispatched(Result<Value, Error>),
}

pub trait HarnessDriver: Send + Sync {
    /// Count active managed sessions without starting or waking a native runtime.
    fn active_sessions(&self) -> Option<usize> {
        None
    }
    fn subscribe(&self, _args: Value) -> crate::plugin::common::EventFuture<'_> {
        Box::pin(async { Err(Error::unsupported()) })
    }
    fn descriptor(&self) -> Value;
    /// Native subtree management must name every affected session before admission.
    fn mutation_resources<'a>(
        &'a self,
        _name: &'a str,
        args: &'a Value,
    ) -> BoxFuture<'a, Result<Vec<String>, Error>> {
        Box::pin(async move {
            Ok(args["sessionId"]
                .as_str()
                .into_iter()
                .map(str::to_owned)
                .collect())
        })
    }
    fn read<'a>(&'a self, name: &'a str, args: Value) -> BoxFuture<'a, Result<Value, Error>>;
    fn execute<'a>(&'a self, name: &'a str, args: &'a Value)
        -> BoxFuture<'a, Result<Value, Error>>;
    /// Legacy adapters cannot prove a transport failure preceded mutation dispatch.
    fn execute_checked<'a>(
        &'a self,
        name: &'a str,
        args: &'a Value,
    ) -> BoxFuture<'a, NativeExecution> {
        Box::pin(async move {
            match self.execute(name, args).await {
                Err(error) if error.code == "NATIVE_WORKSPACE_BUSY" => {
                    NativeExecution::NotStarted(error)
                }
                result => NativeExecution::Dispatched(result),
            }
        })
    }
    /// Return native evidence for an existing uncertain mutation; never execute it again.
    fn reconcile_operation<'a>(
        &'a self,
        _id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, Error>> {
        Box::pin(async { Ok(None) })
    }
    /// Release admission barriers only after the shared receipt has been persisted.
    fn operation_settled(&self, _id: &str, _receipt: &Value) {}
    fn stop(&self);
    /// Conservative default: plugins must explicitly prove no execution or interaction is live.
    fn idle_since(&self) -> Option<std::time::Instant> {
        None
    }
    fn drain(&self) -> BoxFuture<'_, ()>;
}
