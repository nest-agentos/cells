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
    fn subscribe(&self, _args: Value) -> crate::plugin::common::EventFuture<'_> {
        Box::pin(async { Err(Error::unsupported()) })
    }
    fn descriptor(&self) -> Value;
    fn read<'a>(&'a self, name: &'a str, args: Value) -> BoxFuture<'a, Result<Value, Error>>;
    fn execute<'a>(&'a self, name: &'a str, args: &'a Value)
        -> BoxFuture<'a, Result<Value, Error>>;
    /// Legacy adapters cannot prove a transport failure preceded mutation dispatch.
    fn execute_checked<'a>(
        &'a self,
        name: &'a str,
        args: &'a Value,
    ) -> BoxFuture<'a, NativeExecution> {
        Box::pin(async move { NativeExecution::Dispatched(self.execute(name, args).await) })
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
