use crate::{app::State, core::error::Error};
use serde_json::{json, Value};
use std::sync::Arc;
pub(super) fn descriptor(state: &State, identity: usize) -> Result<Value, Error> {
    let base = crate::plugin::common::object(
        json!({"version":1,"httpEndpoint":"./","serverId":state.auth.server_id(),"events":crate::http::subscriptions::capability(),
        "authorization":state.auth.client(identity).authorization(),"plugins":state.plugins.statuses()}),
    );
    state.plugins.capabilities(state, identity, base, false)
}
pub(super) fn list(state: &State, identity: usize) -> Vec<Value> {
    state.plugins.tools(state, identity)
}
pub(super) fn contains(state: &State, identity: usize, name: &str) -> bool {
    state.plugins.tool(state, identity, name).is_some()
}
pub(super) async fn call(state: &Arc<State>, identity: usize, params: &Value) -> Value {
    let name = params["name"].as_str().unwrap_or("");
    let Some(provider) = state.plugins.tool(state, identity, name) else {
        return result(Err(Error::unsupported()), "not-committed");
    };
    let output = provider
        .call(
            state,
            identity,
            name,
            params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({})),
        )
        .await;
    let outcome = output
        .as_ref()
        .err()
        .map(|error| provider.error_outcome(name, *error))
        .unwrap_or("not-committed");
    result(output, outcome)
}
fn result(result: Result<Value, Error>, outcome: &str) -> Value {
    let (value, error) = match result {
        Ok(value) => (value, false),
        Err(error) => (json!({"code":error.code,"outcome":outcome}), true),
    };
    json!({"resultType":"complete","isError":error,"structuredContent":value,"content":[{"type":"text","text":value.to_string()}]})
}
