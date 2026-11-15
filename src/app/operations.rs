//! Application invocation, independent of URL routing and concrete feature implementations.
use crate::{app::State, core::error::Error};
use serde_json::{json, Value};
use std::sync::Arc;
pub async fn invoke(
    state: &Arc<State>,
    identity: usize,
    domain: &str,
    operation: &str,
    args: Value,
) -> Result<Result<Value, Error>, Error> {
    if !args.is_object() || !valid_name(domain) || !valid_name(operation) {
        return Err(Error::invalid());
    }
    let operation_id = args["requestId"]
        .as_str()
        .or(args["scopeId"].as_str())
        .filter(|id| crate::core::ids::is_identifier_within(id, 128))
        .map(str::to_owned);
    let started = std::time::Instant::now();
    let result = state
        .plugins
        .call(state, identity, domain, operation, args)
        .await;
    crate::core::events::emit(
        crate::core::events::Level::Info,
        "api.operation",
        json!({"plugin":state.plugins.operation_owner(domain),"operationId":operation_id,"operation":format!("{domain}.{operation}"),"stage":"execute",
        "result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),
        "outcome":result.as_ref().err().and_then(|e|e.outcome),"elapsedMs":started.elapsed().as_millis()}),
    );
    Ok(result)
}

fn valid_name(domain: &str) -> bool {
    !domain.is_empty()
        && domain.len() <= 64
        && domain.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
}
