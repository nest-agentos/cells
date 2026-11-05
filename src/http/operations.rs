//! Fixed application API, independent of MCP discovery and transport.
use crate::{app::State, core::error::Error, http::access};
use axum::{extract::{Path, State as AxumState}, http::HeaderMap, Json};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn invoke(
    AxumState(state): AxumState<Arc<State>>, Path((domain, operation)): Path<(String, String)>,
    headers: HeaderMap, Json(args): Json<Value>,
) -> Result<Json<Value>, Error> {
    let identity = access::identity(&state, &headers)?;
    if !args.is_object() || operation.is_empty() || operation.len()>64 || !operation.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') {
        return Err(Error::invalid());
    }
    let operation_id = args["requestId"].as_str().or(args["scopeId"].as_str()).filter(|id|crate::core::ids::is_identifier_within(id,128)).map(str::to_owned);
    let started = std::time::Instant::now();
    let result = match domain.as_str() {
        "agents" if state.plugins.enabled("projects") && !state.auth.client(identity).anonymous() => {
            let service=state.services.require::<crate::projects::ProjectService>()?;
            let owner=state.auth.client(identity).id();
            match operation.as_str(){"mcp_list"=>service.channels.server_list(owner),"mcp_save"=>service.channels.server_save(owner,&args),"mcp_remove"=>service.channels.server_remove(owner,&args),_=>Err(Error::unsupported())}
        }
        "harness" if state.plugins.enabled("harness") => {
            let service = state.services.require::<crate::harness::Harnesses>()?;
            let name = format!("harness_{operation}");
            if args.get("projectId").is_some() { service.project_call(&state, identity, &name, args).await }
            else { service.authorized_call(&state, identity, &name, args).await }
        }
        "projects" if state.plugins.enabled("projects") && !state.auth.client(identity).anonymous() =>
            crate::projects::call(&state, identity, &format!("project_{operation}"), args).await,
        _ => Err(Error::unsupported()),
    };
    crate::core::events::emit(crate::core::events::Level::Info, "api.operation",
        json!({"operationId":operation_id,"operation":format!("{domain}.{operation}"),"stage":"execute",
        "result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),
        "elapsedMs":started.elapsed().as_millis()}));
    Ok(Json(match result {
        Ok(value) => json!({"ok":true,"value":value}),
        Err(error) => json!({"ok":false,"error":{"code":error.code,"outcome":
            if error.code == "HARNESS_RECEIPT_STORAGE" {"unknown"} else {error.outcome.unwrap_or("not-committed")}}}),
    }))
}
