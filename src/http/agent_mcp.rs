//! Narrow MCP 2.0 surface authenticated by one immutable execution scope.
use crate::{app::State, core::error::Error, http::access};
use axum::{extract::State as AxumState, http::HeaderMap, Json};
use serde_json::{json, Value};
use std::sync::Arc;

pub fn token(headers: &HeaderMap) -> Option<&str> {
    headers.get("authorization")?.to_str().ok()?.strip_prefix("Bearer ")
}
pub fn authorized(state: &State, headers: &HeaderMap) -> bool {
    token(headers).is_some_and(|token| state.services.get::<crate::projects::ProjectService>().is_some_and(|s| s.authorize_channel(state,token)))
}
pub async fn endpoint(AxumState(state): AxumState<Arc<State>>, headers: HeaderMap, Json(body): Json<Value>) -> Result<Json<Value>, Error> {
    access::origin(&state, &headers)?;
    if !authorized(&state,&headers) {return Err(Error::unauthenticated());}
    let id = &body["id"];
    if body["jsonrpc"] != "2.0" || !(id.is_string() || id.is_u64() || id.is_i64()) {return Err(Error::invalid());}
    let method = body["method"].as_str().ok_or_else(Error::invalid)?;
    let version = body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str();
    if version != Some("2026-07-28") || headers.get("mcp-protocol-version").and_then(|h|h.to_str().ok()) != version
        || headers.get("mcp-method").and_then(|h|h.to_str().ok()) != Some(method) {return Err(Error::invalid());}
    let result = match method {
        "server/discover" => json!({"resultType":"complete","ttlMs":0,"cacheScope":"private","supportedVersions":["2026-07-28"],"capabilities":{"tools":{}},"_meta":{"io.modelcontextprotocol/serverInfo":{"name":"cells-session","version":env!("CARGO_PKG_VERSION")}}}),
        "tools/list" => json!({"resultType":"complete","ttlMs":0,"cacheScope":"private","tools":tools()}),
        "ping" => json!({"resultType":"complete"}),
        "tools/call" => {
            execute(&state,&headers,&body["params"])?
        }
        _ => return Err(Error::unsupported()),
    };
    Ok(Json(json!({"jsonrpc":"2.0","id":id,"result":result})))
}
fn execute(state:&State,headers:&HeaderMap,params:&Value)->Result<Value,Error> {
            let name = params["name"].as_str().ok_or_else(Error::invalid)?;
            if !["session_context","session_report","session_receipt","session_request","session_response"].contains(&name) {return Err(Error::unsupported());}
            if headers.get("mcp-name").and_then(|h|h.to_str().ok()) != Some(name) {return Err(Error::invalid());}
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            if !args.is_object() {return Err(Error::invalid());}
            let service = state.services.require::<crate::projects::ProjectService>()?;
            let started=std::time::Instant::now();
            let response = service.channels.agent(token(&headers).unwrap(),name,&args);
            crate::core::events::emit(crate::core::events::Level::Info,"channel.operation",json!({
                "operation":name,"operationId":args["id"].as_str().filter(|id|crate::core::ids::is_identifier_within(id,128)),
                "projectId":service.channels.token_project(token(&headers).unwrap()),"stage":"execute",
                "result":if response.is_ok(){"success"}else{"failure"},"code":response.as_ref().err().map(|e|e.code),"elapsedMs":started.elapsed().as_millis()}));
            let (value,is_error) = match response {Ok(value)=>(value,false),Err(error)=>(json!({"code":error.code}),true)};
            Ok(json!({"resultType":"complete","isError":is_error,"structuredContent":value,"content":[{"type":"text","text":value.to_string()}]}))
}

fn tools() -> Vec<Value> {
    vec![
        json!({"name":"session_context","description":"Read a frozen context section. Omit section to list available sections. Load only what the current step needs.","inputSchema":{"type":"object","properties":{"section":{"type":"string"}},"additionalProperties":false},"annotations":{"readOnlyHint":true}}),
        json!({"name":"session_report","description":"Durably publish progress, a question, an error or a delivery application. Reuse id unchanged to recover a lost receipt. Acceptance is not task completion.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"message":{"type":"object"}},"required":["id","message"],"additionalProperties":false}}),
        json!({"name":"session_request","description":"Request host-resolved live context. If pending, end the turn without a final result; never poll. Reuse the same id and arguments after a lost response.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"method":{"type":"string"},"params":{"type":"object"}},"required":["id","method","params"],"additionalProperties":false}}),
        json!({"name":"session_response","description":"Read an immutable host response after the host resumes this Session.","inputSchema":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false},"annotations":{"readOnlyHint":true}}),
        json!({"name":"session_receipt","description":"Check whether a report was durably accepted without repeating its effect.","inputSchema":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false},"annotations":{"readOnlyHint":true}}),
    ]
}
