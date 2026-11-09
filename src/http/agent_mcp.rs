//! Shared session channel transports authenticated by one immutable execution scope.
use crate::{app::State, core::error::Error, http::access};
use axum::{extract::State as AxumState, http::{HeaderMap,StatusCode}, response::{Response,IntoResponse}, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use crate::projects::channel::definitions as tools;

pub fn token(headers: &HeaderMap) -> Option<&str> {
    headers.get("authorization")?.to_str().ok()?.strip_prefix("Bearer ")
}
pub fn authorized(state: &State, headers: &HeaderMap) -> bool {
    token(headers).is_some_and(|token| state.services.get::<crate::projects::ProjectService>().is_some_and(|s| s.authorize_channel(state,token)))
}
pub async fn endpoint(AxumState(state): AxumState<Arc<State>>, headers: HeaderMap, Json(body): Json<Value>) -> Result<Response, Error> {
    access::origin(&state, &headers)?;
    if !authorized(&state,&headers) {return Err(Error::unauthenticated());}
    let modern=body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str()==Some("2026-07-28");
    if !modern {return native(&state,&headers,&body);}
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
            execute(&state,&headers,&body["params"],true)?
        }
        _ => return Err(Error::unsupported()),
    };
    Ok(Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response())
}
/// Legacy native-client transport shares the exact scope authorization and tool implementation.
fn native(state:&State,headers:&HeaderMap,body:&Value)->Result<Response,Error> {
    const VERSIONS:&[&str]=&["2024-11-05","2025-03-26","2025-06-18","2025-11-25"];
    let method=body["method"].as_str().ok_or_else(Error::invalid)?;
    if body["jsonrpc"]!="2.0" {return Err(Error::invalid());}
    if let Some(version)=headers.get("mcp-protocol-version") {if !VERSIONS.contains(&version.to_str().map_err(|_|Error::invalid())?){return Err(Error::invalid());}}
    if method=="notifications/initialized"&&body.get("id").is_none(){return Ok(StatusCode::ACCEPTED.into_response());}
    let id=&body["id"];if !(id.is_string()||id.is_u64()||id.is_i64()){return Err(Error::invalid());}
    let result=match method {
        "initialize"=>{let requested=body["params"]["protocolVersion"].as_str().unwrap_or("");
            json!({"protocolVersion":if VERSIONS.contains(&requested){requested}else{"2025-11-25"},"capabilities":{"tools":{}},"serverInfo":{"name":"cells-session","version":env!("CARGO_PKG_VERSION")}})},
        "tools/list"=>json!({"tools":tools()}),
        "tools/call"=>{let mut value=execute(state,headers,&body["params"],false)?;value.as_object_mut().unwrap().remove("resultType");value},
        "ping"=>json!({}),
        _=>return Err(Error::unsupported()),
    };
    Ok(Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response())
}

fn execute(state:&State,headers:&HeaderMap,params:&Value,modern:bool)->Result<Value,Error> {
            let name = params["name"].as_str().ok_or_else(Error::invalid)?;
            if !["session_context","session_report","session_receipt","session_request","session_response"].contains(&name) {return Err(Error::unsupported());}
            if modern && headers.get("mcp-name").and_then(|h|h.to_str().ok()) != Some(name) {return Err(Error::invalid());}
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
