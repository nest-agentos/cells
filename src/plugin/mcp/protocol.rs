//! MCP request metadata, mirrored headers and JSON-RPC envelopes.
use super::{tools, VERSION};
use crate::{app::State, core::error::Error};
use axum::{
    extract::rejection::JsonRejection,
    http::{HeaderMap, StatusCode},
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
pub(super) type Response = (StatusCode, Json<Value>);
pub(super) struct Failure {
    code: i64,
    message: &'static str,
    data: Value,
    status: StatusCode,
}
impl Failure {
    fn new(code: i64, message: &'static str) -> Self {
        Self {
            code,
            message,
            data: json!({}),
            status: StatusCode::BAD_REQUEST,
        }
    }
    pub(super) fn method() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            ..Self::new(-32601, "Method not found")
        }
    }
    pub(super) fn resource(error: Error) -> Self {
        Self {
            data: json!({"code":error.code}),
            status: error.status,
            ..Self::new(
                if error.is_not_found() { -32602 } else { -32603 },
                "Resource unavailable",
            )
        }
    }
    pub(super) fn json(error: JsonRejection) -> Self {
        let syntax = matches!(&error, JsonRejection::JsonSyntaxError(_));
        Self {
            status: error.status(),
            ..Self::new(if syntax { -32700 } else { -32600 }, "Invalid JSON request")
        }
    }
    pub(super) fn response(self, id: Value) -> Response {
        let mut body = json!({"jsonrpc":"2.0","error":{"code":self.code,"message":self.message,"data":self.data}});
        if id.is_string() || id.is_i64() || id.is_u64() {
            body["id"] = id;
        }
        (self.status, Json(body))
    }
}

pub(super) fn validate(
    state: &State,
    identity: usize,
    headers: &HeaderMap,
    body: &Value,
) -> Result<(), Failure> {
    let id = &body["id"];
    if body["jsonrpc"] != "2.0"
        || !(id.is_string() || id.is_i64() || id.is_u64())
        || !body["method"].is_string()
    {
        return Err(Failure::new(-32600, "Invalid request"));
    }
    validate_headers(headers, body)?;
    validate_metadata(body)?;
    validate_tool(state, identity, body)
}

fn validate_tool(state: &State, identity: usize, body: &Value) -> Result<(), Failure> {
    if body["method"] == "tools/call"
        && (body["params"]
            .get("arguments")
            .is_some_and(|args| !args.is_object())
            || !tools::contains(
                state,
                identity,
                body["params"]["name"].as_str().unwrap_or(""),
            ))
    {
        return Err(Failure::new(-32602, "Invalid tool call"));
    }
    Ok(())
}

fn validate_headers(headers: &HeaderMap, body: &Value) -> Result<(), Failure> {
    let version = body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"]
        .as_str()
        .unwrap_or("");
    let method = body["method"].as_str().unwrap_or("");
    let mismatch = [("mcp-protocol-version", version), ("mcp-method", method)]
        .iter()
        .any(|(name, value)| headers.get(*name).and_then(|v| v.to_str().ok()) != Some(*value));
    if mismatch || named_header_mismatch(headers, body) {
        return Err(Failure::new(-32020, "Header mismatch"));
    }
    if version != VERSION {
        return Err(Failure {
            data: json!({"supported":[VERSION],"requested":version}),
            ..Failure::new(-32022, "Unsupported protocol version")
        });
    }
    Ok(())
}

fn validate_metadata(body: &Value) -> Result<(), Failure> {
    let meta = &body["params"]["_meta"];
    if !meta["io.modelcontextprotocol/clientCapabilities"].is_object()
        || meta
            .get("io.modelcontextprotocol/clientInfo")
            .is_some_and(|info| !info["name"].is_string() || !info["version"].is_string())
    {
        return Err(Failure::new(-32602, "Invalid client metadata"));
    }
    Ok(())
}

fn decoded(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?;
    if let Some(encoded) = value
        .strip_prefix("=?base64?")
        .and_then(|s| s.strip_suffix("?="))
    {
        return String::from_utf8(STANDARD.decode(encoded).ok()?).ok();
    }
    Some(value.to_owned())
}

fn named_header_mismatch(headers: &HeaderMap, body: &Value) -> bool {
    let field = match body["method"].as_str() {
        Some("tools/call") => "name",
        Some("resources/read") => "uri",
        _ => return false,
    };
    let expected = body["params"][field].as_str();
    expected.is_none() || decoded(headers, "mcp-name").as_deref() != expected
}
