//! Map an owned native request to the site's current registered access.
use super::bridge::Bridge;
use crate::{core::error::Error, projects::{runtime::ProjectRuntime, ProjectService}};
use serde_json::{json, Value};

pub(super) fn present(runtime: &ProjectRuntime, bridge: &Bridge, request: &mut Value) -> bool {
    if request["method"] != "mcpServer/elicitation/request" { return false; }
    if let Some(params) = request["params"].as_object_mut() { params.remove("cellsSiteApproval"); }
    let Ok(Some(site)) = lookup(runtime, request) else { return false; };
    if site["allowed"] == true && bridge.approve_registered_site(&request["id"], &request["params"]["threadId"]) {
        return true;
    }
    request["params"]["cellsSiteApproval"] = site;
    false
}

pub(super) fn decide(runtime: Option<&ProjectRuntime>, request: &Value, response: &Value) -> Result<Option<Value>, Error> {
    let grant = match response["decision"].as_str() {
        Some("allowSiteRead") => "read",
        Some("allowSiteReadWrite") => "read-write",
        _ => return Ok(None),
    };
    let runtime = runtime.ok_or_else(Error::unsupported)?;
    let site = lookup(runtime, request)?.ok_or_else(Error::unsupported)?;
    let revision = site["revision"].as_u64().ok_or_else(Error::invalid)?;
    if request["params"]["cellsSiteApproval"]["revision"] != revision {
        return Err(Error::conflict("AGENT_SERVER_CHANGED"));
    }
    let params = &request["params"];
    let state = runtime.state()?;
    let service = state.services.require::<ProjectService>()?;
    let started = std::time::Instant::now();
    let result = service.channels.grant_site(&runtime.project.owner, params["threadId"].as_str().unwrap(),
        params["serverName"].as_str().unwrap(), revision, grant);
    crate::core::events::emit(if result.is_ok(){crate::core::events::Level::Info}else{crate::core::events::Level::Warn},
        "harness.site.approval", json!({"operation":"harness.site.approval","operationId":request["id"],
            "stage":"persist","result":if result.is_ok(){"success"}else{"failure"},"permission":grant,
            "code":result.as_ref().err().map(|error|error.code),"durationMs":started.elapsed().as_millis()}));
    result?;
    Ok(Some(reply(grant == "read-write" || site["level"] == "read")))
}

pub(super) fn reply(accept: bool) -> Value {
    if accept { json!({"action":"accept","content":{},"_meta":null}) }
    else { json!({"action":"decline","content":null,"_meta":null}) }
}

fn lookup(runtime: &ProjectRuntime, request: &Value) -> Result<Option<Value>, Error> {
    let params = &request["params"];
    if request["method"] != "mcpServer/elicitation/request" || request["id"].is_null()
        || !super::codex::is_mcp_tool_approval(params) { return Ok(None); }
    let session = params["threadId"].as_str().filter(|id| !id.is_empty()).ok_or_else(Error::invalid)?;
    let server = params["serverName"].as_str().unwrap();
    let tool = params["_meta"]["tool_name"].as_str().unwrap();
    runtime.state()?.services.require::<ProjectService>()?.channels
        .site_approval(&runtime.project.owner, session, server, tool)
}
