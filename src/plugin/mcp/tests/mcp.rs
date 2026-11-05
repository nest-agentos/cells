//! MCP contracts, directory discovery and per-request authorization.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use cells::{app::State, projects};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;
#[allow(dead_code)]
#[path = "../../common/tests/support/directory_acl.rs"]
mod support;
use support::ALICE;

fn request(token: Option<&str>, method: &str, mut params: Value) -> Request<Body> {
    params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28",
        "io.modelcontextprotocol/clientCapabilities":{}});
    let mut builder = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-method", method)
        .header("mcp-protocol-version", "2026-07-28");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(name) = params["name"].as_str().or(params["uri"].as_str()) {
        builder = builder.header("mcp-name", name);
    }
    builder
        .body(Body::from(
            json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string(),
        ))
        .unwrap()
}

async fn response(state: &Arc<State>, request: Request<Body>) -> (StatusCode, Value) {
    let response = cells::router(state.clone(), &[])
        .unwrap()
        .oneshot(request)
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn fixed_api_has_no_mcp_envelopes_and_preserves_principal_checks() {
    let (_root,state)=support::fixture();
    let call=|token:&str| Request::post("/v1/api/projects/list").header("authorization",format!("Bearer {token}"))
        .header("content-type","application/json").body(Body::from("{}")).unwrap();
    let (status,value)=response(&state,call(ALICE)).await;
    assert_eq!(status,StatusCode::OK);assert_eq!(value["ok"],true);assert!(value.get("jsonrpc").is_none());
    assert!(value["value"]["projects"].is_array());
    let (status,_)=response(&state,call("foreign-token")).await;assert_eq!(status,StatusCode::UNAUTHORIZED);
    let (_,caps)=response(&state,Request::get("/v1/capabilities").header("authorization",format!("Bearer {ALICE}")).body(Body::empty()).unwrap()).await;
    assert_eq!(caps["node"]["api"]["version"],1);assert_eq!(caps["node"]["httpEndpoint"],"./");
}

#[tokio::test]
async fn scoped_mcp_reports_are_readable_only_through_the_authorized_fixed_api() {
    let (_root,state)=support::fixture();
    let project=projects::call(&state,state.auth.index("alice").unwrap(),"project_register",json!({"name":"scope-test","alias":"docs","path":"","access":"rw","mounts":[]})).await.unwrap()["project"].clone();
    let service=state.services.get::<projects::ProjectService>().unwrap();
    let id=project["id"].as_str().unwrap();let cwd=format!("/projects/{id}/work");
    service.channels.create(id,&json!({"scopeId":"attempt","cwd":cwd,"endpoint":"http://localhost/v1/channel/mcp","snapshot":{"task":{"title":"Task"}}})).unwrap();
    let config=service.channels.config(id,&cwd,"fixture-agent").unwrap().unwrap();
    let authorization=config["mcp_servers.cells_session"]["http_headers"]["Authorization"].as_str().unwrap();
    let token=authorization.strip_prefix("Bearer ").unwrap();
    let mut publish=request(Some(token),"tools/call",json!({"name":"session_report","arguments":{"id":"result-1","message":{"type":"result","content":"Evidence"}}}));
    *publish.uri_mut()="/v1/channel/mcp".parse().unwrap();
    let (status,receipt)=response(&state,publish).await;assert_eq!(status,StatusCode::OK);assert_eq!(receipt["result"]["structuredContent"]["accepted"],true);
    let call=Request::post("/v1/api/projects/channel_messages").header("authorization",format!("Bearer {ALICE}")).header("content-type","application/json").body(Body::from(json!({"projectId":id,"revision":project["revision"],"scopeId":"attempt"}).to_string())).unwrap();
    let (_,value)=response(&state,call).await;assert_eq!(value["value"]["messages"][0]["id"],"result-1");
    let forbidden=Request::post("/v1/api/projects/list").header("authorization",authorization).header("content-type","application/json").body(Body::from("{}")).unwrap();
    assert_eq!(response(&state,forbidden).await.0,StatusCode::UNAUTHORIZED);
    service.channels.close(id,"attempt").unwrap();
    let mut discover=request(Some(token),"server/discover",json!({}));*discover.uri_mut()="/v1/channel/mcp".parse().unwrap();
    assert_eq!(response(&state,discover).await.0,StatusCode::UNAUTHORIZED);
}
