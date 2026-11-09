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

#[tokio::test]
async fn native_mcp_transport_uses_the_same_scope_reports_and_revocation() {
    let (_root,state)=support::fixture();
    let project=projects::call(&state,state.auth.index("alice").unwrap(),"project_register",json!({"name":"native-scope","alias":"docs","path":"","access":"rw","mounts":[]})).await.unwrap()["project"].clone();
    let service=state.services.require::<projects::ProjectService>().unwrap();let id=project["id"].as_str().unwrap();let cwd=format!("/projects/{id}/work");
    service.channels.create(id,&json!({"scopeId":"native","cwd":cwd,"endpoint":"http://localhost/v1/channel/mcp","snapshot":{"task":{"title":"Shared"}}})).unwrap();
    let config=service.channels.config(id,&cwd,"alice").unwrap().unwrap();let authorization=config["mcp_servers.cells_session"]["http_headers"]["Authorization"].as_str().unwrap();
    let native=|method:&str,params:Value|Request::post("/v1/channel/mcp").header("authorization",authorization).header("content-type","application/json").body(Body::from(json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string())).unwrap();
    let (status,value)=response(&state,native("initialize",json!({"protocolVersion":"2025-11-25"}))).await;assert_eq!(status,StatusCode::OK);assert_eq!(value["result"]["protocolVersion"],"2025-11-25");
    let (_,value)=response(&state,native("tools/list",json!({}))).await;assert_eq!(value["result"]["tools"].as_array().unwrap().len(),5);
    let (_,value)=response(&state,native("tools/call",json!({"name":"session_report","arguments":{"id":"proof","message":{"type":"progress","content":"Native proof"}}}))).await;
    assert_eq!(value["result"]["structuredContent"]["accepted"],true);assert!(value["result"].get("resultType").is_none());
    assert_eq!(service.channels.list(id,"native").unwrap()["messages"][0]["id"],"proof");
    assert_eq!(response(&state,native("tools/call",json!({"name":"project_exec","arguments":{}}))).await.0,StatusCode::UNPROCESSABLE_ENTITY);
    let (_,value)=response(&state,native("tools/call",json!({"name":"session_request","arguments":{"id":"live","method":"context","params":{}}}))).await;
    assert_eq!(value["result"]["structuredContent"]["mustYield"],true);
    let pending=service.channels.requests(id,"native").unwrap();
    service.channels.respond(id,"native",&json!({"id":"live","requestDigest":pending["requests"][0]["requestDigest"],"status":"ok","payload":{"ready":true}})).unwrap();
    let (_,value)=response(&state,native("tools/call",json!({"name":"session_response","arguments":{"id":"live"}}))).await;
    assert_eq!(value["result"]["structuredContent"]["payload"]["ready"],true);
    let (_,value)=response(&state,native("tools/call",json!({"name":"session_receipt","arguments":{"id":"proof"}}))).await;
    assert_eq!(value["result"]["structuredContent"]["accepted"],true);
    service.channels.close(id,"native").unwrap();assert_eq!(response(&state,native("tools/list",json!({}))).await.0,StatusCode::UNAUTHORIZED);
}

fn internal_app(root:&std::path::Path,listen:&str)->cells::app::Application {
    let mut config=support::config(root,false);
    config.projects.as_mut().unwrap().network=true;
    config.plugin_options.insert("mcp".into(),toml::Value::try_from(json!({"listen":listen})).unwrap());
    cells::app::Application::from_config(&config).unwrap()
}
#[tokio::test]
async fn internal_listener_recovers_legacy_scope_without_changing_its_identity_or_grant() {
    let (root,state)=support::fixture();drop(state);
    let app=internal_app(root.path(),"127.0.0.1:0");let state=app.state();
    let project=projects::call(state,state.auth.index("alice").unwrap(),"project_register",json!({"name":"legacy","alias":"docs","path":"","access":"rw","mounts":[]})).await.unwrap()["project"].clone();
    let service=state.services.require::<projects::ProjectService>().unwrap();let id=project["id"].as_str().unwrap();let cwd=format!("/projects/{id}/work");
    let args=json!({"scopeId":"legacy","cwd":cwd,"endpoint":"http://127.0.0.1:18787/v1/channel/mcp","snapshot":{"task":{"title":"Original"}}});
    service.channels.create(id,&args).unwrap();
    let old=service.channels.config(id,&cwd,"alice").unwrap().unwrap();
    app.start().await.unwrap();
    let current=service.channels.config(id,&cwd,"alice").unwrap().unwrap();
    let server=&current["mcp_servers.cells_session"];let url=server["url"].as_str().unwrap();let authorization=server["http_headers"]["Authorization"].as_str().unwrap();
    assert_ne!(url,old["mcp_servers.cells_session"]["url"]);assert_eq!(server["http_headers"],old["mcp_servers.cells_session"]["http_headers"]);
    let client=reqwest::Client::new();
    let body=json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"session_context","arguments":{"section":"task"}}});
    let result=client.post(url).header("authorization",authorization).json(&body).send().await.unwrap();
    assert_eq!(result.status(),StatusCode::OK);let result:Value=result.json().await.unwrap();assert!(result.to_string().contains("Original"));
    assert_eq!(client.post(url).json(&body).send().await.unwrap().status(),StatusCode::UNAUTHORIZED);
    let management=url.replace("/v1/channel/mcp","/v1/api/projects/list");
    assert_eq!(client.post(management).header("authorization",authorization).json(&json!({})).send().await.unwrap().status(),StatusCode::NOT_FOUND);
    let mut rebinding=args.clone();rebinding["endpoint"]=json!(url);service.channels.create(id,&rebinding).unwrap();
    rebinding["snapshot"]=json!({"task":{"title":"Changed"}});assert_eq!(service.channels.create(id,&rebinding).unwrap_err().code,"CHANNEL_SCOPE_CHANGED");
    service.channels.close(id,"legacy").unwrap();assert_eq!(client.post(url).header("authorization",authorization).json(&body).send().await.unwrap().status(),StatusCode::UNAUTHORIZED);
    let url=url.to_owned();app.shutdown().await.unwrap();assert!(client.post(url).json(&body).send().await.is_err());
}
#[tokio::test]
async fn internal_listener_reports_bind_failure_and_unwinds_startup() {
    let (root,state)=support::fixture();drop(state);
    let occupied=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app=internal_app(root.path(),&occupied.local_addr().unwrap().to_string());
    assert!(app.start().await.unwrap_err().contains("MCP_LISTEN_FAILED"));app.shutdown().await.unwrap();
}
#[test]
fn internal_listener_rejects_non_loopback_configuration() {
    let (root,state)=support::fixture();drop(state);let mut config=support::config(root.path(),false);
    config.plugin_options.insert("mcp".into(),toml::Value::try_from(json!({"listen":"0.0.0.0:8788"})).unwrap());
    assert!(cells::app::Application::from_config(&config).is_err());
}
