#[path = "support/mod.rs"]
mod common;
use axum::{body::Body, extract::State as AxumState, routing::get, Json, Router};
use cells::{
    app::{Application, State},
    auth::{Auth, Client},
    fs::Exports,
    plugin::common::{
        object, Capabilities, NodePlugin, PluginConfiguration, PluginRegistry, PluginSettings,
    },
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::Arc,
};
use tower::ServiceExt;
struct Feature {
    id: &'static str,
}
struct OwnState {
    value: String,
}
impl NodePlugin for Feature {
    fn id(&self) -> &'static str {
        self.id
    }
    fn configure(&self, config: &dyn PluginConfiguration, host: &mut State) -> Result<(), String> {
        let value = config
            .option(self.id)
            .and_then(|v| v.get("value"))
            .and_then(|v| v.as_str())
            .ok_or("Missing fixture value")?;
        host.services.insert(Arc::new(OwnState {
            value: value.into(),
        }))
    }
    fn capabilities(&self, _state: &State, _identity: usize, enabled: bool) -> Capabilities {
        Capabilities {
            http: object(json!({"fixture":enabled})),
            node: object(json!({"fixture":enabled})),
        }
    }

}
fn fixture_routes() -> Router<Arc<State>> {
        Router::new().route(
            "/v1/fixture",
            get(|AxumState(state): AxumState<Arc<State>>| async move {
                let value = state.services.require::<OwnState>()?;
                Ok::<_, cells::core::error::Error>(Json(json!({"value":value.value})))
            }),
        )

}
fn build(enabled: bool) -> Application {
    let auth = Auth::new(
        Some("fixture".into()),
        vec![Client::new(common::SECRET.into(), None, vec![], vec![])],
    );
    let mut host = State::host(auth, Exports::new(BTreeMap::new())).unwrap();
    let mut routes = cells::http::HttpRoutes::default();
    routes.register("fixture", fixture_routes()).unwrap();
    host.services.insert(Arc::new(routes)).unwrap();
    let mut registry = PluginRegistry::empty();
    registry
        .register(Arc::new(Feature { id: "fixture" }))
        .unwrap();
    registry
        .register(Arc::new(cells::plugin::mcp::Plugin))
        .unwrap();
    let config = PluginSettings {
        enabled: BTreeMap::from([("fixture".into(), enabled)]),
        options: BTreeMap::from([(
            "fixture".into(),
            toml::from_str("value='configured'").unwrap(),
        )]),
    };
    Application::assemble(host, registry, &config).unwrap()
}
async fn capabilities(app: &Application) -> Value {
    let response=app.router().unwrap().oneshot(common::request("/v1/capabilities").body(Body::empty()).unwrap()).await.unwrap();
    common::body_json(response).await
}
#[tokio::test]
async fn independently_assembles_configuration_state_http_routes_and_node_capabilities() {
    let app = build(true);
    app.start().await.unwrap();
    assert!(app
        .state()
        .services
        .get::<cells::harness::Harnesses>()
        .is_none());
    assert!(app
        .state()
        .services
        .get::<cells::process::Execution>()
        .is_none());
    let response = app
        .router()
        .unwrap()
        .oneshot(common::request("/v1/fixture").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(common::body_json(response).await["value"], "configured");
    assert_eq!(capabilities(&app).await["node"]["fixture"], true);
    app.shutdown().await.unwrap();
}
#[tokio::test]
async fn switching_off_a_plugin_removes_its_state_and_http_routes() {
    let app = build(false);
    app.start().await.unwrap();
    assert!(app.state().services.get::<OwnState>().is_none());
    assert!(app
        .state()
        .services
        .get::<cells::operations::Operations>()
        .is_none());
    assert_eq!(
        app.router()
            .unwrap()
            .oneshot(common::request("/v1/fixture").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(capabilities(&app).await["node"]["fixture"], false);
    app.shutdown().await.unwrap();
}
struct Prepared {
    closed: Arc<std::sync::atomic::AtomicUsize>,
}
impl NodePlugin for Prepared {
    fn id(&self) -> &'static str {
        "prepared"
    }
    fn stop<'a>(&'a self, _state: &'a Arc<State>) -> cells::plugin::common::PluginFuture<'a> {
        Box::pin(async move {
            self.closed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    }
}
#[tokio::test]
async fn shutdown_releases_prepared_plugins_even_before_the_first_start() {
    let closed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = PluginRegistry::empty();
    registry
        .register(Arc::new(Prepared {
            closed: closed.clone(),
        }))
        .unwrap();
    let auth = Auth::new(
        Some("fixture".into()),
        vec![Client::new(common::SECRET.into(), None, vec![], vec![])],
    );
    let host = State::host(auth, Exports::new(BTreeMap::new())).unwrap();
    let app = Application::assemble(host, registry, &PluginSettings::default()).unwrap();
    app.shutdown().await.unwrap();
    app.shutdown().await.unwrap();
    assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 1);
}

struct Operations { id: &'static str }
impl NodePlugin for Operations {
    fn id(&self) -> &'static str { self.id }
    fn operation_domains(&self) -> &'static [&'static str] { &["fixture"] }
    fn call<'a>(&'a self, _: &'a Arc<State>, _: usize, _: &'a str, operation: &'a str, args: Value) -> cells::plugins::OperationFuture<'a> {
        Box::pin(async move {
            if operation == "uncertain" {
                let mut error = cells::core::error::Error::internal();
                error.outcome = Some("unknown");
                return Err(error);
            }
            Ok(json!({"operation":operation,"value":args["value"]}))
        })
    }
}
fn operation_app(enabled: bool) -> Application {
    let auth = Auth::new(None, vec![Client::new(common::SECRET.into(), None, vec![], vec![])]);
    let host = State::host(auth, Exports::default()).unwrap();
    let mut registry = PluginRegistry::empty();
    registry.register(Arc::new(Operations { id: "fixture" })).unwrap();
    let mut settings = PluginSettings::default();
    settings.enabled.insert("fixture".into(), enabled);
    Application::assemble(host, registry, &settings).unwrap()
}
#[tokio::test]
async fn operation_ports_share_http_and_in_process_dispatch_with_business_outcomes() {
    let app = operation_app(true);
    let direct = cells::app::operations::invoke(app.state(), 0, "fixture", "echo", json!({"value":7})).await.unwrap().unwrap();
    let response = app.router().unwrap().oneshot(common::request("/v1/api/fixture/echo").method("POST")
        .header("content-type", "application/json").body(Body::from(r#"{"value":7}"#)).unwrap()).await.unwrap();
    assert_eq!(common::body_json(response).await, json!({"ok":true,"value":direct}));
    let response = app.router().unwrap().oneshot(common::request("/v1/api/fixture/uncertain").method("POST")
        .header("content-type", "application/json").body(Body::from("{}")).unwrap()).await.unwrap();
    assert_eq!(common::body_json(response).await["error"]["outcome"], "unknown");
    let disabled = operation_app(false);
    let result = cells::app::operations::invoke(disabled.state(), 0, "fixture", "echo", json!({})).await.unwrap();
    assert!(result.unwrap_err().is_unsupported());
    assert!(cells::app::operations::invoke(app.state(), 0, "fixture", "Invalid", json!({})).await.is_err());
    app.shutdown().await.unwrap();
    disabled.shutdown().await.unwrap();
}
#[test]
fn duplicate_operation_owners_are_rejected_before_assembly_even_when_disabled() {
    let mut registry = PluginRegistry::empty();
    registry.register(Arc::new(Operations { id: "one" })).unwrap();
    registry.register(Arc::new(Operations { id: "two" })).unwrap();
    let mut settings = PluginSettings::default();
    settings.enabled.insert("two".into(), false);
    assert!(registry.resolve(&settings).err().unwrap().contains("duplicate operation domain"));
}

#[test]
fn transports_reject_duplicate_and_unregistered_owners() {
    let mut routes = cells::http::HttpRoutes::default();
    routes.register("orphan", Router::new()).unwrap();
    assert!(routes.register("orphan", Router::new()).is_err());
    let auth = Auth::new(None, vec![Client::new(common::SECRET.into(), None, vec![], vec![])]);
    let mut host = State::host(auth, Exports::default()).unwrap();
    host.services.insert(Arc::new(routes)).unwrap();
    let app = Application::assemble(host, PluginRegistry::empty(), &PluginSettings::default()).unwrap();
    assert!(app.router().err().unwrap().contains("no registered plugin owner"));
}
