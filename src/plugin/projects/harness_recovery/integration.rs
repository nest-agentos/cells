//! The real Cells adapter and database recover without a NestOS runtime.
use super::*;
use crate::harness::{config::ProfileConfig, HarnessDriver, HarnessPlugin, HarnessPlugins};
use crate::projects::runtime::ProjectRuntime;
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Plugin(Arc<AtomicUsize>);
struct Driver(Arc<AtomicUsize>);
impl HarnessPlugin for Plugin {
    fn kind(&self) -> &'static str {
        "recovery-fixture"
    }
    fn validate(&self, _: &ProfileConfig) -> Result<(), String> {
        Ok(())
    }
    fn create(
        &self,
        _: ProfileConfig,
        _: Option<Arc<ProjectRuntime>>,
    ) -> Result<Box<dyn HarnessDriver>, String> {
        Ok(Box::new(Driver(self.0.clone())))
    }
}
impl HarnessDriver for Driver {
    fn descriptor(&self) -> Value {
        json!({"id":"fixture","kind":"recovery-fixture","projectRuntime":true})
    }
    fn read<'a>(&'a self, _: &'a str, args: Value) -> BoxFuture<'a, Result<Value, Error>> {
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp_millis();
            Ok(
                json!({"session":{"id":args["sessionId"],"title":"Fixture","cwd":null,"updatedAt":null,"resumable":true,"status":"notLoaded","loaded":false,"owned":false,
                "writerLock":{"status":"not-held","checkedAt":now},"executionRecovery":{"state":"detached","checkedAt":now,"runtimeId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","processId":10,"childPid":11,"runtimeStartedAt":1}},"turns":[],"nextCursor":null}),
            )
        })
    }
    fn execute<'a>(&'a self, name: &'a str, _: &'a Value) -> BoxFuture<'a, Result<Value, Error>> {
        Box::pin(async move {
            assert_eq!(name, "harness_turn");
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"turnId":"original"}))
        })
    }
    fn stop(&self) {}
    fn drain(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
fn state(config: &crate::config::Config, count: Arc<AtomicUsize>) -> Arc<State> {
    let mut plugins = HarnessPlugins::default();
    plugins.register(Arc::new(Plugin(count))).unwrap();
    State::from_config_with_plugins(config, plugins).unwrap()
}
async fn fixture() -> (
    tempfile::TempDir,
    crate::config::Config,
    Arc<State>,
    Arc<AtomicUsize>,
    Binding,
) {
    let root = tempfile::tempdir().unwrap();
    for path in ["work", "home", "catalog"] {
        std::fs::create_dir(root.path().join(path)).unwrap();
    }
    let config=toml::from_str(&format!("listen='127.0.0.1:0'\nserver_id='recovery-fixture'\nexecution=true\napi_key='recovery-fixture-secret-at-least-24-bytes'\n[projects]\nroot={:?}\nisolation='none'\n[[exports]]\nalias='work'\npath={:?}\naccess='rw'\n[[harnesses]]\nid='fixture'\nkind='recovery-fixture'\ncommand='/bin/true'\nhome={:?}\nprojects=true\n",root.path().join("catalog"),root.path().join("work"),root.path().join("home"))).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let state = state(&config, count.clone());
    crate::process::enable(&state).await.unwrap();
    let project = super::super::call(
        &state,
        0,
        "project_register",
        json!({"name":"Fixture","alias":"work","path":"","access":"rw"}),
    )
    .await
    .unwrap()["project"]
        .clone();
    let binding = Binding {
        project_id: project["id"].as_str().unwrap().into(),
        owner: state.auth.client(0).id().into(),
        namespace: "a".repeat(64),
        profile_id: "fixture".into(),
        session_id: "native".into(),
        revision: project["revision"].as_u64().unwrap(),
    };
    let channels = &state
        .services
        .require::<super::super::ProjectService>()
        .unwrap()
        .channels;
    let record = super::tests::record();
    super::tests::write(channels, &binding, &record);
    (root, config, state, count, binding)
}
fn request(binding: &Binding, operation: &str) -> Value {
    json!({"projectId":binding.project_id,"revision":binding.revision,"namespace":binding.namespace,"profileId":binding.profile_id,"sessionId":binding.session_id,"action":"recover","instructionId":"old","expectedRevision":7,"operationId":operation})
}
#[tokio::test]
async fn cells_serializes_duplicate_requests_and_keeps_receipts_after_restart() {
    let (_root, config, state, count, binding) = fixture().await;
    let (one, two) = tokio::join!(
        call(&state, 0, request(&binding, "once")),
        call(&state, 0, request(&binding, "once"))
    );
    assert_eq!(one.unwrap()["duplicate"], false);
    assert_eq!(two.unwrap()["duplicate"], true);
    state
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
    drop(state);
    let state = self::state(&config, count.clone());
    crate::process::enable(&state).await.unwrap();
    assert_eq!(
        call(&state, 0, request(&binding, "once")).await.unwrap()["duplicate"],
        true
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let mut changed = request(&binding, "once");
    changed["expectedRevision"] = json!(8);
    assert_eq!(
        call(&state, 0, changed).await.unwrap_err().code,
        "HARNESS_RECOVERY_OPERATION_CHANGED"
    );
    state
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
}
#[tokio::test]
async fn cells_startup_recovers_registered_execution_without_any_nestos_request() {
    let (_root, config, state, count, binding) = fixture().await;
    let harness = state
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap();
    let mut args = request(&binding, "register");
    args["action"] = json!("register");
    call(&state, 0, args).await.unwrap();
    let mut args = request(&binding, "ignored");
    args["requestId"] = json!("old");
    args["epoch"] = harness.profiles()["epoch"].clone();
    args["prompt"] = json!("fixture input");
    assert_eq!(
        harness
            .project_call(&state, 0, "harness_turn", args)
            .await
            .unwrap()["outcome"],
        "committed"
    );
    harness.close().await;
    drop(state);
    let state = self::state(&config, count.clone());
    crate::process::enable(&state).await.unwrap();
    maintain(Arc::downgrade(&state));
    let channels = &state
        .services
        .require::<super::super::ProjectService>()
        .unwrap()
        .channels;
    for _ in 0..100 {
        if store::load(channels, &binding).unwrap().1["instructionQueue"]["revision"] == 8 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let record = store::load(channels, &binding).unwrap().1;
    assert_eq!(record["instructionQueue"]["revision"], 8);
    assert_eq!(
        record["instructionQueue"]["recoveries"][0]["source"],
        "system-restart"
    );
    assert_eq!(record["instructionQueue"]["paused"], false);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    state.event_stop.cancel();
    state
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
}
