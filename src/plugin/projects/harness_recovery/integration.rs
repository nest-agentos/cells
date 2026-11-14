//! The real Cells adapter and database recover without a NestOS runtime.
use super::*;
use crate::harness::{config::ProfileConfig, HarnessDriver, HarnessPlugin, HarnessPlugins};
use crate::projects::runtime::ProjectRuntime;
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Default)]
struct ReadBarrier {
    blocked: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
struct Plugin(Arc<AtomicUsize>, Option<Arc<ReadBarrier>>);
struct Driver(Arc<AtomicUsize>, Option<Arc<ReadBarrier>>);
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
        Ok(Box::new(Driver(self.0.clone(), self.1.clone())))
    }
}
impl HarnessDriver for Driver {
    fn descriptor(&self) -> Value {
        json!({"id":"fixture","kind":"recovery-fixture","projectRuntime":true})
    }
    fn read<'a>(&'a self, _: &'a str, args: Value) -> BoxFuture<'a, Result<Value, Error>> {
        Box::pin(async move {
            if args["sessionId"] == "native" {
                if let Some(barrier) = &self.1 {
                    let released = barrier.release.notified();
                    barrier.entered.notify_one();
                    if barrier.blocked.load(Ordering::SeqCst) {
                        released.await;
                    }
                }
            }
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
    state_with_barrier(config, count, None)
}
fn state_with_barrier(
    config: &crate::config::Config,
    count: Arc<AtomicUsize>,
    barrier: Option<Arc<ReadBarrier>>,
) -> Arc<State> {
    let mut plugins = HarnessPlugins::default();
    plugins.register(Arc::new(Plugin(count, barrier))).unwrap();
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

#[tokio::test]
async fn slow_native_recovery_does_not_block_another_session_or_same_session_queue_reads() {
    let (_root, config, previous, count, binding) = fixture().await;
    previous
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
    drop(previous);
    let barrier = Arc::new(ReadBarrier::default());
    barrier.blocked.store(true, Ordering::SeqCst);
    let state = state_with_barrier(&config, count, Some(barrier.clone()));
    crate::process::enable(&state).await.unwrap();
    let mut other = binding.clone();
    other.session_id = "other".into();
    other.namespace = "b".repeat(64);
    let mut record = super::tests::record();
    record["sessionId"] = json!("other");
    let channels = &state
        .services
        .require::<super::super::ProjectService>()
        .unwrap()
        .channels;
    super::tests::write(channels, &other, &record);
    let waiting = {
        let state = state.clone();
        let args = request(&binding, "slow");
        tokio::spawn(async move { call(&state, 0, args).await })
    };
    barrier.entered.notified().await;
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        call(&state, 0, request(&other, "independent")),
    )
    .await;
    assert!(result.unwrap().is_ok());
    let mut read = request(&binding, "read");
    read["action"] = json!("read");
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(200),
        super::super::harness_queue::call(&state, 0, read)
    )
    .await
    .unwrap()
    .is_ok());
    barrier.blocked.store(false, Ordering::SeqCst);
    barrier.release.notify_waiters();
    assert!(waiting.await.unwrap().is_ok());
    state
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
}

#[tokio::test]
async fn channel_observers_do_not_consume_filesystem_watch_capacity() {
    use futures_util::StreamExt;
    let (_root, _config, state, _count, binding) = fixture().await;
    let service = state
        .services
        .require::<super::super::ProjectService>()
        .unwrap();
    let all = service
        .watch_slots
        .clone()
        .try_acquire_many_owned(16)
        .unwrap();
    service
        .channels
        .create(
            &binding.project_id,
            &json!({"scopeId":"scope","cwd":"/work","snapshot":{}}),
        )
        .unwrap();
    let args =
        json!({"projectId":binding.project_id,"revision":binding.revision,"channelId":"scope"});
    let mut observers = Vec::new();
    for _ in 0..20 {
        let mut stream =
            super::super::channel_subscription::subscribe(state.clone(), 0, args.clone())
                .await
                .unwrap();
        assert!(stream.next().await.unwrap().is_ok());
        observers.push(stream);
    }
    assert_eq!(service.watch_slots.available_permits(), 0);
    drop((observers, all));
    assert_eq!(service.watch_slots.available_permits(), 16);
    state
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
}
