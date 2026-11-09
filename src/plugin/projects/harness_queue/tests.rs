use super::*;
use crate::harness::{config::ProfileConfig, HarnessDriver, HarnessPlugin, HarnessPlugins};
use crate::projects::runtime::ProjectRuntime;
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};
fn empty() -> Value {
    json!({"revision":0,"paused":false,"items":[],"receipts":[]})
}
fn enqueue(id: &str, text: &str) -> Value {
    json!({"action":"enqueue","item":{"id":id,"text":text,"attachments":[],"createdAt":1,"state":"pending"},"input":json!([text,[],null,null]).to_string()})
}
#[test]
fn input_acceptance_is_idempotent_and_rejects_changed_content() {
    let mut queue = empty();
    commands::apply(&mut queue, &enqueue("one", "first")).unwrap();
    let saved = queue.clone();
    commands::apply(&mut queue, &enqueue("one", "first")).unwrap();
    assert_eq!(queue, saved);
    assert!(commands::apply(&mut queue, &enqueue("one", "changed")).is_err());
    let mut injected = enqueue("two", "second");
    injected["item"]["receipt"] = json!({"turnId":"forged"});
    assert!(commands::apply(&mut queue, &injected).is_err());
}
#[test]
fn resume_requires_known_admission_and_allocates_retry_identity_only_for_rejections() {
    let mut queue = empty();
    commands::apply(&mut queue, &enqueue("one", "first")).unwrap();
    queue["items"][0]["state"] = json!("uncertain");
    assert!(commands::apply(
        &mut queue,
        &json!({"action":"edit","edit":{"action":"resume","expectedRevision":1}})
    )
    .is_err());
    queue["items"][0]["state"] = json!("failed");
    queue["paused"] = json!(true);
    commands::apply(
        &mut queue,
        &json!({"action":"edit","edit":{"action":"resume","expectedRevision":1}}),
    )
    .unwrap();
    assert_eq!(queue["items"][0]["state"], "pending");
    assert!(queue["items"][0]["attemptId"].is_string());
    assert_eq!(queue["paused"], false);
}
#[test]
fn merge_and_reorder_preserve_acceptance_receipts_and_revision_fences() {
    let mut queue = empty();
    for id in ["one", "two", "three"] {
        commands::apply(&mut queue, &enqueue(id, id)).unwrap();
    }
    commands::apply(&mut queue,&json!({"action":"edit","edit":{"action":"reorder","expectedRevision":3,"ids":["three","one","two"]}})).unwrap();
    commands::apply(&mut queue,&json!({"action":"edit","edit":{"action":"merge","expectedRevision":4,"ids":["one","two"]}})).unwrap();
    assert_eq!(queue["items"][1]["text"], "one\n\ntwo");
    assert_eq!(queue["receipts"].as_array().unwrap().len(), 3);
    assert!(commands::apply(
        &mut queue,
        &json!({"action":"edit","edit":{"action":"pause","expectedRevision":3}})
    )
    .is_err());
}
#[derive(Default)]
struct Counters {
    sent: AtomicUsize,
    finished: AtomicUsize,
    steered: AtomicUsize,
    command: std::sync::Mutex<Option<(String, usize)>>,
}
struct Plugin(Arc<Counters>);
struct Driver(Arc<Counters>);
impl HarnessPlugin for Plugin {
    fn kind(&self) -> &'static str {
        "queue-fixture"
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
        json!({"id":"fixture","projectRuntime":true,"capabilities":{"steer":true}})
    }
    fn read<'a>(&'a self, name: &'a str, args: Value) -> BoxFuture<'a, Result<Value, Error>> {
        Box::pin(async move {
            if name == "harness_commands" {
                return Ok(
                    json!({"commands":[{"name":"compact","id":"compact-def","revision":"1","kind":"action","execution":"serial"}]}),
                );
            }
            let sent = self.0.sent.load(Ordering::SeqCst);
            let active = sent > self.0.finished.load(Ordering::SeqCst);
            Ok(
                json!({"session":{"id":args["sessionId"],"title":"Fixture","cwd":null,"updatedAt":null,"resumable":true,"owned":true,"loaded":true,"status":if active{"active"}else{"idle"},"activeTurnId":if active{Some(format!("turn-{sent}"))}else{None},"lastTurnId":format!("turn-{sent}"),"lastTurnResult":if active{None}else{Some("completed")}},"turns":[],"nextCursor":null}),
            )
        })
    }
    fn subscribe(&self, _: Value) -> crate::plugin::common::EventFuture<'_> {
        let counters = self.0.clone();
        Box::pin(async move {
            let stream = futures_util::stream::unfold(
                (counters, Value::Null),
                |(counters, mut previous)| async move {
                    loop {
                        let command = counters.command.lock().unwrap().clone();
                        let next = if let Some((id, turn)) = command {
                            json!({"events":[{"seq":1,"message":{"method":"harness/command/updated","params":{"threadId":"native","commandId":id,"turnId":format!("turn-{turn}"),"status":if counters.finished.load(Ordering::SeqCst)>=turn{"completed"}else{"running"},"failed":false}}}],"requests":[]})
                        } else {
                            json!({"events":[],"requests":[]})
                        };
                        if next != previous {
                            previous = next.clone();
                            return Some((Ok(next), (counters, previous)));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                },
            );
            Ok(Box::pin(stream) as crate::plugin::common::EventStream)
        })
    }
    fn execute<'a>(
        &'a self,
        name: &'a str,
        args: &'a Value,
    ) -> BoxFuture<'a, Result<Value, Error>> {
        Box::pin(async move {
            if name == "harness_steer" {
                self.0.steered.fetch_add(1, Ordering::SeqCst);
                return Ok(json!({"turnId":args["turnId"]}));
            }
            let count = self.0.sent.fetch_add(1, Ordering::SeqCst) + 1;
            if name == "harness_command" {
                *self.0.command.lock().unwrap() =
                    Some((args["requestId"].as_str().unwrap().into(), count));
                return Ok(
                    json!({"commandId":args["requestId"],"status":"running","output":"fixture command output"}),
                );
            }
            assert_eq!(name, "harness_turn");
            Ok(json!({"turnId":format!("turn-{count}")}))
        })
    }
    fn stop(&self) {}
    fn drain(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
async fn fixture() -> (tempfile::TempDir, Arc<State>, Arc<Counters>, Binding) {
    let root = tempfile::tempdir().unwrap();
    for path in ["work", "home", "catalog"] {
        std::fs::create_dir(root.path().join(path)).unwrap();
    }
    let config=toml::from_str(&format!("listen='127.0.0.1:0'\nserver_id='queue-fixture'\nexecution=true\napi_key='queue-fixture-secret-at-least-24-bytes'\n[projects]\nroot={:?}\nisolation='none'\n[[exports]]\nalias='work'\npath={:?}\naccess='rw'\n[[harnesses]]\nid='fixture'\nkind='queue-fixture'\ncommand='/bin/true'\nhome={:?}\nprojects=true\n",root.path().join("catalog"),root.path().join("work"),root.path().join("home"))).unwrap();
    let count = Arc::new(Counters::default());
    let mut plugins = HarnessPlugins::default();
    plugins.register(Arc::new(Plugin(count.clone()))).unwrap();
    let state = State::from_config_with_plugins(&config, plugins).unwrap();
    crate::process::enable(&state).await.unwrap();
    let project = super::super::call(
        &state,
        0,
        "project_register",
        json!({"name":"Queue","alias":"work","path":"","access":"rw"}),
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
    let body=json!({"schema":1,"text":json!({"sessionId":"native","draft":"retained","instructionQueue":empty()}).to_string()}).to_string();
    channels.client_state(&binding.project_id,&binding.owner,&json!({"key":format!("{}/recovery",binding.namespace),"action":"compare","expected":null,"value":body})).unwrap();
    (root, state, count, binding)
}
fn request(binding: &Binding, id: &str, text: &str) -> Value {
    let mut args = enqueue(id, text);
    for(k,v)in json!({"projectId":binding.project_id,"revision":binding.revision,"namespace":binding.namespace,"profileId":binding.profile_id,"sessionId":binding.session_id,"operationId":id}).as_object().unwrap(){args[k]=v.clone();}
    args
}
async fn wait_count(count: &Counters, expected: usize) {
    for _ in 0..200 {
        if count.sent.load(Ordering::SeqCst) == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("queued work did not advance");
}
async fn shutdown(state: &Arc<State>) {
    state.event_stop.cancel();
    state
        .services
        .require::<crate::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}
#[tokio::test]
async fn cells_dispatches_and_continues_its_queue_without_a_nestos_runtime() {
    let (_root, state, count, binding) = fixture().await;
    call(&state, 0, request(&binding, "one", "first"))
        .await
        .unwrap();
    call(&state, 0, request(&binding, "two", "second"))
        .await
        .unwrap();
    wait_count(&count, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(count.sent.load(Ordering::SeqCst), 1);
    count.finished.store(1, Ordering::SeqCst);
    wait_count(&count, 2).await;
    count.finished.store(2, Ordering::SeqCst);
    let channels = &state
        .services
        .require::<super::super::ProjectService>()
        .unwrap()
        .channels;
    for _ in 0..200 {
        if harness_recovery::store::load(channels, &binding).unwrap().1["instructionQueue"]["items"]
            == json!([])
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        harness_recovery::store::load(channels, &binding).unwrap().1["instructionQueue"]["items"],
        json!([])
    );
    shutdown(&state).await;
}
#[tokio::test]
async fn ownership_migration_separates_native_queue_from_client_state_and_rejects_old_writers() {
    let (_root, state, _count, binding) = fixture().await;
    let channels = &state
        .services
        .require::<super::super::ProjectService>()
        .unwrap()
        .channels;
    let key = format!("{}/recovery", binding.namespace);
    store::take_ownership(channels, &binding).unwrap();
    let raw = channels
        .client_state(
            &binding.project_id,
            &binding.owner,
            &json!({"action":"read","key":key}),
        )
        .unwrap()["value"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut head: Value = serde_json::from_str(&raw).unwrap();
    let mut client: Value = serde_json::from_str(head["text"].as_str().unwrap()).unwrap();
    assert!(client.get("instructionQueue").is_none());
    client["instructionQueue"] = empty();
    head["text"] = json!(client.to_string());
    let error = channels
        .client_state(
            &binding.project_id,
            &binding.owner,
            &json!({"action":"compare","key":key,"expected":raw,"value":head.to_string()}),
        )
        .unwrap_err();
    assert_eq!(error.code, "HARNESS_QUEUE_OWNED_BY_CELLS");
    shutdown(&state).await;
}
#[tokio::test]
async fn authenticated_fixed_api_reads_the_node_owned_queue_without_an_application_view() {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let (_root, state, _count, binding) = fixture().await;
    store::take_ownership(
        &state
            .services
            .require::<super::super::ProjectService>()
            .unwrap()
            .channels,
        &binding,
    )
    .unwrap();
    let mut args = request(&binding, "read", "unused");
    args["action"] = json!("read");
    let response = crate::router(state.clone(), &[])
        .unwrap()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/api/projects/harness_queue")
                .header(
                    "authorization",
                    "Bearer queue-fixture-secret-at-least-24-bytes",
                )
                .header("content-type", "application/json")
                .body(axum::body::Body::from(args.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["value"]["queue"]["owner"], "cells");
    shutdown(&state).await;
}
#[tokio::test]
async fn different_client_namespaces_share_one_native_session_queue() {
    let (_root, state, _count, binding) = fixture().await;
    let channels = &state
        .services
        .require::<super::super::ProjectService>()
        .unwrap()
        .channels;
    store::take_ownership(channels, &binding).unwrap();
    let (raw, mut record) = harness_recovery::store::load(channels, &binding).unwrap();
    commands::apply(&mut record["instructionQueue"], &enqueue("one", "retained")).unwrap();
    record["instructionQueue"]["paused"] = json!(true);
    store::save(channels, &binding, &raw, &record).unwrap();
    let mut alias = binding.clone();
    alias.namespace = "b".repeat(64);
    let body=json!({"schema":1,"text":json!({"sessionId":"native","instructionQueue":empty()}).to_string()}).to_string();
    channels.client_state(&alias.project_id,&alias.owner,&json!({"key":format!("{}/recovery",alias.namespace),"action":"compare","expected":null,"value":body})).unwrap();
    store::take_ownership(channels, &alias).unwrap();
    assert_eq!(
        harness_recovery::store::load(channels, &alias).unwrap().1["instructionQueue"],
        record["instructionQueue"]
    );
    shutdown(&state).await;
}

#[tokio::test]
async fn asynchronous_commands_keep_output_and_wait_for_the_exact_command_turn() {
    let (_root, state, count, binding) = fixture().await;
    let mut command = request(&binding, "compact", "/compact");
    command["item"]["commandKind"] = json!("action");
    command["item"]["commandId"] = json!("compact-def");
    command["item"]["commandRevision"] = json!("1");
    command["input"] = json!(json!(["/compact", [], null, "action"]).to_string());
    call(&state, 0, command).await.unwrap();
    call(&state, 0, request(&binding, "after", "after command"))
        .await
        .unwrap();
    wait_count(&count, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(count.sent.load(Ordering::SeqCst), 1);
    count.finished.store(1, Ordering::SeqCst);
    wait_count(&count, 2).await;
    let channels = &state
        .services
        .require::<super::super::ProjectService>()
        .unwrap()
        .channels;
    let record = harness_recovery::store::load(channels, &binding).unwrap().1;
    assert_eq!(
        record["instructionQueue"]["commandResults"][0]["output"],
        "fixture command output"
    );
    assert_eq!(
        record["instructionQueue"]["commandResults"][0]["status"],
        "completed"
    );
    shutdown(&state).await;
}
#[tokio::test]
async fn active_turn_supplements_are_admitted_once_without_starting_another_turn() {
    let (_root, state, count, binding) = fixture().await;
    call(&state, 0, request(&binding, "first", "first"))
        .await
        .unwrap();
    wait_count(&count, 1).await;
    let mut supplemental = request(&binding, "extra", "extra");
    supplemental["item"]["allowSteer"] = json!(true);
    supplemental["input"] = json!(json!(["extra", [], null, null, true]).to_string());
    call(&state, 0, supplemental).await.unwrap();
    for _ in 0..200 {
        if count.steered.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(count.steered.load(Ordering::SeqCst), 1);
    assert_eq!(count.sent.load(Ordering::SeqCst), 1);
    shutdown(&state).await;
}
