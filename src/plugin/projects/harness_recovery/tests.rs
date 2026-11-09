use super::*;
fn binding() -> Binding {
    Binding {
        project_id: "p".into(),
        owner: "alice".into(),
        namespace: "a".repeat(64),
        profile_id: "codex".into(),
        session_id: "native".into(),
        revision: 1,
    }
}
pub(super) fn record() -> Value {
    json!({"sessionId":"native","draft":"PRIVATE_BODY","instructionQueue":{"revision":7,"paused":false,"items":[{"id":"old","state":"dispatching","text":"Original","attachments":[],"createdAt":1,"receipt":{"turnId":"original"}}],"receipts":[{"id":"old","input":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","turnId":"original"}]}})
}
fn args() -> Value {
    json!({"instructionId":"old","expectedRevision":7,"operationId":"recovery"})
}
fn detached() -> Value {
    let now = chrono::Utc::now().timestamp_millis();
    json!({"id":"native","status":"notLoaded","owned":false,"loaded":false,"writerLock":{"status":"not-held","checkedAt":now},"executionRecovery":{"state":"detached","checkedAt":now,"interruptedTurns":[{"turnId":"original","requestId":"old","reason":"CELLS_RESTARTED"}]}})
}
pub(super) fn write(channels: &Channels, binding: &Binding, record: &Value) -> String {
    let body = json!({"schema":1,"text":record.to_string()}).to_string();
    assert_eq!(channels.client_state(&binding.project_id,&binding.owner,&json!({"key":format!("{}/recovery",binding.namespace),"action":"compare","expected":null,"value":body})).unwrap()["applied"],true);
    body
}
#[test]
fn recovery_preserves_unknown_result_and_never_creates_a_terminal_or_replays_input() {
    let (after, outcome) =
        decision::recover(record(), &detached(), &json!({}), &args(), false).unwrap();
    assert_eq!(outcome, "recovered");
    assert_eq!(after["draft"], "PRIVATE_BODY");
    assert_eq!(after["instructionQueue"]["revision"], 8);
    assert_eq!(after["instructionQueue"]["items"], json!([]));
    assert_eq!(after["instructionQueue"]["paused"], true);
    assert_eq!(
        after["instructionQueue"]["recoveries"][0]["outcome"],
        "unknown"
    );
    assert!(after["instructionQueue"]["recoveries"][0]["terminal"].is_null());
}
#[test]
fn stale_revisions_locks_running_and_unknown_admission_are_not_recoverable() {
    let mut stale = args();
    stale["expectedRevision"] = json!(6);
    assert!(decision::recover(record(), &detached(), &json!({}), &stale, false).is_err());
    for patch in [
        json!({"activeTurnId":"other"}),
        json!({"owned":true}),
        json!({"loaded":true}),
        json!({"writerLock":{"status":"unknown"}}),
        json!({"executionRecovery":{"state":"unknown"}}),
    ] {
        let mut session = detached();
        for (k, v) in patch.as_object().unwrap() {
            session[k] = v.clone();
        }
        assert_eq!(
            decision::recover(record(), &session, &json!({}), &args(), false)
                .unwrap_err()
                .code,
            "HARNESS_RECOVERY_NOT_DETACHED"
        );
    }
    let mut unknown = record();
    unknown["pending"] = json!({"requestId":"unconfirmed"});
    assert!(decision::recover(unknown, &detached(), &json!({}), &args(), false).is_err());
}
#[test]
fn automatic_recovery_requires_exact_restart_proof_and_preserves_pause() {
    let (after, _) = decision::recover(record(), &detached(), &json!({}), &args(), true).unwrap();
    assert_eq!(after["instructionQueue"]["paused"], false);
    assert_eq!(
        after["instructionQueue"]["recoveries"][0]["source"],
        "system-restart"
    );
    let mut session = detached();
    session["executionRecovery"]["interruptedTurns"][0]["requestId"] = json!("another");
    assert!(decision::recover(record(), &session, &json!({}), &args(), true).is_err());
}
#[test]
fn exact_terminal_is_settled_but_another_turn_does_not_complete_the_head() {
    let history = json!({"turns":[{"id":"original","status":"completed"}]});
    let (after, outcome) =
        decision::recover(record(), &detached(), &history, &args(), false).unwrap();
    assert_eq!(outcome, "no-longer-blocked");
    assert!(after["instructionQueue"]["recoveries"].is_null());
    let history = json!({"turns":[{"id":"different","status":"completed"}]});
    assert_eq!(
        decision::recover(record(), &detached(), &history, &args(), false)
            .unwrap()
            .1,
        "recovered"
    );
}
#[test]
fn recovery_and_receipt_commit_atomically_and_survive_cells_store_reopen() {
    let root = tempfile::tempdir().unwrap();
    let channels = Channels::open(root.path()).unwrap();
    let binding = binding();
    let raw = write(&channels, &binding, &record());
    let (after, outcome) =
        decision::recover(record(), &detached(), &json!({}), &args(), false).unwrap();
    store::commit(&channels, &binding, &args(), &raw, &after, outcome).unwrap();
    drop(channels);
    let channels = Channels::open(root.path()).unwrap();
    assert_eq!(
        store::prior(&channels, &binding, &args()).unwrap().unwrap()["outcome"],
        "recovered"
    );
    assert_eq!(
        store::load(&channels, &binding).unwrap().1["instructionQueue"]["revision"],
        8
    );
    let mut changed = args();
    changed["expectedRevision"] = json!(8);
    assert_eq!(
        store::prior(&channels, &binding, &changed)
            .unwrap_err()
            .code,
        "HARNESS_RECOVERY_OPERATION_CHANGED"
    );
    let mut other = binding.clone();
    other.owner = "bob".into();
    assert!(store::prior(&channels, &other, &args()).unwrap().is_none());
}
#[test]
fn stale_cas_rolls_back_both_queue_and_receipt() {
    let root = tempfile::tempdir().unwrap();
    let channels = Channels::open(root.path()).unwrap();
    let binding = binding();
    write(&channels, &binding, &record());
    assert!(store::commit(
        &channels,
        &binding,
        &args(),
        "stale",
        &record(),
        "recovered"
    )
    .is_err());
    assert!(store::prior(&channels, &binding, &args())
        .unwrap()
        .is_none());
    assert_eq!(store::load(&channels, &binding).unwrap().1, record());
}
#[test]
fn chunked_unicode_state_is_recovered_without_losing_the_draft() {
    let root = tempfile::tempdir().unwrap();
    let channels = Channels::open(root.path()).unwrap();
    let binding = binding();
    let raw = write(&channels, &binding, &record());
    let mut large = record();
    large["draft"] = json!("界😀".repeat(90000));
    store::commit(&channels, &binding, &args(), &raw, &large, "recovered").unwrap();
    assert_eq!(store::load(&channels, &binding).unwrap().1, large);
}
