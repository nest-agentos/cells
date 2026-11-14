//! Reconcile registered sessions without a NestOS process or browser view.
use super::*;
pub(crate) fn maintain(state: std::sync::Weak<State>) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            tokio::select! {_ = timer.tick()=>{}, _ = async {if let Some(state)=state.upgrade(){state.event_stop.cancelled().await}} => break,}
            let Some(state) = state.upgrade() else {
                break;
            };
            if state
                .services
                .get::<crate::harness::Harnesses>()
                .is_none_or(|h| h.is_closed())
            {
                break;
            }
            if let Err(error) = scan(&state).await {
                crate::core::events::emit(
                    crate::core::events::Level::Warn,
                    "harness.recovery.scan",
                    json!({"operation":"harness.recovery","stage":"startup_scan","result":"failure","code":error.code}),
                );
            }
        }
    });
}
async fn scan(state: &Arc<State>) -> Result<(), Error> {
    let channels = &state
        .services
        .require::<super::super::ProjectService>()?
        .channels;
    use futures_util::{stream, StreamExt};
    stream::iter(store::bindings(channels)?)
        .for_each_concurrent(8, |binding| reconcile_binding(state, binding))
        .await;
    Ok(())
}
async fn reconcile_binding(state: &Arc<State>, binding: Binding) {
    let Some(identity) = state
        .auth
        .clients()
        .iter()
        .position(|c| c.id() == binding.owner)
    else {
        return;
    };
    let args = json!({"projectId":binding.project_id,"revision":binding.revision,"namespace":binding.namespace,"profileId":binding.profile_id,"sessionId":binding.session_id});
    if authorize(state, identity, &args).is_err() {
        return;
    }
    super::super::harness_queue::worker_start(state, &binding);
    let Some(harness) = state.services.get::<crate::harness::Harnesses>() else {
        return;
    };
    let Ok(_native_guard) = harness.recovery_guard(state, identity, args).await else {
        return;
    };
    let result = reconcile(state, identity, &binding).await;
    drop(_native_guard);
    if let Err(error) = result {
        if ![
            "HARNESS_RECOVERY_RESTART_PROOF_REQUIRED",
            "HARNESS_RECOVERY_NOT_DETACHED",
            "HARNESS_RECOVERY_HEAD_UNSAFE",
            "HARNESS_RECOVERY_PENDING_OPERATIONS",
        ]
        .contains(&error.code)
        {
            crate::core::events::emit(
                crate::core::events::Level::Warn,
                "harness.recovery.deferred",
                json!({"operation":"harness.recovery","stage":"startup_reconcile","sessionId":binding.session_id,"result":"deferred","code":error.code}),
            );
        }
    }
}
pub(in crate::plugin::projects) async fn reconcile(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
) -> Result<(), Error> {
    let channels = &state
        .services
        .require::<super::super::ProjectService>()?
        .channels;
    let (_, record) = store::load(channels, binding)?;
    let item = &record["instructionQueue"]["items"][0];
    if item["state"] != "dispatching" || !item["receipt"]["turnId"].is_string() {
        return Ok(());
    }
    let session = inspect(state, identity, binding).await?;
    if !decision::restart_matches(&session, item, item["receipt"]["turnId"].as_str().unwrap()) {
        return Ok(());
    }
    let history = history(state, identity, binding).await?;
    let session = inspect(state, identity, binding).await?;
    let operation = format!(
        "restart-{:x}",
        sha2::Sha256::digest(
            json!([
                binding,
                item["id"],
                item["attemptId"],
                record["instructionQueue"]["revision"]
            ])
            .to_string()
            .as_bytes()
        )
    );
    let args = json!({"operationId":operation,"instructionId":item["id"],"expectedRevision":record["instructionQueue"]["revision"]});
    let _guard = locks::session_lock(state, binding, "recovery.startup.commit").await?;
    let (raw, record) = store::load(channels, binding)?;
    let (record, outcome) = decision::recover(record, &session, &history, &args, true)?;
    store::commit(channels, binding, &args, &raw, &record, outcome)?;
    crate::core::events::emit(
        crate::core::events::Level::Info,
        "harness.recovery.restart",
        json!({"operation":"harness.recovery","operationId":operation,"stage":"startup_reconcile","sessionId":binding.session_id,"result":"success","outcome":outcome}),
    );
    Ok(())
}
use sha2::Digest;
