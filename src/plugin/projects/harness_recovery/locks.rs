//! Client namespaces share a queue; the canonical native session owns serialization.
use super::*;
use tokio::sync::OwnedMutexGuard;
use tokio::time::{Duration, Instant};

pub(super) fn key(binding: &Binding) -> String {
    json!([
        binding.owner,
        binding.project_id,
        binding.profile_id,
        binding.session_id
    ])
    .to_string()
}
pub(in crate::plugin::projects) async fn session_lock(
    state: &Arc<State>,
    binding: &Binding,
    stage: &str,
) -> Result<OwnedMutexGuard<()>, Error> {
    acquire(state, binding, key(binding), stage).await
}
pub(super) async fn recovery_lock(
    state: &Arc<State>,
    binding: &Binding,
) -> Result<OwnedMutexGuard<()>, Error> {
    acquire(
        state,
        binding,
        format!("recovery:{}", key(binding)),
        "recovery.operation",
    )
    .await
}
async fn acquire(
    state: &Arc<State>,
    binding: &Binding,
    key: String,
    stage: &str,
) -> Result<OwnedMutexGuard<()>, Error> {
    let channels = &state
        .services
        .require::<super::super::ProjectService>()?
        .channels;
    let started = Instant::now();
    let result = channels
        .3
        .acquire(&key, started + Duration::from_secs(5), &state.event_stop)
        .await;
    if result.is_err() || started.elapsed() >= Duration::from_millis(20) {
        crate::core::events::emit(
            crate::core::events::Level::Warn,
            "harness.session.lock",
            json!({
            "operation":"harness.session.lock", "stage":stage, "projectId":binding.project_id,
            "profileId":binding.profile_id, "sessionId":binding.session_id, "result":if result.is_ok(){"acquired"}else{"failure"},
            "code":result.as_ref().err().map(|e|e.code), "waitMs":started.elapsed().as_millis()}),
        );
    }
    result
}
