//! Serialize only the sessions named by the native adapter; unrelated trees overlap.
use crate::core::{error::Error, keyed_lock::KeyedLocks};
use serde_json::Value;
use tokio::sync::OwnedMutexGuard;

#[derive(Default)]
pub(super) struct MutationQueues {
    sessions: KeyedLocks,
}
pub(crate) struct MutationGuard {
    _sessions: Vec<OwnedMutexGuard<()>>,
}
fn key(args: &Value) -> String {
    args["sessionId"]
        .as_str()
        .map(|s| format!("session:{s}"))
        .unwrap_or_else(|| format!("request:{}", args["requestId"]))
}
impl MutationQueues {
    pub(crate) fn try_recovery(&self, args: &Value) -> Result<MutationGuard, Error> {
        Ok(MutationGuard {
            _sessions: vec![self.sessions.try_acquire(&key(args))?],
        })
    }
    pub async fn acquire(
        &self,
        name: &str,
        args: &Value,
        resources: &[String],
    ) -> Result<MutationGuard, Error> {
        let mut keys = resources
            .iter()
            .map(|id| format!("session:{id}"))
            .collect::<Vec<_>>();
        keys.push(key(args));
        keys.sort();
        keys.dedup();
        let mut sessions = Vec::new();
        for key in keys {
            let guard = if matches!(name, "harness_delete" | "harness_archive") {
                self.sessions.try_acquire(&key)?
            } else {
                self.sessions
                    .acquire(
                        &key,
                        tokio::time::Instant::now() + std::time::Duration::from_secs(25),
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await?
            };
            sessions.push(guard);
        }
        Ok(MutationGuard {
            _sessions: sessions,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use serde_json::json;
    #[tokio::test]
    async fn management_locks_only_the_declared_subtree_and_does_not_queue_a_global_writer() {
        let queues = MutationQueues::default();
        let a = json!({"sessionId":"a"});
        let b = json!({"sessionId":"b"});
        let pending = queues.acquire("harness_turn", &a, &[]).await.unwrap();
        assert!(queues
            .acquire("harness_turn", &b, &[])
            .now_or_never()
            .unwrap()
            .is_ok());
        assert!(queues
            .acquire("harness_turn", &a, &[])
            .now_or_never()
            .is_none());
        assert!(queues.acquire("harness_delete", &b, &[]).await.is_ok());
        assert_eq!(
            queues
                .acquire("harness_delete", &b, &["a".into()])
                .await
                .err()
                .unwrap()
                .code,
            "EBUSY"
        );
        drop(pending);
        let deleting = queues
            .acquire("harness_delete", &a, &["child".into()])
            .await
            .unwrap();
        assert!(queues
            .acquire("harness_turn", &b, &[])
            .now_or_never()
            .unwrap()
            .is_ok());
        assert!(queues
            .acquire("harness_turn", &json!({"sessionId":"child"}), &[])
            .now_or_never()
            .is_none());
        drop(deleting);
        assert!(queues.try_recovery(&a).is_ok());
    }
}
