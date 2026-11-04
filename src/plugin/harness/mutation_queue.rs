//! Serialize one session while allowing independent native submissions to overlap.
use crate::core::error::Error;
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

#[derive(Default)]
pub(super) struct MutationQueues {
    sessions: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    topology: Arc<RwLock<()>>,
}
enum TopologyGuard {
    Read(#[allow(dead_code)] OwnedRwLockReadGuard<()>),
    Write(#[allow(dead_code)] OwnedRwLockWriteGuard<()>),
}
pub(super) struct MutationGuard {
    _topology: TopologyGuard,
    _session: OwnedMutexGuard<()>,
}
impl MutationQueues {
    fn session(&self, args: &Value) -> Arc<AsyncMutex<()>> {
        let key = args["sessionId"]
            .as_str()
            .map(|s| format!("session:{s}"))
            .unwrap_or_else(|| format!("request:{}", args["requestId"]));
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, queue| queue.strong_count() > 0);
        if let Some(queue) = sessions.get(&key).and_then(Weak::upgrade) {
            return queue;
        }
        let queue = Arc::new(AsyncMutex::new(()));
        sessions.insert(key, Arc::downgrade(&queue));
        queue
    }
    pub async fn acquire(&self, name: &str, args: &Value) -> Result<MutationGuard, Error> {
        let session = self.session(args).lock_owned().await;
        // Destructive native subtree changes exclude submissions to every descendant.
        // Never queue a global writer behind a native call waiting for user approval.
        let topology = if matches!(name, "harness_delete" | "harness_archive") {
            TopologyGuard::Write(
                self.topology
                    .clone()
                    .try_write_owned()
                    .map_err(|_| Error::busy())?,
            )
        } else {
            TopologyGuard::Read(
                self.topology
                    .clone()
                    .try_read_owned()
                    .map_err(|_| Error::busy())?,
            )
        };
        Ok(MutationGuard {
            _topology: topology,
            _session: session,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use serde_json::json;
    async fn assert_busy(queues: &MutationQueues, name: &str, args: &Value) {
        assert_eq!(
            queues.acquire(name, args).await.err().unwrap().code,
            "EBUSY"
        );
    }
    #[tokio::test]
    async fn independent_sessions_bypass_a_pending_start_but_deletion_excludes_submissions() {
        let queues = MutationQueues::default();
        let a = json!({"sessionId":"a"});
        let b = json!({"sessionId":"b"});
        let pending = queues.acquire("harness_turn", &a).await.unwrap();
        assert!(queues
            .acquire("harness_turn", &b)
            .now_or_never()
            .unwrap()
            .is_ok());
        assert!(queues.acquire("harness_turn", &a).now_or_never().is_none());
        assert_busy(&queues, "harness_delete", &b).await;
        assert_busy(&queues, "harness_archive", &b).await;
        assert!(queues
            .acquire("harness_turn", &b)
            .now_or_never()
            .unwrap()
            .is_ok());
        drop(pending);
        let deleting = queues.acquire("harness_delete", &a).await.unwrap();
        assert_busy(&queues, "harness_turn", &b).await;
        drop(deleting);
        let archiving = queues.acquire("harness_archive", &a).await.unwrap();
        assert_busy(&queues, "harness_turn", &b).await;
        drop(archiving);
        assert!(queues
            .acquire("harness_turn", &b)
            .now_or_never()
            .unwrap()
            .is_ok());
    }
}
