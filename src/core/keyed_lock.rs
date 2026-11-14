//! In-process serialization belongs to a resource key, never to the registry itself.
use super::error::Error;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct KeyedLocks(Mutex<HashMap<String, Weak<AsyncMutex<()>>>>);
impl KeyedLocks {
    fn resource(&self, key: &str) -> Arc<AsyncMutex<()>> {
        let mut entries = self.0.lock().unwrap();
        entries.retain(|_, value| value.strong_count() > 0);
        if let Some(lock) = entries.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        entries.insert(key.into(), Arc::downgrade(&lock));
        lock
    }
    pub(crate) async fn acquire(
        &self,
        key: &str,
        deadline: Instant,
        stop: &CancellationToken,
    ) -> Result<OwnedMutexGuard<()>, Error> {
        let resource = self.resource(key);
        tokio::select! {
            biased;
            _ = stop.cancelled() => Err(Error::cancelled()),
            result = tokio::time::timeout_at(deadline, resource.lock_owned()) => result.map_err(|_| Error::timed_out()),
        }
    }
    pub(crate) fn try_acquire(&self, key: &str) -> Result<OwnedMutexGuard<()>, Error> {
        self.resource(key)
            .try_lock_owned()
            .map_err(|_| Error::busy())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn independent_resources_progress_while_same_resource_is_bounded_and_cancellable() {
        let locks = KeyedLocks::default();
        let held = locks.try_acquire("session-a").unwrap();
        let stop = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_millis(20);
        assert!(locks.acquire("session-b", deadline, &stop).await.is_ok());
        assert_eq!(
            locks
                .acquire("session-a", deadline, &stop)
                .await
                .err()
                .unwrap()
                .code,
            "ETIMEDOUT"
        );
        stop.cancel();
        assert_eq!(
            locks
                .acquire("session-a", Instant::now() + Duration::from_secs(1), &stop)
                .await
                .err()
                .unwrap()
                .code,
            "ECANCELLED"
        );
        drop(held);
        assert!(locks.try_acquire("session-a").is_ok());
        assert!(locks.0.lock().unwrap().len() <= 1);
    }
}

#[derive(Default)]
pub(crate) struct SyncKeyedLocks(Mutex<HashMap<String, Weak<Mutex<()>>>>);
impl SyncKeyedLocks {
    pub(crate) fn with<T>(
        &self,
        key: &str,
        work: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        let resource = {
            let mut entries = self.0.lock().unwrap();
            entries.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = entries.get(key).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                entries.insert(key.into(), Arc::downgrade(&lock));
                lock
            }
        };
        let _guard = resource.lock().map_err(|_| Error::internal())?;
        work()
    }
}
