//! Separate observational reads from mutation admission.
//!
//! Native turns exclude file mutations, but allow directory and content reads.
//! Supervised commands retain full exclusion. Unknown process ownership poisons
//! both gates; shutdown drains both classes of in-flight file work.

use crate::core::error::Error;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Cloneable handle to the process-wide file/process gate.
#[derive(Clone, Default)]
pub struct FileGate {
    inner: Arc<RwLock<()>>,
    readers: Arc<RwLock<()>>,
    poisoned: Arc<AtomicBool>,
    activity: Arc<Mutex<Activity>>,
}

#[derive(Default, Debug)]
struct Activity {
    count: usize,
    guard: Option<OwnedRwLockWriteGuard<()>>,
}

/// Full exclusion for supervised commands and project topology changes.
#[derive(Debug)]
pub struct ExclusivePermit {
    _mutations: OwnedRwLockWriteGuard<()>,
    _readers: OwnedRwLockWriteGuard<()>,
}

/// Native executions share mutation exclusion while observational reads continue.
#[derive(Debug)]
pub struct ActivityPermit(Arc<Mutex<Activity>>);
impl Drop for ActivityPermit {
    fn drop(&mut self) {
        let mut activity = self.0.lock().unwrap();
        activity.count -= 1;
        if activity.count == 0 {
            activity.guard = None;
        }
    }
}

impl FileGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observational reads may overlap native turns; they are not snapshots.
    pub fn read_only(&self) -> Result<OwnedRwLockReadGuard<()>, Error> {
        self.check()?;
        self.readers
            .clone()
            .try_read_owned()
            .map_err(|_| self.busy("read"))
    }

    /// File mutations must remain excluded from native execution.
    pub fn shared(&self) -> Result<OwnedRwLockReadGuard<()>, Error> {
        self.check()?;
        self.inner
            .clone()
            .try_read_owned()
            .map_err(|_| self.busy("mutation"))
    }

    /// Commands and topology changes exclude reads and file mutations.
    pub fn exclusive(&self) -> Result<ExclusivePermit, Error> {
        self.check()?;
        let mutations = self
            .inner
            .clone()
            .try_write_owned()
            .map_err(|_| self.busy("exclusive"))?;
        let readers = self
            .readers
            .clone()
            .try_write_owned()
            .map_err(|_| self.busy("exclusive-readers"))?;
        Ok(ExclusivePermit {
            _mutations: mutations,
            _readers: readers,
        })
    }

    /// Admit a native turn while excluding file mutations and supervised commands.
    pub fn activity(&self) -> Result<ActivityPermit, Error> {
        let mut activity = self.activity.lock().map_err(|_| Error::internal())?;
        self.check()?;
        if activity.count == 0 {
            activity.guard = Some(
                self.inner
                    .clone()
                    .try_write_owned()
                    .map_err(|_| self.busy("native-admission"))?,
            );
        }
        activity.count += 1;
        Ok(ActivityPermit(self.activity.clone()))
    }

    /// Permanently close the gate after an unreaped command.
    pub fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Wait for every in-flight file operation; used only during shutdown.
    pub async fn drain(&self) -> ExclusivePermit {
        let mutations = self.inner.clone().write_owned().await;
        let readers = self.readers.clone().write_owned().await;
        ExclusivePermit {
            _mutations: mutations,
            _readers: readers,
        }
    }

    fn busy(&self, stage: &'static str) -> Error {
        crate::core::events::emit(
            crate::core::events::Level::Warn,
            "file.gate.conflict",
            serde_json::json!({"operation":"file.gate.acquire",
                "operationId":format!("gate-{}-{}", std::process::id(), chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()),
                "stage":stage,"result":"failure",
                "code":"EBUSY","durationMs":0}),
        );
        Error::busy()
    }

    fn check(&self) -> Result<(), Error> {
        if self.is_poisoned() {
            return Err(Error::unavailable());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn execution_permits_exclude_mutations_until_the_last_owner_leaves() {
        let gate = FileGate::new();
        let read = gate.shared().unwrap();
        assert_eq!(gate.activity().unwrap_err().code, "EBUSY");
        drop(read);
        let a = gate.activity().unwrap();
        let b = gate.activity().unwrap();
        assert!(gate.shared().is_err());
        assert!(gate.read_only().is_ok());
        assert!(gate.exclusive().is_err());
        drop(a);
        assert!(gate.shared().is_err());
        assert!(gate.read_only().is_ok());
        drop(b);
        assert!(gate.shared().is_ok());
        let write = gate.exclusive().unwrap();
        assert_eq!(gate.activity().unwrap_err().code, "EBUSY");
        drop(write);
        assert!(gate.activity().is_ok());
    }
    #[test]
    fn observations_overlap_native_admission_but_exclusive_commands_block_both() {
        let gate = FileGate::new();
        let read = gate.read_only().unwrap();
        let native = gate.activity().unwrap();
        assert!(gate.read_only().is_ok());
        assert!(gate.shared().is_err());
        drop(native);
        assert!(gate.exclusive().is_err());
        assert!(gate.shared().is_ok());
        drop(read);
        let command = gate.exclusive().unwrap();
        assert!(gate.read_only().is_err());
        assert!(gate.activity().is_err());
        assert!(gate.shared().is_err());
        drop(command);
        assert!(gate.read_only().is_ok());
        assert!(gate.activity().is_ok());
    }

    #[test]
    fn poisoning_rejects_new_owners_even_when_a_group_is_already_running() {
        let gate = FileGate::new();
        let permit = gate.activity().unwrap();
        gate.poison();
        assert_eq!(gate.read_only().unwrap_err().code, "EIO");
        assert_eq!(gate.activity().unwrap_err().code, "EIO");
        drop(permit);
        assert_eq!(gate.shared().unwrap_err().code, "EIO");
    }
}
