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
    pub(super) owners: super::gate_owner::Owners,
    pub(super) native_admission: Arc<tokio::sync::Mutex<()>>,
    pub(super) inner: Arc<RwLock<()>>,
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
    // Clear ownership before releasing the locks to avoid clearing a new owner.
    _owner: Option<super::gate_owner::OwnerGuard>,
    _mutations: OwnedRwLockWriteGuard<()>,
    _readers: OwnedRwLockWriteGuard<()>,
}

/// Native executions share mutation exclusion while observational reads continue.
#[derive(Debug)]
pub struct ActivityPermit(Arc<Mutex<Activity>>);
impl ExclusivePermit {
    pub fn process_started(&self, pid: Option<u32>) {
        if let Some(owner) = &self._owner { owner.process_started(pid); }
    }
}
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
        let owner = self.owners.enter("exclusive", None);
        let readers = self
            .readers
            .clone()
            .try_write_owned()
            .map_err(|_| self.busy("exclusive-readers"))?;
        Ok(ExclusivePermit {
            _owner: Some(owner),
            _mutations: mutations,
            _readers: readers,
        })
    }

    /// Queue behind short observational reads without waiting for native execution.
    pub async fn command(&self, request_id: &str, budget_ms: u64) -> Result<ExclusivePermit, Error> {
        self.check()?;
        let mutations = self.inner.clone().try_write_owned().map_err(|_| self.busy("exclusive"))?;
        let owner = self.owners.enter("command", Some(request_id));
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(std::time::Duration::from_millis(budget_ms.min(3000)),
            self.readers.clone().write_owned()).await;
        crate::core::events::emit(crate::core::events::Level::Info, "file.gate.command", serde_json::json!({
            "operation":"file.gate.acquire", "operationId":request_id, "stage":"exclusive-readers",
            "result":if result.is_ok(){"success"}else{"failure"}, "code":if result.is_ok(){None}else{Some("PROCESS_READERS_BUSY")},
            "durationMs":started.elapsed().as_millis()}));
        let readers = result.map_err(|_| Error::conflict("PROCESS_READERS_BUSY"))?;
        self.check()?;
        Ok(ExclusivePermit {_owner: Some(owner), _mutations: mutations, _readers: readers})
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

    pub(super) fn join_activity(&self)->Result<Option<ActivityPermit>,Error> {
        self.check()?;let mut activity=self.activity.lock().map_err(|_|Error::internal())?;
        if activity.count==0{return Ok(None);}
        activity.count+=1;Ok(Some(ActivityPermit(self.activity.clone())))
    }
    pub(super) fn retain_activity(&self,guard:OwnedRwLockWriteGuard<()>)->Result<ActivityPermit,Error> {
        self.check()?;let mut activity=self.activity.lock().map_err(|_|Error::internal())?;
        if activity.count!=0{return Err(Error::internal());}
        activity.guard=Some(guard);activity.count=1;Ok(ActivityPermit(self.activity.clone()))
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
            _owner: None,
            _mutations: mutations,
            _readers: readers,
        }
    }

    fn busy(&self, stage: &'static str) -> Error {
        let mut fields = self.owners.fields();
        fields.as_object_mut().unwrap().extend(serde_json::json!({"operation":"file.gate.acquire",
            "operationId":format!("gate-{}-{}", std::process::id(), chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()),
            "stage":stage,"result":"failure","code":"EBUSY","durationMs":0}).as_object().unwrap().clone());
        crate::core::events::emit(
            crate::core::events::Level::Warn,
            "file.gate.conflict",
            fields,
        );
        Error::busy()
    }

    pub fn check(&self) -> Result<(), Error> {
        if self.is_poisoned() {
            return Err(Error::unavailable());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn conflict_metadata_tracks_command_and_clears_on_release_and_cancel() {
        let gate = FileGate::new();
        let context = serde_json::json!({"requestId":"http-command-1"}).as_object().unwrap().clone();
        let command = crate::core::trace::scope(context, gate.command("command-1", 1000)).await.unwrap();
        let fields = gate.owners.fields();
        assert_eq!(fields["blockerOperationId"], "command-1");
        assert_eq!(fields["blockerRequestId"], "http-command-1");
        assert_eq!(fields["blockerKind"], "command");
        assert!(fields["heldMs"].is_number());
        drop(command);
        assert!(gate.owners.fields().get("blockerOperationId").is_none());
        let _reader = gate.read_only().unwrap();
        let mut pending = Box::pin(gate.command("cancelled", 1000));
        tokio::select! {biased; _ = &mut pending => panic!("reader still held"), _ = tokio::task::yield_now() => {}}
        assert_eq!(gate.owners.fields()["blockerOperationId"], "cancelled");
        drop(pending);
        assert!(gate.owners.fields().get("blockerOperationId").is_none());
    }
    #[tokio::test]
    async fn command_waits_fairly_for_readers_and_keeps_mutation_exclusion() {
        let gate = FileGate::new();
        let reader = gate.read_only().unwrap();
        let mut pending = Box::pin(gate.command("waiting-command", 1000));
        tokio::select! {biased; _ = &mut pending => panic!("reader still held"), _ = tokio::task::yield_now() => {}}
        assert!(gate.read_only().is_err());
        assert!(gate.shared().is_err());
        assert!(gate.activity().is_err());
        drop(reader);
        let command = pending.await.unwrap();
        assert!(gate.read_only().is_err());
        drop(command);
        assert!(gate.read_only().is_ok());
        assert!(gate.activity().is_ok());
    }

    #[tokio::test]
    async fn command_timeout_and_cancel_release_both_gates() {
        let gate = FileGate::new();
        let reader = gate.read_only().unwrap();
        assert_eq!(gate.command("timeout", 10).await.unwrap_err().code, "PROCESS_READERS_BUSY");
        assert!(gate.shared().is_ok());
        assert!(gate.read_only().is_ok());
        let mut pending = Box::pin(gate.command("cancelled-wait", 1000));
        tokio::select! {biased; _ = &mut pending => panic!("reader still held"), _ = tokio::task::yield_now() => {}}
        drop(pending);
        drop(reader);
        assert!(gate.exclusive().is_ok());
    }

    #[tokio::test]
    async fn command_does_not_wait_for_native_turns_or_admit_after_poisoning() {
        let gate = FileGate::new();
        let native = gate.activity().unwrap();
        assert_eq!(gate.command("native-busy", 1000).await.unwrap_err().code, "EBUSY");
        drop(native);
        let reader = gate.read_only().unwrap();
        let mut pending = Box::pin(gate.command("poisoned-wait", 1000));
        tokio::select! {biased; _ = &mut pending => panic!("reader still held"), _ = tokio::task::yield_now() => {}}
        gate.poison();
        drop(reader);
        assert_eq!(pending.await.unwrap_err().code, "EIO");
    }

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
