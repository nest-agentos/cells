//! Track in-flight work for topology changes and shutdown, not file/execution exclusion.
//! File versions, atomic replacement and SeqFile transactions own data consistency.
use crate::core::error::Error;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

#[derive(Clone, Default)]
pub struct FileGate {
    pub(super) owners: super::gate_owner::Owners,
    inner: Arc<RwLock<()>>,
    poisoned: Arc<AtomicBool>,
    scope: Option<String>,
    file_constraints: Option<Arc<super::file_constraints::Constraints>>,
}

/// Only topology changes and shutdown require exclusive lifecycle ownership.
#[derive(Debug)]
pub struct ExclusivePermit {
    _constraint: Option<super::file_constraints::Native>,
    _owner: Option<super::gate_owner::OwnerGuard>,
    _guard: OwnedRwLockWriteGuard<()>,
}

/// Commands share lifecycle ownership with file work and native turns.
#[derive(Debug)]
pub struct ExecutionPermit {
    constraint: Option<super::file_constraints::Native>,
    owner: super::gate_owner::OwnerGuard,
    _guard: OwnedRwLockReadGuard<()>,
    resource: Option<Box<ExecutionPermit>>,
    poisoned: Arc<AtomicBool>,
}
impl ExecutionPermit {
    pub fn with_resource(mut self, resource: ExecutionPermit) -> Self {
        let resource = if let Some(previous) = self.resource.take() {
            resource.with_resource(*previous)
        } else {
            resource
        };
        self.resource = Some(Box::new(resource));
        self
    }
    pub fn poison(&self) {
        if let Some(constraint) = &self.constraint {
            constraint.poison();
        }
        if let Some(resource) = &self.resource {
            resource.poison();
        } else {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    pub fn process_started(&self, pid: Option<u32>) {
        self.owner.process_started(pid);
        if let Some(resource) = &self.resource {
            resource.process_started(pid);
        }
    }
}
#[derive(Debug)]
pub struct ActivityPermit {
    constraint: Option<super::file_constraints::Native>,
    _owner: super::gate_owner::OwnerGuard,
    _guard: OwnedRwLockReadGuard<()>,
    resource: Option<Box<ActivityPermit>>,
}

impl ActivityPermit {
    pub(crate) fn poison_constraints(&self) {
        if let Some(constraint) = &self.constraint {
            constraint.poison();
        }
    }
    pub fn with_resource(mut self, resource: ActivityPermit) -> Self {
        let resource = if let Some(previous) = self.resource.take() {
            resource.with_resource(*previous)
        } else {
            resource
        };
        self.resource = Some(Box::new(resource));
        self
    }
}

impl FileGate {
    pub(crate) fn named(scope: &str) -> Self {
        Self {
            scope: Some(scope.into()),
            ..Self::default()
        }
    }
    pub(crate) fn idle(&self) -> bool {
        Arc::strong_count(&self.inner) == 1 && !self.is_poisoned()
    }
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn enable_file_constraints(&mut self) -> Arc<super::file_constraints::Constraints> {
        let constraints = Arc::new(super::file_constraints::Constraints::new(self.owners.clone()));
        self.file_constraints = Some(constraints.clone());
        constraints
    }
    fn native_constraints(&self) -> Result<Option<super::file_constraints::Native>, Error> {
        self.file_constraints.as_ref().map(super::file_constraints::Native::acquire).transpose()
    }
    pub(crate) fn poison_constraints(&self) {
        if let Some(constraints) = &self.file_constraints {
            constraints.poison();
        }
    }
    pub fn read_only(&self) -> Result<OwnedRwLockReadGuard<()>, Error> {
        self.shared()
    }
    /// Shared admission drains before topology changes without excluding other work.
    pub fn shared(&self) -> Result<OwnedRwLockReadGuard<()>, Error> {
        self.check()?;
        self.inner
            .clone()
            .try_read_owned()
            .map_err(|_| self.busy("lifecycle-admission"))
    }
    pub fn exclusive(&self) -> Result<ExclusivePermit, Error> {
        self.exclusive_using(self)
    }
    pub(crate) fn exclusive_using(&self, node: &Self) -> Result<ExclusivePermit, Error> {
        self.check()?;
        let guard = self
            .inner
            .clone()
            .try_write_owned()
            .map_err(|_| self.busy("topology"))?;
        Ok(ExclusivePermit {
            _constraint: node.native_constraints()?,
            _owner: Some(self.owners.enter("topology", None)),
            _guard: guard,
        })
    }
    pub fn command_now(&self, operation: &str) -> Result<ExecutionPermit, Error> {
        let guard = self.shared()?;
        Ok(ExecutionPermit {
            constraint: self.native_constraints()?,
            owner: self.owners.enter("command", Some(operation)),
            _guard: guard,
            resource: None,
            poisoned: self.poisoned.clone(),
        })
    }
    pub async fn command(
        &self,
        operation: &str,
        _budget_ms: u64,
    ) -> Result<ExecutionPermit, Error> {
        let result = self.command_now(operation);
        crate::core::events::emit(
            crate::core::events::Level::Info,
            "file.gate.command",
            serde_json::json!({
            "operation":"file.lifecycle.admit","operationId":operation,"stage":"command","gateMode":"shared",
            "result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|error|error.code),"durationMs":0}),
        );
        result
    }
    pub fn activity(&self) -> Result<ActivityPermit, Error> {
        self.activity_for(None, None)
    }
    pub fn activity_for(
        &self,
        session: Option<&str>,
        operation: Option<&str>,
    ) -> Result<ActivityPermit, Error> {
        let guard = self.shared()?;
        let owner = self.owners.enter("native", operation);
        owner.session_started(session);
        Ok(ActivityPermit {
            constraint: self.native_constraints()?,
            _owner: owner,
            _guard: guard,
            resource: None,
        })
    }
    /// Unknown process ownership still prevents unsafe new admissions.
    pub fn poison(&self) {
        self.poison_constraints();
        self.poisoned.store(true, Ordering::Release);
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }
    pub async fn drain(&self) -> ExclusivePermit {
        ExclusivePermit {
            _constraint: None,
            _owner: None,
            _guard: self.inner.clone().write_owned().await,
        }
    }
    fn busy(&self, stage: &'static str) -> Error {
        let mut fields = self.owners.fields();
        fields.as_object_mut().unwrap().extend(serde_json::json!({"operation":"file.lifecycle.admit",
            "stage":stage,"resourceId":self.scope,"result":"failure","code":"EBUSY","outcome":"not-committed","durationMs":0}).as_object().unwrap().clone());
        crate::core::events::emit(
            crate::core::events::Level::Warn,
            "file.gate.conflict",
            fields,
        );
        Error {
            outcome: Some("not-committed"),
            ..Error::busy()
        }
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
    async fn files_commands_and_native_turns_share_lifecycle_without_global_contention() {
        let gate = FileGate::new();
        let read = gate.read_only().unwrap();
        let write = gate.shared().unwrap();
        let a = gate
            .activity_for(Some("session-a"), Some("turn-a"))
            .unwrap();
        let b = gate
            .activity_for(Some("session-b"), Some("turn-b"))
            .unwrap();
        let command = gate.command("git-worktree", 1).await.unwrap();
        assert!(gate.read_only().is_ok());
        assert!(gate.shared().is_ok());
        assert_eq!(gate.exclusive().unwrap_err().outcome, Some("not-committed"));
        drop((read, write, a, b, command));
        assert!(gate.exclusive().is_ok());
    }
    #[tokio::test]
    async fn topology_and_shutdown_wait_for_actual_owners_and_release_on_cancellation() {
        let gate = FileGate::new();
        let command = gate.command_now("owned").unwrap();
        let mut draining = Box::pin(gate.drain());
        tokio::select! {biased;_= &mut draining=>panic!("command still owned"),_=tokio::task::yield_now()=>{}}
        drop(draining);
        assert!(gate.shared().is_ok());
        drop(command);
        let topology = gate.exclusive().unwrap();
        assert!(gate.shared().is_err());
        assert!(gate.activity().is_err());
        drop(topology);
        assert!(gate.shared().is_ok());
    }
    #[test]
    fn poisoning_does_not_release_accepted_work_or_admit_unknown_owners() {
        let gate = FileGate::new();
        let native = gate.activity().unwrap();
        gate.poison();
        assert_eq!(gate.shared().unwrap_err().code, "EIO");
        assert_eq!(gate.command_now("new").unwrap_err().code, "EIO");
        drop(native);
        assert!(gate.is_poisoned());
    }
    #[test]
    fn concurrent_owner_diagnostics_preserve_the_remaining_operation_on_release() {
        let gate = FileGate::new();
        let a = gate.command_now("command-a").unwrap();
        let b = gate
            .activity_for(Some("session-b"), Some("turn-b"))
            .unwrap();
        assert_eq!(gate.owners.fields()["blockerCount"], 2);
        drop(a);
        assert_eq!(gate.owners.fields()["blockerOperationId"], "turn-b");
        assert_eq!(gate.owners.fields()["blockerSessionId"], "session-b");
        drop(b);
        assert!(gate.owners.fields().get("blockerOperationId").is_none());
    }
}
