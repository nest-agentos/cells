//! Wire model for the process API.
//!
//! Requests carry *virtual* paths only (`at`, `cwd`); a host path can never
//! reach this layer. [`Process`] owns the mutable status behind a lock and
//! exposes only the transitions the runner is allowed to perform.

use crate::core::error::Error;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio_util::sync::CancellationToken;

/// Maximum in-memory process records; terminal receipts spill to epoch-local disk.
pub const CAPACITY: usize = 1024;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Mount {
    /// Export alias the directory comes from.
    pub alias: String,
    /// Path inside that export.
    pub path: String,
    /// Virtual path inside the execution environment.
    pub at: String,
    /// `"ro"` or `"rw"`.
    pub access: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    pub server_id: String,
    pub epoch: String,
    pub request_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub project_revision: Option<u64>,
    #[serde(default)]
    pub read_only: bool,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub mounts: Vec<Mount>,
    pub timeout_ms: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub state: &'static str,
    pub stdout: String,
    pub stderr: String,
    pub code: Option<i32>,
    pub error: Option<&'static str>,
    pub truncated: bool,
}

impl Status {
    fn running() -> Self {
        Self {
            state: "running",
            stdout: String::new(),
            stderr: String::new(),
            code: None,
            error: None,
            truncated: false,
        }
    }
}

/// Terminal outcome of one command.
pub struct Outcome {
    pub state: &'static str,
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
}

pub struct Process {
    changed: tokio::sync::watch::Sender<u64>,
    cancel: CancellationToken,
    status: Mutex<Status>,
}

impl Default for Process {
    fn default() -> Self {
        Self::new()
    }
}

impl Process {
    pub fn new() -> Self {
        Self {
            changed: tokio::sync::watch::channel(0).0,
            cancel: CancellationToken::new(),
            status: Mutex::new(Status::running()),
        }
    }

    pub(super) fn from_status(status: Status) -> Self {
        Self {
            status: Mutex::new(status),
            ..Self::new()
        }
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub fn token(&self) -> &CancellationToken {
        &self.cancel
    }

    pub fn status(&self) -> Status {
        self.lock().clone()
    }
    pub(super) fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// The launcher never started; nothing ran.
    pub fn spawn_failed(&self) {
        self.fail("failed", "SPAWN_FAILED");
    }

    /// Ownership of the process is uncertain, so the file gate stays closed.
    pub fn unreaped(&self) {
        self.fail("unknown", "REAP_FAILED");
    }

    /// Cancelled before the launcher was reached.
    pub fn cancelled_before_start(&self) {
        self.lock().state = "cancelled";
        self.changed.send_modify(|revision| *revision += 1);
    }

    pub fn complete(&self, outcome: Outcome) {
        let mut status = self.lock();
        status.state = outcome.state;
        status.code = outcome.code;
        status.stdout = outcome.stdout;
        status.stderr = outcome.stderr;
        status.truncated = outcome.truncated;
        self.changed.send_modify(|revision| *revision += 1);
    }

    fn fail(&self, state: &'static str, error: &'static str) {
        let mut status = self.lock();
        status.state = state;
        status.error = Some(error);
        self.changed.send_modify(|revision| *revision += 1);
    }

    fn lock(&self) -> MutexGuard<'_, Status> {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One command per `(identity, requestId)`.
type Key = (usize, String);

#[derive(Default)]
pub struct Registry {
    state: Mutex<RegistryState>,
}
#[derive(Default)]
struct RegistryState {
    entries: HashMap<Key, Arc<Process>>,
    receipts: super::receipts::Receipts,
}
impl RegistryState {
    fn find(&self, key: &Key) -> Result<Option<Arc<Process>>, Error> {
        if let Some(process) = self.entries.get(key) {
            return Ok(Some(process.clone()));
        }
        self.receipts.get(key.0, &key.1)
    }
    fn reserve(&mut self) -> Result<(), Error> {
        if self.entries.len() < CAPACITY {
            return Ok(());
        }
        let terminal: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(key, process)| {
                let status = process.status();
                matches!(
                    status.state,
                    "exited" | "cancelled" | "timed-out" | "failed"
                )
                .then_some((key.clone(), status))
            })
            .collect();
        for (key, status) in terminal {
            self.receipts.store(key.0, &key.1, status)?;
            self.entries.remove(&key);
        }
        if self.entries.len() >= CAPACITY {
            return Err(Error::too_many("PROCESS_LIMIT"));
        }
        Ok(())
    }
}
impl Registry {
    pub fn get(&self, identity: usize, id: &str) -> Result<Option<Arc<Process>>, Error> {
        self.lock().find(&(identity, id.to_owned()))
    }
    /// Admission and archival share the same lock; an archived identity never starts again.
    pub fn get_or_create(
        &self,
        identity: usize,
        id: &str,
        create: impl FnOnce() -> Result<Arc<Process>, Error>,
    ) -> Result<Arc<Process>, Error> {
        let mut state = self.lock();
        let key = (identity, id.to_owned());
        if let Some(existing) = state.find(&key)? {
            return Ok(existing);
        }
        state.reserve()?;
        let process = create()?;
        state.entries.insert(key, process.clone());
        Ok(process)
    }
    pub fn cancel(&self, identity: usize, id: &str) -> Result<Arc<Process>, Error> {
        let mut state = self.lock();
        let key = (identity, id.to_owned());
        if let Some(existing) = state.find(&key)? {
            existing.cancel();
            return Ok(existing);
        }
        state.reserve()?;
        let process = Arc::new(Process::new());
        process.cancelled_before_start();
        process.cancel();
        state.entries.insert(key, process.clone());
        Ok(process)
    }
    pub fn cancel_all(&self) {
        for process in self.lock().entries.values() {
            process.cancel();
        }
    }
    fn lock(&self) -> MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;
    fn completed() -> Arc<Process> {
        let process = Arc::new(Process::new());
        process.complete(Outcome {
            state: "exited",
            code: Some(0),
            stdout: "retained".into(),
            stderr: String::new(),
            truncated: false,
        });
        process
    }
    #[test]
    fn terminal_receipts_spill_without_replaying_and_remain_identity_scoped() {
        let registry = Registry::default();
        for index in 0..CAPACITY * 2 {
            registry
                .get_or_create(1, &index.to_string(), || Ok(completed()))
                .unwrap();
        }
        let old = registry
            .get_or_create(1, "0", || panic!("must never replay"))
            .unwrap();
        assert_eq!(old.status().stdout, "retained");
        assert_eq!(
            registry.get(1, "0").unwrap().unwrap().status().code,
            Some(0)
        );
        assert_eq!(registry.cancel(1, "0").unwrap().status().state, "exited");
        assert!(registry.get(2, "0").unwrap().is_none());
        assert!(registry.lock().entries.len() <= CAPACITY);
    }
    #[test]
    fn active_and_unknown_commands_are_never_evicted() {
        let registry = Registry::default();
        for index in 0..CAPACITY {
            registry
                .get_or_create(1, &index.to_string(), || {
                    let process = Arc::new(Process::new());
                    process.unreaped();
                    Ok(process)
                })
                .unwrap();
        }
        assert_eq!(
            registry
                .get_or_create(1, "overflow", || Ok(completed()))
                .err()
                .unwrap()
                .code,
            "PROCESS_LIMIT"
        );
        assert_eq!(
            registry.get(1, "0").unwrap().unwrap().status().state,
            "unknown"
        );
    }
    #[test]
    fn cancellation_refusals_survive_archival_and_cannot_be_started() {
        let registry = Registry::default();
        registry.cancel(1, "never-started").unwrap();
        for index in 0..CAPACITY {
            registry
                .get_or_create(1, &index.to_string(), || Ok(completed()))
                .unwrap();
        }
        assert_eq!(
            registry
                .get_or_create(1, "never-started", || panic!("cancelled request replayed"))
                .unwrap()
                .status()
                .state,
            "cancelled"
        );
    }
}
