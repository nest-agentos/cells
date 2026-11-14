//! Process admission and restart epochs; the retained root is an access capability.
//! Export directories carry no advisory ownership lock.

use crate::{core::error::Error, process::model::Registry, process::sandbox};
use std::{
    fs::File,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, MutexGuard,
    },
};

pub struct Execution {
    registry: Registry,
    epoch: String,
    ready: AtomicBool,
    root: Mutex<Option<File>>,
}

impl Execution {
    pub fn new(epoch: String) -> Self {
        Self {
            registry: Registry::default(),
            epoch,
            ready: AtomicBool::new(false),
            root: Mutex::new(None),
        }
    }

    /// Epoch advertised to clients and required on every process request.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// Whether the launcher was probed successfully at startup.
    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Admit commands; called once bootstrap probing succeeded.
    pub fn accept(&self) {
        self.ready.store(true, Ordering::Release);
    }

    /// Refuse new commands; called during shutdown.
    pub fn stop_accepting(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Retain the root capability for read-only launch plans.
    pub fn retain_root(&self, root: File) {
        *self.root() = Some(root);
    }

    /// Duplicate the retained directory capability.
    pub fn root_handle(&self) -> Result<File, Error> {
        self.root()
            .as_ref()
            .ok_or_else(Error::invalid)?
            .try_clone()
            .map_err(Error::from)
    }

    /// Ask every tracked command to stop; used during shutdown.
    pub fn cancel_all(&self) {
        self.registry.cancel_all();
    }

    fn root(&self) -> MutexGuard<'_, Option<File>> {
        self.root
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Bootstrap the execution capability, or explain why it is unavailable.
///
/// Failure is fatal by design: advertising `process.exec=true` without a
/// working sandbox would let a client believe it is contained when it is not.
pub async fn enable(state: &crate::app::State) -> Result<(), String> {
    let export = state
        .exports
        .first()
        .ok_or("Execution requires an export")?;
    let lock = export
        .try_clone_root()
        .map_err(|_| "Cannot retain execution capability")?;
    if crate::projects::native::enabled(state) {
        let result = tokio::process::Command::new("/bin/true")
            .kill_on_drop(true)
            .status()
            .await
            .map_err(|_| "Native execution unavailable")?;
        if !result.success() {
            return Err("Native execution probe failed".into());
        }
    } else {
        sandbox::probe().await?;
    }
    state
        .services
        .get::<crate::process::Execution>()
        .ok_or("Process service is not configured")?
        .retain_root(lock);
    state
        .services
        .get::<crate::process::Execution>()
        .ok_or("Process service is not configured")?
        .accept();
    Ok(())
}
