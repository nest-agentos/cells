//! Shared host resources and generic plugin service ownership.
//! Product configuration and built-in registration live in plugin/catalog.rs.
//! New hosts may use State::host and Application::assemble without any built-in services.

use crate::{
    auth::Auth,
    core::{gate::FileGate, workers::Workers},
    fs::Exports,
    plugin::common::{PluginPlan, Services},
};
use std::sync::Arc;
mod application;
pub use application::Application;
pub const WORKER_LIMIT: usize = 16;
const KEY_BYTES: usize = 32;

pub struct State {
    pub plugins: PluginPlan,
    pub services: Services,
    pub epoch: String,
    pub auth: Auth,
    pub exports: Exports,
    pub cursor_key: [u8; KEY_BYTES],
    pub files: FileGate,
    pub workers: Workers,
    pub allowed_origins: Vec<String>,
    pub event_slots: Arc<tokio::sync::Semaphore>,
    pub event_stop: tokio_util::sync::CancellationToken,
}
impl State {
    pub fn host(auth: Auth, exports: Exports) -> Result<Self, String> {
        Ok(Self::bare(auth, exports, random_bytes()?, random_epoch()?))
    }
    pub(crate) fn bare(
        auth: Auth,
        exports: Exports,
        cursor_key: [u8; KEY_BYTES],
        epoch: String,
    ) -> Self {
        Self {
            plugins: PluginPlan::default(),
            services: Services::default(),
            epoch,
            auth,
            exports,
            cursor_key,
            files: FileGate::new(),
            workers: Workers::new(WORKER_LIMIT),
            allowed_origins: vec![],
            event_slots: Arc::new(tokio::sync::Semaphore::new(32)),
            event_stop: tokio_util::sync::CancellationToken::new(),
        }
    }
    /// Assemble the runtime with fresh random material.
    ///
    /// The pagination cursor key and the process epoch are independent secrets:
    /// the epoch is published through `/v1/capabilities`, the key is not.
    pub fn new(auth: Auth, exports: Exports) -> Result<Arc<Self>, String> {
        let mut state = Self::bare(auth, exports, random_bytes()?, random_epoch()?);
        crate::plugin::catalog::compatibility(&mut state)?;
        Ok(Arc::new(state))
    }
    #[doc(hidden)]
    pub fn with_secrets(
        auth: Auth,
        exports: Exports,
        cursor_key: [u8; KEY_BYTES],
        process_epoch: String,
    ) -> Arc<Self> {
        let mut state = Self::bare(auth, exports, cursor_key, process_epoch);
        crate::plugin::catalog::compatibility(&mut state).expect("Empty plugin services are valid");
        Arc::new(state)
    }
}

pub(crate) fn random_bytes() -> Result<[u8; KEY_BYTES], String> {
    let mut bytes = [0u8; KEY_BYTES];
    getrandom::getrandom(&mut bytes).map_err(|_| "Random source unavailable".to_owned())?;
    Ok(bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn random_epoch() -> Result<String, String> {
    Ok(hex(&random_bytes()?))
}
