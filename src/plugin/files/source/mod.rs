//! File-source ports. Providers do not receive HTTP headers, routers or request bodies.
mod local;
pub mod model;
use crate::{core::error::Error, fs::Export};
pub use local::LocalSource;
pub use model::{Entry, Listing, ReadFields, Stat};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
use tokio::io::AsyncRead;

pub type SourceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;
pub type Reader = Pin<Box<dyn AsyncRead + Send>>;

#[derive(Clone, Copy, Default)]
pub struct SourceCapabilities {
    pub write: bool,
    pub metadata: bool,
    pub strong_revision: bool,
    pub local_directory: bool,
    pub seq: bool,
}

/// Keep this context in any detached/blocking work until that work actually stops.
#[derive(Clone)]
pub struct ReadContext {
    pub deadline: tokio::time::Instant,
    pub workers: crate::core::workers::Workers,
    pub(crate) lease: Arc<super::reading::ReadLease>,
}
impl ReadContext {
    pub(crate) fn retain(&self) -> Arc<super::reading::ReadLease> {
        self.lease.clone()
    }
}

/// Opening pins a version. Reading a window must use that version or fail explicitly.
/// Dropping a reader cancels its transfer; it must not spawn unowned background work.
pub trait Content: Send {
    fn read(self: Box<Self>, offset: u64, length: u64) -> SourceFuture<'static, Reader>;
}
pub struct OpenContent {
    pub size: u64,
    pub revision: Option<String>,
    pub content: Box<dyn Content>,
}

/// Read port shared by local directories and future remote providers.
/// Writes continue through the durable local mutation port; unsupported providers fail closed.
pub trait FileSource: Send + Sync {
    fn capabilities(&self) -> SourceCapabilities;
    fn stat<'a>(
        &'a self,
        paths: Vec<String>,
        fields: ReadFields,
        context: ReadContext,
    ) -> SourceFuture<'a, Vec<Result<Option<Stat>, Error>>>;
    fn list<'a>(
        &'a self,
        path: String,
        fields: ReadFields,
        context: ReadContext,
    ) -> SourceFuture<'a, Listing>;
    fn open<'a>(&'a self, path: String, context: ReadContext) -> SourceFuture<'a, OpenContent>;
    /// Native execution and local mutation require an explicit POSIX capability.
    fn local_export(&self) -> Option<Arc<Export>> {
        None
    }
}
#[derive(Default)]
pub struct Sources {
    entries: BTreeMap<String, Arc<dyn FileSource>>,
}
impl Sources {
    pub fn register(&mut self, alias: &str, source: Arc<dyn FileSource>) -> Result<(), String> {
        if !crate::core::ids::is_identifier_within(alias, 128) || self.entries.contains_key(alias) {
            return Err(format!("Invalid or duplicate file source: {alias}"));
        }
        let capabilities = source.capabilities();
        let local = source.local_export().is_some();
        if capabilities.local_directory != local
            || !local && (capabilities.write || capabilities.seq)
        {
            return Err(format!("Inconsistent file source capabilities: {alias}"));
        }
        self.entries.insert(alias.into(), source);
        Ok(())
    }
    pub fn get(&self, alias: &str) -> Option<&Arc<dyn FileSource>> {
        self.entries.get(alias)
    }
    pub(crate) fn install(state: &mut crate::app::State) -> Result<(), String> {
        if state.services.get::<Self>().is_none() {
            state.services.insert(Arc::new(Self::default()))?;
        }
        let local = state
            .exports
            .iter()
            .map(|(alias, export)| (alias.clone(), export.clone()))
            .collect::<Vec<_>>();
        let sources = state
            .services
            .get_mut::<Self>()
            .ok_or("File sources must be exclusively owned during assembly")?;
        for (alias, export) in local {
            sources.register(&alias, Arc::new(LocalSource(export)))?;
        }
        Ok(())
    }
}
