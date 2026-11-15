//! Directory capabilities.
//!
//! An [`Export`] is an open directory descriptor plus, for writable exports, a
//! revision table. Every lookup is performed relative to that descriptor with
//! `openat2(RESOLVE_BENEATH | NO_SYMLINKS | NO_MAGICLINKS)`, so no wire path
//! can escape the root even if it is a symlink or a mount point.
//!
//! Export descriptors grant access without locking the directory. Conditional
//! replacement coordinates only the current file; SQLite owns its file transactions.

use crate::core::error::Error;
use crate::fs::{
    model::{attributes, Entry, Stat},
    path, recovery,
    revision::Revisions,
};
use rustix::fs::{openat2, Mode, OFlags, ResolveFlags};
use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio_util::sync::CancellationToken;

/// Largest number of children inspected in a single directory scan.
const DIRECTORY_LIMIT: usize = 100_000;

pub struct Export {
    pub(crate) root: File,
    pub(crate) path_locks: crate::core::path_locks::PathLocks,
    revisions: Option<Mutex<Revisions>>,
    pub(crate) upload_journal: Option<Arc<super::upload_journal::UploadJournal>>,
}

impl Export {
    pub(crate) fn space(&self, alias: &str) -> serde_json::Value {
        let space = rustix::fs::fstatvfs(&self.root).ok();
        serde_json::json!({"alias":alias,"totalBytes":space.as_ref().and_then(|s|s.f_blocks.checked_mul(s.f_frsize)),
            "availableBytes":space.as_ref().and_then(|s|s.f_bavail.checked_mul(s.f_frsize))})
    }
    /// Open a read-only capability. Fails closed when `openat2` is unavailable.
    pub fn open(root: &Path) -> Result<Self, Error> {
        let root = File::open(root)?;
        if !root.metadata()?.is_dir() {
            return Err(Error::invalid());
        }
        let export = Self {
            root,
            path_locks: Default::default(),
            revisions: None,
            upload_journal: None,
        };
        // Probe the resolution mechanism now rather than at first use.
        export.open_path("", OFlags::PATH)?;
        Ok(export)
    }

    /// Open a writable capability with revisions and resource-scoped recovery.
    pub fn open_writable(root: &Path) -> Result<Self, Error> {
        let mut export = Self::open(root)?;
        export.revisions = Some(Mutex::new(Revisions::new()?));
        export.upload_journal = Some(Arc::new(super::upload_journal::UploadJournal::open(
            &export.root,
        )?));
        recovery::clean_uploads(&export)?;
        Ok(export)
    }

    /// Whether this export accepts mutations.
    pub fn writable(&self) -> bool {
        self.revisions.is_some()
    }

    /// Duplicate the root directory capability.
    pub fn try_clone_root(&self) -> Result<File, Error> {
        Ok(self.root.try_clone()?)
    }

    /// Open a directory inside the export, rejecting symlinks and escapes.
    pub fn open_dir(&self, path: &str) -> Result<File, Error> {
        self.open_path(path, OFlags::RDONLY | OFlags::DIRECTORY)
    }

    /// Look up one entry. `Ok(None)` means "does not exist"; other failures are real.
    pub fn stat(&self, path: &str) -> Result<Option<Stat>, Error> {
        self.path_locks
            .with(&[(path, false)], || self.stat_inner(path))
    }
    fn stat_inner(&self, path: &str) -> Result<Option<Stat>, Error> {
        match self.open_path(path, OFlags::PATH) {
            Ok(file) => {
                let mut stat = attributes(&file)?;
                stat.revision = self
                    .lock_revisions()
                    .as_mut()
                    .map(|r| r.get(&file))
                    .transpose()?;
                let (metadata_root, relative) = self.metadata_scope(path)?;
                if let Some(store) = super::metadata::Metadata::open(&metadata_root, false)? {
                    stat.metadata = store.read(&relative)?;
                    stat.source_identity = store.identity(&relative)?;
                }
                Ok(Some(stat))
            }
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Basic facts never inspect sidecar metadata or reserve mutation/revision locks.
    pub fn stat_fields(
        &self,
        path: &str,
        fields: super::model::ReadFields,
    ) -> Result<Option<Stat>, Error> {
        if fields == super::model::ReadFields::Stat {
            return self.stat(path);
        }
        match self.open_path(path, OFlags::PATH) {
            Ok(file) => Ok(Some(attributes(&file)?)),
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Open a regular file for reading together with its current revision.
    pub fn read(&self, path: &str) -> Result<(File, Option<String>), Error> {
        self.path_locks
            .with(&[(path, false)], || self.read_inner(path))
    }
    fn read_inner(&self, path: &str) -> Result<(File, Option<String>), Error> {
        let file = self.open_path(path, OFlags::RDONLY)?;
        if !file.metadata()?.is_file() {
            return Err(Error::unsupported());
        }
        let revision = self
            .lock_revisions()
            .as_mut()
            .map(|r| r.get(&file))
            .transpose()?;
        if path.ends_with(".seq") {
            use std::os::unix::fs::FileExt;
            let mut header = [0u8; 16];
            if file.read_at(&mut header, 0)? == 16 && &header == b"SQLite format 3\0" {
                return Ok((super::seq::backup_file(self, path)?, revision));
            }
        }
        Ok((file, revision))
    }

    /// Sorted children of `path` plus diagnostics for skipped entries.
    pub fn list(&self, path: &str, cancelled: &CancellationToken) -> Result<Listing, Error> {
        self.list_fields(path, cancelled, super::model::ReadFields::Stat)
    }
    pub fn list_fields(
        &self,
        path: &str,
        cancelled: &CancellationToken,
        fields: super::model::ReadFields,
    ) -> Result<Listing, Error> {
        let directory = self.open_dir(path)?;
        let mut listing = Listing::default();
        for item in rustix::fs::Dir::read_from(&directory)? {
            if cancelled.is_cancelled() {
                return Err(Error::cancelled());
            }
            let item = item?;
            let Ok(name) = item.file_name().to_str() else {
                listing.warnings.push("NON_UTF8_NAME");
                continue;
            };
            if name == "." || name == ".." || path::is_reserved(name) {
                continue;
            }
            if listing.entries.len() + listing.warnings.len() >= DIRECTORY_LIMIT {
                return Err(Error::too_large("DIRECTORY_LIMIT"));
            }
            let child = child_path(path, name);
            match self.stat_fields(&child, fields) {
                Ok(Some(stat)) => listing.entries.push(Entry {
                    name: name.into(),
                    stat,
                }),
                Ok(None) => {}
                Err(error) if error.is_unsupported() => listing.warnings.push("UNSUPPORTED_NODE"),
                Err(error) => return Err(error),
            }
        }
        listing.entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(listing)
    }

    /// Drop every remembered revision, e.g. after a command may have written.
    pub fn invalidate_revisions(&self) {
        if let Some(mut revisions) = self.lock_revisions() {
            revisions.invalidate();
        }
    }

    /// Borrow the revision table of a writable export for one atomic step.
    ///
    /// A poisoned lock is recovered rather than propagated: [`Revisions`] only
    /// ever holds a counter and a map, so a panic elsewhere cannot leave it in
    /// a half-updated state that matters.
    pub(crate) fn lock_revisions(&self) -> Option<MutexGuard<'_, Revisions>> {
        self.revisions
            .as_ref()
            .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
    }

    pub(crate) fn lock_existing_file(&self, path: &str) -> Result<Option<File>, Error> {
        let file = self.open_path(path, OFlags::RDONLY)?;
        if !file.metadata()?.is_file() {
            return Ok(None);
        }
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
            |error| {
                if error == rustix::io::Errno::WOULDBLOCK {
                    Error::busy()
                } else {
                    error.into()
                }
            },
        )?;
        let current = self.open_path(path, OFlags::PATH)?;
        use std::os::unix::fs::MetadataExt;
        let locked = file.metadata()?;
        let current = current.metadata()?;
        if (locked.dev(), locked.ino()) != (current.dev(), current.ino()) {
            return Err(Error::precondition_failed());
        }
        Ok(Some(file))
    }
    /// Resolve `path` under the root capability.
    pub(crate) fn open_path(&self, path: &str, flags: OFlags) -> Result<File, Error> {
        path::validate(path)?;
        let flags = flags | OFlags::CLOEXEC | OFlags::NOFOLLOW;
        // PATH lookups are metadata probes; everything else must not block.
        let flags = if flags.contains(OFlags::PATH) {
            flags
        } else {
            flags | OFlags::NONBLOCK
        };
        let fd = openat2(
            &self.root,
            if path.is_empty() { "." } else { path },
            flags,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?;
        Ok(File::from(fd))
    }

    /// Split a non-root path into its parent directory and final component.
    pub(crate) fn parent(&self, path: &str) -> Result<(File, String), Error> {
        path::validate(path)?;
        if path.is_empty() {
            return Err(Error::invalid());
        }
        let (directory, name) = path.rsplit_once('/').unwrap_or(("", path));
        Ok((self.open_dir(directory)?, name.to_owned()))
    }
}

pub use crate::plugin::files::source::model::Listing;

pub(crate) fn child_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}
