//! Structural mutations: create a directory, rename an entry, remove an entry.
//!
//! The wire protocol carries these as `action` strings; this module is the
//! only place that turns them into syscalls, and it invalidates revisions in
//! the same critical section that performs the change.

use crate::{core::error::Error, fs::export::Export, fs::path};
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags};

/// Mode for a newly created directory. The service does not consult `umask`,
/// so the result is predictable for every deploying host.
const DIRECTORY_MODE: Mode = Mode::RUSR
    .union(Mode::WUSR)
    .union(Mode::XUSR)
    .union(Mode::RGRP)
    .union(Mode::XGRP)
    .union(Mode::ROTH)
    .union(Mode::XOTH);

/// One validated structural change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Mkdir {
        path: String,
    },
    Rename {
        path: String,
        to: String,
    },
    Remove {
        path: String,
    },
    Metadata {
        path: String,
        patch: serde_json::Map<String, serde_json::Value>,
        remove: Vec<String>,
    },
}

impl Change {
    /// Map a protocol action onto a validated change.
    pub fn parse(action: &str, path: String, to: Option<String>) -> Result<Self, Error> {
        path::validate(&path)?;
        Ok(match action {
            "mkdir" => Self::Mkdir { path },
            "rename" => {
                let to = to.ok_or_else(Error::invalid)?;
                path::validate(&to)?;
                Self::Rename { path, to }
            }
            "remove" => Self::Remove { path },
            _ => return Err(Error::invalid()),
        })
    }

    /// The entry the change applies to.
    pub fn path(&self) -> &str {
        match self {
            Self::Mkdir { path }
            | Self::Rename { path, .. }
            | Self::Remove { path }
            | Self::Metadata { path, .. } => path,
        }
    }
}

impl Export {
    /// Apply `change` while holding the revision lock.
    ///
    /// `checkpoint` is polled once before the syscall so a cancelled or
    /// expired operation never mutates the tree.
    pub fn apply(
        &self,
        change: &Change,
        checkpoint: impl Fn() -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut revisions = self
            .lock_revisions()
            .ok_or_else(|| Error::forbidden("EROFS"))?;
        checkpoint()?;
        let _metadata = self
            .metadata_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (metadata_root, metadata_path) = self.metadata_scope(change.path())?;
        let metadata_to = if let Change::Rename {to, ..} = change {
            let (target_root, relative) = self.metadata_scope(to)?;
            use std::os::unix::fs::MetadataExt;
            let from = metadata_root.metadata()?; let to = target_root.metadata()?;
            if from.dev() != to.dev() || from.ino() != to.ino() { return Err(Error::conflict("METADATA_SCOPE_CROSS_MOVE")); }
            Some(relative)
        } else {None};
        let mut metadata = if matches!(change, Change::Rename { .. } | Change::Remove { .. }) {
            super::metadata::Metadata::existing_write(&metadata_root)?
        } else {
            None
        };
        match change {
            Change::Mkdir { path } => {
                let (directory, name) = self.parent(path)?;
                rustix::fs::mkdirat(&directory, name.as_str(), DIRECTORY_MODE)?;
            }
            Change::Rename { path, to } => {
                let (from, name) = self.parent(path)?;
                let (destination, destination_name) = self.parent(to)?;
                rustix::fs::renameat_with(
                    &from,
                    name.as_str(),
                    &destination,
                    destination_name.as_str(),
                    RenameFlags::NOREPLACE,
                )?;
                if let Some(metadata) = metadata.as_mut() {
                    metadata.rename(&metadata_path, metadata_to.as_deref().ok_or_else(Error::internal)?).map_err(|_| {
                        revisions.invalidate();
                        Error::partial("FILE_METADATA_MAINTENANCE_FAILED")
                    })?;
                }
            }
            Change::Remove { path } => {
                let (directory, name) = self.parent(path)?;
                let target = self.open_path(path, OFlags::PATH)?;
                let flags = if target.metadata()?.is_dir() {
                    AtFlags::REMOVEDIR
                } else {
                    AtFlags::empty()
                };
                rustix::fs::unlinkat(&directory, name.as_str(), flags)?;
                if let Some(metadata) = metadata.as_ref() {
                    metadata.remove(&metadata_path).map_err(|_| {
                        revisions.invalidate();
                        Error::partial("FILE_METADATA_MAINTENANCE_FAILED")
                    })?;
                }
            }
            Change::Metadata {
                path,
                patch,
                remove,
            } => {
                self.open_path(path, OFlags::PATH)?;
                if !patch.is_empty() || !remove.is_empty() {
                    let mut metadata = super::metadata::Metadata::open(&metadata_root, true)?
                        .ok_or_else(Error::internal)?;
                    metadata.patch(&metadata_path, patch, remove)?;
                }
            }
        }
        if !matches!(change, Change::Metadata { .. }) {
            revisions.invalidate();
        }
        Ok(())
    }
}
