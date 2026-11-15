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
    /// Coordinate only the paths named by this change.
    ///
    /// `checkpoint` is polled once before the syscall so a cancelled or
    /// expired operation never mutates the tree.
    pub fn apply(
        &self,
        change: &Change,
        checkpoint: impl Fn() -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.apply_checked(change, false, checkpoint)
    }
    pub(crate) fn apply_checked(&self, change: &Change, overwrite: bool, checkpoint: impl Fn() -> Result<(), Error>) -> Result<(), Error> {
        if !self.writable() {
            return Err(Error::forbidden("EROFS"));
        }
        let mut paths = vec![(change.path(), true)];
        if let Change::Rename { to, .. } = change {
            paths.push((to, true));
        }
        self.path_locks.with(&paths, || {
            let mut constraints = self.constrain(&paths.iter().map(|(path, _)| *path).collect::<Vec<_>>())?;
            checkpoint()?;
            let source_resource = constraints.enabled().then(||self.resource(change.path())).transpose()?;
            let _file_lease = if matches!(change, Change::Rename { .. } | Change::Remove { .. }) {
                self.lock_existing_file(change.path())?
            } else {
                None
            };
            let (metadata_root, metadata_path) = self.metadata_scope(change.path())?;
            let metadata_to = if let Change::Rename { to, .. } = change {
                let (target_root, relative) = self.metadata_scope(to)?;
                use std::os::unix::fs::MetadataExt;
                let from = metadata_root.metadata()?;
                let to = target_root.metadata()?;
                if from.dev() != to.dev() || from.ino() != to.ino() {
                    return Err(Error::conflict("METADATA_SCOPE_CROSS_MOVE"));
                }
                Some(relative)
            } else {
                None
            };
            let mut metadata = if matches!(change, Change::Rename { .. } | Change::Remove { .. }) {
                super::metadata::Metadata::existing_write(&metadata_root)?
            } else {
                None
            };
            checkpoint()?;
            if constraints.enabled() {
                let resources = paths.iter().map(|(path,_)|self.resource(path)).collect::<Result<Vec<_>,_>>()?;
                constraints.check(&resources)?;
            }
            match change {
                Change::Mkdir { path } => {
                    let (directory, name) = self.parent(path)?;
                    rustix::fs::mkdirat(&directory, name.as_str(), DIRECTORY_MODE)?;
                    directory.sync_all().map_err(|_|Error::partial("FILE_DIRECTORY_SYNC_FAILED"))?;
                }
                Change::Rename { path, to } => {
                    let (from, name) = self.parent(path)?;
                    let (destination, destination_name) = self.parent(to)?;
                    if overwrite {
                        if let Some(target) = self.stat_inner(to)? {
                            let source = self.stat_inner(path)?.ok_or_else(||Error::not_found("ENOENT"))?;
                            if target.kind != "file" || source.kind != "file" { return Err(Error::unsupported()); }
                        }
                    }
                    let _destination_lease = if overwrite {
                        match self.lock_existing_file(to) { Ok(lease)=>lease,Err(error) if error.is_not_found()=>None,Err(error)=>return Err(error) }
                    } else { None };
                    rustix::fs::renameat_with(
                        &from,
                        name.as_str(),
                        &destination,
                        destination_name.as_str(),
                        if overwrite { RenameFlags::empty() } else { RenameFlags::NOREPLACE },
                    )?;
                    if let Some(resource) = &source_resource { constraints.removed(resource); }
                    from.sync_all().and_then(|()|destination.sync_all()).map_err(|_|Error::partial("FILE_DIRECTORY_SYNC_FAILED"))?;
                    if let Some(metadata) = metadata.as_mut() {
                        metadata
                            .rename(
                                &metadata_path,
                                metadata_to.as_deref().ok_or_else(Error::internal)?,
                            )
                            .map_err(|_| {
                                self.invalidate_revisions();
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
                    if let Some(resource) = &source_resource { constraints.removed(resource); }
                    directory.sync_all().map_err(|_|Error::partial("FILE_DIRECTORY_SYNC_FAILED"))?;
                    if let Some(metadata) = metadata.as_ref() {
                        metadata.remove(&metadata_path).map_err(|_| {
                            self.invalidate_revisions();
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
            Ok(())
        })
    }
}
