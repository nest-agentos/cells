//! Atomic replacement of file contents.
//!
//! An upload is written to a reserved staging name in the destination
//! directory and then published with a single `renameat2`. That makes the
//! namespace change atomic and lets a created-but-unpublished file disappear
//! with the process:
//!
//! * [`Export::stage`] creates the staging file; dropping it unlinks the file.
//! * [`commit`] verifies the expected revision and renames under the
//!   revision lock, so a conditional write cannot interleave with another.
//!
//! Permission policy: because `renameat2` moves the inode, the staging mode
//! would otherwise become the file's mode. A replacement therefore copies the
//! previous file's permission bits, and a creation uses [`DEFAULT_FILE_MODE`].

use crate::{
    core::error::Error,
    fs::{
        export::Export,
        model::{attributes, Stat},
        path,
        upload_journal::{PendingUpload, UploadJournal},
    },
};
use rustix::fs::{Mode, OFlags, RenameFlags};
use std::{
    fs::File,
    os::unix::fs::{MetadataExt, PermissionsExt},
    sync::Arc,
};

/// Mode for a newly created file. The service does not consult `umask`, so the
/// result is predictable for every deploying host.
const DEFAULT_FILE_MODE: Mode = Mode::RUSR
    .union(Mode::WUSR)
    .union(Mode::RGRP)
    .union(Mode::ROTH);
/// Permission bits only; file type and setuid/setgid/sticky are not copied.
const MODE_MASK: u32 = 0o777;
/// Staging files start private and are widened only at commit.
const STAGING_MODE: Mode = Mode::RUSR.union(Mode::WUSR);
const RANDOM_BYTES: usize = 16;

/// A staging file that is removed unless it is committed.
pub struct StagedUpload {
    parent: File,
    name: String,
    file: File,
    journal: Arc<UploadJournal>,
    pending: PendingUpload,
    operation_id: Option<String>,
}

impl Drop for StagedUpload {
    fn drop(&mut self) {
        let started = std::time::Instant::now();
        let result = match super::recovery::cleanup(&self.parent, &self.pending) {
            Ok(true) => self.journal.forget(self.pending.id),
            Ok(false) => Err(Error::conflict("UPLOAD_CLEANUP_IDENTITY_CHANGED")),
            Err(error) => Err(error),
        };
        crate::core::events::emit(if result.is_ok(){crate::core::events::Level::Info}else{crate::core::events::Level::Warn},"file.upload.cleanup",serde_json::json!({"operation":"upload.cleanup","stage":"cleanup","uploadId":self.pending.id,"operationId":self.operation_id,"result":if result.is_ok(){"success"}else{"retained"},"code":result.as_ref().err().map(|error|error.code),"durationMs":started.elapsed().as_millis() as u64}));
    }
}

impl StagedUpload {
    pub(crate) fn track_operation(&mut self, id: &str) {
        if crate::core::ids::is_identifier_within(id,128) { self.operation_id = Some(id.to_owned()); }
    }
    pub(crate) fn journal_id(&self) -> i64 { self.pending.id }
    /// Duplicate the descriptor for the asynchronous body writer.
    pub fn writer(&self) -> Result<File, Error> {
        Ok(self.file.try_clone()?)
    }
}

impl Export {
    /// Create a staging file next to `path`.
    pub fn stage(&self, path: &str) -> Result<StagedUpload, Error> {
        let journal = self.journal()?;
        let (parent, _) = self.parent(path)?;
        let name = staging_name()?;
        let parent_path = path.rsplit_once('/').map_or("", |(parent, _)| parent);
        let mut pending = journal.prepare(parent_path, &name, &parent)?;
        let file = match create_file(&parent, &name) {
            Ok(file) => file,
            Err(error) => {
                journal.forget(pending.id)?;
                return Err(error);
            }
        };
        pending.identify(&file)?;
        let staged = StagedUpload {
            operation_id: None,
            pending,
            parent,
            name,
            file,
            journal,
        };
        staged.parent.sync_all()?;
        staged.journal.identify(staged.pending.id, &staged.file)?;
        Ok(staged)
    }
}

fn staging_name() -> Result<String, Error> {
    let mut random = [0u8; RANDOM_BYTES];
    getrandom::getrandom(&mut random).map_err(|_| Error::internal())?;
    let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!("{}{suffix}", path::RESERVED_PREFIX))
}

fn create_file(parent: &File, name: &str) -> Result<File, Error> {
    Ok(File::from(rustix::fs::openat(
        parent,
        name,
        OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        STAGING_MODE,
    )?))
}

/// Publish a staged upload as `path`.
///
/// `expected` is `None` for create-only writes and `Some(revision)` for
/// replace writes; both cases re-check the condition under the lock.
pub fn commit(
    export: &Export,
    path: &str,
    upload: StagedUpload,
    expected: Option<&str>,
    checkpoint: impl Fn() -> Result<(), Error>,
) -> Result<Stat, Error> {
    publish(export, path, upload, Condition::Revision(expected), None, checkpoint)
}

/// Publish durable sync contents and executable bits in the same rename.
pub fn commit_executable(
    export: &Export,
    path: &str,
    upload: StagedUpload,
    expected: Option<&str>,
    executable: bool,
    checkpoint: impl Fn() -> Result<(), Error>,
) -> Result<Stat, Error> {
    publish(export, path, upload, Condition::Revision(expected), Some(executable), checkpoint)
}
pub(crate) enum Condition<'a> {
    Revision(Option<&'a str>),
    Locked { owner: usize, tokens: &'a [String] },
}
pub(crate) fn commit_locked(export: &Export, path: &str, upload: StagedUpload, owner: usize, tokens: &[String], checkpoint: impl Fn() -> Result<(), Error>) -> Result<Stat, Error> {
    publish(export, path, upload, Condition::Locked { owner, tokens }, None, checkpoint)
}
fn publish(
    export: &Export,
    path: &str,
    upload: StagedUpload,
    condition: Condition<'_>,
    executable: Option<bool>,
    checkpoint: impl Fn() -> Result<(), Error>,
) -> Result<Stat, Error> {
    if !export.writable() {
        return Err(Error::forbidden("EROFS"));
    }
    export.path_locks.with(&[(path, true)], || {
        let constraints = export.constrain(&[path], false)?;
        checkpoint()?;
        // Re-resolve the parent under the lock: renaming it invalidates the upload.
        let (target, name) = export.parent(path)?;
        if identity(&target) != identity(&upload.parent) {
            return Err(Error::precondition_failed());
        }
        let registry = constraints.as_ref().map(|guard| &**guard);
        let (mut mode, _file_lease, replace) = publication_mode(export, path, condition, registry)?;
        if let Some(executable) = executable {
            let bits = if executable {
                mode.bits() | 0o111
            } else {
                mode.bits() & !0o111
            };
            mode = Mode::from_bits_truncate(bits);
        }
        checkpoint()?;
        // Give the published inode the mode the caller would expect to find.
        rustix::fs::fchmod(&upload.file, mode)?;
        upload.file.sync_all()?;
        checkpoint()?;
        if let Some(registry) = registry { registry.check(&[export.resource(path)?])?; }
        rustix::fs::renameat_with(
            &upload.parent,
            upload.name.as_str(),
            &target,
            name.as_str(),
            if !replace {
                RenameFlags::NOREPLACE
            } else {
                RenameFlags::empty()
            },
        )
        .map_err(|error| {
            if !replace && error == rustix::io::Errno::EXIST {
                Error::precondition_failed()
            } else {
                error.into()
            }
        })?;
        target.sync_all().map_err(|_|Error::partial("FILE_PUBLISHED_SYNC_FAILED"))?;
        let result=(||{
            let mut stat=attributes(&upload.file)?;
            stat.revision=Some(export.lock_revisions().unwrap().get(&upload.file)?);
            Ok(stat)
        })();
        result.map_err(|error:Error|{
            crate::core::events::emit(crate::core::events::Level::Error,"file.publish.failed",serde_json::json!({"operation":"file.publish","stage":"published-stat","result":"failure","code":error.code,"outcome":"partial"}));
            Error::partial("FILE_PUBLISHED_STAT_FAILED")
        })
    })
}

/// Reject the write unless `path` still carries exactly `expected`, then
/// return the permission bits to carry over to the replacement.
fn replaced_mode(export: &Export, path: &str, expected: &str) -> Result<(Mode, File), Error> {
    let current = export
        .lock_existing_file(path)?
        .ok_or_else(Error::precondition_failed)?;
    let metadata = current.metadata()?;
    let mut revisions = export
        .lock_revisions()
        .ok_or_else(|| Error::forbidden("EROFS"))?;
    if !metadata.is_file() || revisions.get(&current)? != expected {
        return Err(Error::precondition_failed());
    }
    revisions.retire(&current)?;
    Ok((
        Mode::from_bits_truncate(metadata.permissions().mode() & MODE_MASK),
        current,
    ))
}

fn identity(file: &File) -> Result<(u64, u64), Error> {
    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}

// A lease condition is not a client read validator; never manufacture an expected revision.
fn publication_mode(export: &Export,path: &str,condition: Condition<'_>,constraints: Option<&crate::core::file_constraints::Registry>) -> Result<(Mode,Option<File>,bool),Error> {
    match condition {
        Condition::Revision(Some(expected)) => replaced_mode(export,path,expected).map(|(mode,file)|(mode,Some(file),true)),
        Condition::Revision(None) => Ok((DEFAULT_FILE_MODE,None,false)),
        Condition::Locked {owner,tokens} => {
            if !constraints.ok_or_else(Error::precondition_failed)?.owns(&export.resource(path)?,owner,tokens) { return Err(Error::precondition_failed()); }
            match export.lock_existing_file(path) {
                Err(error) if error.is_not_found() => Ok((DEFAULT_FILE_MODE,None,false)),
                Err(error) => Err(error),
                Ok(None) => Err(Error::conflict("EISDIR")),
                Ok(Some(file)) => {
                    let mode = Mode::from_bits_truncate(file.metadata()?.permissions().mode() & MODE_MASK);
                    export.lock_revisions().ok_or_else(||Error::forbidden("EROFS"))?.retire(&file)?;
                    Ok((mode,Some(file),true))
                },
            }
        },
    }
}
