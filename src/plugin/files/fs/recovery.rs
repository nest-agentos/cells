//! Recover only journaled uploads under the export's exclusive writer lock.
use crate::{
    core::{
        error::Error,
        events::{self, Level},
    },
    fs::{export::Export, path, upload_journal::PendingUpload},
};
use rustix::fs::{AtFlags, FileType};
use std::{fs::File, os::unix::fs::MetadataExt};

pub(crate) fn clean_uploads(export: &Export) -> Result<(), Error> {
    let journal = export.journal()?;
    let (mut cursor, mut recovered, mut retained) = (0, 0, 0);
    loop {
        let batch = journal.batch(cursor)?;
        if batch.is_empty() {
            break;
        }
        for upload in batch {
            cursor = upload.id;
            let cleaned = export
                .open_dir(&upload.parent)
                .ok()
                .and_then(|parent| cleanup(&parent, &upload).ok())
                .unwrap_or(false);
            if cleaned {
                journal.forget(upload.id)?;
                recovered += 1;
            } else {
                retained += 1;
            }
        }
    }
    report(recovered, retained);
    Ok(())
}

fn report(recovered: usize, retained: usize) {
    if recovered > 0 || retained > 0 {
        events::emit(
            if retained > 0 {
                Level::Warn
            } else {
                Level::Info
            },
            "uploads.recovered",
            serde_json::json!({"recovered": recovered, "retained": retained}),
        );
    }
}

/// Retain replacements, symlinks and unverified create intents instead of deleting them.
pub(crate) fn cleanup(parent: &File, upload: &PendingUpload) -> Result<bool, Error> {
    if !valid_name(&upload.name) || !matches_parent(parent, upload)? {
        return Ok(false);
    }
    match matches_file(parent, upload)? {
        Some(false) => return Ok(false),
        Some(true) => rustix::fs::unlinkat(parent, upload.name.as_str(), AtFlags::empty())?,
        None => (),
    }
    parent.sync_all()?;
    Ok(true)
}

fn matches_parent(parent: &File, upload: &PendingUpload) -> Result<bool, Error> {
    let metadata = parent.metadata()?;
    Ok(metadata.dev() as i64 == upload.parent_device
        && metadata.ino() as i64 == upload.parent_inode)
}

fn matches_file(parent: &File, upload: &PendingUpload) -> Result<Option<bool>, Error> {
    let current = match rustix::fs::statat(parent, upload.name.as_str(), AtFlags::SYMLINK_NOFOLLOW)
    {
        Ok(current) => current,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(Some(
        FileType::from_raw_mode(current.st_mode) == FileType::RegularFile
            && upload.device == Some(current.st_dev as i64)
            && upload.inode == Some(current.st_ino as i64),
    ))
}

fn valid_name(name: &str) -> bool {
    name.strip_prefix(path::RESERVED_PREFIX)
        .is_some_and(|suffix| {
            suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}
