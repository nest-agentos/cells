use rustix::{
    fs::{flock, FlockOperation, OFlags},
    io::Errno,
};
use std::{
    fs::{self, OpenOptions},
    io,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

/// The caller holds the directory lock. Active writers reserve one whole segment each.
pub(super) fn prune(
    directory: &Path,
    segment_bytes: u64,
    budget: u64,
    mut reserved: u64,
) -> io::Result<()> {
    let mut closed = Vec::new();
    let mut bytes = 0u64;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let Some((pid, segment)) = log_name(entry.file_name().to_str().unwrap_or_default()) else {
            continue;
        };
        if !entry.file_type()?.is_file() {
            continue;
        }
        let meta = entry.metadata()?;
        if segment == 0 && active(&entry.path(), pid)? {
            reserved = reserved.saturating_add(segment_bytes.max(meta.len()));
        } else {
            bytes = bytes.saturating_add(meta.len());
            closed.push((meta.modified()?, entry.path(), meta.len()));
        }
    }
    let limit = budget
        .checked_sub(reserved)
        .ok_or_else(|| io::Error::other("Cells log budget is reserved by active writers"))?;
    closed.sort_unstable();
    for (_, path, size) in closed {
        if bytes <= limit {
            break;
        }
        fs::remove_file(path)?;
        bytes -= size;
    }
    Ok(())
}

fn log_name(name: &str) -> Option<(u32, u32)> {
    let (instance, extension) = name.strip_prefix("cells-")?.split_once(".jsonl")?;
    let segment = if extension.is_empty() {
        0
    } else {
        let n = extension.strip_prefix('.')?.parse().ok()?;
        if !(1..=100).contains(&n) {
            return None;
        }
        n
    };
    let (time, pid) = instance.rsplit_once('-')?;
    chrono::NaiveDateTime::parse_from_str(time.trim_end_matches('Z'), "%Y%m%dT%H%M%S%.f").ok()?;
    Some((pid.parse().ok()?, segment))
}

fn active(path: &Path, pid: u32) -> io::Result<bool> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(OFlags::NOFOLLOW.bits() as i32)
        .open(path)?;
    match flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Err(Errno::WOULDBLOCK) => Ok(true),
        Err(error) => Err(error.into()),
        Ok(()) => Ok(legacy_writer(path, pid)),
    }
}

/// Pre-budget Cells processes did not lock their current log.
fn legacy_writer(path: &Path, pid: u32) -> bool {
    fs::read_dir(format!("/proc/{pid}/fd")).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|entry| fs::read_link(entry.path()).is_ok_and(|target| target == path))
    })
}
