//! Weak-revision bookkeeping for writable exports.
//!
//! A revision identifies a file *identity* (`dev`,`ino`) within one server
//! lifetime. It is intentionally not a content hash: the guarantee is that a
//! conditional write fails after the file was replaced or after a command ran,
//! not that equal bytes have equal revisions. Native in-place changes also
//! retire validators by comparing size, mtime and ctime on every lookup.

use crate::core::error::Error;
use std::{collections::HashMap, fs::File, os::unix::fs::MetadataExt};

/// Upper bound on tracked files; exceeding it drops every validator.
const CAPACITY: usize = 100_000;
/// Random prefix that makes a stale validator from a previous run unusable.
const NONCE_BYTES: usize = 16;

type Fingerprint = (u64, i64, i64, i64, i64);

pub struct Revisions {
    nonce: String,
    next: u64,
    entries: HashMap<(u64, u64), (u64, Fingerprint)>,
}

impl Revisions {
    pub fn new() -> Result<Self, Error> {
        let mut bytes = [0u8; NONCE_BYTES];
        getrandom::getrandom(&mut bytes).map_err(|_| Error::internal())?;
        Ok(Self {
            nonce: bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
            next: 0,
            entries: HashMap::new(),
        })
    }

    /// Return the current validator for `file`, minting one if needed.
    pub fn get(&mut self, file: &File) -> Result<String, Error> {
        let metadata = file.metadata()?;
        let key = (metadata.dev(), metadata.ino());
        let fingerprint = (
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        );
        if !self
            .entries
            .get(&key)
            .is_some_and(|(_, prior)| *prior == fingerprint)
        {
            // Evicting conservatively invalidates old validators; generations
            // are never reused, so an old revision cannot become current again.
            if self.entries.len() >= CAPACITY {
                self.entries.clear();
            }
            self.next = self.next.checked_add(1).ok_or_else(Error::internal)?;
            self.entries.insert(key, (self.next, fingerprint));
        }
        Ok(format!("\"{}:{}\"", self.nonce, self.entries[&key].0))
    }

    /// Forget one file, e.g. immediately before it is replaced.
    pub fn retire(&mut self, file: &File) -> Result<(), Error> {
        self.entries.remove(&identity(file)?);
        Ok(())
    }

    /// Forget every file, e.g. after a command may have rewritten the export.
    pub fn invalidate(&mut self) {
        self.entries.clear();
    }
}

fn identity(file: &File) -> Result<(u64, u64), Error> {
    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    #[test]
    fn observational_reads_detect_native_in_place_changes() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"before").unwrap();
        let mut revisions = Revisions::new().unwrap();
        let before = revisions.get(&file).unwrap();
        assert_eq!(before, revisions.get(&file).unwrap());
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"after native write").unwrap();
        assert_ne!(before, revisions.get(&file).unwrap());
    }
}
