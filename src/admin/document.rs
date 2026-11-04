//! Bounded, comment-preserving TOML edits with cooperative locking, backup and atomic replacement.
use crate::config::Config;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
use toml_edit::DocumentMut;

const MAX_BYTES: u64 = 1024 * 1024;
pub(super) struct Document {
    pub content: DocumentMut,
    path: PathBuf,
    original: String,
    identity: (u64, u64),
    _lock: Option<File>,
}

impl Document {
    pub fn open(path: &Path, editing: bool) -> Result<Self, String> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = parent
            .canonicalize()
            .map_err(|_| "Configuration directory unavailable")?;
        let path = parent.join(path.file_name().ok_or("Invalid configuration path")?);
        let lock = editing.then(|| lock(&path)).transpose()?;
        let (original, identity) = read(&path)?;
        let content = original.parse().map_err(|_| "Invalid TOML configuration")?;
        Ok(Self {
            content,
            path,
            original,
            identity,
            _lock: lock,
        })
    }
    pub fn config(&self) -> Result<Config, String> {
        let content = self.content.to_string();
        if content.len() as u64 > MAX_BYTES {
            return Err("Configuration exceeds 1 MiB".into());
        }
        toml::from_str(&content)
            .map_err(|_| "Configuration does not match the supported schema".into())
    }
    pub fn changed(&self) -> bool {
        self.content.to_string() != self.original
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn commit(&self) -> Result<Option<PathBuf>, String> {
        if !self.changed() {
            return Ok(None);
        }
        let parent = self
            .path
            .parent()
            .ok_or("Configuration directory unavailable")?;
        let mut replacement =
            tempfile::NamedTempFile::new_in(parent).map_err(|_| "Cannot prepare configuration")?;
        write(
            replacement.as_file_mut(),
            self.content.to_string().as_bytes(),
        )?;
        self.unchanged()?;
        let backup = self.backup(parent)?;
        self.unchanged()?;
        replacement
            .persist(&self.path)
            .map_err(|_| "Cannot replace configuration; original backup retained")?;
        File::open(parent).and_then(|f| f.sync_all()).map_err(|_| {
            "Configuration replaced but directory sync failed; inspect before retrying"
        })?;
        Ok(Some(backup))
    }
    fn unchanged(&self) -> Result<(), String> {
        let (text, identity) = read(&self.path)?;
        if identity != self.identity || text != self.original {
            return Err("Configuration changed concurrently; retry from the new file".into());
        }
        Ok(())
    }
    fn backup(&self, parent: &Path) -> Result<PathBuf, String> {
        let prefix = format!(
            "{}.backup-",
            self.path.file_name().unwrap().to_string_lossy()
        );
        let mut backup = tempfile::Builder::new()
            .prefix(&prefix)
            .suffix(".toml")
            .tempfile_in(parent)
            .map_err(|_| "Cannot prepare configuration backup")?;
        write(backup.as_file_mut(), self.original.as_bytes())?;
        let (_, path) = backup
            .keep()
            .map_err(|_| "Cannot retain configuration backup")?;
        Ok(path)
    }
}
fn read(path: &Path) -> Result<(String, (u64, u64)), String> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)
        .map_err(|_| "Cannot open configuration as a regular file")?;
    let metadata = file
        .metadata()
        .map_err(|_| "Cannot inspect configuration")?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > MAX_BYTES {
        return Err("Configuration must be a single-link regular file of at most 1 MiB".into());
    }
    let mut text = String::new();
    file.take(MAX_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|_| "Cannot read configuration")?;
    if text.len() as u64 > MAX_BYTES {
        return Err("Configuration exceeds 1 MiB".into());
    }
    Ok((text, (metadata.dev(), metadata.ino())))
}
fn lock(path: &Path) -> Result<File, String> {
    let name = format!(".{}.edit.lock", path.file_name().unwrap().to_string_lossy());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path.with_file_name(name))
        .map_err(|_| "Cannot open configuration edit lock")?;
    if !file
        .metadata()
        .map_err(|_| "Cannot inspect edit lock")?
        .is_file()
    {
        return Err("Invalid configuration edit lock".into());
    }
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(|_| "Configuration is being edited by another command")?;
    Ok(file)
}
fn write(file: &mut File, bytes: &[u8]) -> Result<(), String> {
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| "Cannot write private configuration file".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_documents_cannot_overwrite_external_edits() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.toml");
        std::fs::write(&path, "listen='localhost:0'\n").unwrap();
        let mut doc = Document::open(&path, true).unwrap();
        doc.content["execution"] = toml_edit::value(false);
        assert!(Document::open(&path, true).is_err());
        std::fs::write(&path, "listen='localhost:1'\n").unwrap();
        assert!(doc.commit().unwrap_err().contains("concurrently"));
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "listen='localhost:1'\n"
        );
    }
}
