//! Metadata namespaces follow explicitly initialized directory scopes.
use super::{metadata::Metadata, Export};
use crate::core::error::Error;
use rustix::fs::{renameat_with, RenameFlags};
use std::fs::File;

impl Export {
    pub fn initialize_metadata_scope(&self, path: &str, owner: &str, key: &str, value: &serde_json::Value) -> Result<(), Error> {
        if !self.writable() { return Err(Error::forbidden("EROFS")); }
        let _gate = self.metadata_gate.lock().unwrap_or_else(|e| e.into_inner());
        let root = self.open_dir(path)?;
        migrate_directory(&root)?;
        let mut target = Metadata::open(&root, true)?.ok_or_else(Error::internal)?;
        if !path.is_empty() {
            let (parent, relative) = self.metadata_scope(path)?;
            if let Some(mut source) = Metadata::existing_write(&parent)? {
                source.transfer_scope(&mut target, &relative)?;
            }
        }
        target.set_scope_info(owner, key, value)
    }

    pub(crate) fn metadata_scope(&self, path: &str) -> Result<(File, String), Error> {
        let mut parent = path.rsplit_once('/').map(|(parent, _)| parent).unwrap_or("");
        while !parent.is_empty() {
            let root = self.open_dir(parent)?;
            if let Some(store) = Metadata::open(&root, false)? {
                if store.is_scope()? { return Ok((root, path[parent.len() + 1..].to_owned())); }
            }
            parent = parent.rsplit_once('/').map(|(parent, _)| parent).unwrap_or("");
        }
        Ok((self.root.try_clone()?, path.to_owned()))
    }
}

fn migrate_directory(root: &File) -> Result<(), Error> {
    let legacy = super::metadata::metadata_directory(root)? == ".mindos";
    if !legacy { return Ok(()); }
    renameat_with(root, ".mindos", root, ".nestos", RenameFlags::NOREPLACE)?;
    root.sync_all()?;
    crate::core::events::emit(crate::core::events::Level::Info, "metadata.directory.migrated",
        serde_json::json!({"stage":"rename","result":"success"}));
    Ok(())
}
