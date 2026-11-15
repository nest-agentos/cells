//! Inode-based resource names also cover export aliases and bind mounts.
use super::Export;
use crate::core::{
    error::Error,
    file_constraints::{self, Registry, Resource},
};
use std::{os::unix::fs::MetadataExt, sync::MutexGuard};
fn identity(file: &std::fs::File) -> Result<String, Error> {
    let metadata = file.metadata()?;
    Ok(format!("{}:{}", metadata.dev(), metadata.ino()))
}
impl Export {
    pub(crate) fn resource(&self, path: &str) -> Result<Resource, Error> {
        super::path::validate(path)?;
        let mut ancestors = vec![identity(&self.root)?];
        let mut parent = String::new();
        let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
        for part in parts.iter().take(parts.len().saturating_sub(1)) {
            parent = super::export::child_path(&parent, part);
            ancestors.push(identity(&self.open_dir(&parent)?)?);
        }
        let directory = match self.open_dir(path) {
            Ok(file) => Some(identity(&file)?),
            Err(error) if error.is_not_found() || error.code == "ENOTDIR" => None,
            Err(error) => return Err(error),
        };
        let entry = if path.is_empty() {
            format!("root:{}", ancestors[0])
        } else {
            format!("{}/{}", ancestors.last().unwrap(), parts.last().unwrap())
        };
        Ok(Resource {
            entry,
            directory,
            ancestors,
        })
    }
    pub(crate) fn constrain(&self, paths: &[&str]) -> Result<MutexGuard<'static, Registry>, Error> {
        let registry = file_constraints::registry()?;
        if !registry.enabled() {
            return Ok(registry);
        }
        let resources = paths
            .iter()
            .map(|path| self.resource(path))
            .collect::<Result<Vec<_>, _>>()?;
        registry.check(&resources)?;
        Ok(registry)
    }
}
