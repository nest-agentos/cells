//! Inode-based resource names also cover export aliases and bind mounts.
use super::Export;
use crate::core::{
    error::Error,
    file_constraints::{CommitGuard, Constraints, Resource},
};
use std::os::unix::fs::MetadataExt;
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
    pub(crate) fn lease_registry(&self) -> Result<&Constraints, Error> {
        self.file_constraints.as_deref().ok_or_else(Error::internal)
    }
    pub(crate) fn constrain(
        &self,
        paths: &[&str],
        remove_leases: bool,
    ) -> Result<Option<CommitGuard<'_>>, Error> {
        let Some(constraints) = &self.file_constraints else {
            return Ok(None);
        };
        let registry = if self.webdav_locks && remove_leases {
            CommitGuard::Write(constraints.write()?)
        } else {
            CommitGuard::Read(constraints.read()?)
        };
        if self.webdav_locks {
            registry.check_execution("commit", Error::busy())?;
        }
        let resources = paths
            .iter()
            .map(|path| self.resource(path))
            .collect::<Result<Vec<_>, _>>()?;
        registry.check(&resources)?;
        Ok(Some(registry))
    }
}
