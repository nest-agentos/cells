//! Optional native path aliases derived solely from the project's pinned mount capabilities.
use crate::{
    app::State,
    core::error::Error,
    process::{policy, Mount},
};
use std::{
    os::fd::AsRawFd,
    path::{Path, PathBuf},
};

pub(super) fn expose(state: &State, identity: usize, plan: &mut policy::Plan) -> Result<(), Error> {
    let physical = source_root(plan)?;
    let declarations = declarations(plan, physical.to_str().ok_or_else(Error::invalid)?)?;
    // Reuse the normal namespace/permission rules; aliases cannot shadow runtime paths.
    let mut aliases = policy::validate_mounts(state, identity, &declarations)
        .map_err(|_| Error::forbidden("HARNESS_SOURCE_PATH_UNAVAILABLE"))?;
    for (alias, original) in aliases.iter_mut().zip(&plan.mounts) {
        if alias.alias != original.alias || alias.path != original.path {
            return Err(Error::invalid());
        }
        alias.identity = original.identity.clone();
    }
    plan.mounts.extend(aliases);
    Ok(())
}
fn source_root(plan: &policy::Plan) -> Result<PathBuf, Error> {
    let root = plan
        .mounts
        .iter()
        .find(|mount| mount.at == "/workspace")
        .ok_or_else(Error::invalid)?;
    let directory = root.export.open_dir(&root.path)?;
    let physical = std::fs::read_link(format!("/proc/self/fd/{}", directory.as_raw_fd()))?;
    if ["/workspace", "/projects", "/harness"]
        .iter()
        .any(|root| physical.starts_with(Path::new(root)))
    {
        return Err(Error::forbidden("HARNESS_SOURCE_PATH_UNAVAILABLE"));
    }
    Ok(physical)
}
fn declarations(plan: &policy::Plan, physical: &str) -> Result<Vec<Mount>, Error> {
    plan.mounts
        .iter()
        .map(|mount| {
            Ok(Mount {
                alias: mount.alias.clone(),
                path: mount.path.clone(),
                at: format!(
                    "{}{}",
                    physical,
                    mount
                        .at
                        .strip_prefix("/workspace")
                        .ok_or_else(Error::invalid)?
                ),
                access: if mount.writable { "rw" } else { "ro" }.into(),
            })
        })
        .collect()
}
