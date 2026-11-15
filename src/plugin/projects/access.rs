use crate::{app::State, core::error::Error};
use std::collections::BTreeMap;
/// Project-aware clients keep the original file protocol but carry a directory fence.
pub fn project_path(
    state: &State,
    identity: usize,
    claims: &BTreeMap<String, String>,
    alias: &str,
    path: &str,
    write: bool,
    metadata: bool,
) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, Error> {
    let Some(id) = claims.get("x-fsagent-project") else {
        return Ok(None);
    };
    let id = id.as_str();
    let project = load_project(state, identity, claims, id)?;
    let service = state.services.require::<super::ProjectService>()?;
    let guard = service.lifecycle(id).shared()?;
    // Only the revision needs a second read after admission; avoid repeating filesystem authorization.
    if service.get(identity, id)?.revision != project.revision {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }
    if write
        && claims
            .get("x-fsagent-project-readonly")
            .is_some_and(|h| h == "true")
    {
        return Err(Error::forbidden("EROFS"));
    }
    crate::fs::path::validate(path)?;
    if project_allows(&project, alias, path, write, metadata) {
        Ok(Some(guard))
    } else {
        Err(Error::forbidden(if write { "EROFS" } else { "EACCES" }))
    }
}
fn beneath(path: &str, root: &str) -> bool {
    root.is_empty() || path == root || path.starts_with(&(root.to_owned() + "/"))
}
fn project_allows(
    project: &crate::projects::model::Project,
    alias: &str,
    path: &str,
    write: bool,
    metadata: bool,
) -> bool {
    let mounts = crate::projects::service::mounts(project);
    mounts.iter().filter(|m| m.alias == alias).any(|mount| {
        if metadata && !write && beneath(&mount.path, path) {
            return true;
        }
        if !beneath(path, &mount.path) || write && mount.access != "rw" {
            return false;
        }
        let virtual_path = virtual_path(mount, path);
        let shadow = mounts
            .iter()
            .filter(|m| beneath(&virtual_path, &m.at))
            .max_by_key(|m| m.at.len())
            .unwrap();
        let suffix = virtual_path[shadow.at.len()..].trim_start_matches('/');
        let source = [shadow.path.as_str(), suffix]
            .into_iter()
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>()
            .join("/");
        shadow.alias == alias && source == path && (!write || shadow.access == "rw")
    })
}

fn load_project(
    state: &State,
    identity: usize,
    claims: &BTreeMap<String, String>,
    id: &str,
) -> Result<super::model::Project, Error> {
    let project = state
        .services
        .get::<crate::projects::ProjectService>()
        .ok_or_else(Error::unsupported)?
        .get(identity, id)?;
    crate::projects::service::authorize(state, identity, &project)?;
    if claims
        .get("x-fsagent-project-revision")
        .and_then(|h| h.parse().ok())
        != Some(project.revision)
    {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }

    Ok(project)
}

fn virtual_path(mount: &crate::process::Mount, path: &str) -> String {
    let relative = path
        .strip_prefix(&mount.path)
        .unwrap_or(path)
        .trim_start_matches('/');
    let virtual_path = if relative.is_empty() {
        mount.at.clone()
    } else {
        format!("{}/{}", mount.at, relative)
    };

    virtual_path
}
