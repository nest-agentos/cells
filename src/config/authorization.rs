//! Compile deployment policy into immutable, per-principal directory capabilities.
use super::{
    credentials, exports,
    model::{Access, Config, PrincipalConfig},
};
use crate::{
    auth::{Auth, Client},
    fs::Exports,
};
use std::collections::HashSet;

pub(crate) fn identity_ids(config: &Config) -> Vec<String> {
    let mut ids = if config.principals.is_empty() {
        vec!["owner".into()]
    } else {
        config.principals.iter().map(|p| p.id.clone()).collect()
    };
    if config.exports.iter().any(|e| e.public) {
        ids.push("anonymous".into());
    }
    ids
}

pub(crate) fn compile(
    config: &Config,
    roots: &Exports,
    server: Option<String>,
) -> Result<Auth, String> {
    validate(config)?;
    let mut clients = Vec::new();
    if config.principals.is_empty() {
        clients.push(legacy_client(config, roots)?);
    } else {
        for principal in &config.principals {
            clients.push(client(config, principal)?);
        }
    }
    if config.exports.iter().any(|e| e.public) {
        let aliases = config
            .exports
            .iter()
            .enumerate()
            .filter(|(_, e)| e.public)
            .map(|(i, e)| exports::alias(e, i))
            .collect::<Result<_, _>>()?;
        clients.push(Client::public(aliases));
    }
    bind_resources(&mut clients, config, roots)?;
    validate_credentials(&clients)?;
    Ok(Auth::new(server, clients))
}

fn legacy_client(config: &Config, roots: &Exports) -> Result<Client, String> {
    let c = credentials::resolve(config)?;
    let writes = config
        .exports
        .iter()
        .enumerate()
        .filter(|(_, e)| e.access == Access::Rw)
        .map(|(index, e)| exports::alias(e, index))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Client::new(
        c.secret,
        c.username,
        roots.aliases().cloned().collect(),
        writes,
    ))
}
fn validate_credentials(clients: &[Client]) -> Result<(), String> {
    let mut credentials = HashSet::new();
    let mut usernames = HashSet::new();
    for c in clients.iter().filter(|c| !c.anonymous()) {
        if c.username().is_some_and(|name| !usernames.insert(name)) {
            return Err("Duplicate authentication username".into());
        }
        if !credentials.insert((c.username(), c.secret())) {
            return Err("Duplicate authentication credential".into());
        }
    }
    Ok(())
}

fn bind_resources(clients: &mut [Client], config: &Config, roots: &Exports) -> Result<(), String> {
    for client in clients {
        let mut resources = client
            .exports()
            .iter()
            .map(|alias| resource_identity(roots, alias))
            .collect::<Result<Vec<_>, _>>()?;
        for h in config
            .harnesses
            .iter()
            .filter(|h| client.may_harness(&h.id))
        {
            resources.push(format!("harness:{}:{}", h.id, h.home.display()));
        }
        resources.sort();
        client.bind_resources(&resources);
    }
    Ok(())
}

fn resource_identity(roots: &Exports, alias: &str) -> Result<String, String> {
    use std::os::unix::fs::MetadataExt;
    let directory = roots
        .get(alias)
        .ok_or("Missing directory resource")?
        .open_dir("")
        .map_err(|_| "Directory resource unavailable")?;
    let meta = directory
        .metadata()
        .map_err(|_| "Directory identity unavailable")?;
    Ok(format!(
        "{alias}:{}:{}:{:?}",
        meta.dev(),
        meta.ino(),
        meta.created().ok()
    ))
}

fn client(config: &Config, principal: &PrincipalConfig) -> Result<Client, String> {
    let (secret, username) = credential(principal)?;
    let (mut reads, mut writes, mut execution) = (Vec::new(), Vec::new(), Vec::new());
    for (index, export) in config.exports.iter().enumerate() {
        let alias = exports::alias(export, index)?;
        let grant = export.grants.iter().find(|g| g.principal == principal.id);
        if export.public || grant.is_some() {
            reads.push(alias.clone());
        }
        if grant.is_some_and(|g| g.access == Access::Rw) {
            writes.push(alias.clone());
        }
        if grant.is_some_and(|g| g.execute) {
            execution.push(alias);
        }
    }
    let harnesses = config
        .harnesses
        .iter()
        .filter(|h| h.principal_id.as_deref() == Some(&principal.id))
        .map(|h| h.id.clone())
        .collect();
    Ok(Client::new(secret, username, reads, writes).scoped(
        principal.id.clone(),
        execution,
        harnesses,
    ))
}

fn credential(p: &PrincipalConfig) -> Result<(String, Option<String>), String> {
    let token = credentials::secret("token", p.token.as_deref(), p.token_env.as_deref())?;
    let password =
        credentials::secret("password", p.password.as_deref(), p.password_env.as_deref())?;
    match (token, password) {
        (Some(token), None) if p.username.is_none() && token.len() >= 24 => Ok((token, None)),
        (None, Some(password)) if password.len() >= 8 => {
            let name = p
                .username
                .as_ref()
                .filter(|s| !s.is_empty() && !s.contains([':', '\r', '\n']))
                .ok_or("Principal requires a valid username")?;
            Ok((password, Some(name.clone())))
        }
        _ => Err("Principal requires exactly one valid Bearer token or Basic credential".into()),
    }
}

fn validate(config: &Config) -> Result<(), String> {
    if !config.principals.is_empty()
        && (config.server_id.is_none()
            || config.token.is_some()
            || config.token_env.is_some()
            || config.username.is_some()
            || config.password.is_some()
            || config.password_env.is_some())
    {
        return Err(
            "Multiple principals require server_id and cannot mix legacy credentials".into(),
        );
    }
    let ids = identity_ids(config);
    let mut unique = HashSet::new();
    for id in &ids {
        if !crate::core::ids::is_identifier_within(id, 128) || !unique.insert(id) {
            return Err("Invalid or duplicate principal ID".into());
        }
    }
    if config.principals.iter().any(|p| p.id == "anonymous") {
        return Err("anonymous is a reserved identity".into());
    }
    validate_grants(config, &ids)?;
    validate_harnesses(config, &ids)
}
fn validate_grants(config: &Config, ids: &[String]) -> Result<(), String> {
    for export in &config.exports {
        let mut subjects = HashSet::new();
        for grant in &export.grants {
            if grant.principal == "anonymous"
                || !ids.contains(&grant.principal)
                || !subjects.insert(&grant.principal)
                || grant.access == Access::Rw && export.access != Access::Rw
            {
                return Err("Invalid directory grant or grant exceeds export access".into());
            }
        }
    }
    Ok(())
}

fn validate_harnesses(config: &Config, ids: &[String]) -> Result<(), String> {
    for h in &config.harnesses {
        if !config.principals.is_empty() && (!h.projects || !h.workspaces.is_empty()) {
            return Err("Multi-principal harnesses require project-scoped execution without legacy workspaces".into());
        }
        let owner = h.principal_id.as_deref().unwrap_or("owner");
        if owner == "anonymous"
            || !ids.iter().any(|id| id == owner)
            || !config.principals.is_empty() && h.principal_id.is_none()
        {
            return Err("Harness requires an explicit, configured principal_id".into());
        }
        let root = h
            .home
            .canonicalize()
            .map_err(|_| "Harness home unavailable")?;
        validate_harness_home(config, &root, owner)?;
    }
    Ok(())
}
fn validate_harness_home(
    config: &Config,
    root: &std::path::Path,
    owner: &str,
) -> Result<(), String> {
    if !config.principals.is_empty() {
        for export in &config.exports {
            let path = std::path::Path::new(&export.path)
                .canonicalize()
                .map_err(|_| "Export unavailable")?;
            require_disjoint(
                root,
                &path,
                "Harness homes must be outside every exported directory",
            )?;
        }
    }
    for other in &config.harnesses {
        if other.principal_id.as_deref().unwrap_or("owner") == owner {
            continue;
        }
        let path = other
            .home
            .canonicalize()
            .map_err(|_| "Harness home unavailable")?;
        require_disjoint(
            root,
            &path,
            "Different principals must use disjoint harness homes",
        )?;
    }
    Ok(())
}
fn require_disjoint(
    root: &std::path::Path,
    other: &std::path::Path,
    message: &str,
) -> Result<(), String> {
    if root.starts_with(other) || other.starts_with(root) {
        return Err(message.into());
    }
    Ok(())
}
