//! Administrator-owned execution profiles and project environment composition.
use crate::{core::error::Error, process::sandbox::Prepared};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectEnvironment {
    pub profile: Option<String>,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
}
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProfile {
    pub id: String,
    pub name: String,
    pub user: Option<String>,
    #[serde(default)]
    pub principals: Vec<String>,
    #[serde(default)]
    pub inherit: Vec<String>,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    #[serde(default)]
    pub mounts: Vec<ConfigMount>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigMount { pub source: PathBuf, pub target: PathBuf }

pub fn validate_variables(values: &BTreeMap<String, String>) -> Result<(), Error> {
    if values.len() > 64 { return Err(invalid_environment("PROJECT_ENV_TOO_MANY")); }
    for (key, value) in values {
        let valid = !key.is_empty() && key.len() <= 128 && key.bytes().enumerate().all(|(i,c)|
            c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()));
        if !valid { return Err(invalid_environment("PROJECT_ENV_INVALID_NAME")); }
        if value.len() > 8192 { return Err(invalid_environment("PROJECT_ENV_VALUE_TOO_LARGE")); }
        if value.contains('\0') { return Err(invalid_environment("PROJECT_ENV_NUL_VALUE")); }
        if key.starts_with("CELLS_") || key.starts_with("NESTOS_") || key.starts_with("FS_AGENT_") || key.starts_with("PI_AGENT_")
            || ["CODEX_HOME", "CLAUDE_CONFIG_DIR", "DSH_HOME", "LD_PRELOAD", "LD_LIBRARY_PATH"].contains(&key.as_str()) {
            return Err(Error::forbidden("PROJECT_ENV_RESERVED"));
        }
    }
    Ok(())
}
fn invalid_environment(code: &'static str) -> Error {
    Error::new(axum::http::StatusCode::BAD_REQUEST, code)
}
pub fn resolve<'a>(profiles: &'a [ExecutionProfile], owner: &str, environment: &ProjectEnvironment)
    -> Result<Option<&'a ExecutionProfile>, Error> {
    validate_variables(&environment.variables)?;
    let Some(id) = &environment.profile else { return Ok(None); };
    profiles.iter().find(|p| &p.id == id && p.principals.iter().any(|p| p == owner))
        .map(Some).ok_or_else(|| Error::forbidden("PROJECT_EXECUTION_PROFILE_DENIED"))
}
pub fn prepare(prepared: &mut Prepared, profile: Option<&ExecutionProfile>, environment: &ProjectEnvironment) -> Result<(), Error> {
    let mut variables = BTreeMap::new();
    if let Some(profile) = profile {
        for key in &profile.inherit {
            let value = std::env::var(key).map_err(|_| Error::not_found("PROJECT_ENV_SOURCE_MISSING"))?;
            variables.insert(key.clone(), value);
        }
        variables.extend(profile.variables.clone());
        for mount in &profile.mounts {
            if !mount.source.is_absolute() || !mount.target.is_absolute() { return Err(Error::invalid()); }
            prepared.command.arg("--ro-bind").arg(&mount.source).arg(&mount.target);
        }
        if let Some(user) = &profile.user { apply_user(&mut prepared.command, user)?; }
    }
    variables.extend(environment.variables.clone());
    validate_variables(&variables)?;
    for (key,value) in variables { prepared.command.args(["--setenv", &key, &value]); }
    Ok(())
}
fn apply_user(command: &mut tokio::process::Command, user: &str) -> Result<(), Error> {
    let passwd = std::fs::read_to_string("/etc/passwd")?;
    let fields = passwd.lines().find_map(|line| {
        let fields: Vec<_> = line.split(':').collect();
        (fields.len() >= 7 && fields[0] == user).then_some(fields)
    }).ok_or_else(|| Error::not_found("PROJECT_EXECUTION_USER_UNKNOWN"))?;
    let uid: u32 = fields[2].parse().map_err(|_| Error::invalid())?;
    let gid: u32 = fields[3].parse().map_err(|_| Error::invalid())?;
    let current = rustix::process::geteuid().as_raw();
    if current != uid && current != 0 { return Err(Error::forbidden("PROJECT_EXECUTION_USER_UNAVAILABLE")); }
    command.args(["--setenv", "USER", user, "--setenv", "LOGNAME", user, "--setenv", "HOME", fields[5]]);
    if current != uid {
        // SAFETY: setgroups is async-signal-safe and only changes the forked child.
        unsafe { command.pre_exec(move || {
            if libc::setgroups(0, std::ptr::null()) != 0 || libc::setgid(gid) != 0 || libc::setuid(uid) != 0 { return Err(std::io::Error::last_os_error()); }
            Ok(())
        }); }
    }
    Ok(())
}

pub fn validate_profiles(profiles: &[ExecutionProfile]) -> Result<(), String> {
    let mut ids = std::collections::HashSet::new();
    for profile in profiles {
        if profile.id.is_empty() || profile.name.trim().is_empty() || !ids.insert(&profile.id) {
            return Err("Execution profiles require unique non-empty IDs and names".into());
        }
        validate_variables(&profile.variables).map_err(|e| format!("Invalid execution profile variables: {}", e.code))?;
        let inherited: BTreeMap<_,_> = profile.inherit.iter().map(|key| (key.clone(), String::new())).collect();
        validate_variables(&inherited).map_err(|e| format!("Invalid inherited execution variable: {}", e.code))?;
        for mount in &profile.mounts {
            if !mount.source.is_absolute() || !mount.source.exists() || !mount.target.is_absolute()
                || mount.target.components().any(|part| matches!(part, std::path::Component::ParentDir)) {
                return Err("Execution profile mount requires an existing absolute source and absolute target".into());
            }
        }
        if let Some(user) = &profile.user {
            let mut command = tokio::process::Command::new("/bin/true");
            apply_user(&mut command, user).map_err(|e| format!("Execution profile user unavailable: {}", e.code))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_reserved_invalid_and_oversized_variables() {
        for key in ["1KEY", "BAD=KEY", "LD_PRELOAD", "CODEX_HOME", "CELLS_PROJECT"] {
            assert!(validate_variables(&BTreeMap::from([(key.into(), "x".into())])).is_err());
        }
        assert!(validate_variables(&BTreeMap::from([("KEY".into(), "x".repeat(8193))])).is_err());
        assert!(validate_variables(&BTreeMap::from([("KEY".into(), "a\0b".into())])).is_err());
    }
    #[test]
    fn requires_explicit_principal_grant_and_preserves_overrides_as_arguments() {
        let mut profile = ExecutionProfile::default();
        profile.id = "dev".into(); profile.name = "Development".into(); profile.principals = vec!["owner".into()];
        profile.variables.insert("GIT_AUTHOR_NAME".into(), "Default".into());
        let environment = ProjectEnvironment { profile: Some("dev".into()), variables: BTreeMap::from([("GIT_AUTHOR_NAME".into(), "Rain Li $(literal)".into())]) };
        assert!(resolve(std::slice::from_ref(&profile), "other", &environment).is_err());
        assert!(resolve(std::slice::from_ref(&profile), "owner", &environment).unwrap().is_some());
        let mut prepared = Prepared { command: tokio::process::Command::new("bwrap"), directories: vec![] };
        prepare(&mut prepared, Some(&profile), &environment).unwrap();
        let args: Vec<_> = prepared.command.as_std().get_args().map(|a| a.to_string_lossy().to_string()).collect();
        assert_eq!(args, ["--setenv", "GIT_AUTHOR_NAME", "Rain Li $(literal)"]);
    }
}
