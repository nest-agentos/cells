//! Deployment-owned DSH transport settings; never accepted from a remote request.
use crate::harness::config::ProfileConfig;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DshConfig {
    pub url: Option<String>,
    pub token_env: Option<String>,
    pub startup_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub command_timeout_ms: u64,
    pub max_sessions: usize,
}
impl Default for DshConfig {
    fn default() -> Self {
        Self {
            url: None,
            token_env: None,
            startup_timeout_ms: 120_000,
            request_timeout_ms: 25_000,
            command_timeout_ms: 600_000,
            max_sessions: 64,
        }
    }
}
pub fn validate(profile: &ProfileConfig) -> Result<(), String> {
    let config = profile.dsh.clone().unwrap_or_default();
    validate_limits(&config)?;
    if let Some(url) = &config.url {
        origin(url)?;
        if profile.projects || profile.workspaces.is_empty() {
            return Err("External DSH requires legacy workspaces and projects = false; existing servers cannot inherit a project sandbox".into());
        }
        if config.token_env.is_none() {
            return Err("External DSH requires token_env".into());
        }
    } else if profile.command == "codex" || config.token_env.is_some() {
        return Err(
            "Managed DSH requires an explicit command and generates its own transport token".into(),
        );
    }
    credentials(config.token_env.as_deref())
}
fn validate_limits(config: &DshConfig) -> Result<(), String> {
    if !(100..=600_000).contains(&config.startup_timeout_ms)
        || !(100..=120_000).contains(&config.request_timeout_ms)
        || !(100..=1_800_000).contains(&config.command_timeout_ms)
        || !(1..=128).contains(&config.max_sessions)
    {
        return Err("Invalid DSH timeout or session limit".into());
    }
    Ok(())
}
fn credentials(key: Option<&str>) -> Result<(), String> {
    let Some(key) = key else {
        return Ok(());
    };
    if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err("Invalid DSH token environment variable".into());
    }
    if std::env::var(key)
        .ok()
        .is_none_or(|value| value.trim().is_empty())
    {
        return Err("DSH token environment variable is missing or empty".into());
    }
    Ok(())
}

pub fn origin(value: &str) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(value).map_err(|_| "Invalid DSH origin")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("DSH URL must be an HTTP(S) origin without path, query or credentials".into());
    }
    Ok(url)
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    #[test]
    fn commands_have_a_distinct_bounded_budget_without_changing_existing_query_timeouts() {
        let default = DshConfig::default();
        assert_eq!(default.request_timeout_ms, 25_000);
        assert_eq!(default.command_timeout_ms, 600_000);
        for (timeout, valid) in [
            (99, false),
            (100, true),
            (600_000, true),
            (1_800_000, true),
            (1_800_001, false),
        ] {
            let profile: ProfileConfig =
                serde_json::from_value(serde_json::json!({"id":"dsh","kind":"dsh","command":"dsh",
                "home":"/unused","dsh":{"command_timeout_ms":timeout}}))
                .unwrap();
            assert_eq!(
                validate(&profile).is_ok(),
                valid,
                "command timeout {timeout}"
            );
        }
    }
}
