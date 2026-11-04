use super::Config;
use crate::core::ids;

/// Reject configurations that cannot be served safely.
pub(crate) fn validate_identity(config: &Config) -> Result<(), String> {
    if config.projects.is_some() && config.server_id.is_none() {
        return Err("Project management requires a stable server_id".into());
    }
    if let Some(id) = &config.server_id {
        if !ids::is_identifier_within(id, ids::IDENTIFIER_MAX) {
            return Err("server_id must contain 1..128 ASCII letters, digits, '-' or '_'".into());
        }
    }
    // A username with a token would look like Basic authentication but silently
    // serve Bearer only, so it is rejected instead of ignored.
    if config.username.is_some() && (config.token.is_some() || config.token_env.is_some()) {
        return Err("username cannot be combined with token or token_env".into());
    }
    Ok(())
}
