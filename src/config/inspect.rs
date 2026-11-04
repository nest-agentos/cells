//! Read-only policy compilation reuses runtime validation without opening stores or write locks.
use super::{authorization, exports, identity, Config};
use crate::auth::Auth;

pub(crate) fn policy(config: &Config) -> Result<Auth, String> {
    let (effective, _) = crate::plugin::common::PluginRegistry::builtins().resolve(config)?;
    let config = super::plugin_adapter::deployment(effective.as_ref())?;
    identity::validate_identity(config)?;
    exports::validate_presence(config)?;
    crate::harness::config::validate(config)?;
    if let Some(sync) = config.sync.as_ref().filter(|s| s.enabled) {
        crate::sync::SyncService::validate_isolation(sync, config).map_err(|e| e.to_string())?;
    }
    let roots = exports::inspect_all(&config.exports)?;
    authorization::compile(config, &roots, config.server_id.clone())
}
