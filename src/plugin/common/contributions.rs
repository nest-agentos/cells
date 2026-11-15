use super::PluginPlan;
use crate::app::State;
use std::collections::BTreeSet;

impl PluginPlan {
    pub(super) fn validate_declarations(&self) -> Result<(), String> {
        self.validate_fields()?;
        self.validate_operations()
    }
    fn validate_operations(&self) -> Result<(), String> {
        let mut domains = BTreeSet::new();
        for plugin in &self.registered {
            for domain in plugin.operation_domains() {
                if domain.is_empty() || domain.len() > 64
                    || !domain.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                    || !domains.insert(*domain) {
                    return Err(format!("Invalid or duplicate operation domain: {domain}"));
                }
            }
        }
        Ok(())
    }
    pub fn operation_owner(&self, domain: &str) -> Option<&str> {
        self.ordered.iter().find(|p| p.operation_domains().contains(&domain)).map(|p| p.id())
    }
    pub async fn call(&self, state: &std::sync::Arc<State>, identity: usize,
        domain: &str, operation: &str, args: serde_json::Value,
    ) -> Result<serde_json::Value, crate::core::error::Error> {
        let plugin = self.ordered.iter().find(|p| p.operation_domains().contains(&domain))
            .ok_or_else(crate::core::error::Error::unsupported)?;
        plugin.call(state, identity, domain, operation, args).await
    }
    fn validate_fields(&self) -> Result<(), String> {
        let mut fields = BTreeSet::new();
        for plugin in &self.registered {
            for field in plugin.directory_fields() {
                if ["version", "serverId", "authorization"].contains(field)
                    || !fields.insert(*field)
                {
                    return Err(format!("Duplicate plugin directory field: {field}"));
                }
            }
        }
        Ok(())
    }
    pub fn capabilities(
        &self,
        state: &State,
        identity: usize,
        mut base: serde_json::Map<String, serde_json::Value>,
        http: bool,
    ) -> Result<serde_json::Value, crate::core::error::Error> {
        for plugin in &self.registered {
            let declarations = plugin.capabilities(state, identity, self.enabled(plugin.id()));
            super::extension::merge(
                &mut base,
                if http {
                    declarations.http
                } else {
                    declarations.node
                },
            )?;
        }
        Ok(serde_json::Value::Object(base))
    }
    pub fn directories(
        &self,
        state: &State,
        identity: usize,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<serde_json::Value, crate::core::error::Error> {
        super::extension::snapshot(self, state, identity, cancel)
    }
}

impl PluginPlan {
    /// Disabled policies still own their claims and must reject them rather than ignore them.
    pub fn path_claim_headers(&self) -> Vec<&'static str> {
        self.registered.iter().flat_map(|p| p.path_headers().iter()).copied()
            .collect::<BTreeSet<_>>().into_iter().collect()
    }
    pub fn request_headers(&self) -> Vec<&'static str> {
        self.ordered
            .iter()
            .flat_map(|p| p.request_headers().iter().chain(p.path_headers()))
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub fn authorize_path(
        &self,
        state: &State,
        access: &super::PathAccess<'_>,
    ) -> Result<Vec<tokio::sync::OwnedRwLockReadGuard<()>>, crate::core::error::Error> {
        let mut guards = vec![];
        for plugin in &self.registered {
            if !self.enabled(plugin.id()) {
                if plugin
                    .path_headers()
                    .iter()
                    .any(|header| access.claims.contains_key(*header))
                {
                    return Err(crate::core::error::Error::unsupported());
                }
            } else if let Some(guard) = plugin.authorize_path(state, access)? {
                guards.push(guard);
            }
        }
        Ok(guards)
    }
}
