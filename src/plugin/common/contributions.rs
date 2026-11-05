use super::PluginPlan;
use crate::app::State;
use std::collections::BTreeSet;

impl PluginPlan {
    pub(super) fn validate_declarations(&self) -> Result<(), String> {
        self.validate_fields()
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
                    .any(|header| access.headers.contains_key(*header))
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
