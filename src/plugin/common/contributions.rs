use super::PluginPlan;
use crate::app::State;
use std::collections::BTreeSet;

impl PluginPlan {
    pub(super) fn validate_declarations(&self) -> Result<(), String> {
        self.validate_fields()?;
        self.validate_tools()
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
    fn validate_tools(&self) -> Result<(), String> {
        let mut tools = BTreeSet::new();
        for plugin in &self.ordered {
            if let Some(provider) = plugin.mcp() {
                for tool in provider.tools() {
                    let name = tool["name"]
                        .as_str()
                        .filter(|s| crate::core::ids::is_identifier_within(s, 128))
                        .ok_or("Invalid plugin tool name")?;
                    if !tools.insert(name) {
                        return Err(format!("Duplicate plugin tool: {name}"));
                    }
                }
            }
        }
        Ok(())
    }
    pub fn tools(&self, state: &State, identity: usize) -> Vec<serde_json::Value> {
        self.ordered
            .iter()
            .filter_map(|p| p.mcp())
            .flat_map(|p| {
                p.tools()
                    .iter()
                    .filter(|t| p.available(state, identity, t["name"].as_str().unwrap()))
                    .cloned()
            })
            .collect()
    }
    pub fn tool(
        &self,
        state: &State,
        identity: usize,
        name: &str,
    ) -> Option<&dyn super::McpContribution> {
        self.ordered.iter().filter_map(|p| p.mcp()).find(|p| {
            p.tools().iter().any(|t| t["name"] == name) && p.available(state, identity, name)
        })
    }
    pub fn resources(
        &self,
        state: &State,
        identity: usize,
    ) -> Result<Vec<serde_json::Value>, crate::core::error::Error> {
        let mut uris = BTreeSet::new();
        let values: Vec<_> = self
            .ordered
            .iter()
            .filter_map(|p| p.mcp())
            .flat_map(|p| p.resources(state, identity))
            .collect();
        for value in &values {
            let uri = value["uri"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(crate::core::error::Error::invalid)?;
            if !uris.insert(uri) {
                return Err(crate::core::error::Error::conflict(
                    "PLUGIN_RESOURCE_CONFLICT",
                ));
            }
        }
        Ok(values)
    }
    pub fn resource(
        &self,
        state: &State,
        identity: usize,
        uri: &str,
    ) -> Result<Option<&dyn super::McpContribution>, crate::core::error::Error> {
        self.resources(state, identity)?;
        Ok(self
            .ordered
            .iter()
            .filter_map(|p| p.mcp())
            .find(|p| p.resources(state, identity).iter().any(|r| r["uri"] == uri)))
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
                    declarations.mcp
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
