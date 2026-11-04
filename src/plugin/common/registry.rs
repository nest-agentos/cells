use super::{NodePlugin, PluginConfiguration};
use crate::{app::State, core::ids};
use axum::Router;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Default)]
pub struct PluginRegistry {
    entries: BTreeMap<String, Arc<dyn NodePlugin>>,
}

#[derive(Clone, Default)]
pub struct PluginPlan {
    pub(crate) registered: Vec<Arc<dyn NodePlugin>>,
    pub(crate) ordered: Vec<Arc<dyn NodePlugin>>,
    enabled: BTreeMap<String, bool>,
}

impl PluginRegistry {
    pub fn empty() -> Self {
        Self::default()
    }
    pub fn register(&mut self, plugin: Arc<dyn NodePlugin>) -> Result<(), String> {
        let id = plugin.id();
        if !ids::is_identifier_within(id, ids::IDENTIFIER_MAX) || self.entries.contains_key(id) {
            return Err(format!("Invalid or duplicate node plugin: {id}"));
        }
        self.entries.insert(id.into(), plugin);
        Ok(())
    }
    /// Resolve and validate without opening stores, starting tasks or invoking native processes.
    pub fn resolve(
        &self,
        config: &dyn PluginConfiguration,
    ) -> Result<(Box<dyn PluginConfiguration>, PluginPlan), String> {
        let enabled = self.selection(config)?;
        let plan = self.plan(enabled)?;
        let mut effective = config.snapshot();
        for (id, plugin) in &self.entries {
            plugin.settings(effective.as_mut(), plan.enabled(id));
        }
        *effective.switches_mut() = plan.enabled.clone();
        for plugin in &plan.ordered {
            plugin
                .validate(effective.as_ref())
                .map_err(|error| format!("Plugin {} validation failed: {error}", plugin.id()))?;
        }
        plan.validate_declarations()?;
        Ok((effective, plan))
    }
    fn selection(
        &self,
        config: &dyn PluginConfiguration,
    ) -> Result<BTreeMap<String, bool>, String> {
        for id in config.switches().keys() {
            if !self.entries.contains_key(id) {
                return Err(format!("Unknown node plugin: {id}"));
            }
        }
        Ok(self
            .entries
            .iter()
            .map(|(id, plugin)| {
                let enabled = config
                    .switches()
                    .get(id)
                    .copied()
                    .unwrap_or_else(|| plugin.default_enabled(config));
                (id.clone(), enabled)
            })
            .collect())
    }
    pub(crate) fn all_enabled(&self) -> Result<PluginPlan, String> {
        self.plan(self.entries.keys().map(|id| (id.clone(), true)).collect())
    }
    /// Check one plugin and its dependencies without validating unrelated features.
    pub fn check(
        &self,
        config: &dyn PluginConfiguration,
        target: &str,
    ) -> Result<PluginPlan, String> {
        let mut selected = BTreeSet::new();
        self.select(target, &mut selected)?;
        let mut isolated = config.snapshot();
        for id in self.entries.keys() {
            isolated
                .switches_mut()
                .insert(id.clone(), selected.contains(id));
        }
        for id in selected.iter().filter(|id| id.as_str() != target) {
            if config.switches().get(id) == Some(&false) {
                return Err(format!("Plugin {target} requires enabled plugin {id}"));
            }
        }
        self.resolve(isolated.as_ref()).map(|(_, plan)| plan)
    }
    fn select(&self, id: &str, selected: &mut BTreeSet<String>) -> Result<(), String> {
        let plugin = self
            .entries
            .get(id)
            .ok_or_else(|| format!("Unknown node plugin: {id}"))?;
        if selected.insert(id.into()) {
            for dependency in plugin.dependencies() {
                self.select(dependency, selected)?;
            }
        }
        Ok(())
    }
    fn plan(&self, enabled: BTreeMap<String, bool>) -> Result<PluginPlan, String> {
        let mut traversal = Traversal {
            registry: self,
            enabled: &enabled,
            visiting: BTreeSet::new(),
            done: BTreeSet::new(),
            ordered: vec![],
        };
        for id in self
            .entries
            .keys()
            .filter(|id| enabled.get(*id) == Some(&true))
        {
            traversal.visit(id)?;
        }
        Ok(PluginPlan {
            registered: self.entries.values().cloned().collect(),
            ordered: traversal.ordered,
            enabled,
        })
    }
}
struct Traversal<'a> {
    registry: &'a PluginRegistry,
    enabled: &'a BTreeMap<String, bool>,
    visiting: BTreeSet<String>,
    done: BTreeSet<String>,
    ordered: Vec<Arc<dyn NodePlugin>>,
}
impl Traversal<'_> {
    fn visit(&mut self, id: &str) -> Result<(), String> {
        if self.done.contains(id) {
            return Ok(());
        }
        if !self.visiting.insert(id.into()) {
            return Err(format!("Node plugin dependency cycle: {id}"));
        }
        let plugin = self
            .registry
            .entries
            .get(id)
            .ok_or_else(|| format!("Missing node plugin dependency: {id}"))?
            .clone();
        for dependency in dependencies(&plugin, self.enabled)? {
            self.visit(dependency)?;
        }
        self.visiting.remove(id);
        self.done.insert(id.into());
        self.ordered.push(plugin);
        Ok(())
    }
}
fn dependencies(
    plugin: &Arc<dyn NodePlugin>,
    enabled: &BTreeMap<String, bool>,
) -> Result<Vec<&'static str>, String> {
    for dependency in plugin.dependencies() {
        if enabled.get(*dependency) != Some(&true) {
            return Err(format!(
                "Plugin {} requires enabled plugin {dependency}",
                plugin.id()
            ));
        }
    }
    Ok(plugin
        .dependencies()
        .iter()
        .chain(
            plugin
                .optional_dependencies()
                .iter()
                .filter(|id| enabled.get(**id) == Some(&true)),
        )
        .copied()
        .collect())
}

impl PluginPlan {
    pub(crate) fn stop_admission(&self, state: &State) {
        for plugin in &self.ordered {
            plugin.stop_admission(state);
        }
    }
    pub fn enabled(&self, id: &str) -> bool {
        self.enabled.get(id).copied().unwrap_or(false)
    }
    pub fn statuses(&self) -> &BTreeMap<String, bool> {
        &self.enabled
    }
    pub fn order(&self) -> Vec<&str> {
        self.ordered.iter().map(|plugin| plugin.id()).collect()
    }
    pub(crate) fn configure(
        &self,
        config: &dyn PluginConfiguration,
        state: &mut State,
    ) -> Result<(), String> {
        for plugin in &self.ordered {
            let started = std::time::Instant::now();
            let result = plugin.configure(config, state);
            crate::core::events::emit(
                if result.is_ok() {
                    crate::core::events::Level::Debug
                } else {
                    crate::core::events::Level::Error
                },
                "plugin.assembly",
                serde_json::json!({"plugin":plugin.id(),"phase":"configure","operationId":state.epoch.as_str(),"elapsedMs":started.elapsed().as_millis(),
                    "outcome":if result.is_ok(){"completed"}else{"failed"},"code":if result.is_ok(){None}else{Some("PLUGIN_ASSEMBLY_FAILED")}}),
            );
            result.map_err(|error| format!("Plugin {} assembly failed: {error}", plugin.id()))?;
        }
        Ok(())
    }
    pub(crate) fn routes(&self) -> Router<Arc<State>> {
        self.ordered.iter().fold(Router::new(), |routes, plugin| {
            routes.merge(plugin.routes())
        })
    }
}
