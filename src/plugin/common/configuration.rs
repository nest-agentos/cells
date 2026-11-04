//! Configuration views keep the framework independent of product deployment schemas.
use std::{any::Any, collections::BTreeMap};
pub trait PluginConfiguration: Send + Sync {
    fn switches(&self) -> &BTreeMap<String, bool>;
    fn switches_mut(&mut self) -> &mut BTreeMap<String, bool>;
    fn option(&self, id: &str) -> Option<&toml::Value>;
    fn snapshot(&self) -> Box<dyn PluginConfiguration>;
    fn context(&self) -> &dyn Any;
    fn context_mut(&mut self) -> &mut dyn Any;
}
#[derive(Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PluginSettings {
    pub enabled: BTreeMap<String, bool>,
    pub options: BTreeMap<String, toml::Value>,
}
impl PluginConfiguration for PluginSettings {
    fn switches(&self) -> &BTreeMap<String, bool> {
        &self.enabled
    }
    fn switches_mut(&mut self) -> &mut BTreeMap<String, bool> {
        &mut self.enabled
    }
    fn option(&self, id: &str) -> Option<&toml::Value> {
        self.options.get(id)
    }
    fn snapshot(&self) -> Box<dyn PluginConfiguration> {
        Box::new(self.clone())
    }
    fn context(&self) -> &dyn Any {
        self
    }
    fn context_mut(&mut self) -> &mut dyn Any {
        self
    }
}
