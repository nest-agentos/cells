//! Compatibility adapter for the established deployment format; feature implementations own their types.
use super::Config;
use crate::plugin::common::PluginConfiguration;
use std::{any::Any, collections::BTreeMap};
impl PluginConfiguration for Config {
    fn switches(&self) -> &BTreeMap<String, bool> {
        &self.plugins
    }
    fn switches_mut(&mut self) -> &mut BTreeMap<String, bool> {
        &mut self.plugins
    }
    fn option(&self, id: &str) -> Option<&toml::Value> {
        self.plugin_options.get(id)
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
pub(crate) fn deployment(config: &dyn PluginConfiguration) -> Result<&Config, String> {
    config
        .context()
        .downcast_ref()
        .ok_or_else(|| "Built-in plugin requires deployment context".into())
}
pub(crate) fn deployment_mut(config: &mut dyn PluginConfiguration) -> Option<&mut Config> {
    config.context_mut().downcast_mut()
}
