//! HTTP adapters are assembled separately from feature lifecycle and operation dispatch.
use crate::{app::State, plugins::PluginPlan};
use axum::Router;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
pub struct HttpRoutes {
    entries: BTreeMap<String, Router<Arc<State>>>,
}
impl HttpRoutes {
    /// One HTTP adapter per feature; all adapters inherit the host middleware.
    pub fn register(&mut self, owner: &str, router: Router<Arc<State>>) -> Result<(), String> {
        if !crate::core::ids::is_identifier_within(owner, 64) || self.entries.contains_key(owner) {
            return Err(format!("Invalid or duplicate HTTP adapter owner: {owner}"));
        }
        self.entries.insert(owner.into(), router);
        Ok(())
    }
    pub(crate) fn enabled(&self, plan: &PluginPlan) -> Result<Router<Arc<State>>, String> {
        if let Some(owner) = self
            .entries
            .keys()
            .find(|owner| !plan.statuses().contains_key(*owner))
        {
            return Err(format!(
                "HTTP adapter has no registered plugin owner: {owner}"
            ));
        }
        Ok(self
            .entries
            .iter()
            .filter(|(owner, _)| plan.enabled(owner))
            .fold(Router::new(), |router, (_, routes)| {
                router.merge(routes.clone())
            }))
    }
}
