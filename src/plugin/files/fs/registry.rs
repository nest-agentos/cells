//! The set of directories this process exposes.
//!
//! Aliases are chosen at configuration time and never change at runtime, so a
//! plain sorted map is enough and gives deterministic `/v1/exports` ordering.

use crate::fs::export::Export;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
pub struct Exports {
    by_alias: BTreeMap<String, Arc<Export>>,
}

impl Exports {
    pub fn new(by_alias: BTreeMap<String, Arc<Export>>) -> Self {
        Self { by_alias }
    }

    pub fn get(&self, alias: &str) -> Option<&Arc<Export>> {
        self.by_alias.get(alias)
    }

    pub fn contains(&self, alias: &str) -> bool {
        self.by_alias.contains_key(alias)
    }

    pub fn aliases(&self) -> impl Iterator<Item = &String> {
        self.by_alias.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Arc<Export>)> {
        self.by_alias.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.by_alias.is_empty()
    }

    pub(crate) fn enable_webdav_locks(&mut self, gate: &mut crate::core::gate::FileGate) -> Result<(), String> {
        if !self.by_alias.values().any(|export| export.webdav_locks) {
            return Ok(());
        }
        let constraints = gate.enable_file_constraints();
        for export in self.by_alias.values_mut() {
            Arc::get_mut(export).ok_or("Export already in use")?.file_constraints = Some(constraints.clone());
        }
        Ok(())
    }

    /// Any export, used as the process-level lock anchor.
    pub fn first(&self) -> Option<&Arc<Export>> {
        self.by_alias.values().next()
    }

    /// Conservatively invalidate every writable export, e.g. after a command.
    pub fn invalidate_revisions(&self) {
        for export in self.by_alias.values() {
            export.invalidate_revisions();
        }
    }
}
