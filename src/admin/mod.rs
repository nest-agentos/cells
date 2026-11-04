//! Local administration only; HTTP clients cannot invoke these configuration operations.
mod command;
mod document;
mod edit;
mod migrate;
mod view;

use crate::config::{inspect, Config};
pub use command::{AccessCommand, Credential};
use serde_json::{json, Value};
use std::path::Path;

pub fn access(path: &Path, command: AccessCommand, dry_run: bool) -> Result<Value, String> {
    let mut file = document::Document::open(path, command.changes_config() && !dry_run)?;
    if !command.changes_config() {
        if dry_run {
            return Err("--dry-run requires a configuration edit".into());
        }
        let config = file.config()?;
        let auth = inspect::policy(&config)?;
        return view::read(&config, &auth, command);
    }
    edit::apply(&mut file.content, command)?;
    let config: Config = file.config()?;
    let auth = inspect::policy(&config)?;
    let backup = if dry_run { None } else { file.commit()? };
    Ok(
        json!({"changed":file.changed(),"dryRun":dry_run,"restartRequired":!dry_run && file.changed(),
        "scope":"configured-policy","config":file.path(),"backup":backup,"policy":view::summary(&config,&auth)?}),
    )
}
