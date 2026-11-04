//! The declarative configuration file, mapped one-to-one onto TOML.
//!
//! These types carry no behavior beyond deserialization: validation and
//! wiring live in [`crate::config::credentials`], [`crate::config::exports`]
//! and [`crate::app::State`].

use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub plugin_options: std::collections::BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub plugins: std::collections::BTreeMap<String, bool>,
    #[serde(default)]
    pub log_level: crate::core::events::Level,
    #[serde(default)]
    pub logging: crate::core::log_file::Config,
    pub listen: String,
    /// Optional stable identity; otherwise an ephemeral node identity is generated.
    pub server_id: Option<String>,
    #[serde(default = "execution_enabled")]
    pub execution: bool,
    /// Browser origins allowed to read responses (CORS), not an auth control.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub password_env: Option<String>,
    #[serde(alias = "api_key")]
    pub token: Option<String>,
    #[serde(alias = "api_key_env")]
    pub token_env: Option<String>,
    #[serde(default)]
    pub principals: Vec<PrincipalConfig>,
    #[serde(default)]
    pub exports: Vec<ExportConfig>,
    pub sync: Option<crate::sync::Config>,
    pub projects: Option<crate::projects::Config>,
    #[serde(default)]
    pub harnesses: Vec<crate::harness::config::ProfileConfig>,
}

fn execution_enabled() -> bool {
    true
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalConfig {
    pub id: String,
    pub token: Option<String>,
    pub token_env: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub password_env: Option<String>,
}

pub use crate::plugin::files::config::{Access, DirectoryGrant, ExportConfig};
