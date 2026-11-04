//! Executable projects are directory grants, independent of sync datasets.
use crate::process::Mount;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub root: PathBuf,
    #[serde(default)]
    pub network: bool,
    #[serde(default)]
    pub execution_profiles: Vec<super::execution::ExecutionProfile>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub alias: String,
    pub path: String,
    pub access: String,
    pub revision: u64,
    pub mounts: Vec<Mount>,
    #[serde(deserialize_with = "crate::auth::deserialize_owner")]
    pub owner: String,
    pub root_identity: String,
    pub mount_identities: Vec<String>,
    #[serde(default)]
    pub execution: super::execution::ProjectEnvironment,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Register {
    pub id: Option<String>,
    pub name: String,
    pub alias: String,
    pub path: String,
    pub access: String,
    #[serde(default)]
    pub mounts: Vec<Mount>,
    #[serde(default)]
    pub create_directory: bool,
}
