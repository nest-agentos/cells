use serde::Deserialize;
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportConfig {
    /// Stable project/export root. The first metadata write creates .nestos/meta.sqlite here.
    /// Subdirectory client mounts share this source-owned database; include it in backups.
    pub path: String,
    /// Client-visible prefix; defaults to the final path component.
    pub alias: Option<String>,
    #[serde(default)]
    pub access: Access,
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub grants: Vec<DirectoryGrant>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryGrant {
    pub principal: String,
    #[serde(default)]
    pub access: Access,
    #[serde(default)]
    pub execute: bool,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    /// Serve reads only; no lock, no revisions.
    #[default]
    Ro,
    /// Serve reads and conditional writes behind an exclusive advisory lock.
    Rw,
}
