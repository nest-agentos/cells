//! Wire-compatible file metadata, independent of a storage implementation.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReadFields {
    Entry,
    #[default]
    Stat,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stat {
    pub kind: &'static str,
    pub size: u64,
    pub modified_at: u64,
    pub created_at: u64,
    /// Present when the source supplies a validator for this version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(skip_serializing_if = "serde_json::Map::is_empty")]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_identity: Option<String>,
}

#[derive(Serialize)]
pub struct Entry {
    pub name: String,
    pub stat: Stat,
}

/// Result of one directory scan.
#[derive(Default)]
pub struct Listing {
    pub entries: Vec<Entry>,
    pub warnings: Vec<&'static str>,
}
