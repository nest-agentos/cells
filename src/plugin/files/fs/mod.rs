//! Filesystem capability layer.
//!
//! Everything below is expressed as operations on an [`Export`] directory
//! capability. Policy (what a path may look like, when a revision changes)
//! lives in [`path`], [`mutation`] and [`upload`]; mechanism (how a descriptor
//! is obtained and how bytes reach the disk) is confined to [`export`] and the
//! `openat2`/`renameat2` calls inside them.
//!
//! This module never sees an HTTP type beyond the status attached to an error.

pub mod export;
mod metadata;
mod metadata_scope;
pub mod model;
pub mod mutation;
pub mod path;
pub mod recovery;
pub mod registry;
pub mod revision;
pub mod seq;
pub mod upload;
mod upload_journal;

pub use export::Export;
pub use model::{Entry, Stat};
pub use registry::Exports;
