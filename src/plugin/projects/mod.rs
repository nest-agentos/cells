//! Project catalog and authorization shared by files, processes and harnesses.
mod activity;
mod registration;
pub mod channel;
mod channel_subscription;
mod api;
mod descriptor;
pub(crate) mod directory_sync;
pub mod model;
pub mod runtime;
pub(crate) mod native;
mod search;
pub mod service;
mod source_paths;
pub(crate) mod context;
mod store;
mod watch;
pub use api::call;
pub(crate) use descriptor::capabilities;
pub use model::Config;
pub use service::ProjectService;

pub(crate) mod plugin;

mod contribution;

mod directories;

pub use plugin::Projects as Plugin;

mod access;

pub mod execution;

mod identity;

pub(crate) mod harness_recovery;

pub(crate) mod harness_queue;

mod drafts;
