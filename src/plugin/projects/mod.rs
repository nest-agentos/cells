//! Project catalog and authorization shared by files, processes and harnesses.
mod activity;
pub mod channel;
mod channel_subscription;
mod api;
mod descriptor;
pub(crate) mod directory_sync;
pub mod model;
pub mod runtime;
mod search;
pub mod service;
mod source_paths;
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
