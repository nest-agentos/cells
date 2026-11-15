//! Transport-independent primitives shared by every layer.
//!
//! Nothing in `core` knows about Axum routes, exports or business rules: it
//! only provides the error vocabulary, identifier policy, request-scoped
//! scheduling and the small codecs the rest of the service builds on.

pub mod cursor;
pub mod error;
pub mod events;
pub mod gate;
mod gate_admission;
mod gate_owner;
pub(crate) mod gate_read;
pub mod ids;
pub(crate) mod keyed_lock;
pub(crate) mod keyed_gates;
pub mod log_file;
pub mod node_status;
pub(crate) mod path_locks;
pub mod search;
pub(crate) mod sqlite_shards;
pub mod workers;

pub mod auth;

pub mod trace;

pub(crate) mod file_constraints;
