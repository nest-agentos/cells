//! Transport-independent primitives shared by every layer.
//!
//! Nothing in `core` knows about Axum routes, exports or business rules: it
//! only provides the error vocabulary, identifier policy, request-scoped
//! scheduling and the small codecs the rest of the service builds on.

pub mod cursor;
pub mod error;
pub mod events;
pub mod gate;
mod gate_owner;
pub(crate) mod gate_read;
pub mod ids;
pub mod log_file;
pub mod node_status;
pub mod search;
pub mod workers;

pub mod auth;

pub mod trace;
