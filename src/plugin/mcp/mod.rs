//! Agent MCP has a single scope-authorized endpoint; node management uses fixed HTTP APIs.
pub(crate) mod plugin;
mod listener;
pub(crate) use listener::Listener as InternalTransport;
pub use plugin::Mcp as Plugin;

pub(crate) mod http_routes;
