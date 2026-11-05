//! Agent MCP has a single scope-authorized endpoint; node management uses fixed HTTP APIs.
pub(crate) mod plugin;
pub use plugin::Mcp as Plugin;
