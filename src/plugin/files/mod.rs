pub(crate) mod directories;
pub mod fs;
pub mod http;
pub mod operations;
pub(crate) mod plugin;

pub mod config;
mod contribution;

pub use plugin::Files as Plugin;

pub(crate) mod http_routes;

pub mod source;
pub(crate) mod reading;
