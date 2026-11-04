//! HTTP transport tests, one module per route group.

#[path = "../../../common/tests/support/mod.rs"]
mod common;

mod auth;
mod capabilities;
mod content;
mod listing;
mod metadata;
mod mutations;
mod seq;
