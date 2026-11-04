//! Plugin-owned event sources; the transport never interprets domain payloads.
use crate::{app::State, core::error::Error};
use futures_util::{future::BoxFuture, Stream};
use serde_json::Value;
use std::{pin::Pin, sync::Arc};

pub type EventStream = Pin<Box<dyn Stream<Item = Result<Value, Error>> + Send>>;
pub type EventFuture<'a> = BoxFuture<'a, Result<EventStream, Error>>;

impl super::PluginPlan {
    pub fn subscribe<'a>(
        &'a self,
        state: &'a Arc<State>,
        identity: usize,
        plugin: &'a str,
        args: Value,
    ) -> EventFuture<'a> {
        Box::pin(async move {
            let source = self
                .ordered
                .iter()
                .find(|entry| entry.id() == plugin)
                .ok_or_else(Error::unsupported)?;
            source.subscribe(state, identity, args).await
        })
    }
}
