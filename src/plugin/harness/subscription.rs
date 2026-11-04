//! Follow native runtime generations without pinning drivers or restarting idle runtimes.
use super::service::Profile;
use crate::{core::error::Error, plugin::common::EventStream};
use futures_util::{stream, StreamExt};
use serde_json::Value;
use std::sync::Arc;

pub(super) fn stream(
    profile: Arc<Profile>,
    generation: tokio::sync::watch::Receiver<u64>,
    args: Value,
    source: EventStream,
) -> EventStream {
    Box::pin(stream::unfold(
        (profile, generation, args, Some(source)),
        |(profile, mut generation, args, mut source)| async move {
            loop {
                if profile.driver.closed() {
                    return None;
                }
                if source.is_none() {
                    generation.borrow_and_update();
                    source = match profile.driver.peek() {
                        Some(driver) => match driver.subscribe(args.clone()).await {
                            Ok(source) => Some(source),
                            Err(error) => {
                                return Some((Err(error), (profile, generation, args, None)))
                            }
                        },
                        None => {
                            if generation.changed().await.is_err() {
                                return None;
                            }
                            continue;
                        }
                    };
                }
                tokio::select! {
                    changed = generation.changed() => { if changed.is_err() { return None; } source = None; },
                    next = source.as_mut().unwrap().next() => match next {
                        Some(value) => return Some((value, (profile, generation, args, source))),
                        None => { source = None; if generation.changed().await.is_err() { return Some((Err(Error::unavailable()), (profile, generation, args, None))); } }
                    }
                }
            }
        },
    ))
}
