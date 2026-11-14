//! Channel invalidations contain no messages and never acquire the workspace file gate.
use crate::{app::State, core::error::Error, plugin::common::EventStream};
use futures_util::stream;
use serde_json::{json, Value};
use std::sync::Arc;
fn version(state: &State, owner: usize, args: &Value) -> Result<String, Error> {
    let service = state.services.require::<super::ProjectService>()?;
    let project = service.get(
        owner,
        args["projectId"].as_str().ok_or_else(Error::invalid)?,
    )?;
    super::service::authorize(state, owner, &project)?;
    if args["revision"].as_u64() != Some(project.revision) {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }
    service.channels.revision(
        &project.id,
        args["channelId"].as_str().ok_or_else(Error::invalid)?,
    )
}
pub async fn subscribe(state: Arc<State>, owner: usize, args: Value) -> Result<EventStream, Error> {
    let service = state.services.require::<super::ProjectService>()?;
    // A database invalidation receiver owns no filesystem watcher or descriptor.
    // SSE connection and topic budgets already bound these lightweight observers.
    let receiver = service.channels.watch();
    version(&state, owner, &args)?;
    Ok(Box::pin(stream::unfold(
        (state, owner, args, receiver, None),
        |(state, owner, args, mut receiver, mut previous)| async move {
            loop {
                let current = version(&state, owner, &args);
                match current {
                    Err(error) => {
                        return Some((Err(error), (state, owner, args, receiver, previous)))
                    }
                    Ok(current) if previous.as_ref() != Some(&current) => {
                        previous = Some(current.clone());
                        return Some((
                            Ok(json!({"version":current,"scopeId":args["channelId"]})),
                            (state, owner, args, receiver, previous),
                        ));
                    }
                    _ => (),
                }
                tokio::select! {result=receiver.changed()=>{if result.is_err(){return None;}},_=tokio::time::sleep(std::time::Duration::from_secs(15))=>{}}
            }
        },
    )))
}
