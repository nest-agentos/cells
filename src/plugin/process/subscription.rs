//! Process observation never changes process ownership or cancellation.
use crate::{app::State, core::error::Error, plugin::common::EventStream};
use futures_util::stream;
use serde_json::Value;
use std::sync::Arc;

pub(super) fn subscribe(
    state: &Arc<State>,
    identity: usize,
    args: Value,
) -> Result<EventStream, Error> {
    let epoch = args["epoch"].as_str().ok_or_else(Error::invalid)?;
    let id = args["processId"].as_str().ok_or_else(Error::invalid)?;
    super::status(state, identity, epoch, id)?;
    let process = state
        .services
        .require::<super::Execution>()?
        .registry()
        .get(identity, id)?
        .ok_or_else(|| Error::not_found("PROCESS_UNKNOWN"))?;
    let changed = process.changes();
    Ok(Box::pin(stream::unfold(
        (process, changed, true, false),
        |(process, mut changed, first, done)| async move {
            if done || !first && changed.changed().await.is_err() {
                return None;
            }
            changed.borrow_and_update();
            let status = process.status();
            let done = status.state != "running";
            Some((
                Ok(serde_json::to_value(status).unwrap()),
                (process, changed, false, done),
            ))
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn completion_notifies_without_periodic_work_or_cancellation_on_drop() {
        let process = Arc::new(super::super::Process::new());
        let mut changes = process.changes();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), changes.changed())
                .await
                .is_err()
        );
        process.complete(super::super::model::Outcome {
            state: "exited",
            code: Some(0),
            stdout: "OK".into(),
            stderr: String::new(),
            truncated: false,
        });
        changes.changed().await.unwrap();
        assert_eq!(process.status().stdout, "OK");
        drop(changes);
        assert!(!process.cancelled());
    }
}
