//! Correlate asynchronous command work with its exact native turn.
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct CommandRuns(HashMap<String, (String, Option<String>)>);
impl CommandRuns {
    pub fn register(
        &mut self,
        thread: &str,
        operation: &str,
    ) -> Result<(), crate::core::error::Error> {
        if self.0.contains_key(thread) {
            return Err(crate::core::error::Error::busy());
        }
        self.0.insert(thread.into(), (operation.into(), None));
        Ok(())
    }
    pub fn forget(&mut self, thread: &str, operation: &str) {
        if self.0.get(thread).is_some_and(|run| run.0 == operation) {
            self.0.remove(thread);
        }
    }
    pub fn observe(&mut self, message: &Value) -> Option<Value> {
        let thread = message["params"]["threadId"].as_str()?;
        let turn = message["params"]["turn"]["id"].as_str()?;
        let run = self.0.get_mut(thread)?;
        match message["method"].as_str()? {
            "turn/started" if run.1.is_none() => {
                run.1 = Some(turn.into());
                Some(
                    json!({"method":"harness/command/updated","params":{"threadId":thread,"commandId":run.0,"turnId":turn,"status":"running"}}),
                )
            }
            "turn/completed" if run.1.as_deref() == Some(turn) => {
                let operation = run.0.clone();
                self.0.remove(thread);
                Some(
                    json!({"method":"harness/command/updated","params":{"threadId":thread,"commandId":operation,"turnId":turn,"status":"completed",
                    "failed":message["params"]["turn"]["status"] != "completed"}}),
                )
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_requires_the_registered_turn_and_retains_command_identity() {
        let mut runs = CommandRuns::default();
        runs.register("session", "compact-operation").unwrap();
        assert!(runs.observe(&json!({"method":"turn/completed","params":{"threadId":"session","turn":{"id":"old","status":"completed"}}})).is_none());
        let start = runs.observe(&json!({"method":"turn/started","params":{"threadId":"session","turn":{"id":"compact"}}})).unwrap();
        assert_eq!(start["params"]["commandId"], "compact-operation");
        assert!(runs.observe(&json!({"method":"turn/completed","params":{"threadId":"session","turn":{"id":"old","status":"completed"}}})).is_none());
        let complete = runs.observe(&json!({"method":"turn/completed","params":{"threadId":"session","turn":{"id":"compact","status":"failed"}}})).unwrap();
        assert_eq!(complete["params"]["status"], "completed");
        assert_eq!(complete["params"]["failed"], true);
        runs.register("session", "next-operation").unwrap();
    }
}
