//! Retain the response correlated by the original native HTTP RPC until its receipt is durable.
use super::*;

pub(super) struct CommandAdmission {
    pub id: String,
    result: Option<Value>,
    started: std::time::Instant,
}
impl CommandAdmission {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            result: None,
            started: std::time::Instant::now(),
        }
    }
    pub fn record(&mut self, result: Value) -> Result<(), Error> {
        if self.result.as_ref().is_some_and(|prior| prior != &result) {
            return Err(Error::conflict("DSH_COMMAND_EVIDENCE_MISMATCH"));
        }
        if self.result.is_none() {
            report(
                &self.id,
                "native-response",
                "observed",
                self.started.elapsed(),
            );
        }
        self.result = Some(result);
        Ok(())
    }
    fn confirmed_by(&self, receipt: &Value) -> bool {
        receipt["requestId"] == self.id
            && match receipt["outcome"].as_str() {
                Some("committed") => self
                    .result
                    .as_ref()
                    .is_some_and(|value| value == &receipt["result"]),
                Some("not-committed") => self.result.is_none(),
                _ => false,
            }
    }
}

impl Dsh {
    pub(super) fn command_evidence(&self, id: &str) -> Option<Value> {
        self.sessions.lock().unwrap().values().find_map(|live| {
            live.cursor
                .lock()
                .unwrap()
                .command_pending
                .as_ref()
                .filter(|value| value.id == id)
                .and_then(|value| value.result.clone())
        })
    }
    pub(super) fn settle_command(&self, id: &str, receipt: &Value) {
        for (session_id, live) in self.sessions.lock().unwrap().iter() {
            let released = {
                let mut cursor = live.cursor.lock().unwrap();
                let matched = cursor
                    .command_pending
                    .as_ref()
                    .filter(|value| value.id == id && value.confirmed_by(receipt))
                    .map(|value| value.started.elapsed());
                if matched.is_some() {
                    cursor.command_pending = None;
                }
                matched
            };
            if let Some(duration) = released {
                if let Some(runtime) = &self.runtime {
                    runtime.reject(session_id, id);
                }
                report(id, "receipt-confirmed", "released", duration);
            }
        }
    }
}

fn report(id: &str, stage: &str, outcome: &str, duration: std::time::Duration) {
    crate::core::events::emit(
        crate::core::events::Level::Info,
        "harness.command.evidence",
        json!({"operation":"harness.command.reconcile","operationId":id,"stage":stage,"outcome":outcome,"durationMs":duration.as_millis()}),
    );
}
