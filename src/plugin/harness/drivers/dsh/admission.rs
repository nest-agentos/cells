//! Exact native user-message evidence for uncertain prompt admission.
use super::*;
use sha2::{Digest, Sha256};

pub(super) struct PromptAdmission {
    pub id: String,
    digest: [u8; 32],
    started: std::time::Instant,
    pub accepted: bool,
    pub mismatch: bool,
}
impl PromptAdmission {
    pub fn new(id: &str, prompt: &str) -> Self {
        Self {
            id: id.into(),
            digest: Sha256::digest(prompt.as_bytes()).into(),
            started: std::time::Instant::now(),
            accepted: false,
            mismatch: false,
        }
    }
    pub fn observe(&mut self, event: &Value, session: &str) {
        let data = &event["data"];
        if event["type"] != "user/message"
            || !protocol::surface_appends(&event["surfaceOp"])
            || data["source"]["kind"] != "user"
            || data["source"]["rpcId"] != self.id
        {
            return;
        }
        let matched = digest(&data["content"]) == Some(self.digest);
        if self.accepted && matched || self.mismatch {
            return;
        }
        self.accepted = matched;
        self.mismatch = !matched;
        self.record(session, matched);
    }

    fn record(&self, session: &str, matched: bool) {
        crate::core::events::emit(
            if matched {
                crate::core::events::Level::Info
            } else {
                crate::core::events::Level::Warn
            },
            "harness.prompt.evidence",
            json!({"operation":"harness.prompt.reconcile","operationId":self.id,
                "sessionId":crate::core::ids::is_identifier_within(session, 128).then_some(session),
                "stage":"native_user_message","outcome":if matched {"accepted"} else {"unknown"},
                "code":if matched {"DSH_PROMPT_IDENTIFIED"} else {"DSH_PROMPT_EVIDENCE_MISMATCH"},"durationMs":self.started.elapsed().as_millis()}),
        );
    }
    pub fn result(&self) -> Option<Result<Value, Error>> {
        if self.mismatch {
            return Some(Err(Error::unavailable()));
        }
        self.accepted.then(|| Ok(json!({"turnId":self.id})))
    }
}
pub(super) fn digest(value: &Value) -> Option<[u8; 32]> {
    let parts = value.as_array().filter(|parts| parts.len() == 1)?;
    if parts[0]["type"] != "text" {
        return None;
    }
    Some(Sha256::digest(parts[0]["text"].as_str()?.as_bytes()).into())
}

pub(super) fn require_ready(cursor: &protocol::Cursor) -> Result<(), Error> {
    if cursor.control_pending.is_some() {
        return Err(Error::conflict("DSH_CONTROL_RESULT_PENDING"));
    }
    if cursor.command_pending.is_some() {
        return Err(Error::conflict("DSH_COMMAND_RESULT_PENDING"));
    }
    if cursor.admission.is_some() {
        return Err(Error::conflict("DSH_SUBMISSION_RESULT_PENDING"));
    }
    Ok(())
}

impl Dsh {
    pub(super) fn prompt_evidence(&self, id: &str) -> Result<Option<Value>, Error> {
        for live in self.sessions.lock().unwrap().values() {
            let cursor = live.cursor.lock().unwrap();
            if let Some(admission) = cursor.admission.as_ref().filter(|value| value.id == id) {
                if admission.mismatch {
                    return Err(Error::conflict("DSH_PROMPT_EVIDENCE_MISMATCH"));
                }
                return Ok(admission.accepted.then(|| json!({"turnId":id})));
            }
        }
        Ok(None)
    }
    pub(super) fn settle_prompt(&self, id: &str, receipt: &Value) {
        if !matches!(
            receipt["outcome"].as_str(),
            Some("committed" | "not-committed")
        ) {
            return;
        }
        for live in self.sessions.lock().unwrap().values() {
            let mut cursor = live.cursor.lock().unwrap();
            if cursor
                .admission
                .as_ref()
                .is_some_and(|admission| admission.id == id)
            {
                cursor.admission = None;
            }
            if cursor.control_pending.as_deref() == Some(id) {
                cursor.control_pending = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacing_historical_content_is_not_new_prompt_admission() {
        let mut admission = PromptAdmission::new("op", "frozen text");
        let mut event = json!({"type":"user/message","surfaceOp":{"op":"replace","startSeq":0,"endSeq":1},
            "data":{"turn":1,"source":{"kind":"user","rpcId":"op"},"content":[{"type":"text","text":"frozen text"}]}});
        admission.observe(&event, "s");
        assert!(!admission.accepted);
        event["surfaceOp"] = json!({"op":"append"});
        admission.observe(&event, "s");
        assert!(admission.accepted);
    }
}
