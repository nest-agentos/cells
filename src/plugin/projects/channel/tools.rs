//! Canonical tool contract shared by every native harness adapter.
use serde_json::{json, Value};

pub(crate) fn definitions() -> Vec<Value> {
    vec![
        json!({"name":"session_context","description":"Read a frozen context section. Omit section to list available sections. Load only what the current step needs.","inputSchema":{"type":"object","properties":{"section":{"type":"string"}},"additionalProperties":false},"annotations":{"readOnlyHint":true}}),
        json!({"name":"session_report","description":"Durably publish progress, a question, an error or a delivery application. Reuse id unchanged to recover a lost receipt. Acceptance is not task completion. Resolve clarification reports with type resolved and replyTo before a final result. Read session_context channel-status for report IDs and host acknowledgments.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"message":{"type":"object","properties":{"type":{"type":"string","enum":["progress","clarification","error","result","proposal","resolved"]},"content":{"type":"string","minLength":1,"maxLength":8000},"payload":{},"replyTo":{"type":"string","minLength":1,"maxLength":128}},"required":["type","content"],"additionalProperties":false}},"required":["id","message"],"additionalProperties":false}}),
        json!({"name":"session_request","description":"Request host-resolved live context. If pending, end the turn without a final result; never poll. Reuse the same id and arguments after a lost response.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"method":{"type":"string"},"params":{"type":"object"}},"required":["id","method","params"],"additionalProperties":false}}),
        json!({"name":"session_response","description":"Read an immutable host response after the host resumes this Session.","inputSchema":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false},"annotations":{"readOnlyHint":true}}),
        json!({"name":"session_receipt","description":"Check whether a report was durably accepted without repeating its effect.","inputSchema":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false},"annotations":{"readOnlyHint":true}}),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn report_schema_declares_the_runtime_message_contract() {
        let tools=definitions();
        let tool=tools.iter().find(|tool|tool["name"]=="session_report").unwrap();
        let message=&tool["inputSchema"]["properties"]["message"];
        assert_eq!(message["required"],json!(["type","content"]));
        assert_eq!(message["additionalProperties"],false);
        assert_eq!(message["properties"]["type"]["enum"],json!(["progress","clarification","error","result","proposal","resolved"]));
    }
}
