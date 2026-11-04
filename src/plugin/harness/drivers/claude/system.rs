//! Preserve native control output and runtime state without inventing turn completion.
use super::{process::Context, protocol::push};
use crate::core::error::Error;
use serde_json::{json, Value};

pub(super) fn consume(context: &mut Context, frame: &Value) -> Result<(), Error> {
    match frame["subtype"].as_str() {
        Some("session_state_changed") => state(context, &frame["state"]),
        Some("local_command" | "local_command_output") => {
            let output = frame["content"]
                .as_str()
                .or(frame["text"].as_str())
                .or(frame["message"].as_str())
                .unwrap_or("");
            note(context, "output", output)
        }
        Some("compact_boundary") => note(context, "compact", "Context compaction completed"),
        _ => Ok(()),
    }
}
pub(super) fn note(context: &mut Context, kind: &str, text: &str) -> Result<(), Error> {
    if text.is_empty() {
        return Ok(());
    }
    let text = text.chars().take(128 * 1024).collect::<String>();
    context.command_output = true;
    let item = json!({"id":format!("command:{}:{kind}",context.turn),"type":"systemMessage","text":text,"status":"completed","turnId":context.turn});
    context.remember(item.clone());
    push(
        context,
        "item/completed",
        json!({"threadId":context.id,"turnId":context.turn,"item":item}),
    )
}
fn state(context: &mut Context, value: &Value) -> Result<(), Error> {
    let name = value.as_str().or(value["type"].as_str()).unwrap_or("");
    let (status, flags) = match name {
        "idle" => ("idle", vec![]),
        "working" | "running" | "busy" => ("active", vec![]),
        "waiting_for_permission" | "waiting-permission" => ("active", vec!["waitingOnApproval"]),
        "needs_input" | "waiting_for_input" | "waiting-input" => {
            ("active", vec!["waitingOnUserInput"])
        }
        "error" => ("systemError", vec![]),
        _ => return Ok(()),
    };
    let state = json!({"type":status,"activeFlags":flags});
    let mut session = context.session.lock().unwrap();
    session["status"] = json!(status);
    session["statusDetails"] = state.clone();
    session["updatedAt"] = json!(chrono::Utc::now().timestamp_millis());
    drop(session);
    push(
        context,
        "thread/status/changed",
        json!({"threadId":context.id,"status":state}),
    )
}
