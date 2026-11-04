//! Exact native forks preserve a committed checkpoint, never a guessed latest head.
use super::*;

impl Codex {
    pub(super) async fn require_checkpoint(
        &self,
        bridge: &Bridge,
        args: &Value,
    ) -> Result<(), Error> {
        let Some(checkpoint) = args.get("lastTurnId") else {
            return Ok(());
        };
        let checkpoint = checkpoint
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 256)
            .ok_or_else(Error::invalid)?;
        let mut cursor = Value::Null;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let page = checkpoint_page(bridge, string(args, "sessionId")?, cursor.clone(), 100)
                .await
                .map_err(|_| Error::conflict("HARNESS_CHECKPOINT_UNAVAILABLE"))?;
            if let Some(turn) = page["data"]
                .as_array()
                .and_then(|rows| rows.iter().find(|turn| turn["id"] == checkpoint))
            {
                return if terminal(turn) {
                    Ok(())
                } else {
                    Err(Error::conflict("HARNESS_CHECKPOINT_NOT_TERMINAL"))
                };
            }
            cursor = page["nextCursor"].clone();
            if cursor.is_null() {
                return Err(Error::not_found("HARNESS_CHECKPOINT_NOT_FOUND"));
            }
            if !cursor.is_string() || !seen.insert(cursor.to_string()) {
                break;
            }
        }
        Err(Error::too_large("HARNESS_CHECKPOINT_HISTORY_LIMIT"))
    }

    pub(super) async fn checkpoint_result(
        &self,
        bridge: &Bridge,
        args: &Value,
        session: &mut Value,
    ) -> Option<Value> {
        let checkpoint = args.get("lastTurnId")?.as_str()?;
        let source = args["sessionId"].as_str()?;
        let id = session["id"].as_str().unwrap_or("");
        let checked = if id.is_empty() || id == source {
            Err(Error::conflict("HARNESS_CHECKPOINT_FORK_IDENTITY"))
        } else {
            verify_fork(bridge, id, checkpoint).await
        };
        let mut result =
            json!({"sourceSessionId":source,"turnId":checkpoint,"verified":checked.is_ok()});
        match checked {
            Ok(status) => {
                session["lastTurnId"] = json!(checkpoint);
                session["lastTurnResult"] = status;
            }
            Err(error) => {
                result["code"] = json!(error.code);
            }
        }
        Some(result)
    }
}

async fn checkpoint_page(
    bridge: &Bridge,
    id: &str,
    cursor: Value,
    limit: u32,
) -> Result<Value, Error> {
    let page = bridge
        .call(
            "thread/turns/list",
            json!({"threadId":id,"cursor":cursor,"limit":limit,
        "sortDirection":"desc","itemsView":"notLoaded"}),
        )
        .await?;
    if !page["data"].is_array() {
        return Err(Error::unavailable());
    }
    Ok(page)
}
async fn verify_fork(bridge: &Bridge, id: &str, checkpoint: &str) -> Result<Value, Error> {
    let page = checkpoint_page(bridge, id, Value::Null, 1)
        .await
        .map_err(|_| Error::conflict("HARNESS_CHECKPOINT_BOUNDARY_UNKNOWN"))?;
    let turn = page["data"]
        .as_array()
        .and_then(|rows| rows.first())
        .ok_or_else(|| Error::conflict("HARNESS_CHECKPOINT_BOUNDARY_UNKNOWN"))?;
    if turn["id"] != checkpoint || !terminal(turn) {
        return Err(Error::conflict("HARNESS_CHECKPOINT_BOUNDARY_MISMATCH"));
    }
    Ok(if turn["status"] == "interrupted" {
        json!("cancelled")
    } else {
        turn["status"].clone()
    })
}
fn terminal(turn: &Value) -> bool {
    matches!(
        turn["status"].as_str(),
        Some("completed" | "failed" | "interrupted" | "cancelled")
    )
}
