//! Pure transition rules consume node-observed evidence, never client assertions.
use super::*;
pub(super) fn recover(
    mut record: Value,
    session: &Value,
    history: &Value,
    args: &Value,
    automatic: bool,
) -> Result<(Value, &'static str), Error> {
    let id = &args["instructionId"];
    let queue = &record["instructionQueue"];
    if rows(&queue["recoveries"]).any(|row| &row["id"] == id) {
        return Ok((record, "recovered"));
    }
    if !rows(&queue["items"]).any(|row| &row["id"] == id)
        && rows(&queue["receipts"]).any(|row| &row["id"] == id && row["turnId"].is_string())
    {
        return Ok((record, "no-longer-blocked"));
    }
    let item = &queue["items"][0];
    if queue["revision"] != args["expectedRevision"] || &item["id"] != id {
        return Err(Error::conflict("HARNESS_RECOVERY_QUEUE_CHANGED"));
    }
    eligible(&record, item)?;
    let turn = item["receipt"]["turnId"]
        .as_str()
        .ok_or_else(|| Error::conflict("HARNESS_RECOVERY_ADMISSION_UNKNOWN"))?;
    if let Some(result) = terminal(session, history, item, turn)? {
        if session["activeTurnId"].as_str().is_some() {
            return Err(Error::busy());
        }
        let failed = result != "completed";
        let queue = &mut record["instructionQueue"];
        queue["items"]
            .as_array_mut()
            .ok_or_else(Error::invalid)?
            .remove(0);
        if failed {
            queue["paused"] = json!(true);
        }
        increment(queue)?;
        return Ok((record, "no-longer-blocked"));
    }
    detached(session)?;
    if automatic && !restart_matches(session, item, turn) {
        return Err(Error::conflict("HARNESS_RECOVERY_RESTART_PROOF_REQUIRED"));
    }
    let recovery = json!({"id":item["id"],"attemptId":item["attemptId"],"turnId":turn,"originalExecution":item["execution"],"recoveredAt":chrono::Utc::now().timestamp_millis(),"outcome":"unknown","evidence":session["executionRecovery"]});
    let queue = &mut record["instructionQueue"];
    if queue["recoveries"].is_null() {
        queue["recoveries"] = json!([]);
    }
    let recoveries = queue["recoveries"]
        .as_array_mut()
        .ok_or_else(Error::invalid)?;
    if recoveries.len() >= 64 {
        return Err(Error::too_large("HARNESS_RECOVERY_HISTORY_FULL"));
    }
    let mut recovery = recovery;
    for key in ["attemptId", "originalExecution"] {
        if recovery[key].is_null() {
            recovery.as_object_mut().unwrap().remove(key);
        }
    }
    if automatic {
        recovery["source"] = json!("system-restart");
    }
    recoveries.push(recovery);
    queue["items"]
        .as_array_mut()
        .ok_or_else(Error::invalid)?
        .remove(0);
    if !automatic {
        queue["paused"] = json!(true);
    }
    increment(queue)?;
    Ok((record, "recovered"))
}
fn rows(value: &Value) -> impl Iterator<Item = &Value> {
    value.as_array().into_iter().flatten()
}
fn eligible(record: &Value, item: &Value) -> Result<(), Error> {
    let blocked = record["pending"].is_object()
        || rows(&record["responses"]).next().is_some()
        || rows(&record["controls"]).any(|r| {
            r["receipt"] == "pending"
                || r["receipt"] == "unknown"
                || r["receipt"] == "accepted" && r["terminal"].is_null()
        })
        || rows(&record["commandResults"])
            .any(|r| r["status"] == "running" || r["resultError"].is_object());
    if blocked {
        return Err(Error::conflict("HARNESS_RECOVERY_PENDING_OPERATIONS"));
    }
    if item["state"] != "dispatching"
        || !item["commandKind"].is_null()
        || !item["steerTurnId"].is_null()
        || item["receipt"]["resultError"].is_object()
    {
        return Err(Error::conflict("HARNESS_RECOVERY_HEAD_UNSAFE"));
    }
    Ok(())
}
fn terminal<'a>(
    session: &'a Value,
    history: &'a Value,
    item: &'a Value,
    turn: &str,
) -> Result<Option<&'a str>, Error> {
    let saved = item["terminal"]["result"]
        .as_str()
        .filter(|_| item["terminal"]["turnId"] == turn);
    let observed = rows(&history["turns"])
        .find(|row| row["id"] == turn)
        .and_then(|row| row["status"].as_str())
        .or_else(|| {
            session["lastTurnResult"]
                .as_str()
                .filter(|_| session["lastTurnId"] == turn)
        });
    let terminal =
        |value: Option<&'a str>| value.filter(|r| ["completed", "failed", "cancelled"].contains(r));
    let (saved, observed) = (terminal(saved), terminal(observed));
    if saved.is_some() && observed.is_some() && saved != observed {
        return Err(Error::conflict("HARNESS_RECOVERY_TERMINAL_CONFLICT"));
    }
    Ok(observed.or(saved))
}
fn increment(queue: &mut Value) -> Result<(), Error> {
    let revision = queue["revision"]
        .as_u64()
        .filter(|r| *r < 9_007_199_254_740_991)
        .ok_or_else(Error::invalid)?;
    queue["revision"] = json!(revision + 1);
    Ok(())
}
fn detached(session: &Value) -> Result<(), Error> {
    let evidence = &session["executionRecovery"];
    let lock = &session["writerLock"];
    let checked = evidence["checkedAt"]
        .as_i64()
        .ok_or_else(|| Error::conflict("HARNESS_RECOVERY_NOT_DETACHED"))?;
    let age = chrono::Utc::now().timestamp_millis() - checked;
    if evidence["state"] != "detached"
        || session["owned"] != false
        || session["loaded"] != false
        || session["status"] != "notLoaded"
        || session["activeTurnId"].as_str().is_some()
        || lock["status"] != "not-held"
        || lock["checkedAt"] != evidence["checkedAt"]
        || !(0..30000).contains(&age)
    {
        return Err(Error::conflict("HARNESS_RECOVERY_NOT_DETACHED"));
    }
    Ok(())
}
pub(in crate::plugin::projects) fn restart_matches(
    session: &Value,
    item: &Value,
    turn: &str,
) -> bool {
    let request = item["attemptId"].as_str().or_else(|| item["id"].as_str());
    rows(&session["executionRecovery"]["interruptedTurns"]).any(|row| {
        row["turnId"] == turn
            && row["requestId"].as_str() == request
            && row["reason"] == "CELLS_RESTARTED"
    })
}
