//! Queue edits are deterministic; only the worker may dispatch native operations.
use super::*;
use sha2::{Digest, Sha256};
pub(super) fn apply(queue: &mut Value, args: &Value) -> Result<(), Error> {
    let before = queue.clone();
    match args["action"].as_str() {
        Some("enqueue") => enqueue(queue, args)?,
        Some("edit") => edit(queue, &args["edit"])?,
        _ => return Err(Error::invalid()),
    }
    if *queue != before {
        bump(queue)?;
    }
    limits(queue)
}
fn enqueue(queue: &mut Value, args: &Value) -> Result<(), Error> {
    let item = &args["item"];
    validate(item)?;
    if item["state"] != "pending"
        || [
            "receipt",
            "terminal",
            "attemptId",
            "steerTurnId",
            "preparation",
            "queueEpoch",
            "traceContext",
        ]
        .iter()
        .any(|k| item.get(k).is_some())
    {
        return Err(Error::invalid());
    }
    let input = args["input"].as_str().ok_or_else(Error::invalid)?;
    let parsed: Value = serde_json::from_str(input).map_err(|_| Error::invalid())?;
    let mut expected = json!([
        item["text"],
        item["attachments"],
        item["selection"],
        item["commandKind"]
    ]);
    if item["allowSteer"] == true {
        expected.as_array_mut().unwrap().push(json!(true));
    }
    if parsed != expected {
        return Err(Error::invalid());
    }
    let hash = format!("{:x}", Sha256::digest(input.as_bytes()));
    if let Some(row) = queue["receipts"]
        .as_array()
        .ok_or_else(Error::invalid)?
        .iter()
        .find(|r| r["id"] == item["id"])
    {
        if row["input"] != hash {
            return Err(Error::conflict("HARNESS_QUEUE_INPUT_CHANGED"));
        }
        return Ok(());
    }
    queue["items"]
        .as_array_mut()
        .ok_or_else(Error::invalid)?
        .push({let mut accepted=item.clone();accepted["traceContext"]=crate::core::trace::capture();accepted});
    let active = queue["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].clone())
        .collect::<Vec<_>>();
    let receipts = queue["receipts"]
        .as_array_mut()
        .ok_or_else(Error::invalid)?;
    receipts.push(json!({"id":item["id"],"input":hash,"createdAt":item["createdAt"]}));
    while receipts.len() > 256 {
        let index = receipts
            .iter()
            .position(|r| !active.contains(&r["id"]))
            .ok_or_else(Error::invalid)?;
        receipts.remove(index);
    }
    Ok(())
}
fn edit(queue: &mut Value, edit: &Value) -> Result<(), Error> {
    if edit["expectedRevision"] != queue["revision"] {
        return Err(Error::conflict("HARNESS_QUEUE_REVISION_CHANGED"));
    }
    match edit["action"].as_str() {
        Some("pause") => queue["paused"] = json!(true),
        Some("resume") => resume(queue)?,
        Some("retry") => retry(queue, &edit["id"])?,
        Some("remove") => {
            let index = editable(queue, &edit["id"], true)?;
            queue["items"].as_array_mut().unwrap().remove(index);
        }
        Some("update") => update(queue, edit)?,
        Some("reorder") => reorder(queue, &edit["ids"])?,
        Some("merge") => merge(queue, &edit["ids"])?,
        _ => return Err(Error::invalid()),
    }
    Ok(())
}
fn editable(queue: &Value, id: &Value, failed: bool) -> Result<usize, Error> {
    queue["items"]
        .as_array()
        .ok_or_else(Error::invalid)?
        .iter()
        .position(|r| {
            &r["id"] == id && (r["state"] == "pending" || failed && r["state"] == "failed")
        })
        .ok_or_else(|| Error::conflict("HARNESS_QUEUE_ITEM_NOT_EDITABLE"))
}
fn retry(queue: &mut Value, id: &Value) -> Result<(), Error> {
    let index = editable(queue, id, true)?;
    let item = &mut queue["items"][index];
    if item["state"] != "failed" {
        return Err(Error::conflict("HARNESS_QUEUE_RETRY_NOT_REJECTED"));
    }
    validate(item)?;
    item["state"] = json!("pending");
    item["attemptId"] = json!(crate::app::random_epoch().map_err(storage)?);
    for key in [
        "error",
        "dispatchedAt",
        "acceptedAt",
        "steerTurnId",
        "receipt",
        "queueEpoch",
        "preparation",
    ] {
        item.as_object_mut().unwrap().remove(key);
    }
    item["traceContext"]=crate::core::trace::capture();
    queue["paused"] = json!(false);
    Ok(())
}
fn resume(queue: &mut Value) -> Result<(), Error> {
    let items = queue["items"].as_array().ok_or_else(Error::invalid)?;
    if items.iter().any(|r| r["state"] == "uncertain") {
        return Err(Error::conflict("HARNESS_QUEUE_ADMISSION_UNKNOWN"));
    }
    let ids = items
        .iter()
        .filter(|r| r["state"] == "failed")
        .map(|r| r["id"].clone())
        .collect::<Vec<_>>();
    for id in ids {
        retry(queue, &id)?;
    }
    queue["paused"] = json!(false);
    Ok(())
}
fn update(queue: &mut Value, edit: &Value) -> Result<(), Error> {
    let index = editable(queue, &edit["id"], false)?;
    let item = &mut queue["items"][index];
    item["text"] = edit["text"].clone();
    if let Some(binding) = edit.get("commandBinding") {
        item["commandId"] = binding["id"].clone();
        item["commandRevision"] = binding["revision"].clone();
    }
    validate(item)
}
fn ids(value: &Value) -> Result<Vec<Value>, Error> {
    let ids = value
        .as_array()
        .filter(|r| r.len() <= 100)
        .ok_or_else(Error::invalid)?;
    if ids.iter().enumerate().any(|(i, id)| {
        !id.as_str().is_some_and(harness_recovery::valid_id) || ids[..i].contains(id)
    }) {
        return Err(Error::invalid());
    }
    Ok(ids.clone())
}
fn reorder(queue: &mut Value, value: &Value) -> Result<(), Error> {
    let ids = ids(value)?;
    let items = queue["items"].as_array().ok_or_else(Error::invalid)?;
    if ids.len() != items.iter().filter(|r| r["state"] == "pending").count() {
        return Err(Error::invalid());
    }
    let mut ordered = ids
        .iter()
        .map(|id| editable(queue, id, false).map(|i| items[i].clone()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter();
    for item in queue["items"].as_array_mut().unwrap() {
        if item["state"] == "pending" {
            *item = ordered.next().unwrap();
        }
    }
    Ok(())
}
fn merge(queue: &mut Value, value: &Value) -> Result<(), Error> {
    let ids = ids(value)?;
    if ids.len() < 2 {
        return Err(Error::invalid());
    }
    for id in &ids {
        editable(queue, id, false)?;
    }
    let items = queue["items"].as_array().unwrap();
    let selected = items
        .iter()
        .enumerate()
        .filter(|(_, r)| ids.contains(&r["id"]))
        .collect::<Vec<_>>();
    let first = selected[0].0;
    if selected.iter().enumerate().any(|(n, (i, r))| {
        *i != first + n
            || !r["commandKind"].is_null()
            || r["selection"] != selected[0].1["selection"]
    }) {
        return Err(Error::conflict("HARNESS_QUEUE_MERGE_INCOMPATIBLE"));
    }
    let text = selected
        .iter()
        .map(|(_, r)| r["text"].as_str().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n\n");
    let attachments = selected
        .iter()
        .flat_map(|(_, r)| r["attachments"].as_array().into_iter().flatten().cloned())
        .collect::<Vec<_>>();
    let mut item = selected[0].1.clone();
    item["text"] = json!(text);
    item["attachments"] = json!(attachments);
    validate(&item)?;
    let end = first + selected.len();
    queue["items"]
        .as_array_mut()
        .unwrap()
        .splice(first..end, [item]);
    Ok(())
}
pub(super) fn validate(item: &Value) -> Result<(), Error> {
    let text = item["text"].as_str().ok_or_else(Error::invalid)?;
    if !item["id"].as_str().is_some_and(harness_recovery::valid_id)
        || text.len() > 131072
        || item["createdAt"].as_u64().is_none()
    {
        return Err(Error::invalid());
    }
    if text.trim().is_empty() && item["attachments"].as_array().is_none_or(Vec::is_empty) {
        return Err(Error::invalid());
    }
    crate::harness::validate_queue_attachments(text, &item["attachments"])?;
    Ok(())
}
pub(super) fn bump(queue: &mut Value) -> Result<(), Error> {
    let revision = queue["revision"]
        .as_u64()
        .filter(|n| *n < 9_007_199_254_740_991)
        .ok_or_else(Error::invalid)?;
    queue["revision"] = json!(revision + 1);
    Ok(())
}
fn limits(queue: &Value) -> Result<(), Error> {
    let mut payload = queue.clone();
    payload
        .as_object_mut()
        .ok_or_else(Error::invalid)?
        .remove("commandResults");
    if queue["items"].as_array().is_none_or(|r| r.len() > 100)
        || payload.to_string().len() > 4 * 1024 * 1024
    {
        return Err(Error::too_large("HARNESS_QUEUE_LIMIT"));
    }
    Ok(())
}
