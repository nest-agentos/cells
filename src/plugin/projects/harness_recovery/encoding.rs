//! Preserve the existing bounded manifest format while atomically updating private state.
use super::*;
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
pub(in crate::plugin::projects) fn decode(
    db: &Connection,
    binding: &Binding,
    head: &Value,
) -> Result<Value, Error> {
    if head["schema"] != 1 {
        return Err(Error::invalid());
    }
    if let Some(text) = head["text"].as_str() {
        return serde_json::from_str(text).map_err(storage);
    }
    let parts = head["parts"]
        .as_u64()
        .filter(|n| (1..=258).contains(n))
        .ok_or_else(Error::invalid)?;
    let generation = head["generation"]
        .as_str()
        .filter(|s| valid_id(s))
        .ok_or_else(Error::invalid)?;
    let mut text = String::new();
    for index in 0..parts {
        let key = format!("{}/recovery/{generation}/{index}", binding.namespace);
        let part: String = db
            .query_row(
                "SELECT body FROM client_state WHERE project=?1 AND owner=?2 AND key=?3",
                params![binding.project_id, binding.owner, key],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?
            .ok_or_else(|| Error::conflict("HARNESS_RECOVERY_STATE_INCOMPLETE"))?;
        if part.len() > 524288 || text.len() + part.len() > 32 * 1024 * 1024 {
            return Err(Error::too_large("HARNESS_RECOVERY_STATE_LIMIT"));
        }
        text.push_str(&part);
    }
    if head["sha256"].as_str() != Some(format!("{:x}", Sha256::digest(text.as_bytes())).as_str()) {
        return Err(Error::conflict("HARNESS_RECOVERY_STATE_INTEGRITY"));
    }
    serde_json::from_str(&text).map_err(storage)
}
pub(in crate::plugin::projects) fn encode(
    db: &Connection,
    binding: &Binding,
    record: &Value,
) -> Result<String, Error> {
    let text = record.to_string();
    if text.len() > 32 * 1024 * 1024 {
        return Err(Error::too_large("HARNESS_RECOVERY_STATE_LIMIT"));
    }
    if text.len() <= 131072 {
        return Ok(json!({"schema":1,"text":text}).to_string());
    }
    let generation = crate::app::random_epoch().map_err(storage)?;
    let mut parts = 0;
    let mut offset = 0;
    while offset < text.len() {
        let mut end = (offset + 131072).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let key = format!("{}/recovery/{generation}/{parts}", binding.namespace);
        db.execute(
            "INSERT INTO client_state VALUES(?1,?2,?3,?4)",
            params![binding.project_id, binding.owner, key, &text[offset..end]],
        )
        .map_err(storage)?;
        offset = end;
        parts += 1;
    }
    Ok(json!({"schema":1,"sha256":format!("{:x}",Sha256::digest(text.as_bytes())),"generation":generation,"parts":parts}).to_string())
}
pub(in crate::plugin::projects) fn cleanup(
    db: &Connection,
    binding: &Binding,
    before: &str,
) -> Result<(), Error> {
    let old: Value = serde_json::from_str(before).map_err(storage)?;
    let Some(generation) = old["generation"].as_str().filter(|s| valid_id(s)) else {
        return Ok(());
    };
    let parts = old["parts"]
        .as_u64()
        .filter(|n| *n <= 258)
        .ok_or_else(Error::invalid)?;
    for index in 0..parts {
        db.execute(
            "DELETE FROM client_state WHERE project=?1 AND owner=?2 AND key=?3",
            params![
                binding.project_id,
                binding.owner,
                format!("{}/recovery/{generation}/{index}", binding.namespace)
            ],
        )
        .map_err(storage)?;
    }
    Ok(())
}
