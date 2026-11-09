//! Recovery commands and queue updates commit in the same private Cells transaction.
use super::*;
use rusqlite::{params, OptionalExtension, Transaction};

pub(in crate::plugin::projects) fn schema(db: &rusqlite::Connection) -> Result<(), Error> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS harness_bindings(project TEXT,owner TEXT,namespace TEXT,binding TEXT NOT NULL,PRIMARY KEY(project,owner,namespace));
        CREATE TABLE IF NOT EXISTS harness_recoveries(owner TEXT,operation TEXT,fingerprint TEXT NOT NULL,result TEXT NOT NULL,PRIMARY KEY(owner,operation));").map_err(storage)
}
pub(in crate::plugin::projects) fn load(
    channels: &Channels,
    binding: &Binding,
) -> Result<(String, Value), Error> {
    let db = channels.2.lock().map_err(storage)?;
    let key = format!("{}/recovery", binding.namespace);
    let raw: String = db
        .query_row(
            "SELECT body FROM client_state WHERE project=?1 AND owner=?2 AND key=?3",
            params![binding.project_id, binding.owner, key],
            |r| r.get(0),
        )
        .optional()
        .map_err(storage)?
        .ok_or_else(|| Error::not_found("HARNESS_RECOVERY_STATE_MISSING"))?;
    let head: Value = serde_json::from_str(&raw).map_err(storage)?;
    let mut record = super::encoding::decode(&db, binding, &head)?;
    if let Some(queue) = super::super::harness_queue::store::load(&db, binding)? {
        record["instructionQueue"] = queue;
    }
    if record["sessionId"] != binding.session_id {
        return Err(Error::conflict("HARNESS_RECOVERY_SESSION_CHANGED"));
    }
    Ok((raw, record))
}
pub(in crate::plugin::projects) fn register(
    channels: &Channels,
    binding: &Binding,
) -> Result<(), Error> {
    let db = channels.2.lock().map_err(storage)?;
    let previous: Option<String> = db
        .query_row(
            "SELECT binding FROM harness_bindings WHERE project=?1 AND owner=?2 AND namespace=?3",
            params![binding.project_id, binding.owner, binding.namespace],
            |r| r.get(0),
        )
        .optional()
        .map_err(storage)?;
    if let Some(previous) = previous {
        let old: Binding = serde_json::from_str(&previous).map_err(storage)?;
        if old.profile_id != binding.profile_id || old.session_id != binding.session_id {
            return Err(Error::conflict("HARNESS_RECOVERY_BINDING_CHANGED"));
        }
    }
    db.execute("INSERT INTO harness_bindings VALUES(?1,?2,?3,?4) ON CONFLICT(project,owner,namespace) DO UPDATE SET binding=excluded.binding", params![binding.project_id,binding.owner,binding.namespace,serde_json::to_string(binding).map_err(storage)?]).map_err(storage)?;
    Ok(())
}
pub(in crate::plugin::projects) fn prior(
    channels: &Channels,
    binding: &Binding,
    args: &Value,
) -> Result<Option<Value>, Error> {
    let db = channels.2.lock().map_err(storage)?;
    receipt(&db, binding, args)
}
fn receipt(
    db: &rusqlite::Connection,
    binding: &Binding,
    args: &Value,
) -> Result<Option<Value>, Error> {
    let found: Option<(String, String)> = db
        .query_row(
            "SELECT fingerprint,result FROM harness_recoveries WHERE owner=?1 AND operation=?2",
            params![binding.owner, args["operationId"].as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    found
        .map(|(fingerprint, result)| {
            if fingerprint != fingerprint_of(binding, args)? {
                return Err(Error::conflict("HARNESS_RECOVERY_OPERATION_CHANGED"));
            }
            let result: Value = serde_json::from_str(&result).map_err(storage)?;
            if result["status"] == "pending" {
                return Ok(None);
            }
            Ok(Some(result))
        })
        .transpose()
        .map(Option::flatten)
}
pub(in crate::plugin::projects) fn reserve(
    channels: &Channels,
    binding: &Binding,
    args: &Value,
) -> Result<(), Error> {
    let mut db = channels.2.lock().map_err(storage)?;
    let tx = db.transaction().map_err(storage)?;
    receipt(&tx, binding, args)?;
    tx.execute(
        "INSERT OR IGNORE INTO harness_recoveries VALUES(?1,?2,?3,?4)",
        params![
            binding.owner,
            args["operationId"].as_str(),
            fingerprint_of(binding, args)?,
            json!({"status":"pending"}).to_string()
        ],
    )
    .map_err(storage)?;
    tx.commit().map_err(storage)
}
fn fingerprint_of(binding: &Binding, args: &Value) -> Result<String, Error> {
    Ok(json!([binding, args["instructionId"], args["expectedRevision"]]).to_string())
}
pub(in crate::plugin::projects) fn commit(
    channels: &Channels,
    binding: &Binding,
    args: &Value,
    before: &str,
    record: &Value,
    outcome: &str,
) -> Result<Value, Error> {
    let mut db = channels.2.lock().map_err(storage)?;
    let tx = db.transaction().map_err(storage)?;
    if let Some(result) = receipt(&tx, binding, args)? {
        return Ok(result);
    }
    replace(&tx, binding, before, record)?;
    let result = json!({"operationId":args["operationId"],"outcome":outcome});
    tx.execute("INSERT INTO harness_recoveries VALUES(?1,?2,?3,?4) ON CONFLICT(owner,operation) DO UPDATE SET result=excluded.result",params![binding.owner,args["operationId"].as_str(),fingerprint_of(binding,args)?,result.to_string()]).map_err(storage)?;
    tx.commit().map_err(storage)?;
    Ok(result)
}
pub(in crate::plugin::projects) fn replace(
    tx: &Transaction<'_>,
    binding: &Binding,
    before: &str,
    record: &Value,
) -> Result<(), Error> {
    if super::super::harness_queue::store::load(tx, binding)?.is_some() {
        let current: Option<String> = tx
            .query_row(
                "SELECT body FROM client_state WHERE project=?1 AND owner=?2 AND key=?3",
                params![
                    binding.project_id,
                    binding.owner,
                    format!("{}/recovery", binding.namespace)
                ],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        if current.as_deref() != Some(before) {
            return Err(Error::conflict("HARNESS_RECOVERY_QUEUE_CHANGED"));
        }
        return super::super::harness_queue::store::replace(
            tx,
            binding,
            &record["instructionQueue"],
        );
    }
    let body = super::encoding::encode(tx, binding, record)?;
    let changed = tx
        .execute(
            "UPDATE client_state SET body=?4 WHERE project=?1 AND owner=?2 AND key=?3 AND body=?5",
            params![
                binding.project_id,
                binding.owner,
                format!("{}/recovery", binding.namespace),
                body,
                before
            ],
        )
        .map_err(storage)?;
    if changed != 1 {
        return Err(Error::conflict("HARNESS_RECOVERY_QUEUE_CHANGED"));
    }
    super::encoding::cleanup(tx, binding, before)?;
    Ok(())
}
pub(in crate::plugin::projects) fn bindings(channels: &Channels) -> Result<Vec<Binding>, Error> {
    let db = channels.2.lock().map_err(storage)?;
    let mut statement = db
        .prepare("SELECT binding FROM harness_bindings")
        .map_err(storage)?;
    let rows = statement
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|row| serde_json::from_str(&row.map_err(storage)?).map_err(storage))
        .collect()
}
