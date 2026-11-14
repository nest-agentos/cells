use super::*;
use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};
pub(super) fn schema(db: &rusqlite::Connection) -> Result<(), Error> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS owned_harness_queues(project TEXT,owner TEXT,namespace TEXT,profile TEXT NOT NULL,session TEXT NOT NULL,PRIMARY KEY(project,owner,namespace));
    CREATE TABLE IF NOT EXISTS harness_queue_sessions(project TEXT,owner TEXT,profile TEXT,session TEXT,body TEXT NOT NULL,PRIMARY KEY(project,owner,profile,session));
    CREATE TABLE IF NOT EXISTS harness_queue_commands(owner TEXT,operation TEXT,fingerprint TEXT NOT NULL,result TEXT NOT NULL,PRIMARY KEY(owner,operation));").map_err(storage)
}
pub(in crate::plugin::projects) fn take_ownership(
    channels: &Channels,
    binding: &Binding,
) -> Result<(), Error> {
    harness_recovery::store::load(channels, binding)?;
    channels.2.bind(binding)?;
    if owns(channels, binding)? {
        return Ok(());
    }
    let (raw, mut record) = harness_recovery::store::load(channels, binding)?;
    if record["instructionQueue"].is_null() {
        record["instructionQueue"] = json!({"revision":0,"paused":false,"items":[],"receipts":[]});
    }
    let mut queue = record
        .as_object_mut()
        .unwrap()
        .remove("instructionQueue")
        .unwrap();
    queue["owner"] = json!("cells");
    channels.2.with(&binding.project_id,&binding.owner,&binding.namespace,|db|{
    let tx = db.transaction().map_err(storage)?;
    let existing:Option<String>=tx.query_row("SELECT body FROM harness_queue_sessions WHERE project=?1 AND owner=?2 AND profile=?3 AND session=?4",params![binding.project_id,binding.owner,binding.profile_id,binding.session_id],|r|r.get(0)).optional().map_err(storage)?;
    if let Some(existing) = existing {
        let canonical: Value = serde_json::from_str(&existing).map_err(storage)?;
        if queue["items"].as_array().is_some_and(|r| !r.is_empty()) && queue != canonical {
            return Err(Error::conflict("HARNESS_QUEUE_MIGRATION_CONFLICT"));
        }
    } else {
        tx.execute(
            "INSERT INTO harness_queue_sessions VALUES(?1,?2,?3,?4,?5)",
            params![
                binding.project_id,
                binding.owner,
                binding.profile_id,
                binding.session_id,
                queue.to_string()
            ],
        )
        .map_err(storage)?;
    }
    harness_recovery::store::replace(&tx, binding, &raw, &record)?;
    tx.execute(
        "INSERT INTO owned_harness_queues VALUES(?1,?2,?3,?4,?5)",
        params![
            binding.project_id,
            binding.owner,
            binding.namespace,
            binding.profile_id,
            binding.session_id
        ],
    )
    .map_err(storage)?;
    tx.commit().map_err(storage)
})
}

pub(super) fn owns(channels: &Channels, binding: &Binding) -> Result<bool, Error> {
    channels.2.with(&binding.project_id,&binding.owner,&binding.namespace,|db|db.query_row("SELECT EXISTS(SELECT 1 FROM owned_harness_queues WHERE project=?1 AND owner=?2 AND namespace=?3)",params![binding.project_id,binding.owner,binding.namespace],|r|r.get(0)).map_err(storage))
}
fn fingerprint(binding: &Binding, args: &Value) -> String {
    format!(
        "{:x}",
        Sha256::digest(json!([binding, args]).to_string().as_bytes())
    )
}
pub(super) fn receipt(
    channels: &Channels,
    binding: &Binding,
    args: &Value,
) -> Result<Option<Value>, Error> {
    channels.2.with(
        &binding.project_id,
        &binding.owner,
        &binding.namespace,
        |db| {
            let row: Option<(String, String)> = db
        .query_row(
            "SELECT fingerprint,result FROM harness_queue_commands WHERE owner=?1 AND operation=?2",
            params![binding.owner, args["operationId"].as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
            row.map(|(prior, result)| {
                if prior != fingerprint(binding, args) {
                    return Err(Error::conflict("HARNESS_QUEUE_OPERATION_CHANGED"));
                }
                serde_json::from_str(&result).map_err(storage)
            })
            .transpose()
        },
    )
}
pub(super) fn commit(
    channels: &Channels,
    binding: &Binding,
    args: &Value,
    before: &str,
    record: &Value,
) -> Result<(), Error> {
    channels.2.with(
        &binding.project_id,
        &binding.owner,
        &binding.namespace,
        |db| {
            let tx = db.transaction().map_err(storage)?;
            harness_recovery::store::replace(&tx, binding, before, record)?;
            tx.execute(
                "INSERT INTO harness_queue_commands VALUES(?1,?2,?3,?4)",
                params![
                    binding.owner,
                    args["operationId"].as_str(),
                    fingerprint(binding, args),
                    json!({"revision":record["instructionQueue"]["revision"]}).to_string()
                ],
            )
            .map_err(storage)?;
            tx.commit().map_err(storage)
        },
    )
}
pub(super) fn save(
    channels: &Channels,
    binding: &Binding,
    before: &str,
    record: &Value,
) -> Result<(), Error> {
    channels.2.with(
        &binding.project_id,
        &binding.owner,
        &binding.namespace,
        |db| {
            let tx = db.transaction().map_err(storage)?;
            harness_recovery::store::replace(&tx, binding, before, record)?;
            tx.commit().map_err(storage)
        },
    )
}
pub(super) fn protect(
    db: &rusqlite::Connection,
    project: &str,
    owner: &str,
    key: &str,
    _before: Option<&str>,
    after: Option<&str>,
) -> Result<(), Error> {
    let Some(namespace) = key.strip_suffix("/recovery") else {
        return Ok(());
    };
    let binding = Binding {
        project_id: project.into(),
        owner: owner.into(),
        namespace: namespace.into(),
        profile_id: String::new(),
        session_id: String::new(),
        revision: 0,
    };
    if load(db, &binding)?.is_none() {
        return Ok(());
    }
    let head: Value =
        serde_json::from_str(after.ok_or_else(|| Error::conflict("HARNESS_QUEUE_OWNED_BY_CELLS"))?)
            .map_err(storage)?;
    if harness_recovery::encoding::decode(db, &binding, &head)?
        .get("instructionQueue")
        .is_some()
    {
        return Err(Error::conflict("HARNESS_QUEUE_OWNED_BY_CELLS"));
    }
    Ok(())
}
pub(in crate::plugin::projects) fn load(
    db: &rusqlite::Connection,
    binding: &Binding,
) -> Result<Option<Value>, Error> {
    let alias:Option<(String,String)>=db.query_row("SELECT profile,session FROM owned_harness_queues WHERE project=?1 AND owner=?2 AND namespace=?3",params![binding.project_id,binding.owner,binding.namespace],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(storage)?;
    let Some((profile, session)) = alias else {
        return Ok(None);
    };
    if !binding.profile_id.is_empty()
        && (profile != binding.profile_id || session != binding.session_id)
    {
        return Err(Error::conflict("HARNESS_QUEUE_BINDING_CHANGED"));
    }
    let raw:Option<String>=db.query_row("SELECT body FROM harness_queue_sessions WHERE project=?1 AND owner=?2 AND profile=?3 AND session=?4",params![binding.project_id,binding.owner,profile,session],|r|r.get(0)).optional().map_err(storage)?;
    raw.map(|r| serde_json::from_str(&r).map_err(storage))
        .transpose()
}
pub(in crate::plugin::projects) fn replace(
    db: &rusqlite::Connection,
    binding: &Binding,
    queue: &Value,
) -> Result<(), Error> {
    db.execute("UPDATE harness_queue_sessions SET body=?5 WHERE project=?1 AND owner=?2 AND profile=?3 AND session=?4",params![binding.project_id,binding.owner,binding.profile_id,binding.session_id,queue.to_string()]).map_err(storage)?;
    Ok(())
}
