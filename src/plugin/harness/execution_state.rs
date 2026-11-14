//! Durable execution identities; native history remains the source of terminal results.
use crate::core::error::Error;
use crate::core::sqlite_shards::SqliteShards;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub(super) struct ExecutionState {
    databases: SqliteShards,
    legacy: PathBuf,
    requests: PathBuf,
    epoch: String,
}
impl ExecutionState {
    pub fn open(home: &Path, scope: &str, epoch: &str) -> Result<Self, String> {
        let root = home.join(".cells/executions");
        let scope_id = format!("{:x}", Sha256::digest(scope.as_bytes()));
        let directory = root.join(&scope_id);
        let requests = directory.join("requests");
        std::fs::create_dir_all(&requests)
            .map_err(|_| "Harness execution directory unavailable")?;
        Ok(Self {
            databases: SqliteShards::new(directory.join("sessions"), execution_schema)
                .map_err(|_| "Harness execution storage unavailable")?,
            legacy: root.join(format!("{scope_id}.sqlite")),
            requests,
            epoch: epoch.into(),
        })
    }
    fn with_session<T>(
        &self,
        session: &str,
        work: impl FnOnce(&mut Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.databases.with(session, |db| {
            migrate_session(db, &self.legacy, session)?;
            recover(db, &self.epoch)?;
            work(db)
        })
    }
    fn request_session(&self, request: &str) -> Result<Option<String>, Error> {
        let path = self
            .requests
            .join(format!("{:x}", Sha256::digest(request.as_bytes())));
        match std::fs::read_to_string(path) {
            Ok(session) => Ok(Some(session)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !self.legacy.exists() {
                    return Ok(None);
                }
                use rusqlite::OptionalExtension;
                let db = Connection::open_with_flags(
                    &self.legacy,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
                )
                .map_err(|_| storage())?;
                db.query_row(
                    "SELECT session FROM executions WHERE request=?1",
                    [request],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|_| storage())
            }
            Err(_) => Err(storage()),
        }
    }
    fn bind_request(&self, request: &str, session: &str) -> Result<(), Error> {
        use std::io::Write;
        if let Some(current) = self.request_session(request)? {
            return if current == session {
                Ok(())
            } else {
                Err(storage())
            };
        }
        let path = self
            .requests
            .join(format!("{:x}", Sha256::digest(request.as_bytes())));
        let mut file = tempfile::NamedTempFile::new_in(&self.requests).map_err(|_| storage())?;
        file.write_all(session.as_bytes()).map_err(|_| storage())?;
        file.as_file().sync_all().map_err(|_| storage())?;
        match file.persist_noclobber(path) {
            Ok(_) => std::fs::File::open(&self.requests)
                .and_then(|dir| dir.sync_all())
                .map_err(|_| storage()),
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                if self.request_session(request)?.as_deref() == Some(session) {
                    Ok(())
                } else {
                    Err(storage())
                }
            }
            Err(_) => Err(storage()),
        }
    }
    pub fn begin(&self, name: &str, args: &Value) -> Result<(), Error> {
        if !matches!(name, "harness_turn" | "harness_command") {
            return Ok(());
        }
        let Some(session) = args["sessionId"].as_str() else {
            return Ok(());
        };
        let request = args["requestId"].as_str().ok_or_else(storage)?;
        self.bind_request(request, session)?;
        self.with_session(session,|db|{db.execute(
            "INSERT INTO executions(request,session,epoch,status,updated) VALUES(?1,?2,?3,'submitting',?4)",
            params![request,session,self.epoch,now()]).map_err(|_|storage())?;
        Ok(())})
    }
    pub fn admitted(&self, request: &str, receipt: &Value) -> Result<(), Error> {
        let turn = receipt["result"]["turnId"].as_str();
        let status = match receipt["outcome"].as_str() {
            Some("committed") if turn.is_some() => "running",
            Some("committed" | "not-committed") => "settled",
            _ => "unknown",
        };
        let Some(session) = self.request_session(request)? else {
            return Ok(());
        };
        self.with_session(&session,|db|{db.execute(
            "UPDATE executions SET turn=?2,status=?3,updated=?4 WHERE request=?1 AND epoch=?5 AND (status IN ('submitting','unknown') OR (status='running' AND ?3='running')) AND (turn IS NULL OR turn=?2)",
            params![request,turn,status,now(),self.epoch]).map_err(|_|storage())?;
        Ok(())})
    }
    pub fn admitted_turn(&self, request: &str, session: &str) -> Result<Option<String>, Error> {
        use rusqlite::OptionalExtension;
        self.with_session(session, |db| {
            db.query_row(
                "SELECT turn FROM executions WHERE request=?1 AND session=?2 AND turn IS NOT NULL",
                params![request, session],
                |r| r.get(0),
            )
            .optional()
            .map_err(|_| storage())
        })
    }
    pub fn project(&self, value: &mut Value) -> Result<(), Error> {
        let Some(session) = value["session"]["id"].as_str().map(str::to_owned) else {
            return Ok(());
        };
        self.with_session(&session,|db|{
        let tx = db.transaction().map_err(|_| storage())?;
        if !value["recovery"].is_object() {
            reconcile(&tx, &session, value)?;
        }
        let rows = tx.prepare("SELECT turn,request,epoch,updated FROM executions WHERE session=?1 AND status='unknown' AND epoch<>?2 AND turn IS NOT NULL ORDER BY updated DESC LIMIT 64").map_err(|_|storage())?
            .query_map(params![session,self.epoch],|r|Ok(json!({"turnId":r.get::<_,String>(0)?,"requestId":r.get::<_,String>(1)?,"previousEpoch":r.get::<_,String>(2)?,"recoveredAt":r.get::<_,i64>(3)?,"reason":"CELLS_RESTARTED"}))).map_err(|_|storage())?
            .collect::<Result<Vec<_>,_>>().map_err(|_|storage())?;
        tx.commit().map_err(|_| storage())?;
        if value["session"]["executionRecovery"].is_object() {
            value["session"]["executionRecovery"]["interruptedTurns"] = json!(rows);
        }
        Ok(())})
    }
}
fn execution_schema(db: &Connection) -> Result<(), Error> {
    db.execute_batch("PRAGMA max_page_count=16384;
        CREATE TABLE IF NOT EXISTS executions(request TEXT PRIMARY KEY,session TEXT NOT NULL,turn TEXT,epoch TEXT NOT NULL,status TEXT NOT NULL,updated INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS execution_session ON executions(session,turn);
        CREATE TABLE IF NOT EXISTS migration(complete INTEGER);").map_err(|_|storage())
}
fn migrate_session(db: &mut Connection, legacy: &Path, session: &str) -> Result<(), Error> {
    if !legacy.exists()
        || db
            .query_row("SELECT EXISTS(SELECT 1 FROM migration)", [], |r| {
                r.get::<_, bool>(0)
            })
            .map_err(|_| storage())?
    {
        return Ok(());
    }
    db.execute(
        "ATTACH DATABASE ?1 AS legacy",
        [legacy.to_string_lossy().as_ref()],
    )
    .map_err(|_| storage())?;
    let result = (|| {
        let tx = db.transaction().map_err(|_| storage())?;
        tx.execute(
            "INSERT OR IGNORE INTO executions SELECT * FROM legacy.executions WHERE session=?1",
            [session],
        )
        .map_err(|_| storage())?;
        tx.execute("INSERT INTO migration VALUES(1)", [])
            .map_err(|_| storage())?;
        tx.commit().map_err(|_| storage())
    })();
    let detached = db
        .execute_batch("DETACH DATABASE legacy")
        .map_err(|_| storage());
    result.and(detached)
}
fn reconcile(db: &Connection, session: &str, value: &Value) -> Result<(), Error> {
    let head =
        json!({"id":value["session"]["lastTurnId"],"status":value["session"]["lastTurnResult"]});
    let turns = value["turns"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(std::iter::once(&head));
    for turn in turns {
        if !["completed", "failed", "cancelled", "interrupted"]
            .contains(&turn["status"].as_str().unwrap_or(""))
        {
            continue;
        }
        db.execute("UPDATE executions SET status=?3,updated=?4 WHERE session=?1 AND turn=?2 AND status IN ('submitting','running','unknown')",
            params![session,turn["id"].as_str(),turn["status"].as_str(),now()]).map_err(|_|storage())?;
    }
    Ok(())
}
fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn storage() -> Error {
    Error::new(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "HARNESS_EXECUTION_STORAGE",
    )
}

fn recover(db: &Connection, epoch: &str) -> Result<(), Error> {
    let started = std::time::Instant::now();
    let changed = db.execute("UPDATE executions SET status='unknown',updated=?2 WHERE epoch<>?1 AND status IN ('submitting','running')", params![epoch,now()]).map_err(|_|storage())?;
    if changed > 0 {
        crate::core::events::emit(
            crate::core::events::Level::Info,
            "harness.execution.recovered",
            json!({"operation":"harness.execution.recover","operationId":epoch,"stage":"startup","result":"success","count":changed,"code":"CELLS_RESTARTED","durationMs":started.elapsed().as_millis()}),
        );
    }
    Ok(())
}
pub(super) fn recover_all(home: &Path, epoch: &str) -> Result<(), String> {
    let root = home.join(".cells/executions");
    if !root.exists() {
        return Ok(());
    }
    recover_directory(&root, epoch)
}
fn recover_directory(root: &Path, epoch: &str) -> Result<(), String> {
    for entry in std::fs::read_dir(root).map_err(|_| "Harness execution directory unavailable")? {
        let entry = entry.map_err(|_| "Harness execution entry unavailable")?;
        let path = entry.path();
        if path.is_dir() {
            let sessions = path.join("sessions");
            if sessions.is_dir() {
                recover_directory(&sessions, epoch)?;
            }
            continue;
        }
        let Some(name) = path.file_stem().and_then(|v| v.to_str()) else {
            continue;
        };
        if path.extension().and_then(|v| v.to_str()) != Some("sqlite")
            || name.len() != 64
            || !name.bytes().all(|b| b.is_ascii_hexdigit())
        {
            continue;
        }
        let db = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(|_| "Harness execution storage unavailable")?;
        recover(&db, epoch).map_err(|_| "Harness startup execution recovery failed")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn admitted(store: &ExecutionState) {
        store
            .begin(
                "harness_turn",
                &json!({"requestId":"request","sessionId":"session"}),
            )
            .unwrap();
        store
            .admitted(
                "request",
                &json!({"outcome":"committed","result":{"turnId":"turn"}}),
            )
            .unwrap();
    }
    fn session() -> Value {
        json!({"session":{"id":"session","executionRecovery":{"state":"detached"}},"turns":[]})
    }
    #[test]
    fn legacy_execution_rows_migrate_only_into_their_session() {
        let root = tempfile::tempdir().unwrap();
        let store = ExecutionState::open(root.path(), "scope", "new").unwrap();
        let db = Connection::open(&store.legacy).unwrap();
        execution_schema(&db).unwrap();
        db.execute(
            "INSERT INTO executions VALUES('request','session','turn','old','running',1)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO executions VALUES('other-request','other','other-turn','old','running',1)",
            [],
        )
        .unwrap();
        drop(db);
        assert_eq!(
            store.request_session("request").unwrap().as_deref(),
            Some("session")
        );
        let mut value = session();
        store.project(&mut value).unwrap();
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"][0]["turnId"],
            "turn"
        );
        store
            .databases
            .with("session", |db| {
                assert_eq!(
                    db.query_row("SELECT COUNT(*) FROM executions", [], |r| r
                        .get::<_, usize>(0))
                        .unwrap(),
                    1
                );
                Ok(())
            })
            .unwrap();
        store
            .begin(
                "harness_turn",
                &json!({"requestId":"new-request","sessionId":"other"}),
            )
            .unwrap();
        assert_ne!(
            store.databases.path("session"),
            store.databases.path("other")
        );
        assert_eq!(
            store.request_session("new-request").unwrap().as_deref(),
            Some("other")
        );
        let db = Connection::open(&store.legacy).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT status FROM executions WHERE request='request'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "running"
        );
    }
    #[test]
    fn startup_recovers_execution_without_replaying_or_fabricating_a_terminal() {
        let root = tempfile::tempdir().unwrap();
        let store = ExecutionState::open(root.path(), "project-owner-profile", "old").unwrap();
        admitted(&store);
        drop(store);
        recover_all(root.path(), "new").unwrap();
        let store = ExecutionState::open(root.path(), "project-owner-profile", "new").unwrap();
        let mut value = session();
        store.project(&mut value).unwrap();
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"][0]["turnId"],
            "turn"
        );
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"][0]["reason"],
            "CELLS_RESTARTED"
        );
        assert!(value["session"]["lastTurnResult"].is_null());
        drop(store);
        let store = ExecutionState::open(root.path(), "project-owner-profile", "new").unwrap();
        store.project(&mut value).unwrap();
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn only_matching_native_terminal_clears_recovery_and_scopes_are_isolated() {
        let root = tempfile::tempdir().unwrap();
        let store = ExecutionState::open(root.path(), "alice", "old").unwrap();
        admitted(&store);
        drop(store);
        let store = ExecutionState::open(root.path(), "alice", "new").unwrap();
        let mut value = session();
        value["turns"] = json!([{"id":"other","status":"completed"}]);
        store.project(&mut value).unwrap();
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let other = ExecutionState::open(root.path(), "bob", "new").unwrap();
        let mut isolated = session();
        other.project(&mut isolated).unwrap();
        assert_eq!(
            isolated["session"]["executionRecovery"]["interruptedTurns"],
            json!([])
        );
        value["turns"] = json!([{"id":"turn","status":"completed"}]);
        store.project(&mut value).unwrap();
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"],
            json!([])
        );
    }
    #[test]
    fn same_epoch_and_unconfirmed_admission_never_produce_interrupted_turn_proofs() {
        let root = tempfile::tempdir().unwrap();
        let store = ExecutionState::open(root.path(), "scope", "old").unwrap();
        admitted(&store);
        let mut value = session();
        store.project(&mut value).unwrap();
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"],
            json!([])
        );
        drop(store);
        let store = ExecutionState::open(root.path(), "scope", "new").unwrap();
        store
            .begin(
                "harness_turn",
                &json!({"requestId":"unconfirmed","sessionId":"session"}),
            )
            .unwrap();
        drop(store);
        let store = ExecutionState::open(root.path(), "scope", "third").unwrap();
        store.project(&mut value).unwrap();
        assert_eq!(
            value["session"]["executionRecovery"]["interruptedTurns"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
}
