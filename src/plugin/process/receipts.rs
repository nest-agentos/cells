//! Epoch-local disk receipts preserve request identities without retaining output in RAM.
use super::model::{Process, Status};
use crate::core::error::Error;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Default)]
pub(super) struct Receipts(Option<Connection>);
#[derive(Serialize, Deserialize)]
struct Stored {
    state: String,
    stdout: String,
    stderr: String,
    code: Option<i32>,
    error: Option<String>,
    truncated: bool,
}
impl Receipts {
    fn connection(&mut self) -> Result<&Connection, Error> {
        if self.0.is_none() {
            // Empty filename selects a private temporary on-disk database, removed at epoch shutdown.
            let db = Connection::open("").map_err(|_| storage_error())?;
            db.execute_batch("PRAGMA page_size=4096; PRAGMA max_page_count=65536; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=-2048; CREATE TABLE receipts(identity INTEGER NOT NULL,id TEXT NOT NULL,status TEXT NOT NULL,PRIMARY KEY(identity,id));")
                .map_err(|_| storage_error())?;
            self.0 = Some(db);
        }
        Ok(self.0.as_ref().unwrap())
    }
    pub(super) fn store(&mut self, identity: usize, id: &str, status: Status) -> Result<(), Error> {
        let value = Stored {
            state: status.state.into(),
            stdout: status.stdout,
            stderr: status.stderr,
            code: status.code,
            error: status.error.map(str::to_owned),
            truncated: status.truncated,
        };
        let text = serde_json::to_string(&value).map_err(|_| storage_error())?;
        self.connection()?
            .execute(
                "INSERT INTO receipts(identity,id,status) VALUES(?1,?2,?3)",
                params![identity as i64, id, text],
            )
            .map_err(|_| storage_error())?;
        Ok(())
    }
    pub(super) fn get(&self, identity: usize, id: &str) -> Result<Option<Arc<Process>>, Error> {
        let Some(db) = &self.0 else {
            return Ok(None);
        };
        let text: Option<String> = db
            .query_row(
                "SELECT status FROM receipts WHERE identity=?1 AND id=?2",
                params![identity as i64, id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| storage_error())?;
        text.map(|text| restore(&text)).transpose()
    }
}
fn restore(text: &str) -> Result<Arc<Process>, Error> {
    let stored: Stored = serde_json::from_str(text).map_err(|_| storage_error())?;
    let state = match stored.state.as_str() {
        "exited" => "exited",
        "cancelled" => "cancelled",
        "timed-out" => "timed-out",
        "failed" => "failed",
        _ => return Err(storage_error()),
    };
    let error = match stored.error.as_deref() {
        None => None,
        Some("SPAWN_FAILED") => Some("SPAWN_FAILED"),
        _ => return Err(storage_error()),
    };
    Ok(Arc::new(Process::from_status(Status {
        state,
        stdout: stored.stdout,
        stderr: stored.stderr,
        code: stored.code,
        error,
        truncated: stored.truncated,
    })))
}
fn storage_error() -> Error {
    Error::new(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "PROCESS_RECEIPT_STORAGE_FAILED",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn archive_storage_errors_do_not_overwrite_existing_receipts() {
        let mut receipts = Receipts::default();
        let status = || Status {
            state: "exited",
            stdout: "original".into(),
            stderr: String::new(),
            code: Some(0),
            error: None,
            truncated: false,
        };
        receipts.store(1, "original", status()).unwrap();
        receipts.connection().unwrap().execute_batch("CREATE TRIGGER reject_new BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'full'); END;").unwrap();
        assert_eq!(
            receipts.store(1, "new", status()).err().unwrap().code,
            "PROCESS_RECEIPT_STORAGE_FAILED"
        );
        assert_eq!(
            receipts
                .get(1, "original")
                .unwrap()
                .unwrap()
                .status()
                .stdout,
            "original"
        );
        assert!(receipts.get(1, "new").unwrap().is_none());
    }
    #[test]
    fn corrupted_archived_status_fails_closed() {
        let mut receipts = Receipts::default();
        receipts
            .connection()
            .unwrap()
            .execute(
                "INSERT INTO receipts(identity,id,status) VALUES(1,'old','{}')",
                [],
            )
            .unwrap();
        assert_eq!(
            receipts.get(1, "old").err().unwrap().code,
            "PROCESS_RECEIPT_STORAGE_FAILED"
        );
    }
}
