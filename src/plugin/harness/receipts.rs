//! Epoch-scoped receipts outlive runtime replacement and spill to private disk.
use crate::core::error::Error;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct Receipts {
    database: Mutex<Connection>,
    active: Arc<Semaphore>,
}
#[derive(Clone)]
pub(super) struct ReceiptScope {
    store: Arc<Receipts>,
    key: String,
}
pub(super) enum Reservation {
    Existing(Value),
    New(OwnedSemaphorePermit),
}
impl Receipts {
    pub fn new() -> Result<Arc<Self>, String> {
        // An unnamed SQLite database spills to a private temporary file and is
        // removed on close, including process death. A new process has a new epoch.
        let database = Connection::open("").map_err(|_| "Harness receipt storage unavailable")?;
        database
            .execute_batch(
                "PRAGMA page_size=4096; PRAGMA max_page_count=65536;
             PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=-2048;
             CREATE TABLE receipts (scope TEXT NOT NULL, id TEXT NOT NULL,
             fingerprint TEXT NOT NULL, receipt TEXT NOT NULL, PRIMARY KEY(scope,id));",
            )
            .map_err(|_| "Harness receipt storage unavailable")?;
        Ok(Arc::new(Self {
            database: Mutex::new(database),
            active: Arc::new(Semaphore::new(1024)),
        }))
    }
    pub fn scoped(self: &Arc<Self>, key: String) -> ReceiptScope {
        ReceiptScope {
            store: self.clone(),
            key,
        }
    }
}
impl ReceiptScope {
    pub fn same_scope(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.store, &other.store) && self.key == other.key
    }
    pub async fn existing(&self, id: &str, fingerprint: String) -> Result<Option<Value>, Error> {
        let id = identifier(id)?;
        self.access(move |db, scope, _| matching(db, scope, &id, &fingerprint))
            .await
    }
    async fn access<T: Send + 'static>(
        &self,
        action: impl FnOnce(&Connection, &str, &Arc<Semaphore>) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let scope = self.clone();
        tokio::task::spawn_blocking(move || {
            let database = scope.store.database.lock().map_err(|_| storage_error())?;
            action(&database, &scope.key, &scope.store.active)
        })
        .await
        .map_err(|_| storage_error())?
    }
    pub async fn reserve(&self, id: &str, fingerprint: String) -> Result<Reservation, Error> {
        let id = identifier(id)?;
        self.access(move |db, scope, active| {
            if let Some(receipt) = matching(db, scope, &id, &fingerprint)? {
                return Ok(Reservation::Existing(receipt));
            }
            let permit = active
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::too_many("HARNESS_OPERATION_LIMIT"))?;
            insert(
                db,
                scope,
                &id,
                &fingerprint,
                &json!({"outcome":"unknown","requestId":id}),
            )?;
            Ok(Reservation::New(permit))
        })
        .await
    }
    pub async fn operation(&self, id: &str) -> Result<Value, Error> {
        let id = identifier(id)?;
        self.access(move |db, scope, _| {
            if let Some((_, receipt)) = lookup(db, scope, &id)? { return Ok(receipt); }
            // Fence an unseen request before confirming that it never started.
            let receipt = json!({"outcome":"not-committed","requestId":id,"code":"HARNESS_REQUEST_NOT_STARTED"});
            insert(db, scope, &id, "", &receipt)?;
            Ok(receipt)
        }).await
    }
    pub async fn output(&self, args: Value, epoch: String) -> Result<Value, Error> {
        let started = std::time::Instant::now();
        let id = args["requestId"]
            .as_str()
            .filter(|id| crate::core::ids::is_identifier_within(id, 128))
            .map(str::to_owned);
        let result = self.read_output(args, epoch).await;
        crate::core::events::emit(
            if result.is_ok() {
                crate::core::events::Level::Info
            } else {
                crate::core::events::Level::Warn
            },
            "harness.command.output",
            json!({"operation":"harness.command.output","operationId":id,"stage":"receipt-read",
                "outcome":if result.is_ok() {"success"} else {"failure"},"code":result.as_ref().err().map(|error|error.code),
                "durationMs":started.elapsed().as_millis()}),
        );
        result
    }
    async fn read_output(&self, args: Value, epoch: String) -> Result<Value, Error> {
        let id = identifier(args["requestId"].as_str().ok_or_else(Error::invalid)?)?;
        self.access(move |db, scope, _| {
            let (_, receipt) =
                lookup(db, scope, &id)?.ok_or_else(super::command_outputs::missing)?;
            super::command_outputs::chunk(receipt, &args, &epoch)
        })
        .await
    }
    pub async fn finish(&self, id: &str, receipt: Value) -> Result<Value, Error> {
        let id = identifier(id)?;
        self.access(move |db, scope, _| {
            let (_, current) = lookup(db, scope, &id)?.ok_or_else(storage_error)?;
            // Native reconciliation and the original worker may finish concurrently.
            // A late timeout must not overwrite an already confirmed receipt.
            if current["outcome"] != "unknown" {
                return Ok(current);
            }
            let receipt = super::command_outputs::prepare(receipt);
            let changed = db
                .execute(
                    "UPDATE receipts SET receipt=?3 WHERE scope=?1 AND id=?2",
                    params![scope, id, receipt.to_string()],
                )
                .map_err(|_| storage_error())?;
            if changed != 1 {
                return Err(storage_error());
            }
            Ok(receipt)
        })
        .await
    }
}
fn storage_error() -> Error {
    Error::new(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "HARNESS_RECEIPT_STORAGE",
    )
}
fn identifier(id: &str) -> Result<String, Error> {
    if !crate::core::ids::is_identifier_within(id, 128) {
        return Err(Error::invalid());
    }
    Ok(id.into())
}
fn lookup(db: &Connection, scope: &str, id: &str) -> Result<Option<(String, Value)>, Error> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT fingerprint, receipt FROM receipts WHERE scope=?1 AND id=?2",
            params![scope, id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| storage_error())?;
    row.map(|(fingerprint, receipt)| {
        serde_json::from_str(&receipt)
            .map(|receipt| (fingerprint, receipt))
            .map_err(|_| storage_error())
    })
    .transpose()
}
fn matching(
    db: &Connection,
    scope: &str,
    id: &str,
    fingerprint: &str,
) -> Result<Option<Value>, Error> {
    let Some((previous, receipt)) = lookup(db, scope, id)? else {
        return Ok(None);
    };
    if !previous.is_empty() && previous != fingerprint {
        return Err(Error::conflict("HARNESS_REQUEST_REUSED"));
    }
    Ok(Some(receipt))
}
fn insert(
    db: &Connection,
    scope: &str,
    id: &str,
    fingerprint: &str,
    receipt: &Value,
) -> Result<(), Error> {
    db.execute(
        "INSERT INTO receipts(scope,id,fingerprint,receipt) VALUES (?1,?2,?3,?4)",
        params![scope, id, fingerprint, receipt.to_string()],
    )
    .map_err(|_| storage_error())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn completed_receipts_and_fences_do_not_consume_concurrent_capacity() {
        let store = Receipts::new().unwrap();
        let scope = store.scoped("project".into());
        for index in 0..1030 {
            let id = format!("finished-{index}");
            let Reservation::New(permit) = scope.reserve(&id, "body".into()).await.unwrap() else {
                panic!("new request");
            };
            scope
                .finish(
                    &id,
                    json!({"outcome":"committed","requestId":id,"result":{"turnId":"t"}}),
                )
                .await
                .unwrap();
            drop(permit);
            scope.operation(&format!("fenced-{index}")).await.unwrap();
        }
        assert_eq!(
            scope.operation("finished-0").await.unwrap()["outcome"],
            "committed"
        );
        assert!(matches!(
            scope.reserve("fenced-0", "late".into()).await.unwrap(),
            Reservation::Existing(_)
        ));
        assert!(scope
            .reserve("finished-0", "different".into())
            .await
            .is_err());
        assert!(matches!(
            scope.reserve("next", "body".into()).await.unwrap(),
            Reservation::New(_)
        ));
    }
    #[tokio::test]
    async fn concurrent_limit_releases_and_scopes_do_not_share_receipts() {
        let store = Receipts::new().unwrap();
        let first = store.scoped("first".into());
        let held = store.active.clone().acquire_many_owned(1024).await.unwrap();
        assert_eq!(
            first.reserve("id", "body".into()).await.err().unwrap().code,
            "HARNESS_OPERATION_LIMIT"
        );
        assert_eq!(
            first.operation("fence").await.unwrap()["outcome"],
            "not-committed"
        );
        drop(held);
        assert!(matches!(
            first.reserve("id", "body".into()).await.unwrap(),
            Reservation::New(_)
        ));
        assert_eq!(
            store.scoped("second".into()).operation("id").await.unwrap()["outcome"],
            "not-committed"
        );
        assert_eq!(first.operation("id").await.unwrap()["outcome"], "unknown");
    }
    #[tokio::test]
    async fn reconciled_receipts_survive_late_unknown_or_conflicting_worker_results() {
        let store = Receipts::new().unwrap();
        let scope = store.scoped("native-proof".into());
        let _reservation = scope
            .reserve("prompt", "frozen-input".into())
            .await
            .unwrap();
        let committed =
            json!({"outcome":"committed","requestId":"prompt","result":{"turnId":"prompt"}});
        assert_eq!(
            scope.finish("prompt", committed.clone()).await.unwrap(),
            committed
        );
        for late in [
            json!({"outcome":"unknown","requestId":"prompt"}),
            json!({"outcome":"not-committed","requestId":"prompt"}),
        ] {
            assert_eq!(scope.finish("prompt", late).await.unwrap(), committed);
        }
        assert_eq!(scope.operation("prompt").await.unwrap(), committed);
        let fenced = scope.operation("unseen").await.unwrap();
        assert_eq!(scope.finish("unseen", committed).await.unwrap(), fenced);
        assert!(scope.same_scope(&store.scoped("native-proof".into())));
        assert!(!scope.same_scope(&store.scoped("different-owner".into())));
    }
    #[tokio::test]
    async fn full_receipt_database_keeps_the_admitted_operation_unknown() {
        let store = Receipts::new().unwrap();
        let scope = store.scoped("project".into());
        let _reservation = scope.reserve("id", "body".into()).await.unwrap();
        {
            let database = store.database.lock().unwrap();
            let pages: u64 = database
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .unwrap();
            database
                .execute_batch(&format!("PRAGMA max_page_count={pages}"))
                .unwrap();
        }
        assert_eq!(
            scope
                .finish(
                    "id",
                    json!({"outcome":"committed","requestId":"id","result":"x".repeat(16_384)})
                )
                .await
                .err()
                .unwrap()
                .code,
            "HARNESS_RECEIPT_STORAGE"
        );
        assert_eq!(scope.operation("id").await.unwrap()["outcome"], "unknown");
        assert!(matches!(
            scope.reserve("id", "body".into()).await.unwrap(),
            Reservation::Existing(_)
        ));
    }
}
