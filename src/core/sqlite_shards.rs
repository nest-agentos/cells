//! A cached connection and its transaction lock belong to one physical database file.
use super::error::Error;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
type Entry = Arc<Mutex<Option<Connection>>>;
pub(crate) struct SqliteShards {
    root: PathBuf,
    entries: Mutex<HashMap<String, Entry>>,
    schema: fn(&Connection) -> Result<(), Error>,
}
impl SqliteShards {
    pub(crate) fn new(
        root: PathBuf,
        schema: fn(&Connection) -> Result<(), Error>,
    ) -> Result<Self, Error> {
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            entries: Mutex::new(HashMap::new()),
            schema,
        })
    }
    pub(crate) fn path(&self, key: &str) -> PathBuf {
        self.root
            .join(format!("{:x}.sqlite", Sha256::digest(key.as_bytes())))
    }
    pub(crate) fn with<T>(
        &self,
        key: &str,
        work: impl FnOnce(&mut Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let entry = {
            let mut entries = self.entries.lock().map_err(storage)?;
            if entries.len() >= 128 {
                entries.retain(|_, entry| Arc::strong_count(entry) > 1);
            }
            entries
                .entry(key.into())
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let started = std::time::Instant::now();
        let result = (|| {
            let mut slot = entry.lock().map_err(storage)?;
            if slot.is_none() {
                let db = open(&self.path(key))?;
                (self.schema)(&db)?;
                *slot = Some(db);
            }
            work(slot.as_mut().unwrap())
        })();
        if result.is_err() || started.elapsed().as_millis() >= 100 {
            crate::core::events::emit(
                if result.is_err() {
                    crate::core::events::Level::Warn
                } else {
                    crate::core::events::Level::Info
                },
                "storage.resource.completed",
                serde_json::json!({"operation":"storage.resource","stage":"transaction","resourceId":format!("{:x}",Sha256::digest(key.as_bytes())),"result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}),
            );
        }
        result
    }
}
pub(crate) fn open(path: &Path) -> Result<Connection, Error> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)?;
    let db = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
            | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(storage)?;
    db.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(storage)?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
        .map_err(storage)?;
    Ok(db)
}
fn storage(_: impl std::fmt::Display) -> Error {
    Error::new(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "RESOURCE_STORAGE_UNAVAILABLE",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn schema(db: &Connection) -> Result<(), Error> {
        db.execute_batch("CREATE TABLE IF NOT EXISTS entries(id INTEGER PRIMARY KEY,value TEXT);")
            .map_err(storage)
    }
    #[test]
    fn a_busy_resource_does_not_hold_the_connection_registry() {
        let root = tempfile::tempdir().unwrap();
        let shards = Arc::new(SqliteShards::new(root.path().into(), schema).unwrap());
        let (ready, entered) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let worker = {
            let shards = shards.clone();
            std::thread::spawn(move || {
                shards
                    .with("a", |db| {
                        let tx = db.transaction().map_err(storage)?;
                        ready.send(()).unwrap();
                        released.recv().unwrap();
                        tx.commit().map_err(storage)
                    })
                    .unwrap()
            })
        };
        entered.recv().unwrap();
        let (done, finished) = std::sync::mpsc::channel();
        let independent = {
            let shards = shards.clone();
            std::thread::spawn(move || {
                let result = shards.with("b", |db| {
                    db.execute("INSERT INTO entries VALUES(1,'independent')", [])
                        .map_err(storage)?;
                    Ok(())
                });
                done.send(result).unwrap();
            })
        };
        let result = finished.recv_timeout(std::time::Duration::from_secs(1));
        release.send(()).unwrap();
        worker.join().unwrap();
        independent.join().unwrap();
        result.unwrap().unwrap();
        assert_ne!(shards.path("a"), shards.path("b"));
    }
}
