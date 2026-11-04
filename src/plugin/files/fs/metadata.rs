//! Lazy export-owned metadata. Keys are source-relative paths, never client mount paths.
use crate::core::error::Error;
use rusqlite::{params, Connection, OptionalExtension};
use rustix::fs::{Mode, OFlags};
use serde_json::{Map, Value};
use std::{
    fs::File,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    time::Duration,
};

const DIRECTORY: &str = ".nestos";
const DATABASE: &str = "meta.sqlite";
const APPLICATION_ID: i64 = 0x4d444d54;

pub(crate) struct Metadata {
    database: Connection,
    _directory: File,
    has_identity: bool,
}
impl Metadata {
    pub fn open(root: &File, write: bool) -> Result<Option<Self>, Error> {
        let directory_name = metadata_directory(root)?;
        if write {
            match rustix::fs::mkdirat(root, directory_name, Mode::from_bits_truncate(0o700)) {
                Ok(()) => root.sync_all()?,
                Err(rustix::io::Errno::EXIST) => (),
                Err(error) => return Err(error.into()),
            }
        }
        let directory = match rustix::fs::openat(
            root,
            directory_name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(value) => File::from(value),
            Err(rustix::io::Errno::NOENT) if !write => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if directory.metadata()?.uid() != rustix::process::geteuid().as_raw() {
            return Err(Error::forbidden("METADATA_OWNER"));
        }
        let flags = OFlags::CLOEXEC
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | if write {
                OFlags::RDWR | OFlags::CREATE
            } else {
                OFlags::RDONLY
            };
        let file = match rustix::fs::openat(
            &directory,
            DATABASE,
            flags,
            Mode::from_bits_truncate(0o600),
        ) {
            Ok(value) => File::from(value),
            Err(rustix::io::Errno::NOENT) if !write => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let info = file.metadata()?;
        if !info.is_file()
            || info.nlink() != 1
            || info.uid() != rustix::process::geteuid().as_raw()
            || info.mode() & 0o077 != 0
        {
            return Err(Error::forbidden("METADATA_PERMISSIONS"));
        }
        if write {
            let journal = File::from(rustix::fs::openat(
                &directory,
                "meta.sqlite-journal",
                flags,
                Mode::from_bits_truncate(0o600),
            )?);
            let info = journal.metadata()?;
            if !info.is_file()
                || info.nlink() != 1
                || info.uid() != rustix::process::geteuid().as_raw()
                || info.mode() & 0o077 != 0
            {
                return Err(Error::forbidden("METADATA_PERMISSIONS"));
            }
        }
        let path = format!("/proc/self/fd/{}/{DATABASE}", directory.as_raw_fd());
        let database = Connection::open_with_flags(
            path,
            if write {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            } else {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            },
        )
        .map_err(sql)?;
        database.busy_timeout(Duration::from_secs(5)).map_err(sql)?;
        let application: i64 = database
            .pragma_query_value(None, "application_id", |row| row.get(0))
            .map_err(sql)?;
        let version: i64 = database
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(sql)?;
        if write && application == 0 && version == 0 {
            database.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; BEGIN IMMEDIATE; CREATE TABLE file_metadata(path TEXT PRIMARY KEY, data TEXT NOT NULL, identity TEXT NOT NULL DEFAULT (lower(hex(randomblob(16))))); PRAGMA application_id=1296321876; PRAGMA user_version=1; COMMIT;").map_err(sql)?;
            directory.sync_all()?;
        } else if !write && application == 0 && version == 0 {
            return Ok(None);
        } else if application != APPLICATION_ID || version != 1 {
            return Err(Error::conflict("METADATA_VERSION"));
        }
        let has_identity = prepare_identity(&database, write)?;
        Ok(Some(Self {
            has_identity,
            database,
            _directory: directory,
        }))
    }
    pub(super) fn is_scope(&self) -> Result<bool, Error> {
        self.database.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='scope_info' AND type='table')", [], |row| row.get(0)).map_err(sql)
    }
    pub(super) fn set_scope_info(&mut self, owner: &str, key: &str, value: &Value) -> Result<(), Error> {
        let tx = self.database.transaction().map_err(sql)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS scope_info(owner TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(owner,key));").map_err(sql)?;
        tx.execute("INSERT INTO scope_info VALUES(?1,?2,?3) ON CONFLICT(owner,key) DO UPDATE SET value=excluded.value", params![owner,key,value.to_string()]).map_err(sql)?;
        tx.commit().map_err(sql)
    }
    pub(super) fn transfer_scope(&mut self, target: &mut Self, prefix: &str) -> Result<(), Error> {
        // Publication of all records is atomic. A retried registration never overwrites newer scoped records.
        let source = self.database.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(sql)?;
        {
            let rows = {
                let mut statement = source.prepare("SELECT substr(path,length(?1)+2),data,identity FROM file_metadata WHERE substr(path,1,length(?1)+1)=?1||'/'").map_err(sql)?;
                let rows = statement.query_map([prefix], |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,String>(2)?))).map_err(sql)?;
                rows.collect::<Result<Vec<_>,_>>().map_err(sql)?
            };
            let tx = target.database.transaction().map_err(sql)?;
            for (path,data,identity) in rows {
                let prior: Option<(String,String)> = tx.query_row("SELECT data,identity FROM file_metadata WHERE path=?1", [&path], |row| Ok((row.get(0)?,row.get(1)?))).optional().map_err(sql)?;
                if prior.as_ref().is_some_and(|prior| prior != &(data.clone(),identity.clone())) { return Err(Error::conflict("METADATA_SCOPE_CONFLICT")); }
                tx.execute("INSERT OR IGNORE INTO file_metadata(path,data,identity) VALUES(?1,?2,?3)", params![path,data,identity]).map_err(sql)?;
            }
            tx.execute_batch("CREATE TABLE IF NOT EXISTS scope_info(owner TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(owner,key));").map_err(sql)?;
            tx.commit().map_err(sql)?;
        }
        source.execute("DELETE FROM file_metadata WHERE substr(path,1,length(?1)+1)=?1||'/'", [prefix]).map_err(sql)?;
        source.commit().map_err(sql)
    }
    pub fn existing_write(root: &File) -> Result<Option<Self>, Error> {
        if Self::open(root, false)?.is_none() {
            return Ok(None);
        }
        Self::open(root, true)
    }
    pub fn identity(&self, path: &str) -> Result<Option<String>, Error> {
        if !self.has_identity { return Ok(None); }
        self.database
            .query_row(
                "SELECT identity FROM file_metadata WHERE path=?1",
                [path],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)
    }
    pub fn read(&self, path: &str) -> Result<Map<String, Value>, Error> {
        let raw: Option<String> = self
            .database
            .query_row(
                "SELECT data FROM file_metadata WHERE path=?1",
                [path],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        raw.map(|value| {
            serde_json::from_str(&value).map_err(|_| Error::conflict("METADATA_INVALID"))
        })
        .unwrap_or_else(|| Ok(Map::new()))
    }
    pub fn patch(
        &mut self,
        path: &str,
        patch: &Map<String, Value>,
        remove: &[String],
    ) -> Result<(), Error> {
        let transaction = self
            .database
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql)?;
        let raw: Option<String> = transaction
            .query_row(
                "SELECT data FROM file_metadata WHERE path=?1",
                [path],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        let mut value: Map<String, Value> = raw
            .map(|raw| serde_json::from_str(&raw))
            .transpose()
            .map_err(|_| Error::conflict("METADATA_INVALID"))?
            .unwrap_or_default();
        value.extend(patch.clone());
        for key in remove {
            value.remove(key);
        }
        let data = serde_json::to_string(&value).map_err(|_| Error::invalid())?;
        if data.len() > 512 * 1024 {
            return Err(Error::too_large("METADATA_LIMIT"));
        }
        transaction.execute("INSERT INTO file_metadata(path,data,identity) VALUES(?1,?2,lower(hex(randomblob(16)))) ON CONFLICT(path) DO UPDATE SET data=excluded.data", params![path,data]).map_err(sql)?;
        transaction.commit().map_err(sql)
    }
    pub fn remove(&self, path: &str) -> Result<(), Error> {
        self.database
            .execute(
                "DELETE FROM file_metadata WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
                [path],
            )
            .map_err(sql)?;
        Ok(())
    }
    pub fn rename(&mut self, path: &str, to: &str) -> Result<(), Error> {
        let tx = self.database.transaction().map_err(sql)?;
        tx.execute(
            "DELETE FROM file_metadata WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
            [to],
        )
        .map_err(sql)?;
        tx.execute("UPDATE file_metadata SET path=?2||substr(path,length(?1)+1) WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'", params![path,to]).map_err(sql)?;
        tx.commit().map_err(sql)
    }
}
// Legacy version-1 stores predate identity. Reads remain read-only; writes upgrade atomically.
fn prepare_identity(database: &Connection, write: bool) -> Result<bool, Error> {
    let has_identity: bool = database.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('file_metadata') WHERE name='identity')",
        [], |row| row.get(0),
    ).map_err(sql)?;
    if has_identity || !write { return Ok(has_identity); }
    let started = std::time::Instant::now();
    database.execute_batch("BEGIN IMMEDIATE; ALTER TABLE file_metadata ADD COLUMN identity TEXT; UPDATE file_metadata SET identity=lower(hex(randomblob(16))); COMMIT;").map_err(sql)?;
    crate::core::events::emit(crate::core::events::Level::Info, "metadata.schema.upgraded",
        serde_json::json!({"stage":"identity-column","result":"success","elapsedMs":started.elapsed().as_millis()}));
    Ok(true)
}
fn sql(error: rusqlite::Error) -> Error {
    let category = match &error {
        rusqlite::Error::SqliteFailure(code, _) => format!("{:?}", code.code),
        _ => "SQLITE_RESULT_INVALID".to_owned(),
    };
    crate::core::events::emit(crate::core::events::Level::Error, "metadata.database.failed",
        serde_json::json!({"code":"METADATA_IO","category":category}));
    Error::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "METADATA_IO")
}

// Continue an existing export's metadata store; never silently create a second identity namespace.
pub(super) fn metadata_directory(root: &File) -> Result<&'static str, Error> {
    let mut found = None;
    for name in [DIRECTORY, ".mindos"] {
        match rustix::fs::openat(root, name, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty()) {
            Ok(fd) => {
                drop(fd);
                if found.is_some() { return Err(Error::forbidden("METADATA_NAMESPACE_CONFLICT")); }
                found = Some(name);
            }
            Err(rustix::io::Errno::NOENT) => (),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(found.unwrap_or(DIRECTORY))
}
