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
            database.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; BEGIN IMMEDIATE; CREATE TABLE IF NOT EXISTS file_metadata(path TEXT PRIMARY KEY, data TEXT NOT NULL, identity TEXT NOT NULL DEFAULT (lower(hex(randomblob(16))))); PRAGMA application_id=1296321876; PRAGMA user_version=1; COMMIT;").map_err(sql)?;
            directory.sync_all()?;
        } else if !write && application == 0 && version == 0 {
            return Ok(None);
        } else if application != APPLICATION_ID || version != 1 {
            return Err(Error::conflict("METADATA_VERSION"));
        }
        let has_identity = prepare_identity(&database, write)?;
        if write && !database.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='scope_redirects' AND type='table')",[],|r|r.get::<_,bool>(0)).map_err(sql)? {
            database
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS scope_redirects(prefix TEXT PRIMARY KEY);",
                )
                .map_err(sql)?;
        }
        Ok(Some(Self {
            has_identity,
            database,
            _directory: directory,
        }))
    }
    pub(super) fn is_scope(&self) -> Result<bool, Error> {
        self.database.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='scope_info' AND type='table')", [], |row| row.get(0)).map_err(sql)
    }
    pub(super) fn set_scope_info(
        &mut self,
        owner: &str,
        key: &str,
        value: &Value,
    ) -> Result<(), Error> {
        let tx = self.database.transaction().map_err(sql)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS scope_info(owner TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(owner,key));").map_err(sql)?;
        tx.execute("INSERT INTO scope_info VALUES(?1,?2,?3) ON CONFLICT(owner,key) DO UPDATE SET value=excluded.value", params![owner,key,value.to_string()]).map_err(sql)?;
        tx.commit().map_err(sql)
    }
    pub(super) fn transfer_scope(&mut self, target: &mut Self, prefix: &str) -> Result<(), Error> {
        let rows = {
            let mut statement=self.database.prepare("SELECT path,substr(path,length(?1)+2),data,identity FROM file_metadata WHERE substr(path,1,length(?1)+1)=?1||'/'").map_err(sql)?;
            let rows = statement
                .query_map([prefix], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })
                .map_err(sql)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(sql)?
        };
        let mut ordered = rows.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|row| &row.3);
        let mut payload_locks = Vec::new();
        for (_old_path, path, legacy, identity) in ordered {
            let locked = super::metadata_payload::lock(&self._directory, identity)?;
            let raw =
                super::metadata_payload::locked_body(&locked)?.unwrap_or_else(|| legacy.clone());
            let body: Map<String, Value> =
                serde_json::from_str(&raw).map_err(|_| Error::conflict("METADATA_INVALID"))?;
            payload_locks.push(locked);
            let prior: Option<String> = target
                .database
                .query_row(
                    "SELECT identity FROM file_metadata WHERE path=?1",
                    [path],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql)?;
            if prior.as_ref().is_some_and(|id| id != identity)
                || prior.is_some() && target.read(path)? != body
            {
                return Err(Error::conflict("METADATA_SCOPE_CONFLICT"));
            }
            super::metadata_payload::write(
                &target._directory,
                identity,
                &serde_json::to_string(&body).map_err(|_| Error::invalid())?,
            )?;
        }
        let source = self
            .database
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql)?;
        let current: Vec<(String, String)> = {
            let mut statement=source.prepare("SELECT path,identity FROM file_metadata WHERE substr(path,1,length(?1)+1)=?1||'/' ORDER BY path").map_err(sql)?;
            let rows = statement
                .query_map([prefix], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(sql)?;
            rows.collect::<Result<_, _>>().map_err(sql)?
        };
        let mut expected = rows
            .iter()
            .map(|(path, _, _, id)| (path.clone(), id.clone()))
            .collect::<Vec<_>>();
        expected.sort();
        if current != expected {
            return Err(Error::conflict("METADATA_SCOPE_CHANGED"));
        }
        let tx = target.database.transaction().map_err(sql)?;
        for (_, path, _, identity) in &rows {
            tx.execute(
                "INSERT OR IGNORE INTO file_metadata(path,data,identity) VALUES(?1,'{}',?2)",
                params![path, identity],
            )
            .map_err(sql)?;
        }
        tx.execute_batch("CREATE TABLE IF NOT EXISTS scope_info(owner TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(owner,key));").map_err(sql)?;
        tx.commit().map_err(sql)?;
        source
            .execute("INSERT OR IGNORE INTO scope_redirects VALUES(?1)", [prefix])
            .map_err(sql)?;
        source
            .execute(
                "DELETE FROM file_metadata WHERE substr(path,1,length(?1)+1)=?1||'/'",
                [prefix],
            )
            .map_err(sql)?;
        source.commit().map_err(sql)?;
        drop(payload_locks);
        self.collect_payloads(
            rows.into_iter()
                .map(|(_, _, _, identity)| identity)
                .collect(),
        )
    }
    pub fn existing_write(root: &File) -> Result<Option<Self>, Error> {
        if Self::open(root, false)?.is_none() {
            return Ok(None);
        }
        Self::open(root, true)
    }
    pub fn identity(&self, path: &str) -> Result<Option<String>, Error> {
        if !self.has_identity {
            return Ok(None);
        }
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
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        let raw = if let Some(identity) = self.identity(path)? {
            super::metadata_payload::read(&self._directory, &identity)?.or(raw)
        } else {
            raw
        };
        raw.map(|raw| serde_json::from_str(&raw).map_err(|_| Error::conflict("METADATA_INVALID")))
            .unwrap_or_else(|| Ok(Map::new()))
    }
    pub fn patch(
        &mut self,
        path: &str,
        patch: &Map<String, Value>,
        remove: &[String],
    ) -> Result<(), Error> {
        let (legacy, identity) = self.entry_for_patch(path)?;
        super::metadata_payload::patch(
            &self._directory,
            &identity,
            &legacy,
            patch,
            remove,
            || {
                check_route(&self.database, path)?;
                if self.identity(path)?.as_deref() != Some(&identity) {
                    return Err(Error::conflict("METADATA_RESOURCE_CHANGED"));
                }
                Ok(())
            },
        )?;
        if legacy != "{}" {
            self.database
                .execute(
                    "UPDATE file_metadata SET data='{}' WHERE path=?1 AND identity=?2 AND data=?3",
                    params![path, identity, legacy],
                )
                .map_err(sql)?;
        }
        Ok(())
    }
    fn entry_for_patch(&mut self, path: &str) -> Result<(String, String), Error> {
        let current = self
            .database
            .query_row(
                "SELECT data,identity FROM file_metadata WHERE path=?1",
                [path],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        if let Some(current) = current {
            return Ok(current);
        }
        let tx = self
            .database
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql)?;
        check_route(&tx, path)?;
        tx.execute("INSERT OR IGNORE INTO file_metadata(path,data,identity) VALUES(?1,'{}',lower(hex(randomblob(16))))",[path]).map_err(sql)?;
        let entry = tx
            .query_row(
                "SELECT data,identity FROM file_metadata WHERE path=?1",
                [path],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(entry)
    }
    pub fn remove(&self, path: &str) -> Result<(), Error> {
        let retired = self.identities(path)?;
        self.database
            .execute(
                "DELETE FROM file_metadata WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
                [path],
            )
            .map_err(sql)?;
        self.collect_payloads(retired)
    }
    pub fn rename(&mut self, path: &str, to: &str) -> Result<(), Error> {
        let retired = self.identities(to)?;
        let tx = self.database.transaction().map_err(sql)?;
        tx.execute(
            "DELETE FROM file_metadata WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
            [to],
        )
        .map_err(sql)?;
        tx.execute("UPDATE file_metadata SET path=?2||substr(path,length(?1)+1) WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'", params![path,to]).map_err(sql)?;
        tx.commit().map_err(sql)?;
        self.collect_payloads(retired)
    }
    fn identities(&self, path: &str) -> Result<Vec<String>, Error> {
        let mut statement=self.database.prepare("SELECT identity FROM file_metadata WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'").map_err(sql)?;
        let rows = statement.query_map([path], |r| r.get(0)).map_err(sql)?;
        rows.collect::<Result<_, _>>().map_err(sql)
    }
    fn collect_payloads(&self, identities: Vec<String>) -> Result<(), Error> {
        for identity in identities {
            let retained: bool = self
                .database
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM file_metadata WHERE identity=?1)",
                    [&identity],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            if !retained {
                super::metadata_payload::remove(&self._directory, &identity)?;
            }
        }
        Ok(())
    }
}
// Legacy version-1 stores predate identity. Reads remain read-only; writes upgrade atomically.
fn prepare_identity(database: &Connection, write: bool) -> Result<bool, Error> {
    let has_identity: bool = database
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('file_metadata') WHERE name='identity')",
            [],
            |row| row.get(0),
        )
        .map_err(sql)?;
    if has_identity || !write {
        return Ok(has_identity);
    }
    let started = std::time::Instant::now();
    let tx = database.unchecked_transaction().map_err(sql)?;
    // Acquire the schema writer before rechecking an upgrade attempted by another opener.
    tx.execute("UPDATE file_metadata SET path=path WHERE 0", [])
        .map_err(sql)?;
    let upgraded: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('file_metadata') WHERE name='identity')",
            [],
            |row| row.get(0),
        )
        .map_err(sql)?;
    if !upgraded {
        tx.execute_batch("ALTER TABLE file_metadata ADD COLUMN identity TEXT; UPDATE file_metadata SET identity=lower(hex(randomblob(16)));").map_err(sql)?;
    }
    tx.commit().map_err(sql)?;
    crate::core::events::emit(
        crate::core::events::Level::Info,
        "metadata.schema.upgraded",
        serde_json::json!({"stage":"identity-column","result":"success","elapsedMs":started.elapsed().as_millis()}),
    );
    Ok(true)
}
fn sql(error: rusqlite::Error) -> Error {
    let category = match &error {
        rusqlite::Error::SqliteFailure(code, _) => format!("{:?}", code.code),
        _ => "SQLITE_RESULT_INVALID".to_owned(),
    };
    crate::core::events::emit(
        crate::core::events::Level::Error,
        "metadata.database.failed",
        serde_json::json!({"code":"METADATA_IO","category":category}),
    );
    Error::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "METADATA_IO")
}

// Continue an existing export's metadata store; never silently create a second identity namespace.
pub(super) fn metadata_directory(root: &File) -> Result<&'static str, Error> {
    let mut found = None;
    for name in [DIRECTORY, ".mindos"] {
        match rustix::fs::openat(
            root,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => {
                drop(fd);
                if found.is_some() {
                    return Err(Error::forbidden("METADATA_NAMESPACE_CONFLICT"));
                }
                found = Some(name);
            }
            Err(rustix::io::Errno::NOENT) => (),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(found.unwrap_or(DIRECTORY))
}

fn check_route(db: &Connection, path: &str) -> Result<(), Error> {
    let redirected:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM scope_redirects WHERE substr(?1,1,length(prefix)+1)=prefix||'/')",[path],|r|r.get(0)).map_err(sql)?;
    if redirected {
        Err(Error::conflict("METADATA_SCOPE_CHANGED"))
    } else {
        Ok(())
    }
}
