//! Property bodies belong to stable file identities; the directory database is only an index.
use crate::core::error::Error;
use rusqlite::{Connection, OptionalExtension};
use rustix::fs::{Mode, OFlags};
use std::{
    fs::File,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
};
fn name(identity: &str) -> Result<String, Error> {
    if identity.len() != 32 || !identity.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::conflict("METADATA_IDENTITY"));
    }
    Ok(format!("metadata-{identity}.sqlite"))
}
fn check(file: &File) -> Result<(), Error> {
    let m = file.metadata()?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != rustix::process::geteuid().as_raw()
        || m.mode() & 0o077 != 0
    {
        return Err(Error::forbidden("METADATA_PERMISSIONS"));
    }
    Ok(())
}
fn open(directory: &File, identity: &str, write: bool) -> Result<Option<Connection>, Error> {
    let name = name(identity)?;
    let flags = OFlags::NOFOLLOW
        | OFlags::NONBLOCK
        | OFlags::CLOEXEC
        | if write {
            OFlags::CREATE | OFlags::RDWR
        } else {
            OFlags::RDONLY
        };
    let file = match rustix::fs::openat(
        directory,
        name.as_str(),
        flags,
        Mode::from_bits_truncate(0o600),
    ) {
        Ok(file) => File::from(file),
        Err(rustix::io::Errno::NOENT) if !write => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    check(&file)?;
    if write {
        let journal = File::from(rustix::fs::openat(
            directory,
            format!("{name}-journal"),
            flags,
            Mode::from_bits_truncate(0o600),
        )?);
        check(&journal)?;
    }
    let flags = if write {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
    } else {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    let db = Connection::open_with_flags(
        format!("/proc/self/fd/{}/{name}", directory.as_raw_fd()),
        flags,
    )
    .map_err(|error| sql(error))?;
    db.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|error| sql(error))?;
    if write {
        db.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS metadata(id INTEGER PRIMARY KEY CHECK(id=1),body TEXT NOT NULL);").map_err(|error|sql(error))?;
        directory.sync_all()?;
    }
    Ok(Some(db))
}
pub(super) fn read(directory: &File, identity: &str) -> Result<Option<String>, Error> {
    let Some(db) = open(directory, identity, false)? else {
        return Ok(None);
    };
    let initialized: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='metadata' AND type='table')",
            [],
            |r| r.get(0),
        )
        .map_err(sql)?;
    if !initialized {
        return Ok(None);
    }
    db.query_row("SELECT body FROM metadata WHERE id=1", [], |r| r.get(0))
        .optional()
        .map_err(sql)
}
pub(super) fn write(directory: &File, identity: &str, body: &str) -> Result<(), Error> {
    let db = open(directory, identity, true)?.ok_or_else(Error::internal)?;
    db.execute(
        "INSERT INTO metadata VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
        [body],
    )
    .map_err(|error| sql(error))?;
    Ok(())
}
pub(super) fn lock(directory: &File, identity: &str) -> Result<Connection, Error> {
    let db = open(directory, identity, true)?.ok_or_else(Error::internal)?;
    db.execute_batch("BEGIN IMMEDIATE;")
        .map_err(|error| sql(error))?;
    Ok(db)
}
pub(super) fn locked_body(db: &Connection) -> Result<Option<String>, Error> {
    db.query_row("SELECT body FROM metadata WHERE id=1", [], |r| r.get(0))
        .optional()
        .map_err(|error| sql(error))
}
pub(super) fn patch(
    directory: &File,
    identity: &str,
    legacy: &str,
    patch: &serde_json::Map<String, serde_json::Value>,
    remove: &[String],
    validate: impl FnOnce() -> Result<(), Error>,
) -> Result<(), Error> {
    let db = lock(directory, identity)?;
    validate()?;
    let raw = locked_body(&db)?.unwrap_or_else(|| legacy.into());
    let mut value: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&raw).map_err(|_| Error::conflict("METADATA_INVALID"))?;
    value.extend(patch.clone());
    for key in remove {
        value.remove(key);
    }
    let raw = serde_json::to_string(&value).map_err(|_| Error::invalid())?;
    if raw.len() > 512 * 1024 {
        return Err(Error::too_large("METADATA_LIMIT"));
    }
    db.execute(
        "INSERT INTO metadata VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
        [raw],
    )
    .map_err(|error| sql(error))?;
    db.execute_batch("COMMIT;").map_err(|error| sql(error))
}
pub(super) fn remove(directory: &File, identity: &str) -> Result<(), Error> {
    let Some(db) = open(directory, identity, false)? else {
        return Ok(());
    };
    drop(db);
    let name = name(identity)?;
    rustix::fs::unlinkat(directory, name.as_str(), rustix::fs::AtFlags::empty())?;
    match rustix::fs::unlinkat(
        directory,
        format!("{name}-journal"),
        rustix::fs::AtFlags::empty(),
    ) {
        Ok(()) | Err(rustix::io::Errno::NOENT) => (),
        Err(error) => return Err(error.into()),
    }
    directory.sync_all()?;
    Ok(())
}
#[track_caller]
fn sql(error: rusqlite::Error) -> Error {
    let (category, extended) = match error {
        rusqlite::Error::SqliteFailure(code, _) => {
            (format!("{:?}", code.code), Some(code.extended_code))
        }
        _ => ("InvalidStorage".into(), None),
    };
    let caller = std::panic::Location::caller();
    crate::core::events::emit(
        crate::core::events::Level::Error,
        "metadata.payload.failed",
        serde_json::json!({"stage":"storage","code":"METADATA_PAYLOAD_IO","category":category,"extendedCode":extended,"line":caller.line()}),
    );
    Error::new(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "METADATA_PAYLOAD_IO",
    )
}
