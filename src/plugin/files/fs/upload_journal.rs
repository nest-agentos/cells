//! Durable upload ownership records, independent of the size of the exported tree.
use crate::{core::error::Error, fs::export::Export};
use rusqlite::{params, Connection};
use rustix::fs::{Mode, OFlags};
use std::{fs::File, os::fd::AsRawFd, os::unix::fs::MetadataExt, sync::Mutex};

pub(crate) const DIRECTORY: &str = ".itookit-upload-state";
const DATABASE: &str = "uploads.sqlite";
const APPLICATION_ID: i64 = 0x50415550;

pub(crate) struct UploadJournal {
    database: Mutex<Connection>,
    _directory: File,
    _root_lock: File,
}

#[derive(Clone)]
pub(crate) struct PendingUpload {
    pub id: i64,
    pub parent: String,
    pub name: String,
    pub parent_device: i64,
    pub parent_inode: i64,
    pub device: Option<i64>,
    pub inode: Option<i64>,
}

impl UploadJournal {
    pub fn open(root: &File) -> Result<Self, Error> {
        let directory = private_directory(root)?;
        for name in [DATABASE.to_owned(), format!("{DATABASE}-journal")] {
            private_file(&directory, &name)?;
        }
        directory.sync_all()?;
        let file = format!("/proc/self/fd/{}/{DATABASE}", directory.as_raw_fd());
        let database =
            Connection::open_with_flags(file, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
                .map_err(sql)?;
        initialize(&database)?;
        Ok(Self {
            database: Mutex::new(database),
            _directory: directory,
            _root_lock: root.try_clone()?,
        })
    }

    pub fn prepare(
        &self,
        parent: &str,
        name: &str,
        directory: &File,
    ) -> Result<PendingUpload, Error> {
        let metadata = directory.metadata()?;
        let database = self
            .database
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        database.execute("INSERT INTO uploads(parent, name, parent_device, parent_inode) VALUES (?1, ?2, ?3, ?4)",
            params![parent, name, metadata.dev() as i64, metadata.ino() as i64]).map_err(sql)?;
        Ok(PendingUpload {
            id: database.last_insert_rowid(),
            parent: parent.into(),
            name: name.into(),
            parent_device: metadata.dev() as i64,
            parent_inode: metadata.ino() as i64,
            device: None,
            inode: None,
        })
    }

    pub fn identify(&self, id: i64, file: &File) -> Result<(), Error> {
        let metadata = file.metadata()?;
        self.database
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .execute(
                "UPDATE uploads SET device=?2, inode=?3 WHERE id=?1",
                params![id, metadata.dev() as i64, metadata.ino() as i64],
            )
            .map_err(sql)?;
        Ok(())
    }

    pub fn forget(&self, id: i64) -> Result<(), Error> {
        self.database
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .execute("DELETE FROM uploads WHERE id=?1", [id])
            .map_err(sql)?;
        Ok(())
    }

    pub fn batch(&self, after: i64) -> Result<Vec<PendingUpload>, Error> {
        let database = self
            .database
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut statement = database
            .prepare(
                "SELECT id,parent,name,parent_device,parent_inode,device,inode
            FROM uploads WHERE id>?1 ORDER BY id LIMIT 128",
            )
            .map_err(sql)?;
        let rows = statement
            .query_map([after], |row| {
                Ok(PendingUpload {
                    id: row.get(0)?,
                    parent: row.get(1)?,
                    name: row.get(2)?,
                    parent_device: row.get(3)?,
                    parent_inode: row.get(4)?,
                    device: row.get(5)?,
                    inode: row.get(6)?,
                })
            })
            .map_err(sql)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(sql)
    }
}

impl PendingUpload {
    pub fn identify(&mut self, file: &File) -> Result<(), Error> {
        let metadata = file.metadata()?;
        self.device = Some(metadata.dev() as i64);
        self.inode = Some(metadata.ino() as i64);
        Ok(())
    }
}

fn private_directory(root: &File) -> Result<File, Error> {
    match rustix::fs::mkdirat(root, DIRECTORY, Mode::from_bits_truncate(0o700)) {
        Ok(()) => root.sync_all()?,
        Err(rustix::io::Errno::EXIST) => (),
        Err(error) => return Err(error.into()),
    }
    let directory = File::from(rustix::fs::openat(
        root,
        DIRECTORY,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let metadata = directory.metadata()?;
    if metadata.mode() & 0o077 != 0 || metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(Error::forbidden("UPLOAD_JOURNAL_PERMISSIONS"));
    }
    Ok(directory)
}

fn private_file(directory: &File, name: &str) -> Result<(), Error> {
    let file = File::from(rustix::fs::openat(
        directory,
        name,
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::from_bits_truncate(0o600),
    )?);
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(Error::forbidden("UPLOAD_JOURNAL_PERMISSIONS"));
    }
    Ok(())
}

fn initialize(database: &Connection) -> Result<(), Error> {
    let application: i64 = database
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(sql)?;
    let version: i64 = database
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(sql)?;
    if (application != 0 && application != APPLICATION_ID) || !(0..=1).contains(&version) {
        return Err(Error::conflict("UPLOAD_JOURNAL_VERSION"));
    }
    database.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT, parent TEXT NOT NULL, name TEXT NOT NULL UNIQUE,
            parent_device INTEGER NOT NULL, parent_inode INTEGER NOT NULL, device INTEGER, inode INTEGER);
        PRAGMA application_id=1346458960; PRAGMA user_version=1; COMMIT;").map_err(sql)
}

fn sql(_: rusqlite::Error) -> Error {
    Error::new(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "UPLOAD_JOURNAL_IO",
    )
}

impl Export {
    pub(crate) fn journal(&self) -> Result<std::sync::Arc<UploadJournal>, Error> {
        self.upload_journal
            .clone()
            .ok_or_else(|| Error::forbidden("EROFS"))
    }
}
