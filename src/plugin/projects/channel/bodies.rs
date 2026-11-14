//! Channel payloads and owner configuration have independent physical stores.
use super::*;
use crate::core::sqlite_shards::SqliteShards;
use std::path::PathBuf;
pub(super) struct Bodies {
    shards: SqliteShards,
    legacy: PathBuf,
}
fn schema(db: &Connection) -> Result<(), Error> {
    super::channel_schema(db)?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS snapshots(project TEXT,scope TEXT,body TEXT,PRIMARY KEY(project,scope));CREATE TABLE IF NOT EXISTS migrated(complete INTEGER);").map_err(storage)
}
impl Bodies {
    pub(super) fn open(root: &Path) -> Result<Self, Error> {
        Ok(Self {
            shards: SqliteShards::new(root.join("channel-scopes"), schema)?,
            legacy: root.join("session-channels.sqlite"),
        })
    }
    pub(super) fn scope<T>(
        &self,
        project: &str,
        scope: &str,
        work: impl FnOnce(&mut Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.shards
            .with(&json!(["scope", project, scope]).to_string(), |db| {
                self.migrate(db, Some((project, scope)), None)?;
                work(db)
            })
    }
    pub(super) fn owner<T>(
        &self,
        owner: &str,
        work: impl FnOnce(&mut Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.shards
            .with(&json!(["owner", owner]).to_string(), |db| {
                self.migrate(db, None, Some(owner))?;
                work(db)
            })
    }
    pub(super) fn snapshot(&self, project: &str, scope: &str) -> Result<Value, Error> {
        self.scope(project, scope, |db| {
            let raw: String = db
                .query_row(
                    "SELECT body FROM snapshots WHERE project=?1 AND scope=?2",
                    params![project, scope],
                    |r| r.get(0),
                )
                .map_err(storage)?;
            serde_json::from_str(&raw).map_err(storage)
        })
    }
    pub(super) fn put_snapshot(&self, project: &str, scope: &str, raw: &str) -> Result<(), Error> {
        self.scope(project, scope, |db| {
            let current: Option<String> = db
                .query_row(
                    "SELECT body FROM snapshots WHERE project=?1 AND scope=?2",
                    params![project, scope],
                    |r| r.get(0),
                )
                .optional()
                .map_err(storage)?;
            if let Some(current) = current {
                if current != raw {
                    return Err(Error::conflict("CHANNEL_SCOPE_CHANGED"));
                }
            } else {
                db.execute(
                    "INSERT INTO snapshots VALUES(?1,?2,?3)",
                    params![project, scope, raw],
                )
                .map_err(storage)?;
            }
            Ok(())
        })
    }
    fn migrate(
        &self,
        db: &mut Connection,
        scope: Option<(&str, &str)>,
        owner: Option<&str>,
    ) -> Result<(), Error> {
        if !self.legacy.exists()
            || db
                .query_row("SELECT EXISTS(SELECT 1 FROM migrated)", [], |r| {
                    r.get::<_, bool>(0)
                })
                .map_err(storage)?
        {
            return Ok(());
        }
        db.execute(
            "ATTACH DATABASE ?1 AS source",
            [self.legacy.to_string_lossy().as_ref()],
        )
        .map_err(storage)?;
        let result = (|| {
            let tx = db.transaction().map_err(storage)?;
            if let Some((project, scope)) = scope {
                tx.execute("INSERT OR IGNORE INTO scopes SELECT * FROM source.scopes WHERE project=?1 AND id=?2",params![project,scope]).map_err(storage)?;
                tx.execute("INSERT OR IGNORE INTO snapshots SELECT project,id,snapshot FROM source.scopes WHERE project=?1 AND id=?2",params![project,scope]).map_err(storage)?;
                for table in ["messages", "exchanges", "publications"] {
                    tx.execute(&format!("INSERT OR IGNORE INTO {table} SELECT * FROM source.{table} WHERE project=?1 AND scope=?2"),params![project,scope]).map_err(storage)?;
                }
            }
            if let Some(owner) = owner {
                tx.execute("INSERT OR IGNORE INTO agent_servers SELECT * FROM source.agent_servers WHERE owner=?1",[owner]).map_err(storage)?;
            }
            tx.execute("INSERT INTO migrated VALUES(1)", [])
                .map_err(storage)?;
            tx.commit().map_err(storage)
        })();
        result.and(db.execute_batch("DETACH DATABASE source").map_err(storage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn old_channel_bodies_migrate_without_putting_them_in_the_node_index() {
        let root = tempfile::tempdir().unwrap();
        let db = Connection::open(root.path().join("session-channels.sqlite")).unwrap();
        super::super::channel_schema(&db).unwrap();
        db.execute("INSERT INTO scopes(project,id,token,cwd,endpoint,snapshot,expires_at) VALUES('p','scope','token','/one','','{\"section\":\"context\"}',9999999999999)",[]).unwrap();
        db.execute("INSERT INTO messages(project,scope,id,body,ack) VALUES('p','scope','message','{\"report\":true}','acknowledged')",[]).unwrap();
        db.execute(
            "INSERT INTO agent_servers VALUES('alice','server',1,'{\"command\":\"tool\"}')",
            [],
        )
        .unwrap();
        drop(db);
        let channels = Channels::open(root.path()).unwrap();
        assert_eq!(
            channels.6.snapshot("p", "scope").unwrap(),
            json!({"section":"context"})
        );
        channels
            .6
            .scope("p", "scope", |db| {
                let ack: String = db
                    .query_row("SELECT ack FROM messages WHERE id='message'", [], |r| {
                        r.get(0)
                    })
                    .unwrap();
                assert_eq!(ack, "acknowledged");
                Ok(())
            })
            .unwrap();
        channels
            .6
            .owner("alice", |db| {
                assert_eq!(
                    db.query_row("SELECT COUNT(*) FROM agent_servers", [], |r| r
                        .get::<_, usize>(0))
                        .unwrap(),
                    1
                );
                Ok(())
            })
            .unwrap();
        let db = channels.0.lock().unwrap();
        assert_eq!(
            db.query_row("SELECT snapshot FROM scopes", [], |r| r.get::<_, String>(0))
                .unwrap(),
            ""
        );
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM messages", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            0
        );
        drop(db);
        assert_ne!(
            channels
                .6
                .shards
                .path(&json!(["scope", "p", "scope"]).to_string()),
            channels
                .6
                .shards
                .path(&json!(["scope", "p", "other"]).to_string())
        );
        drop(channels);
        let channels = Channels::open(root.path()).unwrap();
        assert_eq!(
            channels.6.snapshot("p", "scope").unwrap(),
            json!({"section":"context"})
        );
    }
}
