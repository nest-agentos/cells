//! Route private draft namespaces to isolated session aggregates before exposing the binding.
use super::super::harness_recovery::Binding;
use super::storage;
use crate::core::{error::Error, keyed_lock::SyncKeyedLocks, sqlite_shards::SqliteShards};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
};
pub(in crate::plugin::projects) struct ClientStores {
    shards: SqliteShards,
    catalog: SqliteShards,
    legacy: PathBuf,
    gates: SyncKeyedLocks,
    routes: Mutex<HashMap<String, String>>,
}
fn catalog_schema(db: &Connection) -> Result<(), Error> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS routes(project TEXT,owner TEXT,namespace TEXT,resource TEXT NOT NULL,binding TEXT,PRIMARY KEY(project,owner,namespace));").map_err(storage)
}
fn schema(db: &Connection) -> Result<(), Error> {
    super::client_state::schema(db)?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS imports(source TEXT,namespace TEXT,PRIMARY KEY(source,namespace));").map_err(storage)
}
impl ClientStores {
    pub(super) fn open(root: &Path) -> Result<Self, Error> {
        Ok(Self {
            shards: SqliteShards::new(root.join("harness-sessions"), schema)?,
            catalog: SqliteShards::new(root.join("harness-index"), catalog_schema)?,
            legacy: root.join("harness-client-state.sqlite"),
            gates: SyncKeyedLocks::default(),
            routes: Mutex::default(),
        })
    }
    fn identity(project: &str, owner: &str, namespace: &str) -> String {
        json!([project, owner, namespace]).to_string()
    }
    pub(in crate::plugin::projects) fn with<T>(
        &self,
        project: &str,
        owner: &str,
        namespace: &str,
        work: impl FnOnce(&mut Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let key = Self::identity(project, owner, namespace);
        self.gates.with(&key, || {
            let route = self.route(project, owner, namespace)?;
            self.shards.with(&route, work)
        })
    }
    fn route(&self, project: &str, owner: &str, namespace: &str) -> Result<String, Error> {
        let key = Self::identity(project, owner, namespace);
        if let Some(route) = self.routes.lock().map_err(storage)?.get(&key).cloned() {
            return Ok(route);
        }
        let existing: Option<String> = self.catalog.with("routes", |db| {
            db.query_row(
                "SELECT resource FROM routes WHERE project=?1 AND owner=?2 AND namespace=?3",
                params![project, owner, namespace],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)
        })?;
        let missing = existing.is_none();
        let route =
            existing.unwrap_or_else(|| json!(["draft", project, owner, namespace]).to_string());
        self.shards.with(&route, |db| {
            import(db, &self.legacy, project, owner, namespace, None)
        })?;
        if missing {
            self.catalog.with("routes", |db| {
                db.execute(
                    "INSERT OR IGNORE INTO routes VALUES(?1,?2,?3,?4,NULL)",
                    params![project, owner, namespace, route],
                )
                .map_err(storage)?;
                Ok(())
            })?;
        }
        self.cache_route(key, route.clone())?;
        Ok(route)
    }
    pub(in crate::plugin::projects) fn bind(&self, binding: &Binding) -> Result<(), Error> {
        let key = Self::identity(&binding.project_id, &binding.owner, &binding.namespace);
        self.gates.with(&key, || self.bind_resource(binding, &key))
    }
    fn bind_resource(&self, binding: &Binding, key: &str) -> Result<(), Error> {
        let before = self.route(&binding.project_id, &binding.owner, &binding.namespace)?;
        let after = json!([
            "session",
            binding.project_id,
            binding.owner,
            binding.profile_id,
            binding.session_id
        ])
        .to_string();
        if before != after {
            self.validate_binding(binding, &before)?;
            self.shards.with(&after, |db| {
                import(
                    db,
                    &self.shards.path(&before),
                    &binding.project_id,
                    &binding.owner,
                    &binding.namespace,
                    Some(binding),
                )
            })?;
        }
        let encoded = serde_json::to_string(binding).map_err(storage)?;
        self.catalog.with("routes",|db|{
            db.execute("UPDATE routes SET resource=?4,binding=?5 WHERE project=?1 AND owner=?2 AND namespace=?3 AND (resource<>?4 OR binding IS NOT ?5)",params![binding.project_id,binding.owner,binding.namespace,after,encoded]).map_err(storage)?;
            Ok(())
        })?;
        self.cache_route(key.into(), after)
    }
    fn validate_binding(&self, binding: &Binding, before: &str) -> Result<(), Error> {
        let previous:Option<String>=self.shards.with(before,|db|db.query_row("SELECT binding FROM harness_bindings WHERE project=?1 AND owner=?2 AND namespace=?3",params![binding.project_id,binding.owner,binding.namespace],|r|r.get(0)).optional().map_err(storage))?;
        if let Some(previous) = previous {
            let old: Binding = serde_json::from_str(&previous).map_err(storage)?;
            if old.profile_id != binding.profile_id || old.session_id != binding.session_id {
                return Err(Error::conflict("HARNESS_RECOVERY_BINDING_CHANGED"));
            }
        }
        let existing: Option<String> = self.catalog.with("routes", |db| {
            db.query_row(
                "SELECT binding FROM routes WHERE project=?1 AND owner=?2 AND namespace=?3",
                params![binding.project_id, binding.owner, binding.namespace],
                |r| r.get(0),
            )
            .map_err(storage)
        })?;
        if existing.is_some() {
            return Err(Error::conflict("HARNESS_RECOVERY_BINDING_CHANGED"));
        }
        Ok(())
    }
    fn cache_route(&self, key: String, route: String) -> Result<(), Error> {
        let mut routes = self.routes.lock().map_err(storage)?;
        if routes.len() >= 4096 {
            routes.clear();
        }
        routes.insert(key, route);
        Ok(())
    }
    pub(in crate::plugin::projects) fn bindings(&self) -> Result<Vec<Binding>, Error> {
        // Legacy bindings remain discoverable until their first resource migration.
        let mut bindings = if self.legacy.exists() {
            let db = Connection::open_with_flags(
                &self.legacy,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .map_err(storage)?;
            read_bindings(&db, "SELECT binding FROM harness_bindings")?
        } else {
            Vec::new()
        };
        for binding in self.catalog.with("routes", |db| {
            read_bindings(db, "SELECT binding FROM routes WHERE binding IS NOT NULL")
        })? {
            bindings.retain(|old| {
                Self::identity(&old.project_id, &old.owner, &old.namespace)
                    != Self::identity(&binding.project_id, &binding.owner, &binding.namespace)
            });
            bindings.push(binding);
        }
        Ok(bindings)
    }
}
fn read_bindings(db: &Connection, sql: &str) -> Result<Vec<Binding>, Error> {
    let mut statement = db.prepare(sql).map_err(storage)?;
    let rows = statement
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|r| serde_json::from_str(&r.map_err(storage)?).map_err(storage))
        .collect()
}
fn import(
    db: &mut Connection,
    source: &Path,
    project: &str,
    owner: &str,
    namespace: &str,
    binding: Option<&Binding>,
) -> Result<(), Error> {
    if !source.exists() {
        return Ok(());
    }
    let source_id = source.to_string_lossy().to_string();
    if binding.is_none()
        && db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM imports WHERE source=?1 AND namespace=?2)",
                params![source_id, namespace],
                |r| r.get::<_, bool>(0),
            )
            .map_err(storage)?
    {
        return Ok(());
    }
    db.execute("ATTACH DATABASE ?1 AS source", [source_id.as_str()])
        .map_err(storage)?;
    let result = copy(db, &source_id, project, owner, namespace, binding);
    let detached = db.execute_batch("DETACH DATABASE source").map_err(storage);
    result.and(detached)
}
fn copy(
    db: &mut Connection,
    source: &str,
    project: &str,
    owner: &str,
    namespace: &str,
    binding: Option<&Binding>,
) -> Result<(), Error> {
    let tx = db.transaction().map_err(storage)?;
    if binding.is_some() {
        tx.execute("DELETE FROM client_state WHERE project=?1 AND owner=?2 AND substr(key,1,length(?3)+1)=?3||'/'",params![project,owner,namespace]).map_err(storage)?;
    }
    tx.execute("INSERT OR IGNORE INTO client_state SELECT * FROM source.client_state WHERE project=?1 AND owner=?2 AND substr(key,1,length(?3)+1)=?3||'/'",params![project,owner,namespace]).map_err(storage)?;
    for table in ["harness_bindings", "owned_harness_queues"] {
        tx.execute(&format!("INSERT OR IGNORE INTO {table} SELECT * FROM source.{table} WHERE project=?1 AND owner=?2 AND namespace=?3"),params![project,owner,namespace]).map_err(storage)?;
    }
    if let Some(b) = binding {
        tx.execute("INSERT OR IGNORE INTO harness_queue_sessions SELECT * FROM source.harness_queue_sessions WHERE project=?1 AND owner=?2 AND profile=?3 AND session=?4",params![project,owner,b.profile_id,b.session_id]).map_err(storage)?;
    } else {
        tx.execute("INSERT OR IGNORE INTO harness_queue_sessions SELECT q.* FROM source.harness_queue_sessions q JOIN source.owned_harness_queues a ON q.project=a.project AND q.owner=a.owner AND q.profile=a.profile AND q.session=a.session WHERE a.project=?1 AND a.owner=?2 AND a.namespace=?3",params![project,owner,namespace]).map_err(storage)?;
    }
    // Old command rows lack a session column; preserve the principal's immutable receipts.
    for table in ["harness_recoveries", "harness_queue_commands"] {
        tx.execute(
            &format!("INSERT OR IGNORE INTO {table} SELECT * FROM source.{table} WHERE owner=?1"),
            [owner],
        )
        .map_err(storage)?;
    }
    tx.execute(
        "INSERT OR REPLACE INTO imports VALUES(?1,?2)",
        params![source, namespace],
    )
    .map_err(storage)?;
    tx.commit().map_err(storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn binding(namespace: &str, session: &str) -> Binding {
        Binding {
            project_id: "p".into(),
            owner: "alice".into(),
            namespace: namespace.into(),
            profile_id: "codex".into(),
            session_id: session.into(),
            revision: 1,
        }
    }
    fn put(store: &ClientStores, namespace: &str, value: &str) {
        store
            .with("p", "alice", namespace, |db| {
                db.execute(
                    "INSERT OR REPLACE INTO client_state VALUES('p','alice',?1,?2)",
                    params![format!("{namespace}/composer"), value],
                )
                .map_err(storage)?;
                Ok(())
            })
            .unwrap();
    }
    fn get(store: &ClientStores, namespace: &str) -> String {
        store
            .with("p", "alice", namespace, |db| {
                db.query_row(
                    "SELECT body FROM client_state WHERE key=?1",
                    [format!("{namespace}/composer")],
                    |r| r.get(0),
                )
                .map_err(storage)
            })
            .unwrap()
    }
    #[test]
    fn bindings_move_drafts_to_session_files_and_preserve_aliases_and_revisions() {
        let root = tempfile::tempdir().unwrap();
        let store = ClientStores::open(root.path()).unwrap();
        put(&store, "first", "a");
        put(&store, "alias", "b");
        put(&store, "second", "c");
        let mut first = binding("first", "one");
        store.bind(&first).unwrap();
        store.bind(&binding("alias", "one")).unwrap();
        store.bind(&binding("second", "two")).unwrap();
        assert_eq!(
            store.route("p", "alice", "first").unwrap(),
            store.route("p", "alice", "alias").unwrap()
        );
        assert_ne!(
            store.route("p", "alice", "first").unwrap(),
            store.route("p", "alice", "second").unwrap()
        );
        first.revision = 2;
        store.bind(&first).unwrap();
        assert_eq!(
            store
                .bindings()
                .unwrap()
                .iter()
                .find(|b| b.namespace == "first")
                .unwrap()
                .revision,
            2
        );
        assert!(store.bind(&binding("first", "other")).is_err());
        drop(store);
        let store = ClientStores::open(root.path()).unwrap();
        assert_eq!(get(&store, "first"), "a");
        assert_eq!(get(&store, "alias"), "b");
        assert_eq!(get(&store, "second"), "c");
    }
    #[test]
    fn interrupted_route_publication_reimports_the_latest_draft() {
        let root = tempfile::tempdir().unwrap();
        let store = ClientStores::open(root.path()).unwrap();
        put(&store, "first", "old");
        let binding = binding("first", "one");
        store.catalog.with("routes",|db|db.execute_batch("CREATE TRIGGER unavailable BEFORE UPDATE ON routes BEGIN SELECT RAISE(ABORT,'unavailable'); END;").map_err(storage)).unwrap();
        assert!(store.bind(&binding).is_err());
        put(&store, "first", "new");
        store
            .catalog
            .with("routes", |db| {
                db.execute_batch("DROP TRIGGER unavailable;")
                    .map_err(storage)
            })
            .unwrap();
        store.bind(&binding).unwrap();
        assert_eq!(get(&store, "first"), "new");
    }
    #[test]
    fn legacy_namespace_migration_is_read_only_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("harness-client-state.sqlite");
        let db = Connection::open(&path).unwrap();
        schema(&db).unwrap();
        db.execute(
            "INSERT INTO client_state VALUES('p','alice','first/composer','legacy')",
            [],
        )
        .unwrap();
        drop(db);
        let store = ClientStores::open(root.path()).unwrap();
        assert_eq!(get(&store, "first"), "legacy");
        store.bind(&binding("first", "one")).unwrap();
        put(&store, "first", "current");
        drop(store);
        let store = ClientStores::open(root.path()).unwrap();
        assert_eq!(get(&store, "first"), "current");
        let db = Connection::open(&path).unwrap();
        assert_eq!(
            db.query_row("SELECT body FROM client_state", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "legacy"
        );
    }
}
