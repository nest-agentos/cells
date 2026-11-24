//! Read the local Codex index without starting or resuming an app-server.
use super::*;
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};

fn database(home: &Path) -> Result<Option<PathBuf>, Error> {
    let mut candidates = Vec::new();
    for entry in std::fs::read_dir(home)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {continue};
        let Some(version) = name.strip_prefix("state_").and_then(|s| s.strip_suffix(".sqlite")).and_then(|s| s.parse::<u32>().ok()) else {continue};
        if entry.file_type()?.is_symlink() {return Err(Error::forbidden("HARNESS_CATALOG_SYMLINK"))}
        candidates.push((version,entry.path()));
    }
    Ok(candidates.into_iter().max_by_key(|(version,_)|*version).map(|(_,path)|path))
}
fn roots(config: &ProfileConfig, runtime: Option<&crate::projects::runtime::ProjectRuntime>) -> Result<Vec<String>,Error> {
    if let Some(runtime)=runtime {
        return Ok(vec![runtime.cwd(),crate::projects::native::root(runtime.state()?.as_ref(),&runtime.project)?]);
    }
    Ok(config.workspaces.iter().filter_map(|w|w.path.to_str().map(str::to_owned)).collect())
}
fn schema_error(_: impl std::fmt::Display) -> Error {Error::conflict("CODEX_LOCAL_CATALOG_UNAVAILABLE")}
fn scan(config: &ProfileConfig, runtime: Option<&crate::projects::runtime::ProjectRuntime>, args: &Value) -> Result<Option<Vec<Value>>,Error> {
    let Some(path)=database(&config.home)? else {return Ok(None)};
    let db=Connection::open_with_flags(path,OpenFlags::SQLITE_OPEN_READ_ONLY|OpenFlags::SQLITE_OPEN_NO_MUTEX|OpenFlags::SQLITE_OPEN_NOFOLLOW).map_err(schema_error)?;
    db.busy_timeout(std::time::Duration::from_millis(250)).map_err(schema_error)?;
    let limit=args["limit"].as_u64().unwrap_or(100).clamp(1,100);
    let offset=args["cursor"].as_str().map(|s|s.parse::<u32>().map_err(|_|Error::invalid())).transpose()?.unwrap_or(0);
    let named=db.prepare("PRAGMA table_info(threads)").map_err(schema_error)?.query_map([],|row|row.get::<_,String>(1)).map_err(schema_error)?.collect::<Result<Vec<_>,_>>().map_err(schema_error)?.iter().any(|name|name=="name");
    let name=if named {"name"} else {"NULL"};
    let sql=format!("SELECT id,rollout_path,cwd,{name},title,created_at,updated_at,archived FROM threads WHERE archived=?1 AND EXISTS(SELECT 1 FROM json_each(?2) WHERE cwd=value OR substr(cwd,1,length(value)+1)=value||'/') ORDER BY updated_at DESC,id LIMIT ?3 OFFSET ?4");
    let mut query=db.prepare(&sql).map_err(schema_error)?;
    let rows=query.query_map(rusqlite::params![args["archived"].as_bool().unwrap_or(false),json!(roots(config,runtime)?).to_string(),limit+1,offset],|row|{
        Ok(json!({"id":row.get::<_,String>(0)?,"path":row.get::<_,String>(1)?,"cwd":row.get::<_,String>(2)?,"name":row.get::<_,Option<String>>(3)?,"preview":row.get::<_,Option<String>>(4)?,
            "createdAt":row.get::<_,i64>(5)?,"updatedAt":row.get::<_,i64>(6)?,"archived":row.get::<_,bool>(7)?,"status":{"type":"notLoaded"}}))
    }).map_err(schema_error)?;
    rows.map(|row|row.map_err(schema_error)).collect::<Result<Vec<_>,_>>().map(Some)
}
impl Codex {
    pub(super) async fn local_sessions(&self,args:&Value)->Result<Option<Value>,Error> {
        let config=self.config.clone(); let runtime=self.runtime.clone(); let input=args.clone();
        let started=std::time::Instant::now();
        let result=tokio::task::spawn_blocking(move||scan(&config,runtime.as_deref(),&input)).await.map_err(|_|Error::internal())?;
        crate::core::events::emit(if result.is_err(){crate::core::events::Level::Warn}else{crate::core::events::Level::Debug},"harness.catalog.local",
            json!({"harness":"codex","stage":"read-index","result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}));
        let Some(rows)=result? else {return Ok(None)};
        let limit=args["limit"].as_u64().unwrap_or(100).clamp(1,100) as usize;
        let offset=args["cursor"].as_str().unwrap_or("0").parse::<usize>().map_err(|_|Error::invalid())?;
        let more=rows.len()>limit;
        let bridge=self.bridge.lock().await.clone();
        let sessions=rows.into_iter().take(limit).filter(|r|r["cwd"].as_str().is_some_and(|cwd|self.authorized(cwd)))
            .map(|row|self.session(&bridge.as_ref().map(|b|self.runtime_thread(b,&row)).unwrap_or(row))).collect::<Vec<_>>();
        Ok(Some(json!({"sessions":sessions,"nextCursor":if more{json!((offset+limit).to_string())}else{Value::Null}})))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn local_index_discovers_nested_sessions_without_starting_codex_or_rewriting_cwd() {
        let home=tempfile::tempdir().unwrap(); let work=tempfile::tempdir().unwrap();
        let nested=work.path().join("nested/deep"); std::fs::create_dir_all(&nested).unwrap();
        let sibling=PathBuf::from(format!("{}-other",work.path().display()));
        std::fs::create_dir_all(&sibling).unwrap();
        let db=Connection::open(home.path().join("state_5.sqlite")).unwrap();
        db.execute_batch("CREATE TABLE threads(id TEXT,rollout_path TEXT,cwd TEXT,title TEXT,created_at INTEGER,updated_at INTEGER,archived INTEGER);").unwrap();
        for (id,cwd) in [("root",work.path()),("nested",nested.as_path()),("sibling",sibling.as_path())] {
            db.execute("INSERT INTO threads VALUES(?1,'sessions/test.jsonl',?2,'Title',1,2,0)",rusqlite::params![id,cwd.to_str().unwrap()]).unwrap();
        }
        drop(db);
        let config:ProfileConfig=toml::from_str(&format!("id='codex'\nkind='codex'\ncommand='/bin/false'\nhome={:?}\n[[workspaces]]\nid='p'\npath={:?}",home.path(),work.path())).unwrap();
        let codex=Codex::new(config);
        let page=codex.local_sessions(&json!({"limit":10})).await.unwrap().unwrap();
        assert_eq!(page["sessions"].as_array().unwrap().len(),2);
        assert_eq!(page["sessions"][0]["cwd"],nested.to_str().unwrap());
        assert_eq!(page["sessions"][0]["title"],"Title");
        assert!(codex.bridge.lock().await.is_none());
        std::fs::remove_dir(sibling).unwrap();
    }
    #[tokio::test]
    async fn catalog_and_thread_detail_share_titles_for_named_and_default_sessions() {
        let home = tempfile::tempdir().unwrap();
        let work = tempfile::tempdir().unwrap();
        let rows = [
            ("auto", "2026-11-24 06-37-20Z", "First request"),
            ("named", "Named thread", "Another request"),
        ];
        let db = Connection::open(home.path().join("state_5.sqlite")).unwrap();
        db.execute_batch("CREATE TABLE threads(id TEXT,rollout_path TEXT,cwd TEXT,name TEXT,title TEXT,created_at INTEGER,updated_at INTEGER,archived INTEGER);").unwrap();
        for (id, name, title) in rows {
            db.execute("INSERT INTO threads VALUES(?1,'sessions/test.jsonl',?2,?3,?4,1,2,0)",
                rusqlite::params![id, work.path().to_str().unwrap(), name, title]).unwrap();
        }
        drop(db);
        let config: ProfileConfig = toml::from_str(&format!(
            "id='codex'\nkind='codex'\ncommand='/bin/false'\nhome={:?}\n[[workspaces]]\nid='p'\npath={:?}",
            home.path(), work.path())).unwrap();
        let codex = Codex::new(config);
        let page = codex.local_sessions(&json!({})).await.unwrap().unwrap();
        let sessions = page["sessions"].as_array().unwrap();
        for (row, (id, name, preview)) in sessions.iter().zip(rows) {
            let details = codex.session(&json!({"id":id,"name":name,"preview":preview}));
            assert_eq!(row["id"], details["id"]);
            assert_eq!(row["title"], details["title"]);
        }
        assert_eq!(sessions[0]["title"], "First request");
        assert_eq!(sessions[1]["title"], "Named thread");
    }

    #[test]
    fn refuses_symlinked_native_indexes() {
        let home=tempfile::tempdir().unwrap(); let other=tempfile::NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(other.path(),home.path().join("state_5.sqlite")).unwrap();
        assert_eq!(database(home.path()).unwrap_err().code,"HARNESS_CATALOG_SYMLINK");
    }
}

#[cfg(test)]
mod smoke {
    use super::*;
    #[tokio::test]
    #[ignore = "Explicit read-only smoke against the Cells OS user's native home"]
    async fn runtime_user_codex_catalog_smoke() {
        let home=std::env::var("CELLS_CATALOG_HOME").unwrap();
        let work=std::env::var("CELLS_CATALOG_WORKSPACE").unwrap();
        let profile:ProfileConfig=toml::from_str(&format!("id='codex'\nkind='codex'\ncommand='/bin/false'\nhome={home:?}\n[[workspaces]]\nid='p'\npath={work:?}")).unwrap();
        let codex=Codex::new(profile); let started=std::time::Instant::now();
        let result=codex.local_sessions(&json!({"limit":100})).await.unwrap().unwrap();
        println!("{}",json!({"operation":"catalog.smoke","harness":"codex","count":result["sessions"].as_array().unwrap().len(),"durationMs":started.elapsed().as_millis(),"nativeStarted":codex.bridge.lock().await.is_some()}));
        assert!(codex.bridge.lock().await.is_none());
    }
}
