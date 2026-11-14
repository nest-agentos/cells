//! Local DSH projection metadata; discovery never acquires a native session writer.
use super::*;
use std::{io::Read,os::fd::AsRawFd,path::Path};
fn read(home:&crate::fs::Export,path:&str)->Result<Value,Error> {
    let directory=home.open_dir("")?;
    let fd=rustix::fs::openat2(&directory,path,rustix::fs::OFlags::RDONLY|rustix::fs::OFlags::CLOEXEC,rustix::fs::Mode::empty(),rustix::fs::ResolveFlags::BENEATH|rustix::fs::ResolveFlags::NO_SYMLINKS|rustix::fs::ResolveFlags::NO_MAGICLINKS)?;
    let file=std::fs::File::from(fd);
    if !file.metadata()?.is_file() || file.metadata()?.len()>8*1024*1024 {return Err(Error::too_large("DSH_LOCAL_CATALOG_LIMIT"))}
    let mut bytes=Vec::new(); file.take(8*1024*1024+1).read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|_|Error::conflict("DSH_LOCAL_CATALOG_INVALID"))
}
fn scan(home:&Path)->Result<Option<Vec<Value>>,Error> {
    let home=crate::fs::Export::open(home)?;
    let directory=match home.open_dir("storages/session_projcache/sessions") {
        Ok(directory)=>directory,Err(error) if error.code=="ENOENT"=>return Ok(None),Err(error)=>return Err(error),
    };
    let workspace=read(&home,"storages/workspace.json")?;
    let archived=workspace["global"]["archivedSessionIds"].as_array().ok_or_else(||Error::conflict("DSH_LOCAL_CATALOG_INVALID"))?;
    let mut sessions=Vec::new();
    for (index,entry) in std::fs::read_dir(format!("/proc/self/fd/{}",directory.as_raw_fd()))?.enumerate() {
        if index>=32768 {return Err(Error::too_large("DSH_LOCAL_CATALOG_LIMIT"))}
        let entry=entry?; let name=entry.file_name(); let Some(name)=name.to_str() else {continue};
        let Some(id)=name.strip_suffix(".json").filter(|id|crate::core::ids::is_identifier_within(id,128)) else {continue};
        let value=read(&home,&format!("storages/session_projcache/sessions/{name}"))?;
        let Some(row)=session(id,&value,archived.iter().any(|v|v==id)) else {continue};
        sessions.push(row);
    }
    Ok(Some(sessions))
}
fn session(id:&str,value:&Value,archived:bool)->Option<Value> {
    let record=&value["record"]; let cwd=record["identity"]["cwd"].as_str()?;
    let created=record["identity"]["createdAt"].as_i64();
    let updated=record["rows"]["sessionListMetadata"]["val"]["lastPromptAt"].as_i64().or(created);
    Some(json!({"id":id,"cwd":cwd,"title":record["rows"]["title"]["val"].as_str().unwrap_or(""),"createdAt":created,"updatedAt":updated,
        "status":"notLoaded","owned":false,"resumable":true,"archived":archived}))
}
impl Dsh {
    pub(super) async fn local_sessions(&self,args:&Value)->Result<Option<Value>,Error> {
        // External legacy endpoints have their own filesystem; only managed local profiles read this home.
        if self.config.dsh.as_ref().is_some_and(|c|c.url.is_some()) {return Ok(None)}
        let home=self.config.home.clone(); let started=std::time::Instant::now();
        let result=tokio::task::spawn_blocking(move||scan(&home)).await.map_err(|_|Error::internal())?;
        crate::core::events::emit(if result.is_err(){crate::core::events::Level::Warn}else{crate::core::events::Level::Debug},"harness.catalog.local",
            json!({"harness":"dsh","stage":"read-projections","result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}));
        let Some(mut sessions)=result? else {return Ok(None)};
        sessions.retain(|s|s["archived"]==args["archived"].as_bool().unwrap_or(false) && s["cwd"].as_str().is_some_and(|cwd|self.authorized(cwd)));
        let live=self.sessions.lock().unwrap();
        for session in &mut sessions {if let Some(process)=session["id"].as_str().and_then(|id|live.get(id)) {*session=process.session.lock().unwrap().clone();}}
        sessions.sort_by(|a,b|b["updatedAt"].as_i64().cmp(&a["updatedAt"].as_i64()).then(a["id"].as_str().cmp(&b["id"].as_str())));
        let offset=args["cursor"].as_str().unwrap_or("0").parse::<usize>().map_err(|_|Error::invalid())?;
        if offset>sessions.len() {return Err(Error::invalid())}
        let end=(offset+args["limit"].as_u64().unwrap_or(100).clamp(1,100) as usize).min(sessions.len());
        Ok(Some(json!({"sessions":sessions[offset..end],"nextCursor":if end<sessions.len(){json!(end.to_string())}else{Value::Null}})))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn local_projection_discovery_includes_descendants_without_starting_dsh() {
        let home=tempfile::tempdir().unwrap(); let work=tempfile::tempdir().unwrap();
        let nested=work.path().join("nested/deep"); std::fs::create_dir_all(&nested).unwrap();
        let directory=home.path().join("storages/session_projcache/sessions"); std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(home.path().join("storages/workspace.json"),json!({"global":{"archivedSessionIds":[]}}).to_string()).unwrap();
        for (id,cwd) in [("root",work.path()),("nested",nested.as_path()),("foreign",home.path())] {
            std::fs::write(directory.join(format!("{id}.json")),json!({"version":1,"record":{"identity":{"cwd":cwd,"createdAt":1},"rows":{"title":{"val":"Title"},"sessionListMetadata":{"val":{"lastPromptAt":2}}}}}).to_string()).unwrap();
        }
        let config:ProfileConfig=toml::from_str(&format!("id='dsh'\nkind='dsh'\ncommand='/bin/false'\nhome={:?}\n[[workspaces]]\nid='p'\npath={:?}",home.path(),work.path())).unwrap();
        let dsh=Dsh::new(config,None);
        let page=dsh.local_sessions(&json!({})).await.unwrap().unwrap();
        assert_eq!(page["sessions"].as_array().unwrap().len(),2);
        assert_eq!(page["sessions"][0]["cwd"],nested.to_str().unwrap());
        assert!(dsh.connection.lock().await.is_none());
    }
}

#[cfg(test)]
mod smoke {
    use super::*;
    #[tokio::test]
    #[ignore = "Explicit read-only smoke against the Cells OS user's native home"]
    async fn runtime_user_dsh_catalog_smoke() {
        let home=std::env::var("CELLS_CATALOG_HOME").unwrap();
        let work=std::env::var("CELLS_CATALOG_WORKSPACE").unwrap();
        let profile:ProfileConfig=toml::from_str(&format!("id='dsh'\nkind='dsh'\ncommand='/bin/false'\nhome={home:?}\n[[workspaces]]\nid='p'\npath={work:?}")).unwrap();
        let dsh=Dsh::new(profile,None); let started=std::time::Instant::now();
        let result=dsh.local_sessions(&json!({"limit":100})).await.unwrap().unwrap();
        println!("{}",json!({"operation":"catalog.smoke","harness":"dsh","count":result["sessions"].as_array().unwrap().len(),"durationMs":started.elapsed().as_millis(),"nativeStarted":dsh.connection.lock().await.is_some()}));
        assert!(dsh.connection.lock().await.is_none());
    }
}
