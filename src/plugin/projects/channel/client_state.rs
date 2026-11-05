use super::{Channels,storage};
use crate::core::error::Error;
use rusqlite::{params,Connection,OptionalExtension};
use serde_json::{json,Value};
pub(super) fn open(root:&std::path::Path)->Result<Connection,Error>{
    use std::os::unix::fs::OpenOptionsExt;
    let path=root.join("harness-client-state.sqlite");
    std::fs::OpenOptions::new().create(true).append(true).mode(0o600).custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32).open(&path)?;
    let db=Connection::open(path).map_err(storage)?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS client_state(project TEXT,owner TEXT,key TEXT,body TEXT NOT NULL,PRIMARY KEY(project,owner,key));").map_err(storage)?;
    Ok(db)
}
impl Channels {
    /// Private client state is owned by the authenticated project principal, never exposed to agent MCP.
    pub fn client_state(&self,project:&str,owner:&str,args:&Value)->Result<Value,Error>{
        let key=args["key"].as_str().filter(|key|!key.is_empty()&&key.len()<=256&&key.bytes().all(|c|c.is_ascii_alphanumeric()||b"-_/".contains(&c))).ok_or_else(Error::invalid)?;
        let mut db=self.2.lock().map_err(storage)?;let tx=db.transaction().map_err(storage)?;
        let current:Option<String>=tx.query_row("SELECT body FROM client_state WHERE project=?1 AND owner=?2 AND key=?3",params![project,owner,key],|r|r.get(0)).optional().map_err(storage)?;
        if args["action"]=="read" {return Ok(json!({"value":current}));}
        if args["action"]!="compare" {return Err(Error::invalid());}
        let text=|field:&str|->Result<Option<String>,Error>{match &args[field]{Value::Null=>Ok(None),Value::String(value) if value.len()<=1024*1024=>Ok(Some(value.clone())),_=>Err(Error::too_large("CLIENT_STATE_LIMIT"))}};
        let expected=text("expected")?;let value=text("value")?;
        if current!=expected{return Ok(json!({"applied":false}));}
        if let Some(value)=value{tx.execute("INSERT INTO client_state(project,owner,key,body) VALUES(?1,?2,?3,?4) ON CONFLICT(project,owner,key) DO UPDATE SET body=excluded.body",params![project,owner,key,value]).map_err(storage)?;}
        else{tx.execute("DELETE FROM client_state WHERE project=?1 AND owner=?2 AND key=?3",params![project,owner,key]).map_err(storage)?;}
        tx.commit().map_err(storage)?;Ok(json!({"applied":true}))
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn private_state_is_cas_fenced_and_survives_restart(){
        let root=tempfile::tempdir().unwrap();let store=Channels::open(root.path()).unwrap();
        let write=json!({"action":"compare","key":"session/composer","expected":null,"value":"draft"});
        assert_eq!(store.client_state("p","alice",&write).unwrap()["applied"],true);
        assert_eq!(store.client_state("p","alice",&write).unwrap()["applied"],false);
        drop(store);let store=Channels::open(root.path()).unwrap();let read=json!({"action":"read","key":"session/composer"});
        assert_eq!(store.client_state("p","alice",&read).unwrap()["value"],"draft");
        assert!(store.client_state("p","bob",&read).unwrap()["value"].is_null());
        assert!(store.client_state("q","alice",&read).unwrap()["value"].is_null());
    }
}
