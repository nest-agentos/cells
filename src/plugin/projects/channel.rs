//! Durable, scope-bound agent communication, separate from workspace file gates.
use crate::core::error::Error;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{path::Path, sync::Mutex};

mod exchange;
mod servers;

pub struct Channels(Mutex<Connection>, tokio::sync::watch::Sender<u64>);
impl Channels {
    pub fn open(root: &Path) -> Result<Self, Error> {
        let path = root.join("session-channels.sqlite");
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new().create(true).append(true).mode(0o600).custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32).open(&path)?;
        let db = Connection::open(path).map_err(storage)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA max_page_count=65536;
            CREATE TABLE IF NOT EXISTS scopes(project TEXT, id TEXT, token TEXT UNIQUE, cwd TEXT, endpoint TEXT,
            snapshot TEXT, execution TEXT NOT NULL DEFAULT '{}', closed INTEGER NOT NULL DEFAULT 0, expires_at INTEGER NOT NULL, PRIMARY KEY(project,id));
            CREATE TABLE IF NOT EXISTS exchanges(seq INTEGER PRIMARY KEY AUTOINCREMENT,project TEXT,scope TEXT,id TEXT,request TEXT,response TEXT,UNIQUE(project,scope,id));
            CREATE TABLE IF NOT EXISTS messages(seq INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, scope TEXT,
            id TEXT, body TEXT, ack TEXT, UNIQUE(project,scope,id));
            CREATE TABLE IF NOT EXISTS agent_servers(owner TEXT,id TEXT,revision INTEGER,body TEXT,PRIMARY KEY(owner,id));
            CREATE UNIQUE INDEX IF NOT EXISTS active_scope ON scopes(project,cwd) WHERE closed=0;").map_err(storage)?;
        db.execute("UPDATE scopes SET execution=json_set(execution,'$.status','unknown','$.recoveryReason','CELLS_RESTARTED') WHERE json_extract(execution,'$.status') IN ('running','submitting')",[]).map_err(storage)?;
        Ok(Self(Mutex::new(db),tokio::sync::watch::channel(0).0))
    }
    fn touch(&self) {self.1.send_modify(|revision|*revision=revision.wrapping_add(1));}
    pub fn watch(&self) -> tokio::sync::watch::Receiver<u64> {self.1.subscribe()}
    pub fn revision(&self, project:&str,scope:&str)->Result<String,Error> {
        self.0.lock().map_err(storage)?.query_row("SELECT COALESCE((SELECT MAX(seq) FROM messages WHERE project=?1 AND scope=?2),0)+(SELECT COUNT(ack) FROM messages WHERE project=?1 AND scope=?2)+(SELECT COUNT(*)+COUNT(response) FROM exchanges WHERE project=?1 AND scope=?2),COALESCE(json_extract(execution,'$.revision'),0),closed FROM scopes WHERE project=?1 AND id=?2",params![project,scope],|r|Ok(format!("{}:{}:{}",r.get::<_,u64>(0)?,r.get::<_,u64>(1)?,r.get::<_,u64>(2)?))).optional().map_err(storage)?.ok_or_else(||Error::not_found("CHANNEL_SCOPE_UNKNOWN"))
    }
    pub fn create(&self, project: &str, args: &Value) -> Result<Value, Error> {
        let id = identifier(args, "scopeId")?;
        let ttl=args.get("ttlMs").map(|v|v.as_i64().filter(|n|(60_000..=2_592_000_000).contains(n)).ok_or_else(Error::invalid)).transpose()?.unwrap_or(604_800_000);
        let expires=chrono::Utc::now().timestamp_millis()+ttl;
        let snapshot = serde_json::to_string(&args["snapshot"]).map_err(storage)?;
        if snapshot.len() > 2 * 1024 * 1024 || !args["snapshot"].is_object() { return Err(Error::too_large("CHANNEL_CONTEXT_LIMIT")); }
        if args["snapshot"].as_object().unwrap().values().any(|v|v.to_string().len()>65536) {return Err(Error::too_large("CHANNEL_SECTION_LIMIT"));}
        let cwd = args["cwd"].as_str().ok_or_else(Error::invalid)?;
        let endpoint = args["endpoint"].as_str().ok_or_else(Error::invalid)?;
        if cwd.len() > 4096 || endpoint.len() > 2048 {return Err(Error::invalid());}
        let mut connection = self.0.lock().map_err(storage)?;
        let db = connection.transaction().map_err(storage)?;
        let existing: Option<(String, String, String, bool)> = db.query_row("SELECT snapshot,cwd,endpoint,closed FROM scopes WHERE project=?1 AND id=?2", params![project,id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(storage)?;
        if let Some(previous) = existing {
            if previous.3 {return Err(Error::conflict("CHANNEL_SCOPE_CLOSED"));}
            if (previous.0,previous.1,previous.2) != (snapshot, cwd.into(), endpoint.into()) { return Err(Error::conflict("CHANNEL_SCOPE_CHANGED")); }
        } else {
            let execution = replace_scope(&db,project,cwd,args)?;
            let mut bytes = [0u8; 32]; getrandom::getrandom(&mut bytes).map_err(storage)?;
            let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            db.execute("INSERT INTO scopes(project,id,token,cwd,endpoint,snapshot,execution,expires_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", params![project,id,token,cwd,endpoint,snapshot,execution.to_string(),expires]).map_err(storage)?;
        }
        db.commit().map_err(storage)?; self.touch();
        Ok(json!({"scopeId":id,"accepted":true}))
    }
    pub fn config(&self, project: &str, cwd: &str, owner:&str) -> Result<Option<Value>, Error> {
        let db = self.0.lock().map_err(storage)?;
        let rows: Vec<(String,String)> = db.prepare("SELECT token,endpoint FROM scopes WHERE project=?1 AND cwd=?2 AND closed=0 AND expires_at>unixepoch()*1000 ORDER BY rowid DESC LIMIT 2").map_err(storage)?
            .query_map(params![project,cwd], |r| Ok((r.get(0)?,r.get(1)?))).map_err(storage)?.collect::<Result<_,_>>().map_err(storage)?;
        if rows.len() > 1 {return Err(Error::conflict("CHANNEL_BINDING_AMBIGUOUS"));}
        let scoped=rows.first().map(|(token,endpoint)| json!({"features.mcp_2026_07_28":true,"mcp_servers.cells_session":{"url":endpoint,"required":true,"default_tools_approval_mode":"approve","enabled_tools":["session_context","session_report","session_receipt","session_request","session_response"],"http_headers":{"Authorization":format!("Bearer {token}")}}}));
        drop(db);self.agent_config(owner,scoped)
    }
    pub fn agent(&self, token: &str, method: &str, args: &Value) -> Result<Value, Error> {
        let mut db = self.0.lock().map_err(storage)?;
        let tx = db.transaction().map_err(storage)?;
        let (project, scope, snapshot): (String,String,String) = tx.query_row("SELECT project,id,snapshot FROM scopes WHERE token=?1 AND closed=0 AND expires_at>unixepoch()*1000", [token], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(storage)?.ok_or_else(Error::unauthenticated)?;
        let result = match method {
            "session_context" => {
                let section = args["section"].as_str().unwrap_or("index");
                let snapshot: Value = serde_json::from_str(&snapshot).map_err(storage)?;
                if section == "index" {json!({"scopeId":scope,"sections":snapshot.as_object().unwrap().keys().collect::<Vec<_>>()})}
                else {snapshot.get(section).cloned().ok_or_else(|| Error::not_found("CHANNEL_SECTION_UNKNOWN"))?}
            }
            "session_request" => exchange::request(&tx,&project,&scope,args)?,
            "session_response" => exchange::response(&tx,&project,&scope,args)?,
            "session_report" => publish(&tx,&project,&scope,args)?,
            "session_receipt" => {
                let id = identifier(args, "id")?;
                let found: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM messages WHERE project=?1 AND scope=?2 AND id=?3)",params![project,scope,id],|r|r.get(0)).map_err(storage)?;
                let ack:Option<String>=tx.query_row("SELECT ack FROM messages WHERE project=?1 AND scope=?2 AND id=?3",params![project,scope,id],|r|r.get(0)).optional().map_err(storage)?.flatten();
                json!({"id":id,"accepted":found,"scopeId":scope,"ack":ack.map(|s|serde_json::from_str::<Value>(&s)).transpose().map_err(storage)?})
            }
            _ => return Err(Error::unsupported()),
        };
        tx.commit().map_err(storage)?; if ["session_report","session_request"].contains(&method) {self.touch();} Ok(result)
    }
    pub fn requests(&self,project:&str,scope:&str)->Result<Value,Error> {
        exchange::list(&*self.0.lock().map_err(storage)?,project,scope)
    }
    pub fn respond(&self,project:&str,scope:&str,args:&Value)->Result<Value,Error> {
        let result=exchange::respond(&*self.0.lock().map_err(storage)?,project,scope,args)?;
        self.touch();Ok(result)
    }
    pub fn current(&self,project:&str,cwd:&str)->Result<Value,Error> {
        self.0.lock().map_err(storage)?.query_row("SELECT id,execution FROM scopes WHERE project=?1 AND cwd=?2 AND closed=0",params![project,cwd],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional().map_err(storage)?.map(|(id,execution)|Ok(json!({"scopeId":id,"execution":serde_json::from_str::<Value>(&execution).map_err(storage)?}))).unwrap_or_else(||Err(Error::not_found("CHANNEL_SCOPE_UNKNOWN")))
    }
    pub fn acknowledge(&self,project:&str,scope:&str,args:&Value)->Result<Value,Error> {
        let id=identifier(args,"id")?;let raw=args["ack"].to_string();
        if raw.len()>65536 || !args["ack"].is_object() {return Err(Error::invalid());}
        let mut connection=self.0.lock().map_err(storage)?;let db=connection.transaction().map_err(storage)?;
        let previous:Option<Option<String>>=db.query_row("SELECT ack FROM messages WHERE project=?1 AND scope=?2 AND id=?3",params![project,scope,id],|r|r.get(0)).optional().map_err(storage)?;
        match previous {None=>return Err(Error::not_found("CHANNEL_MESSAGE_UNKNOWN")),Some(Some(previous)) if previous!=raw=>return Err(Error::conflict("CHANNEL_ACK_CHANGED")),_=>()}
        db.execute("UPDATE messages SET ack=?4 WHERE project=?1 AND scope=?2 AND id=?3",params![project,scope,id,raw]).map_err(storage)?;
        db.commit().map_err(storage)?;self.touch();Ok(json!({"id":id,"acknowledged":true}))
    }
    pub fn token_project(&self, token: &str) -> Option<String> {
        self.0.lock().ok()?.query_row("SELECT project FROM scopes WHERE token=?1 AND closed=0 AND expires_at>unixepoch()*1000",[token],|r|r.get(0)).optional().ok()?
    }
    pub fn token_valid(&self, token: &str) -> bool {
        self.0.lock().ok().and_then(|db| db.query_row("SELECT EXISTS(SELECT 1 FROM scopes WHERE token=?1 AND closed=0 AND expires_at>unixepoch()*1000)",[token],|r|r.get::<_,bool>(0)).ok()).unwrap_or(false)
    }
    pub fn list(&self, project: &str, scope: &str) -> Result<Value, Error> {
        let db = self.0.lock().map_err(storage)?;
        let mut stmt = db.prepare("SELECT id,body FROM messages WHERE project=?1 AND scope=?2 ORDER BY seq LIMIT 200").map_err(storage)?;
        let rows = stmt.query_map(params![project,scope],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(storage)?;
        let mut messages = Vec::new();
        for row in rows {let (id,body) = row.map_err(storage)?; messages.push(json!({"id":id,"message":serde_json::from_str::<Value>(&body).map_err(storage)?}));}
        let execution:Option<String>=db.query_row("SELECT execution FROM scopes WHERE project=?1 AND id=?2",params![project,scope],|r|r.get(0)).optional().map_err(storage)?;
        Ok(json!({"messages":messages,"execution":execution.and_then(|s|serde_json::from_str::<Value>(&s).ok())}))
    }
    pub fn session_execution(&self,project:&str,session:&str)->Result<Option<Value>,Error> {
        let raw:Option<String>=self.0.lock().map_err(storage)?.query_row("SELECT execution FROM scopes WHERE project=?1 AND json_extract(execution,'$.sessionId')=?2 AND closed=0",params![project,session],|r|r.get(0)).optional().map_err(storage)?;
        raw.map(|s|serde_json::from_str(&s).map_err(storage)).transpose()
    }
    pub fn bind(&self, project: &str, cwd: &str, session: &str) -> Result<(),Error> {
        let value=json!({"sessionId":session,"status":"idle","revision":1});
        let db=self.0.lock().map_err(storage)?;
        let existing:Option<Option<String>>=db.query_row("SELECT json_extract(execution,'$.sessionId') FROM scopes WHERE project=?1 AND cwd=?2 AND closed=0",params![project,cwd],|r|r.get(0)).optional().map_err(storage)?;
        if existing.flatten().is_some_and(|id|id!=session) {return Err(Error::conflict("CHANNEL_SESSION_CHANGED"));}
        db.execute("UPDATE scopes SET execution=?3 WHERE project=?1 AND cwd=?2 AND closed=0 AND json_extract(execution,'$.sessionId') IS NULL",params![project,cwd,value.to_string()]).map_err(storage)?; self.touch(); Ok(())
    }
    pub fn prepare_turn(&self,project:&str,session:&str,operation:&str)->Result<(),Error> {
        let Some(mut value)=self.session_execution(project,session)? else {return Ok(());};
        if value["turnId"].is_string() {value["previousTurnId"]=value["turnId"].clone();} value["turnId"]=Value::Null;
        value["operationId"]=json!(operation);value["status"]=json!("submitting");
        value["revision"]=json!(value["revision"].as_u64().unwrap_or(0)+1);
        self.0.lock().map_err(storage)?.execute("UPDATE scopes SET execution=?3 WHERE project=?1 AND json_extract(execution,'$.sessionId')=?2 AND closed=0",params![project,session,value.to_string()]).map_err(storage)?;
        self.touch();Ok(())
    }
    pub fn submission_failed(&self,project:&str,session:&str,operation:&str,error:&Error)->Result<(),Error> {
        let status=if matches!(error.code,"EIO"|"ETIMEDOUT"){"unknown"}else{"rejected"};
        self.0.lock().map_err(storage)?.execute("UPDATE scopes SET execution=json_set(execution,'$.status',?4,'$.errorCode',?5,'$.revision',COALESCE(json_extract(execution,'$.revision'),0)+1) WHERE project=?1 AND json_extract(execution,'$.sessionId')=?2 AND json_extract(execution,'$.operationId')=?3 AND closed=0",params![project,session,operation,status,error.code]).map_err(storage)?;
        self.touch();Ok(())
    }
    pub fn observe(&self, project: &str, message: &Value) -> Result<(),Error> {
        let session=message["params"]["threadId"].as_str().unwrap_or("");
        let method=message["method"].as_str().unwrap_or("");
        if !["turn/started","turn/completed","harness/disconnected"].contains(&method) {return Ok(());}
        let mut db=self.0.lock().map_err(storage)?; let tx=db.transaction().map_err(storage)?;
        let old:Option<(String,String)>=tx.query_row("SELECT id,execution FROM scopes WHERE project=?1 AND json_extract(execution,'$.sessionId')=?2 AND closed=0 ORDER BY rowid DESC LIMIT 1",params![project,session],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(storage)?;
        if let Some((scope,raw))=old {
            let mut value:Value=serde_json::from_str(&raw).map_err(storage)?;
            let turn=&message["params"]["turn"];
            if value["previousTurnId"].is_string() && value["previousTurnId"]==turn["id"] {return Ok(());}
            if method=="turn/completed" && value["turnId"].is_string() && value["turnId"]!=turn["id"] {return Ok(());}
            value["status"]=json!(if method=="turn/started" {"running"} else if method=="harness/disconnected" {"unknown"} else {turn["status"].as_str().unwrap_or("unknown")});
            if turn["id"].is_string() {value["turnId"]=turn["id"].clone();}
            value["revision"]=json!(value["revision"].as_u64().unwrap_or(0)+1);
            value["observedAt"]=json!(chrono::Utc::now().timestamp_millis());
            tx.execute("UPDATE scopes SET execution=?3 WHERE project=?1 AND id=?2",params![project,scope,value.to_string()]).map_err(storage)?;
        }
        tx.commit().map_err(storage)?; self.touch(); Ok(())
    }
    pub fn close(&self, project: &str, scope: &str) -> Result<Value, Error> {
        self.0.lock().map_err(storage)?.execute("UPDATE scopes SET closed=1 WHERE project=?1 AND id=?2",params![project,scope]).map_err(storage)?;
        self.touch(); Ok(json!({"closed":true}))
    }
}
fn replace_scope(db:&rusqlite::Transaction,project:&str,cwd:&str,args:&Value)->Result<Value,Error> {
    let active:Option<(String,String)>=db.query_row("SELECT id,execution FROM scopes WHERE project=?1 AND cwd=?2 AND closed=0",params![project,cwd],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(storage)?;
    let Some((id,raw))=active else {
        if args.get("previousScopeId").is_some() {return Err(Error::conflict("CHANNEL_PREDECESSOR_MISSING"));}
        return Ok(json!({}));
    };
    if args["previousScopeId"].as_str()!=Some(&id) {return Err(Error::conflict("CHANNEL_PREDECESSOR_CHANGED"));}
    let execution:Value=serde_json::from_str(&raw).map_err(storage)?;
    let unsubmitted=execution["status"]=="idle" && execution["operationId"].is_null() && execution["turnId"].is_null();
    if !unsubmitted && !["completed","failed","interrupted","cancelled","rejected"].contains(&execution["status"].as_str().unwrap_or("")) {return Err(Error::busy());}
    if !args["previousSessionId"].is_string() || args["previousSessionId"]!=execution["sessionId"] {return Err(Error::conflict("CHANNEL_SESSION_CHANGED"));}
    db.execute("UPDATE scopes SET closed=1 WHERE project=?1 AND id=?2",params![project,id]).map_err(storage)?;
    Ok(json!({"sessionId":args["sessionId"],"previousTurnId":if args["sessionId"]==execution["sessionId"] {execution["turnId"].clone()}else{Value::Null},"status":"idle","revision":1}))
}

fn publish(tx:&rusqlite::Transaction,project:&str,scope:&str,args:&Value)->Result<Value,Error> {
                let id = identifier(args, "id")?; let message = &args["message"];
                if !message.is_object() || message.as_object().unwrap().keys().any(|key|!["type","content","payload","replyTo"].contains(&key.as_str()))
                    || !["progress","clarification","error","result","proposal","resolved"].contains(&message["type"].as_str().unwrap_or(""))
                    || message["content"].as_str().is_none_or(|s|s.trim().is_empty()||s.chars().count()>8000)
                    || message.get("replyTo").is_some_and(|v|v.as_str().is_none_or(|s|!crate::core::ids::is_identifier_within(s,128))) {return Err(Error::new(axum::http::StatusCode::BAD_REQUEST,"CHANNEL_MESSAGE_INVALID"));}
                let body = serde_json::to_string(message).map_err(storage)?;
                if !message.is_object() || body.len() > 64 * 1024 {return Err(Error::too_large("CHANNEL_MESSAGE_LIMIT"));}
                let old: Option<String> = tx.query_row("SELECT body FROM messages WHERE project=?1 AND scope=?2 AND id=?3", params![project,scope,id], |r|r.get(0)).optional().map_err(storage)?;
                if let Some(old) = old {if old != body {return Err(Error::conflict("CHANNEL_REQUEST_CHANGED"));}}
                else {
                    let count: u64 = tx.query_row("SELECT COUNT(*) FROM messages WHERE project=?1 AND scope=?2",params![project,scope],|r|r.get(0)).map_err(storage)?;
                    let bytes:u64=tx.query_row("SELECT COALESCE(SUM(length(body)),0) FROM messages WHERE project=?1 AND scope=?2",params![project,scope],|r|r.get(0)).map_err(storage)?;
                    if bytes+body.len() as u64>4*1024*1024 {return Err(Error::too_large("CHANNEL_MESSAGE_LIMIT"));}
                    if count >= 200 {return Err(Error::too_many("CHANNEL_MESSAGE_LIMIT"));}
                    tx.execute("INSERT INTO messages(project,scope,id,body) VALUES(?1,?2,?3,?4)",params![project,scope,id,body]).map_err(storage)?;
                }
                Ok(json!({"id":id,"accepted":true,"scopeId":scope}))
}

fn identifier<'a>(args: &'a Value, key: &str) -> Result<&'a str,Error> {
    args[key].as_str().filter(|s|crate::core::ids::is_identifier_within(s,128)).ok_or_else(Error::invalid)
}
#[track_caller]
fn storage<T>(_: T) -> Error {
    let caller = std::panic::Location::caller();
    crate::core::events::emit(crate::core::events::Level::Warn, "channel.storage.failed",
        json!({"operation":"channel.storage","stage":"persist","result":"failure","code":"CHANNEL_STORAGE",
        "errorType":std::any::type_name::<T>(),"source":caller.file(),"line":caller.line()}));
    Error::new(axum::http::StatusCode::SERVICE_UNAVAILABLE,"CHANNEL_STORAGE")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args() -> Value {json!({"scopeId":"attempt-1","cwd":"/projects/p/work","endpoint":"http://localhost/v1/channel/mcp","snapshot":{"task":{"title":"test"}}})}
    fn token(store:&Channels)->String {store.0.lock().unwrap().query_row("SELECT token FROM scopes",[],|r|r.get(0)).unwrap()}
    #[test]
    fn scoped_native_config_authorizes_only_the_bound_communication_tools() {
        let root = tempfile::tempdir().unwrap();
        let store = Channels::open(root.path()).unwrap();
        store.create("p", &args()).unwrap();
        let config = store.config("p", "/projects/p/work", "owner").unwrap().unwrap();
        let server = &config["mcp_servers.cells_session"];
        assert_eq!(server["default_tools_approval_mode"], "approve");
        assert_eq!(server["enabled_tools"], json!(["session_context","session_report","session_receipt","session_request","session_response"]));
        assert_eq!(server["required"], true);
        assert!(config.get("approval_policy").is_none());
    }
    #[test]
    fn immutable_scope_and_message_survive_restart() {
        let root=tempfile::tempdir().unwrap();
        let store=Channels::open(root.path()).unwrap();store.create("p",&args()).unwrap();
        let token=token(&store); let message=json!({"id":"report-1","message":{"type":"result","content":"done"}});
        store.agent(&token,"session_report",&message).unwrap();store.agent(&token,"session_report",&message).unwrap();
        let changed=json!({"id":"report-1","message":{"type":"result","content":"changed"}});
        assert_eq!(store.agent(&token,"session_report",&changed).unwrap_err().code,"CHANNEL_REQUEST_CHANGED");
        drop(store);let store=Channels::open(root.path()).unwrap();
        assert_eq!(store.agent(&token,"session_receipt",&json!({"id":"report-1"})).unwrap()["accepted"],true);
        assert_eq!(store.list("p","attempt-1").unwrap()["messages"].as_array().unwrap().len(),1);
        let mut changed=args();changed["snapshot"]["task"]=json!("other");
        assert_eq!(store.create("p",&changed).unwrap_err().code,"CHANNEL_SCOPE_CHANGED");
    }
    #[test]
    fn scope_tokens_cannot_read_other_sections_and_close_revokes_access() {
        let root=tempfile::tempdir().unwrap();let store=Channels::open(root.path()).unwrap();store.create("p",&args()).unwrap();
        let token=token(&store);assert!(store.agent("foreign","session_context",&json!({})).is_err());
        assert_eq!(store.agent(&token,"session_context",&json!({"section":"task"})).unwrap()["title"],"test");
        assert!(store.agent(&token,"project_exec",&json!({})).is_err());
        store.close("p","attempt-1").unwrap();assert!(!store.token_valid(&token));
    }
    #[test]
    fn terminal_is_bound_to_the_exact_turn_and_running_restarts_unknown() {
        let root=tempfile::tempdir().unwrap();let store=Channels::open(root.path()).unwrap();store.create("p",&args()).unwrap();
        store.bind("p","/projects/p/work","s").unwrap();
        store.observe("p",&json!({"method":"turn/started","params":{"threadId":"s","turn":{"id":"t"}}})).unwrap();
        store.observe("p",&json!({"method":"turn/completed","params":{"threadId":"s","turn":{"id":"other","status":"completed"}}})).unwrap();
        assert_eq!(store.list("p","attempt-1").unwrap()["execution"]["status"],"running");
        drop(store);let store=Channels::open(root.path()).unwrap();
        assert_eq!(store.list("p","attempt-1").unwrap()["execution"]["status"],"unknown");
    }
    #[test]
    fn scope_replacement_fences_the_session_and_revokes_the_previous_token() {
        let root=tempfile::tempdir().unwrap();let store=Channels::open(root.path()).unwrap();store.create("p",&args()).unwrap();
        let old_token=token(&store);store.bind("p","/projects/p/work","s").unwrap();
        assert_eq!(store.bind("p","/projects/p/work","foreign").unwrap_err().code,"CHANNEL_SESSION_CHANGED");
        store.prepare_turn("p","s","op").unwrap();
        store.observe("p",&json!({"method":"turn/started","params":{"threadId":"s","turn":{"id":"t"}}})).unwrap();
        let mut next=args();next["scopeId"]=json!("attempt-2");next["previousScopeId"]=json!("attempt-1");next["sessionId"]=json!("s");next["previousSessionId"]=json!("s");
        assert_eq!(store.create("p",&next).unwrap_err().code,"EBUSY");
        store.observe("p",&json!({"method":"turn/completed","params":{"threadId":"s","turn":{"id":"t","status":"failed"}}})).unwrap();
        next["previousSessionId"]=json!("foreign");assert_eq!(store.create("p",&next).unwrap_err().code,"CHANNEL_SESSION_CHANGED");
        next["previousSessionId"]=json!("s");store.create("p",&next).unwrap();assert!(!store.token_valid(&old_token));
        store.observe("p",&json!({"method":"turn/completed","params":{"threadId":"s","turn":{"id":"t","status":"completed"}}})).unwrap();
        assert_eq!(store.list("p","attempt-2").unwrap()["execution"]["status"],"idle");
    }
    #[test]
    fn unsubmitted_idle_scope_can_be_replaced_but_a_pending_submission_cannot() {
        let root=tempfile::tempdir().unwrap();let store=Channels::open(root.path()).unwrap();
        store.create("p",&args()).unwrap();store.bind("p","/projects/p/work","s").unwrap();
        let old=token(&store);
        let mut next=args();next["scopeId"]=json!("next");next["previousScopeId"]=json!("attempt-1");next["previousSessionId"]=json!("s");next["sessionId"]=json!("s");
        store.create("p",&next).unwrap();assert!(!store.token_valid(&old));
        store.prepare_turn("p","s","pending").unwrap();
        next["scopeId"]=json!("later");next["previousScopeId"]=json!("next");
        assert_eq!(store.create("p",&next).unwrap_err().code,"EBUSY");
    }
    #[test]
    fn terminal_scope_can_handoff_to_a_separate_hook_then_resume_the_original_session() {
        let root=tempfile::tempdir().unwrap();let store=Channels::open(root.path()).unwrap();store.create("p",&args()).unwrap();
        store.bind("p","/projects/p/work","original").unwrap();
        store.observe("p",&json!({"method":"turn/completed","params":{"threadId":"original","turn":{"id":"t1","status":"completed"}}})).unwrap();
        let mut hook=args();hook["scopeId"]=json!("hook");hook["previousScopeId"]=json!("attempt-1");hook["previousSessionId"]=json!("original");
        store.create("p",&hook).unwrap();store.bind("p","/projects/p/work","hook-native").unwrap();
        store.observe("p",&json!({"method":"turn/completed","params":{"threadId":"hook-native","turn":{"id":"h1","status":"completed"}}})).unwrap();
        let mut resumed=args();resumed["scopeId"]=json!("review");resumed["previousScopeId"]=json!("hook");resumed["previousSessionId"]=json!("hook-native");resumed["sessionId"]=json!("original");
        store.create("p",&resumed).unwrap();assert_eq!(store.list("p","review").unwrap()["execution"]["sessionId"],"original");
        assert_eq!(store.create("p",&args()).unwrap_err().code,"CHANNEL_SCOPE_CLOSED");
    }
    #[test]
    fn context_requests_and_host_acknowledgments_are_durable_and_immutable() {
        let root=tempfile::tempdir().unwrap();let store=Channels::open(root.path()).unwrap();store.create("p",&args()).unwrap();let secret=token(&store);
        let query=json!({"id":"q","method":"context.read","params":{}});
        assert_eq!(store.agent(&secret,"session_request",&query).unwrap()["status"],"pending");
        let requests=store.requests("p","attempt-1").unwrap();let digest=&requests["requests"][0]["requestDigest"];
        let response=json!({"id":"q","requestDigest":digest,"status":"ok","payload":{"revision":1},"error":null});
        store.respond("p","attempt-1",&response).unwrap();
        let mut changed=response.clone();changed["payload"]=json!({"revision":2});assert_eq!(store.respond("p","attempt-1",&changed).unwrap_err().code,"CHANNEL_RESPONSE_CHANGED");
        store.agent(&secret,"session_report",&json!({"id":"m","message":{"type":"proposal","content":"Proposal"}})).unwrap();
        let ack=json!({"id":"m","ack":{"status":"accepted"}});store.acknowledge("p","attempt-1",&ack).unwrap();
        drop(store);let store=Channels::open(root.path()).unwrap();
        assert_eq!(store.agent(&secret,"session_response",&json!({"id":"q"})).unwrap()["payload"]["revision"],1);
        assert_eq!(store.agent(&secret,"session_receipt",&json!({"id":"m"})).unwrap()["ack"]["status"],"accepted");
    }

}
