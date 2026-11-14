//! Authoritative project composer identities and receipt-gated retirement.
use super::{channel::Channels, harness_recovery::{self, Binding}};
use crate::core::error::Error;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
mod submission;

fn storage(_: impl std::fmt::Display) -> Error { Error::unavailable() }
fn conflict() -> Error { Error::conflict("PROJECT_DRAFT_CHANGED") }
fn uuid() -> Result<String, Error> {
    let mut b = [0u8; 16];
    getrandom::getrandom(&mut b).map_err(storage)?;
    b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    let h: String = b.iter().map(|v| format!("{v:02x}")).collect();
    Ok(format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..]))
}
fn empty() -> Result<Value, Error> { Ok(json!({"version":1,"id":uuid()?,"state":"draft","data":""})) }
fn text<'a>(v: &'a Value, field: &str) -> Result<&'a str, Error> {
    v[field].as_str().filter(|s| harness_recovery::valid_id(s)).ok_or_else(Error::invalid)
}
fn hash(value: &Value) -> String { format!("{:x}", Sha256::digest(value["data"].as_str().unwrap_or("").as_bytes())) }
fn schema(db: &rusqlite::Connection) -> Result<(), Error> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS project_draft(key TEXT PRIMARY KEY,body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS draft_assets(scope TEXT,id TEXT,part INTEGER,body TEXT NOT NULL,PRIMARY KEY(scope,id,part));").map_err(storage)
}
fn read(db: &rusqlite::Connection, key: &str) -> Result<Option<Value>, Error> {
    let raw: Option<String> = db.query_row("SELECT body FROM project_draft WHERE key=?1", [key], |r| r.get(0)).optional().map_err(storage)?;
    raw.map(|s| serde_json::from_str(&s).map_err(storage)).transpose()
}
fn write(db: &rusqlite::Connection, key: &str, value: &Value) -> Result<(), Error> {
    db.execute("INSERT INTO project_draft VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET body=excluded.body", params![key,value.to_string()]).map_err(storage)?; Ok(())
}
// Read the node-owned journal; caller supplied turn IDs never authorize promotion.
fn receipt(channels: &Channels, project: &str, owner: &str, record: &Value) -> Result<Option<Value>, Error> {
    let Some(namespace) = record["namespace"].as_str() else {return Ok(None)};
    let profile = text(record, "profileId")?;
    channels.2.with(project, owner, namespace, |db| {
        let key = format!("{namespace}/recovery");
        let head: Option<String> = db.query_row("SELECT body FROM client_state WHERE project=?1 AND owner=?2 AND key=?3",params![project,owner,key],|r|r.get(0)).optional().map_err(storage)?;
        let Some(head) = head else {return Ok(None)};
        let binding = Binding {project_id:project.into(),owner:owner.into(),namespace:namespace.into(),profile_id:profile.into(),session_id:String::new(),revision:0};
        let mut record=harness_recovery::encoding::decode(db, &binding, &serde_json::from_str(&head).map_err(storage)?)?;
        if let Some(session)=record["sessionId"].as_str() {
            let binding=Binding {session_id:session.into(),..binding};
            if let Some(queue)=super::harness_queue::store::load(db,&binding)? {
                if record["submittedTurnId"].is_null() {
                    if let Some(accepted)=queue["receipts"].as_array().and_then(|rows|rows.iter().find(|row|row["turnId"].is_string() && row["attemptId"].is_string())) {
                        record["submittedTurnId"]=accepted["turnId"].clone();
                        record["submission"]=json!({"id":accepted["attemptId"],"status":"accepted"});
                    }
                }
                record["instructionQueue"]=queue;
            }
        }
        Ok(Some(record))
    })
}
fn resettable(receipt: &Option<Value>) -> Result<(), Error> {
    if receipt.as_ref().is_some_and(|r| !r["pending"].is_null()) {return Err(Error::conflict("PROJECT_DRAFT_SUBMISSION_UNKNOWN"))}
    Ok(())
}
/// One private current draft per server project and authenticated owner.
fn call(channels: &Channels, project: &str, owner: &str, args: &Value, verified: Option<(String,String)>) -> Result<Value, Error> {
    // Read the journal before taking the draft transaction; native journals never acquire this lock.
    let needs_receipt=["promote","reset"].contains(&args["action"].as_str().unwrap_or(""));
    let before = if needs_receipt {channels.2.with(project, owner, "project-drafts", |db| {schema(db)?; read(db,"current")})?} else {None};
    let evidence = if let Some(record) = before.as_ref().filter(|r|r["id"]==args["draftId"]) {receipt(channels, project, owner, record)?} else {None};
    channels.2.with(project, owner, "project-drafts", |db| {
        schema(db)?;
        let tx = db.transaction().map_err(storage)?;
        if args["action"]=="resolve" {
            let key=format!("native/{}",json!([text(args,"profileId")?,text(args,"sessionId")?]));
            let mut identity=read(&tx,&key)?;
            if identity.is_none() {
                if let Some(current)=read(&tx,"current")?.filter(|r|r["profileId"]==args["profileId"]) {
                    if receipt(channels,project,owner,&current)?.is_some_and(|r|r["sessionId"]==args["sessionId"]) {identity=Some(current["id"].clone());}
                }
            }
            return Ok(json!({"conversationId":identity}));
        }
        if args["action"]=="promotion" {return Ok(json!({"promotion":read(&tx,&format!("promotion/{}",text(args,"draftId")?))?}));}
        let found = read(&tx,"current")?;
        let original=found.clone();
        if needs_receipt && found != before {return Err(conflict())}
        let mut record = match found {Some(v)=>v,None=>empty()?};
        let result = if args["action"]=="promote" && evidence.as_ref().is_some_and(|r|
            verified.as_ref().is_some_and(|(session,turn)|r["sessionId"]==*session && r["submittedTurnId"]==*turn)) {
            execute(&tx,&mut record,args,&evidence)?
        } else if args["action"]=="promote" {execute(&tx,&mut record,args,&None)?}
        else {execute(&tx,&mut record,args,&evidence)?};
        if original.as_ref()!=Some(&record) {write(&tx,"current",&record)?;}
        tx.commit().map_err(storage)?;
        Ok(result)
    })
}
fn execute(db: &rusqlite::Connection, record: &mut Value, args: &Value, evidence: &Option<Value>) -> Result<Value, Error> {
    let action = args["action"].as_str().ok_or_else(Error::invalid)?;
    if action == "resolve" {
        let key=format!("native/{}",json!([text(args,"profileId")?,text(args,"sessionId")?]));
        return Ok(json!({"conversationId":read(db,&key)?}));
    }
    if action == "open" { return Ok(json!({"record":record,"promotion":read(db,"latest-promotion")?})); }
    if action == "promotion" {return Ok(json!({"promotion":read(db,&format!("promotion/{}",text(args,"draftId")?))?}));}
    if args["draftId"] != record["id"] {
        if ["promote","commit_session"].contains(&action) {
            if let Some(promotion)=read(db,&format!("promotion/{}",text(args,"draftId")?))? {return Ok(json!({"record":record,"promotion":promotion}))}
        }
        return Err(conflict())
    }
    match action {
        "save" => {
            if args["expectedData"] != record["data"] {return Err(conflict())}
            let data = args["data"].as_str().filter(|s| s.len() <= 16*1024*1024).ok_or_else(Error::invalid)?;
            record["data"] = json!(data);
        }
        "begin" => begin(record,args)?,
        "reset" => {resettable(evidence)?;
            if let Some(id)=record["firstOperationId"].as_str() {
                if read(db,&format!("first/{id}"))?.is_none_or(|op|op["receipt"]["outcome"]!="not-committed") {return Err(Error::conflict("PROJECT_DRAFT_SUBMISSION_UNKNOWN"))}
            }
            if record["state"]=="submitting" && !evidence.as_ref().is_some_and(|r|r["submission"]["status"]=="rejected") {return Err(Error::conflict("PROJECT_DRAFT_SUBMISSION_LOCKED"))} retire_assets(db,record)?; *record=empty()?;}
        "promote" => return promote(db, record, evidence),
        "commit_session" => {
            if record["harnessConnectionId"].as_str().is_none() || args["submissionId"]!=record["submissionId"] {return Err(conflict())}
            return finish(db,record,None);
        },
        "asset_put" | "asset_read" => return asset(db,record,args),
        _ => return Err(Error::invalid()),
    }
    Ok(json!({"record":record}))
}
fn begin(record: &mut Value, args: &Value) -> Result<(), Error> {
    if let Some(connection)=args["harnessConnectionId"].as_str() {
        if !harness_recovery::valid_id(connection) {return Err(Error::invalid())}
        if record["state"]=="submitting" {
            if record["harnessConnectionId"]!=connection {return Err(conflict())} return Ok(())
        }
        if args["expectedData"] != record["data"] {return Err(conflict())}
        record["state"]=json!("submitting"); record["sessionId"]=record["id"].clone();
        record["submissionId"]=json!(uuid()?); record["submissionDraftHash"]=json!(hash(record));
        record["harnessConnectionId"]=json!(connection); record["title"]=args["title"].clone();
        return Ok(())
    }
    let namespace = text(args,"namespace")?;
    if namespace.len()!=64 || !namespace.bytes().all(|c| c.is_ascii_hexdigit()) {return Err(Error::invalid())}
    let profile = text(args,"profileId")?;
    if record["state"] == "submitting" {
        if record["namespace"] != namespace || record["profileId"] != profile || record["remoteTarget"] != args["target"] {return Err(conflict())}
        return Ok(())
    }
    if args["expectedData"] != record["data"] {return Err(conflict())}
    record["state"]=json!("submitting"); record["sessionId"]=record["id"].clone();
    record["submissionId"]=json!(uuid()?); record["submissionDraftHash"]=json!(hash(record));
    record["namespace"]=json!(namespace); record["profileId"]=json!(profile);
    record["remoteTarget"]=args["target"].clone(); record["title"]=args["title"].clone();
    Ok(())
}
fn promote(db: &rusqlite::Connection, record: &mut Value, evidence: &Option<Value>) -> Result<Value, Error> {
    if evidence.as_ref().is_some_and(|r| !r["pending"].is_null()) {return Ok(json!({"record":record}))}
    let Some(receipt)=evidence else {return Ok(json!({"record":record}))};
    if record["state"] != "submitting" {return Ok(json!({"record":record}))}
    let (Some(session),Some(turn))=(receipt["sessionId"].as_str(),receipt["submittedTurnId"].as_str()) else {return Ok(json!({"record":record}))};
    if session.is_empty() || turn.is_empty() {return Err(Error::invalid())}
    finish(db,record,Some((session,turn)))
}
fn finish(db: &rusqlite::Connection, record: &mut Value, native: Option<(&str,&str)>) -> Result<Value, Error> {
    let mut next=empty()?;
    if record["submissionDraftHash"] != hash(record) {next["data"]=record["data"].clone(); next["attachmentScopeId"]=record["attachmentScopeId"].as_str().map(json_string).unwrap_or_else(||record["id"].clone());}
    else {retire_assets(db,record)?;}
    let promotion=json!({"draftId":record["id"],"nextDraftId":next["id"],"submissionId":record["submissionId"],"remoteTarget":record["remoteTarget"],"nativeSessionId":native.map(|v|v.0),"turnId":native.map(|v|v.1)});
    write(db,&format!("promotion/{}",text(record,"id")?),&promotion)?;
    write(db,"latest-promotion",&promotion)?;
    if let Some((session,_))=native {
        let key=format!("native/{}",json!([record["profileId"],session]));
        if read(db,&key)?.is_some_and(|id|id!=record["id"]) {return Err(conflict())}
        write(db,&key,&record["id"])?;
    }
    *record=next;
    Ok(json!({"record":record,"promotion":promotion}))
}
fn json_string(s: &str) -> Value {json!(s)}
fn retire_assets(db: &rusqlite::Connection, record: &Value) -> Result<(), Error> {
    let scope=record["attachmentScopeId"].as_str().or(record["id"].as_str()).ok_or_else(Error::invalid)?;
    db.execute("DELETE FROM draft_assets WHERE scope=?1",[scope]).map_err(storage)?; Ok(())
}
fn asset(db: &rusqlite::Connection, record: &Value, args: &Value) -> Result<Value, Error> {
    let scope=record["attachmentScopeId"].as_str().unwrap_or(text(record,"id")?);
    let id=text(args,"assetId")?;
    let part=args["part"].as_u64().filter(|n| *n<=512).ok_or_else(Error::invalid)?;
    if args["action"]=="asset_read" {
        let body: Option<String>=db.query_row("SELECT body FROM draft_assets WHERE scope=?1 AND id=?2 AND part=?3",params![scope,id,part],|r|r.get(0)).optional().map_err(storage)?;
        return Ok(json!({"value":body}));
    }
    let value=args["value"].as_str().filter(|s| s.len()<=131072).ok_or_else(Error::invalid)?;
    let previous:Option<String>=db.query_row("SELECT body FROM draft_assets WHERE scope=?1 AND id=?2 AND part=?3",params![scope,id,part],|r|r.get(0)).optional().map_err(storage)?;
    if previous.as_deref().is_some_and(|s| s!=value) {return Err(conflict())}
    db.execute("INSERT OR IGNORE INTO draft_assets VALUES(?1,?2,?3,?4)",params![scope,id,part,value]).map_err(storage)?;
    Ok(json!({"stored":true}))
}


pub(super) async fn handle(state: &std::sync::Arc<crate::app::State>, identity: usize, args: &Value) -> Result<Value, Error> {
    let started=std::time::Instant::now();
    let result=authorized(state,identity,args).await;
    crate::core::events::emit(if result.is_err(){crate::core::events::Level::Warn}else{crate::core::events::Level::Debug},"project.draft",
        json!({"operationId":args["operationId"].as_str().or(args["requestId"].as_str()).filter(|v|harness_recovery::valid_id(v)),"stage":args["action"].as_str().filter(|v|["submit","submission","open","save","begin","reset","promote","promotion","commit_session","asset_put","asset_read","resolve"].contains(v)),"result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}));
    result
}
async fn authorized(state: &std::sync::Arc<crate::app::State>, identity: usize, args: &Value) -> Result<Value, Error> {
    let service=state.services.require::<super::ProjectService>()?;
    let project=service.get(identity,text(args,"projectId")?)?;
    super::service::authorize(state,identity,&project)?;
    if args["revision"].as_u64()!=Some(project.revision) {return Err(Error::conflict("PROJECT_REVISION_CHANGED"))}
    if !["resolve","promotion"].contains(&args["action"].as_str().unwrap_or(""))
        && (project.access!="rw" || super::capabilities(state,identity,&project)["sessionControl"]!=true) {return Err(Error::forbidden("PROJECT_DRAFT_READ_ONLY"))}
    if let Some(profile)=args["profileId"].as_str() {
        if !state.auth.client(identity).may_harness(profile) {return Err(Error::forbidden("EACCES"))}
    }
    if ["submit", "submission"].contains(&args["action"].as_str().unwrap_or("")) {
        return submission::call(state, identity, &project, args).await;
    }
    if args["action"]=="begin" && args["harnessConnectionId"].is_null()
        && (args["target"]["serverProjectId"]!=project.id || args["target"]["profileId"]!=args["profileId"]) {return Err(Error::invalid())}
    if args["action"]=="open" {submission::retire_legacy(state,identity,&project).await?;}
    let verified=if args["action"]=="promote" {verify(state,identity,&service.channels,&project,args).await?} else {None};
    let latest=service.get(identity,&project.id)?;
    super::service::authorize(state,identity,&latest)?;
    if latest.revision!=project.revision {return Err(Error::conflict("PROJECT_REVISION_CHANGED"))}
    call(&service.channels,&project.id,&project.owner,args,verified)
}
async fn verify(state: &std::sync::Arc<crate::app::State>, identity: usize, channels: &Channels, project: &super::model::Project, args: &Value) -> Result<Option<(String,String)>,Error> {
    let current=channels.2.with(&project.id,&project.owner,"project-drafts",|db|{schema(db)?;read(db,"current")})?;
    let Some(current)=current.filter(|r|r["id"]==args["draftId"] && r["state"]=="submitting") else {return Ok(None)};
    let Some(evidence)=receipt(channels,&project.id,&project.owner,&current)? else {return Ok(None)};
    if !evidence["pending"].is_null() {return Ok(None)}
    let (Some(request),Some(session),Some(turn))=(evidence["submission"]["id"].as_str(),evidence["sessionId"].as_str(),evidence["submittedTurnId"].as_str()) else {return Ok(None)};
    let admitted=state.services.require::<crate::harness::Harnesses>()?.admitted_project_turn(state,identity,
        json!({"projectId":project.id,"revision":project.revision,"profileId":current["profileId"],"requestId":request,"sessionId":session})).await?;
    if admitted.as_deref()!=Some(turn) {return Err(Error::conflict("PROJECT_DRAFT_RECEIPT_UNCONFIRMED"))}
    Ok(Some((session.into(),turn.into())))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn invoke(channels:&Channels, args:Value)->Value {call(channels,"p","alice",&args,None).unwrap()}
    fn open(channels:&Channels)->Value {invoke(channels,json!({"action":"open"}))["record"].clone()}
    fn begin_draft(channels:&Channels, id:&Value)->Value {
        invoke(channels,json!({"action":"begin","draftId":id,"expectedData":"","namespace":"a".repeat(64),"profileId":"codex","target":{"connectionId":"c","serverProjectId":"p","profileId":"codex"}}))["record"].clone()
    }
    fn journal(channels:&Channels, value:Value) {
        let key=format!("{}/recovery","a".repeat(64));
        let before=channels.client_state("p","alice",&json!({"key":key,"action":"read"})).unwrap()["value"].clone();
        channels.client_state("p","alice",&json!({"key":key,"action":"compare","expected":before,"value":json!({"schema":1,"text":value.to_string()}).to_string()})).unwrap();
    }
    #[test]
    fn cells_assigns_one_durable_private_identity_and_cas_fences_stale_edits() {
        let root=tempfile::tempdir().unwrap(); let channels=Channels::open(root.path()).unwrap();
        let first=open(&channels); let id=first["id"].as_str().unwrap();
        assert_eq!(id.len(),36); assert_eq!(&id[14..15],"4");
        invoke(&channels,json!({"action":"save","draftId":id,"expectedData":"","data":"private"}));
        assert!(call(&channels,"p","alice",&json!({"action":"save","draftId":id,"expectedData":"","data":"stale"}),None).is_err());
        assert_ne!(call(&channels,"p","bob",&json!({"action":"open"}),None).unwrap()["record"]["id"],first["id"]);
        drop(channels); let channels=Channels::open(root.path()).unwrap();
        assert_eq!(open(&channels)["id"],first["id"]); assert_eq!(open(&channels)["data"],"private");
    }
    #[test]
    fn promotion_requires_verified_native_admission_and_is_idempotent_after_restart() {
        let root=tempfile::tempdir().unwrap(); let channels=Channels::open(root.path()).unwrap();
        let id=open(&channels)["id"].clone(); let prepared=begin_draft(&channels,&id);
        assert_eq!(begin_draft(&channels,&id)["submissionId"],prepared["submissionId"]);
        journal(&channels,json!({"sessionId":"native","submittedTurnId":"turn","pending":{"requestId":"input"}}));
        assert_eq!(invoke(&channels,json!({"action":"resolve","profileId":"codex","sessionId":"native"}))["conversationId"],id);
        assert!(invoke(&channels,json!({"action":"promote","draftId":id}))["promotion"].is_null());
        assert!(call(&channels,"p","alice",&json!({"action":"reset","draftId":id}),None).is_err());
        journal(&channels,json!({"sessionId":"native","submittedTurnId":"turn"}));
        assert!(invoke(&channels,json!({"action":"promote","draftId":id}))["promotion"].is_null());
        let result=call(&channels,"p","alice",&json!({"action":"promote","draftId":id}),Some(("native".into(),"turn".into()))).unwrap();
        assert_eq!(result["promotion"]["draftId"],id); assert_ne!(result["record"]["id"],id);
        assert_eq!(result["record"]["data"],"");
        drop(channels); let channels=Channels::open(root.path()).unwrap();
        assert_eq!(invoke(&channels,json!({"action":"promote","draftId":id}))["promotion"],result["promotion"]);
        assert!(call(&channels,"p","alice",&json!({"action":"save","draftId":id,"data":"late"}),None).is_err());
    }
    #[test]
    fn retirement_preserves_later_edits_and_their_assets_without_rebinding_old_native_session() {
        let root=tempfile::tempdir().unwrap(); let channels=Channels::open(root.path()).unwrap();
        let id=open(&channels)["id"].clone(); begin_draft(&channels,&id);
        invoke(&channels,json!({"action":"asset_put","draftId":id,"assetId":"image","part":0,"value":"asset"}));
        invoke(&channels,json!({"action":"save","draftId":id,"expectedData":"","data":"later"}));
        journal(&channels,json!({"sessionId":"native","submittedTurnId":"turn"}));
        let result=call(&channels,"p","alice",&json!({"action":"promote","draftId":id}),Some(("native".into(),"turn".into()))).unwrap();
        let next=&result["record"];
        assert_eq!(next["data"],"later"); assert_eq!(next["attachmentScopeId"],id);
        assert!(next["sessionId"].is_null()); assert!(next["remoteTarget"].is_null());
        assert_eq!(invoke(&channels,json!({"action":"asset_read","draftId":next["id"],"assetId":"image","part":0}))["value"],"asset");
        invoke(&channels,json!({"action":"reset","draftId":next["id"]}));
        assert_eq!(open(&channels)["data"],"");
    }
    #[test]
    fn managed_session_commit_is_separate_from_native_admission() {
        let root=tempfile::tempdir().unwrap(); let channels=Channels::open(root.path()).unwrap();
        let id=open(&channels)["id"].clone();
        let intent=invoke(&channels,json!({"action":"begin","draftId":id,"expectedData":"","harnessConnectionId":"connection"}));
        assert!(call(&channels,"p","alice",&json!({"action":"commit_session","draftId":id,"submissionId":"wrong"}),None).is_err());
        let result=invoke(&channels,json!({"action":"commit_session","draftId":id,"submissionId":intent["record"]["submissionId"]}));
        assert_ne!(result["record"]["id"],id); assert!(result["promotion"]["nativeSessionId"].is_null());
    }
}
