//! First input owns native creation, admission and empty-session compensation.
use super::*;
use crate::{app::State, harness::Harnesses};
use std::sync::Arc;

struct Context<'a> {
    state: &'a Arc<State>, identity: usize, project: &'a super::super::model::Project,
    channels: &'a Channels, harness: &'a Harnesses,
}
impl Context<'_> {
    fn store<T>(&self, work: impl FnOnce(&rusqlite::Connection) -> Result<T, Error>) -> Result<T, Error> {
        self.channels.2.with(&self.project.id, &self.project.owner, "project-drafts", |db| {
            schema(db)?; let tx = db.transaction().map_err(storage)?;
            let result = work(&tx)?; tx.commit().map_err(storage)?; Ok(result)
        })
    }
    fn save(&self, operation: &Value) -> Result<(), Error> {
        self.store(|db| write(db, &format!("first/{}", text(operation,"requestId")?), operation))
    }
    fn base(&self, op: &Value, stage: &str) -> Value {
        json!({"projectId":self.project.id,"revision":self.project.revision,"profileId":op["profileId"],
            "epoch":self.harness.profiles()["epoch"],"sessionId":op["sessionId"],
            "requestId":format!("{:x}",Sha256::digest(json!(["first-input",op["requestId"],stage]).to_string().as_bytes()))})
    }
    async fn native(&self, name: &str, args: Value) -> Result<Value, Error> {
        self.harness.project_call(self.state,self.identity,name,args).await
    }
    fn diagnostic(&self, op: &Value, result: &Value, started: std::time::Instant) {
        crate::core::events::emit(crate::core::events::Level::Info,"project.draft.submission",
            json!({"operation":"draft.first_input","operationId":op["requestId"],"projectId":self.project.id,
                "profileId":op["profileId"],"sessionId":op["sessionId"],"stage":op["stage"],
                "result":result["outcome"],"code":result["code"],"durationMs":started.elapsed().as_millis()}));
    }
}
pub(super) async fn call(state: &Arc<State>, identity: usize, project: &super::super::model::Project, args: &Value) -> Result<Value, Error> {
    let service=state.services.require::<super::super::ProjectService>()?;
    let ctx=Context {state,identity,project,channels:&service.channels,harness:state.services.require::<Harnesses>()?};
    let key=json!(["draft.first_input",project.id,project.owner]).to_string();
    let _lock=service.channels.3.acquire(&key,tokio::time::Instant::now()+std::time::Duration::from_secs(5),&state.event_stop).await?;
    let started=std::time::Instant::now();
    let (mut op,fresh)=reserve(&ctx,args)?;
    let result=if fresh {dispatch(&ctx,&mut op,args).await?} else {reconcile(&ctx,&mut op).await?};
    ctx.diagnostic(&op,&result,started);
    Ok(json!({"receipt":result}))
}
fn reserve(ctx: &Context<'_>, args: &Value) -> Result<(Value,bool),Error> {
    let request=text(args,"operationId")?;
    let key=format!("first/{request}");
    ctx.store(|db| {
        let fingerprint=format!("{:x}",Sha256::digest(json!([args["draftId"],args["profileId"],args["prompt"],args["attachments"],args["selection"],args["command"]]).to_string().as_bytes()));
        if let Some(op)=read(db,&key)? {
            if op["draftId"]!=args["draftId"] || op["profileId"]!=args["profileId"] || args["action"]=="submit" && !op["fingerprint"].is_null() && op["fingerprint"]!=fingerprint {return Err(conflict())}
            return Ok((op,false))
        }
        if args["action"]=="submission" {
            text(args,"draftId")?;text(args,"profileId")?;
            let op=json!({"requestId":request,"draftId":args["draftId"],"profileId":args["profileId"],"stage":"settled",
                "receipt":{"requestId":request,"outcome":"not-committed","code":"HARNESS_REQUEST_NOT_STARTED"}});
            write(db,&key,&op)?;return Ok((op,false))
        }
        let current=read(db,"current")?.ok_or_else(conflict)?;
        if current["id"]!=args["draftId"] || current["profileId"]!=args["profileId"] || current["state"]!="submitting" {return Err(conflict())}
        if let Some(previous)=current["firstOperationId"].as_str() {
            if read(db,&format!("first/{previous}"))?.is_none_or(|p|p["receipt"]["outcome"]!="not-committed") {return Err(Error::conflict("PROJECT_DRAFT_SUBMISSION_UNKNOWN"))}
        }
        let prompt=args["prompt"].as_str().filter(|s|!s.trim().is_empty() && s.len()<=1024*1024).ok_or_else(Error::invalid)?;
        crate::harness::validate_queue_attachments(prompt,&args.get("attachments").cloned().unwrap_or_else(||json!([])))?;
        let op=json!({"requestId":request,"draftId":current["id"],"profileId":current["profileId"],"fingerprint":fingerprint,"stage":"create"});
        let mut current=current;current["firstOperationId"]=json!(request);write(db,"current",&current)?;write(db,&key,&op)?;
        Ok((op,true))
    })
}
async fn dispatch(ctx: &Context<'_>, op: &mut Value, args: &Value) -> Result<Value,Error> {
    let mut create=ctx.base(op,"create");create["workspaceId"]=json!(ctx.project.id);
    let created=ctx.native("harness_create",create).await?;
    if created["outcome"]!="committed" {return settle(ctx,op,created).await}
    op["sessionId"]=created["result"]["session"]["id"].clone();op["session"]=created["result"]["session"].clone();
    if op["sessionId"].as_str().is_none() {return Err(Error::unavailable())}
    op["stage"]=json!("turn");ctx.save(op)?;
    let mut turn=ctx.base(op,"turn");turn["prompt"]=args["prompt"].clone();
    for field in ["attachments","selection"] {if let Some(v)=args.get(field) {turn[field]=v.clone();}}
    let name=if let Some(command)=args["command"].as_object() {
        turn["command"]=args["command"]["name"].clone();
        for field in ["arguments","expectedCommand"] {if let Some(v)=command.get(field) {turn[field]=v.clone();}}
        "harness_command"
    } else {"harness_turn"};
    let result=ctx.native(name,turn).await?;
    settle(ctx,op,result).await
}
async fn reconcile(ctx: &Context<'_>, op: &mut Value) -> Result<Value,Error> {
    if !op["receipt"].is_null() {return Ok(op["receipt"].clone())}
    if op["stage"]=="cleanup" {return cleanup(ctx,op).await}
    let stage=op["stage"].as_str().ok_or_else(Error::invalid)?;
    let receipt=ctx.native("harness_operation",ctx.base(op,stage)).await?;
    if stage=="create" && receipt["outcome"]=="committed" {
        op["sessionId"]=receipt["result"]["session"]["id"].clone();
        return settle(ctx,op,json!({"outcome":"not-committed","code":"HARNESS_FIRST_INPUT_NOT_SUBMITTED"})).await
    }
    settle(ctx,op,receipt).await
}
async fn settle(ctx: &Context<'_>, op: &mut Value, mut result: Value) -> Result<Value,Error> {
    result["requestId"]=op["requestId"].clone();
    if result["outcome"]=="unknown" {return Ok(result)}
    if result["outcome"]=="not-committed" && op["sessionId"].is_string() {
        op["stage"]=json!("cleanup");op["rejection"]=result;ctx.save(op)?;return cleanup(ctx,op).await
    }
    if result["outcome"]=="committed" {
        let session=op["session"].clone();result["result"]["session"]=session;
        if result["result"]["turnId"].as_str().is_none() {return Err(Error::unavailable())}
    }
    commit(ctx,op,&result)?;Ok(result)
}
fn commit(ctx: &Context<'_>, op: &mut Value, result: &Value) -> Result<(),Error> {
    let latest=ctx.state.services.require::<super::super::ProjectService>()?.get(ctx.identity,&ctx.project.id)?;
    super::super::service::authorize(ctx.state,ctx.identity,&latest)?;
    if latest.revision!=ctx.project.revision {return Err(Error::conflict("PROJECT_REVISION_CHANGED"))}
    ctx.store(|db| {
        if result["outcome"]=="committed" {
            let mut current=read(db,"current")?.ok_or_else(conflict)?;
            if current["id"]!=op["draftId"] {return Err(conflict())}
            finish(db,&mut current,Some((text(op,"sessionId")?,text(&result["result"],"turnId")?)))?;
            write(db,"current",&current)?;
        }
        op["receipt"]=result.clone();op["stage"]=json!("settled");
        write(db,&format!("first/{}",text(op,"requestId")?),op)
    })
}
async fn cleanup(ctx: &Context<'_>, op: &mut Value) -> Result<Value,Error> {
    let history=ctx.native("harness_session_read",ctx.base(op,"inspect")).await;
    let absent=match &history {Err(e)=>matches!(e.code,"HARNESS_SESSION_NOT_LOADED"|"HARNESS_SESSION_NOT_FOUND"),_=>false};
    if !absent {
        let history=history?;
        if history["turns"].as_array().is_none_or(|t|!t.is_empty()) || !history["nextCursor"].is_null() || !history["recovery"].is_null()
            || history["session"]["activeTurnId"].is_string() {return Err(Error::conflict("HARNESS_EMPTY_SESSION_UNCONFIRMED"))}
        let mut delete=ctx.base(op,"delete");delete["emptyOnly"]=json!(true);
        let removed=if op["deleteStarted"]==true {ctx.native("harness_operation",delete).await?}
            else {op["deleteStarted"]=json!(true);ctx.save(op)?;ctx.native("harness_delete",delete).await?};
        if removed["outcome"]!="committed" {return Ok(json!({"requestId":op["requestId"],"outcome":"unknown","code":"HARNESS_EMPTY_SESSION_CLEANUP_PENDING"}))}
    }
    let result=op["rejection"].clone();commit(ctx,op,&result)?;Ok(result)
}

/// Old hosts could expose an empty native binding before the first input was accepted.
pub(super) async fn retire_legacy(state: &Arc<State>, identity: usize, project: &super::super::model::Project) -> Result<(),Error> {
    let service=state.services.require::<super::super::ProjectService>()?;
    let current=service.channels.2.with(&project.id,&project.owner,"project-drafts",|db|{schema(db)?;read(db,"current")})?;
    let Some(current)=current.filter(|r|r["state"]=="submitting" && r["firstOperationId"].is_null() && r["namespace"].is_string()) else {return Ok(())};
    let ctx=Context {state,identity,project,channels:&service.channels,harness:state.services.require::<Harnesses>()?};
    let Some(evidence)=receipt(ctx.channels,&project.id,&project.owner,&current)? else {return Ok(())};
    if evidence["submission"]["status"]!="rejected" || !evidence["pending"].is_null() || !evidence["submittedTurnId"].is_null()
        || evidence["instructionQueue"]["items"].as_array().is_some_and(|r|!r.is_empty()) {return Ok(())}
    let mut op=json!({"requestId":format!("legacy-{}",text(&current,"id")?),"draftId":current["id"],"profileId":current["profileId"],"sessionId":evidence["sessionId"],"stage":"cleanup",
        "rejection":{"outcome":"not-committed","code":"HARNESS_EMPTY_SESSION_RETIRED"}});
    let mut check=ctx.base(&op,"turn");check["requestId"]=evidence["submission"]["id"].clone();
    if ctx.native("harness_operation",check).await?["outcome"]!="not-committed" {return Ok(())}
    let binding=Binding {project_id:project.id.clone(),owner:project.owner.clone(),namespace:text(&current,"namespace")?.into(),profile_id:text(&current,"profileId")?.into(),session_id:text(&evidence,"sessionId")?.into(),revision:project.revision};
    let _lock=harness_recovery::locks::session_lock(state,&binding,"draft.empty.retire").await?;
    if ctx.store(|db|read(db,"current"))?.is_none_or(|r|r["id"]!=current["id"]) {return Ok(())}
    let aliases=ctx.channels.2.bindings()?.into_iter().filter(|b|b.project_id==binding.project_id && b.owner==binding.owner && b.profile_id==binding.profile_id && b.session_id==binding.session_id).count();
    if aliases>1 {return Ok(())}
    if receipt(ctx.channels,&project.id,&project.owner,&current)?.as_ref()!=Some(&evidence) {return Err(conflict())}
    let started=std::time::Instant::now();
    match ctx.native("harness_session_read",ctx.base(&op,"inspect")).await {
        Ok(history) if history["turns"].as_array().is_some_and(|t|!t.is_empty()) || !history["nextCursor"].is_null() || !history["recovery"].is_null()=>return Ok(()),
        Err(e) if !matches!(e.code,"HARNESS_SESSION_NOT_LOADED"|"HARNESS_SESSION_NOT_FOUND")=>return Err(e),
        _=>{},
    }
    let result=cleanup(&ctx,&mut op).await?;
    if result["outcome"]!="not-committed" {return Ok(())}
    super::call(ctx.channels,&project.id,&project.owner,&json!({"action":"reset","draftId":current["id"]}),None)?;
    ctx.channels.2.retire_namespace(&project.id,&project.owner,&binding.namespace)?;
    ctx.diagnostic(&op,&result,started);Ok(())
}
