use super::service::{authorize, request, ProjectService};
use crate::{app::State, core::error::Error, process};
use serde_json::{json, Value};
use std::sync::Arc;
pub async fn call(
    state: &Arc<State>,
    identity: usize,
    name: &str,
    mut args: Value,
) -> Result<Value, Error> {
    if name == "project_harness_queue" {return super::harness_queue::call(state,identity,args).await;}
    if name == "project_harness_recovery" {return super::harness_recovery::call(state,identity,args).await;}
    if name.starts_with("project_sync_") {
        return super::directory_sync::call(state, identity, name, args).await;
    }
    let service = state
        .services
        .get::<crate::projects::ProjectService>()
        .ok_or_else(Error::unsupported)?;
    match name {
        "project_client_state"=>{
            let project=service.get(identity,text(&args,"projectId")?)?;authorize(state,identity,&project)?;
            if project.revision!=args["revision"].as_u64().ok_or_else(Error::invalid)? {return Err(Error::conflict("CLIENT_STATE_BINDING_CHANGED"));}
            if args["action"]!="read"&&project.access!="rw" {return Err(Error::forbidden("CLIENT_STATE_READ_ONLY"));}
            service.channels.client_state(&project.id,&project.owner,&args)
        },
        "project_channel_open" | "project_channel_messages" | "project_channel_close" | "project_channel_requests" | "project_channel_respond" | "project_channel_current" | "project_channel_acknowledge" => {
            let project = service.get(identity, text(&args, "projectId")?)?;
            authorize(state, identity, &project)?;
            if project.revision != args["revision"].as_u64().ok_or_else(Error::invalid)? || project.access != "rw" {return Err(Error::forbidden("CHANNEL_BINDING_CHANGED"));}
            if matches!(name, "project_channel_open" | "project_channel_current") {
                let started = std::time::Instant::now();
                let result = super::runtime::ProjectRuntime::new(state, identity, project.clone())
                    .and_then(|runtime| runtime.channel_directory(text(&args,"cwd")?));
                crate::core::events::emit(crate::core::events::Level::Info, "project.channel.workspace",
                    json!({"operation":name,"operationId":args["scopeId"],"projectId":project.id,
                        "stage":"resolve","result":if result.is_ok(){"success"}else{"failure"},
                        "code":result.as_ref().err().map(|error|error.code),
                        "isolation":if super::native::enabled(state){"none"}else{"bubblewrap"},
                        "elapsedMs":started.elapsed().as_millis()}));
                args["cwd"] = json!(result?);
            }
            match name {
                "project_channel_open" => {
                    if !service.channels_enabled || !state.plugins.enabled("mcp") {return Err(Error::unsupported());}
                    if let Some(endpoint) = &service.channel_endpoint {args["endpoint"] = json!(endpoint);}
                    if !valid_channel_endpoint(text(&args,"endpoint")?) {return Err(Error::invalid());}
                    let started=std::time::Instant::now();let resolved=super::context::prepare(state,identity,&project,&mut args);
                    if args.get("contextProjects").is_some() {
                        crate::core::events::emit(crate::core::events::Level::Info,"project.context.resolve",json!({"projectId":project.id,"operationId":args["scopeId"],"stage":"directories","result":if resolved.is_ok(){"success"}else{"failure"},"projectCount":args["contextProjects"].as_array().map(Vec::len),"code":resolved.as_ref().err().map(|error|error.code),"durationMs":started.elapsed().as_millis()}));
                    }
                    resolved?;
                    let mut receipt=service.channels.create(&project.id, &args)?;
                    if let Some(context)=args["snapshot"].get("project-context") {receipt["projectContext"]=context.clone();}
                    Ok(receipt)
                },
                "project_channel_current" => service.channels.current(&project.id,text(&args,"cwd")?),
                "project_channel_acknowledge" => service.channels.acknowledge(&project.id,text(&args,"scopeId")?,&args),
                "project_channel_requests" => service.channels.requests(&project.id,text(&args,"scopeId")?),
                "project_channel_respond" => service.channels.respond(&project.id,text(&args,"scopeId")?,&args),
                "project_channel_messages" => service.channels.list(&project.id, text(&args,"scopeId")?),
                _ if args["requireIdle"]==true => service.channels.close_idle(&project.id,text(&args,"scopeId")?,&args),
                _ => service.channels.close(&project.id,text(&args,"scopeId")?),
            }
        }
        "project_roots" => Ok(
            json!({"roots":state.auth.client(identity).exports().iter().map(|alias|json!({"alias":alias,"access":if state.auth.client(identity).may_write(alias){"rw"}else{"ro"},"capabilities":{"fileRead":true,"fileWrite":state.auth.client(identity).may_write(alias),"run":state.auth.client(identity).may_execute(alias)}})).collect::<Vec<_>>(),"execution":state.services.get::<crate::process::Execution>().is_some_and(|service| service.ready()) && state.auth.client(identity).has_execution(),"authorization":state.auth.client(identity).authorization(),"backend":if service.isolation==super::model::Isolation::None{"native"}else{"bubblewrap"}}),
        ),
        "project_execution" => {
            let project = service.get(identity, text(&args, "projectId")?)?;
            authorize(state, identity, &project)?;
            let profiles: Vec<_> = service.execution_profiles.iter()
                .filter(|p| p.principals.contains(&project.owner))
                .map(|p| json!({"id":p.id,"name":p.name,"user":p.user})).collect();
            Ok(json!({"execution":project.execution,"profiles":profiles}))
        }
        "project_set_execution" => {
            let started = std::time::Instant::now();
            let _gate = state.files.exclusive()?;
            let id = text(&args, "projectId")?;
            let environment = serde_json::from_value(args["execution"].clone()).map_err(|_| Error::invalid())?;
            let result = service.set_environment(state, identity, id,
                args["revision"].as_u64().ok_or_else(Error::invalid)?, environment);
            crate::core::events::emit(crate::core::events::Level::Info, "project.execution.configure",
                json!({"projectId":id,"operationId":format!("{}:{}",id,args["revision"]),"stage":"persist","result":if result.is_ok(){"success"}else{"failure"},
                    "code":result.as_ref().err().map(|e|e.code),"elapsedMs":started.elapsed().as_millis()}));
            let project = result?;
            if let Some(harness) = state.services.get::<crate::harness::Harnesses>() { harness.close_project(id).await; }
            Ok(json!({"project":super::descriptor::project(state,identity,&project)?}))
        }
        "project_list" => Ok(
            json!({"projects":service.list(identity)?.iter().filter_map(|p| super::descriptor::project(state, identity, p).ok()).collect::<Vec<_>>()}),
        ),
        "project_read" => Ok(
            json!({"project":super::descriptor::project(state,identity,&service.get(identity,text(&args,"projectId")?)?)?}),
        ),
        "project_register" => {
            let _gate = state.files.exclusive()?;
            let input = serde_json::from_value(args).map_err(|_| Error::invalid())?;
            Ok(
                json!({"project":super::descriptor::project(state,identity,&service.register(state,identity,input)?)?}),
            )
        }
        "project_configure" => configure(state, identity, service, args).await,
        "project_remount" => remount(state, identity, service, args).await,
        "project_forget" => {
            let _gate = state.files.exclusive()?;
            let id = text(&args, "projectId")?;
            service.forget(
                identity,
                id,
                args["revision"].as_u64().ok_or_else(Error::invalid)?,
            )?;
            if let Some(harness) = state.services.get::<crate::harness::Harnesses>() {
                harness.close_project(id).await;
            }
            Ok(json!({"forgotten":id}))
        }
        "project_exec" => execute(state, identity, service, args).await,
        "project_search" => super::search::search(state, identity, args).await,
        _ => Err(Error::unsupported()),
    }
}
pub(super) fn valid_channel_endpoint(value: &str) -> bool {
    value.len() <= 2048 && reqwest::Url::parse(value).is_ok_and(|endpoint|
        ["http","https"].contains(&endpoint.scheme()) && endpoint.host_str().is_some()
        && endpoint.username().is_empty() && endpoint.password().is_none()
        && endpoint.query().is_none() && endpoint.fragment().is_none())
}
fn text<'a>(args: &'a Value, key: &str) -> Result<&'a str, Error> {
    args[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(Error::invalid)
}

async fn remount(
    state: &Arc<State>,
    identity: usize,
    service: &ProjectService,
    args: Value,
) -> Result<Value, Error> {
    let _gate = state.files.exclusive()?;
    let id = text(&args, "projectId")?;
    let project = service.remount(
        state,
        identity,
        id,
        args["revision"].as_u64().ok_or_else(Error::invalid)?,
        text(&args, "access")?,
    )?;
    if let Some(harness) = state.services.get::<crate::harness::Harnesses>() {
        harness.close_project(id).await;
    }
    Ok(json!({"project":super::descriptor::project(state,identity,&project)?}))
}

async fn configure(
    state: &Arc<State>,
    identity: usize,
    service: &ProjectService,
    args: Value,
) -> Result<Value, Error> {
    let _gate = state.files.exclusive()?;
    let id = text(&args, "projectId")?;
    let revision = args["revision"].as_u64().ok_or_else(Error::invalid)?;
    let current = service.get(identity, id)?;
    authorize(state, identity, &current)?;
    if current.revision != revision {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }
    let mounts = serde_json::from_value(args["mounts"].clone()).map_err(|_| Error::invalid())?;
    if let Some(harness) = state.services.get::<crate::harness::Harnesses>() {
        harness.close_project(id).await;
    }
    let project = service.configure(
        state,
        identity,
        id,
        revision,
        text(&args, "name")?.into(),
        mounts,
    )?;
    Ok(json!({"project":super::descriptor::project(state,identity,&project)?}))
}

async fn execute(
    state: &Arc<State>,
    identity: usize,
    service: &ProjectService,
    args: Value,
) -> Result<Value, Error> {
    let project = service.get(identity, text(&args, "projectId")?)?;
    authorize(state, identity, &project)?;
    let project = super::service::effective(state, identity, &project);
    if args["revision"].as_u64() != Some(project.revision) {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }
    let mut request = request(
        state,
        &project,
        text(&args, "command")?.into(),
        serde_json::from_value(args["args"].clone()).map_err(|_| Error::invalid())?,
        args["timeoutMs"].as_u64().unwrap_or(30_000),
    );
    request.project_id = Some(project.id.clone());
    request.project_revision = Some(project.revision);
    request.read_only = args["readOnly"].as_bool().unwrap_or(false);
    if request.read_only {
        for mount in &mut request.mounts {
            mount.access = "ro".into();
        }
    }
    request.request_id = text(&args, "requestId")?.into();
    request.epoch = text(&args, "epoch")?.into();
    request.cwd = args["cwd"].as_str().unwrap_or("/workspace").into();
    if request.cwd != "/workspace" && !request.cwd.starts_with("/workspace/") {
        return Err(Error::forbidden("EACCES"));
    }
    Ok(json!({"process":process::start_command(state,identity,request).await?}))
}
