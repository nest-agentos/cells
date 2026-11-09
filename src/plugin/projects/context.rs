//! Selected related project directories are resolved and reauthorized by the Cell.
use super::{model::Project, runtime::ProjectRuntime, service};
use crate::{app::State, core::error::Error};
use serde_json::{json, Value};
use std::sync::Arc;

pub fn prepare(state:&Arc<State>,identity:usize,primary:&Project,args:&mut Value)->Result<(),Error> {
    if args["snapshot"].get("project-context").is_some() {return Err(Error::invalid());}
    let Some(values)=args.get("contextProjects") else {return Ok(());};
    if !args["snapshot"].is_object() {return Err(Error::invalid());}
    let projects=resolve(state,identity,primary,values)?;
    let directories=projects.iter().map(|project|Ok(json!({"projectId":project.id,"revision":project.revision,
        "directory":ProjectRuntime::new(state,identity,project.clone())?.cwd()}))).collect::<Result<Vec<_>,Error>>()?;
    args["snapshot"]["project-context"]=json!({"primaryProjectId":primary.id,"projects":directories});
    Ok(())
}
pub fn resolve(state:&Arc<State>,identity:usize,primary:&Project,values:&Value)->Result<Vec<Project>,Error> {
    let values=values.as_array().filter(|rows|!rows.is_empty()&&rows.len()<=16).ok_or_else(Error::invalid)?;
    let service=state.services.require::<super::ProjectService>()?;
    let mut seen=std::collections::HashSet::new();let mut projects=Vec::new();
    for value in values {
        let id=value["projectId"].as_str().ok_or_else(Error::invalid)?;
        if !seen.insert(id) {return Err(Error::invalid());}
        let project=service.get(identity,id)?;service::authorize(state,identity,&project)?;
        if value["revision"].as_u64()!=Some(project.revision) {return Err(Error::conflict("PROJECT_CONTEXT_CHANGED"));}
        projects.push(service::effective(state,identity,&project));
    }
    if !seen.contains(primary.id.as_str()) {return Err(Error::forbidden("PROJECT_CONTEXT_PRIMARY_REQUIRED"));}
    Ok(projects)
}
pub fn for_harness(state:&Arc<State>,identity:usize,primary:&Project,args:&Value)->Result<Vec<Project>,Error> {
    let runtime=ProjectRuntime::new(state,identity,primary.clone())?;
    let cwd=args["directory"].as_str().map(|directory|runtime.working_directory(directory)).transpose()?;
    let context=state.services.require::<super::ProjectService>()?.channels.project_context(&primary.id,cwd.as_deref(),args["sessionId"].as_str())?;
    let Some(context)=context else {return Ok(Vec::new());};
    resolve(state,identity,primary,&context["projects"])
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn harness_context_follows_the_directory_and_bound_session_and_rechecks_revisions() {
        let root=tempfile::tempdir().unwrap();
        for name in ["exports/primary/discussion","exports/unrelated","exports/secondary","catalog"] {std::fs::create_dir_all(root.path().join(name)).unwrap();}
        let config=toml::from_str(&format!("listen=\"127.0.0.1:0\"\nserver_id=\"context-cell\"\ntoken=\"context-test-secret-0123456789\"\n[projects]\nroot={:?}\n[[exports]]\nalias=\"home\"\npath={:?}\naccess=\"rw\"\n",root.path().join("catalog"),root.path().join("exports"))).unwrap();
        let state=State::from_config(&config).unwrap();
        let register=|path:&str|json!({"name":path,"alias":"home","path":path,"access":"rw"});
        let primary=super::super::call(&state,0,"project_register",register("primary")).await.unwrap()["project"].clone();
        let secondary=super::super::call(&state,0,"project_register",register("secondary")).await.unwrap()["project"].clone();
        let service=state.services.require::<super::super::ProjectService>().unwrap();
        let project=service.get(0,primary["id"].as_str().unwrap()).unwrap();let runtime=ProjectRuntime::new(&state,0,project.clone()).unwrap();
        let cwd=runtime.working_directory("discussion").unwrap();
        let mut args=json!({"scopeId":"scope","cwd":cwd,"endpoint":"http://localhost/v1/channel/mcp","snapshot":{},"contextProjects":[
            {"projectId":primary["id"],"revision":primary["revision"]},{"projectId":secondary["id"],"revision":secondary["revision"]}]});
        prepare(&state,0,&project,&mut args).unwrap();service.channels.create(&project.id,&args).unwrap();
        assert_eq!(for_harness(&state,0,&project,&json!({"directory":"discussion"})).unwrap().len(),2);
        assert!(for_harness(&state,0,&project,&json!({"sessionId":"unrelated"})).unwrap().is_empty());
        service.channels.bind(&project.id,&cwd,"native").unwrap();
        assert_eq!(for_harness(&state,0,&project,&json!({"sessionId":"native"})).unwrap().len(),2);
        super::super::call(&state,0,"project_configure",json!({"projectId":secondary["id"],"revision":secondary["revision"],"name":"Updated","mounts":[]})).await.unwrap();
        assert!(matches!(for_harness(&state,0,&project,&json!({"sessionId":"native"})),Err(error) if error.code=="PROJECT_CONTEXT_CHANGED"));
    }
}
