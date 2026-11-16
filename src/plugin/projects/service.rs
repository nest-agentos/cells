use super::{
    model::{Project, Register},
    store::Store,
    Config,
};
use crate::{
    app::State,
    core::error::Error,
    fs::{mutation::Change, path},
    process::{self, Mount},
};
use std::sync::{Arc, Mutex};
use super::registration::RegistrationAudit;
use serde_json::Value;

pub struct ProjectService {
    pub channels: super::channel::Channels,
    pub channels_enabled: bool,
    pub channel_endpoint: Option<String>,
    pub(super) watch_slots: Arc<tokio::sync::Semaphore>,
    pub(super) store: Store,
    pub launcher: Arc<dyn super::runtime::ProjectLauncher>,
    pub isolation: super::model::Isolation,
    reserved_ids: Mutex<std::collections::HashSet<String>>,
    registrations: crate::core::keyed_lock::SyncKeyedLocks,
    lifecycles: crate::core::keyed_gates::KeyedGates,
    pub(super) sync_bindings: crate::core::keyed_lock::KeyedLocks,
    owners: Vec<String>,
    pub execution_profiles: Vec<super::execution::ExecutionProfile>,
}
impl ProjectService {
    pub fn open(config: &Config, deployment: &crate::config::Config) -> Result<Self, String> {
        super::watch::resources::prepare();
        if config.channel_endpoint.as_deref().is_some_and(|endpoint| !super::api::valid_channel_endpoint(endpoint)) {
            return Err("Invalid project channel endpoint".into());
        }
        super::execution::validate_profiles(&config.execution_profiles)?;
        let store = Store::open(
            &config.root,
            deployment
                .server_id
                .as_deref()
                .ok_or("Project management requires server_id")?,
        )
        .map_err(|_| "Cannot open project catalog")?;
        for root in deployment
            .exports
            .iter()
            .map(|e| std::path::Path::new(&e.path))
            .chain(deployment.harnesses.iter().map(|h| h.home.as_path()))
            .chain(
                deployment
                    .sync
                    .iter()
                    .filter(|s| s.enabled)
                    .map(|s| s.root.as_path()),
            )
        {
            let root = root
                .canonicalize()
                .map_err(|_| "Project isolation root unavailable")?;
            if root.starts_with(store.root()) || store.root().starts_with(root) {
                return Err(
                    "Project catalog must be outside exports, sync and harness homes".into(),
                );
            }
        }
        Ok(Self {
            channels_enabled: config.network,
            channel_endpoint: config.channel_endpoint.clone(),
            channels: super::channel::Channels::open(store.root()).map_err(|_| "Cannot open session channels")?,
            watch_slots: Arc::new(tokio::sync::Semaphore::new(super::watch::MAX_SUBSCRIPTIONS)),
            store,
            isolation: config.isolation,
            launcher: match config.isolation {
                super::model::Isolation::Bubblewrap=>Arc::new(super::runtime::Bubblewrap {network:config.network}),
                super::model::Isolation::None=>Arc::new(super::native::Native),
            },
            reserved_ids: Default::default(),registrations: Default::default(),
            lifecycles: Default::default(),sync_bindings:Default::default(),
            execution_profiles: config.execution_profiles.clone(),
            owners: crate::config::authorization::identity_ids(deployment),
        })
    }
    pub fn lifecycle(&self,id:&str)->crate::core::gate::FileGate{self.lifecycles.get(id)}
    pub fn authorize_channel(&self, state: &State, token: &str) -> bool {
        let Some(id)=self.channels.token_project(token) else {return false;};
        let Ok(projects)=self.store.all() else {return false;};
        let Some(project)=projects.iter().find(|p|p.id==id) else {return false;};
        let Ok(identity)=state.auth.index(&project.owner) else {return false;};
        authorize(state,identity,project).is_ok()
    }
    pub fn restore_metadata(&self, state: &State) -> Result<(), Error> {
        for project in self.store.all()? {
            let Some(identity) = self.owners.iter().position(|owner| owner == &project.owner) else {continue};
            if let Err(error) = authorize(state, identity, &project).and_then(|_| maintain_metadata(state, identity, &project)) {
                crate::core::events::emit(crate::core::events::Level::Warn, "project.metadata.restore.failed",
                    serde_json::json!({"projectId":project.id,"code":error.code}));
            }
        }
        Ok(())
    }
    pub fn list(&self, identity: usize) -> Result<Vec<Project>, Error> {
        let owner = self.owner(identity)?;
        Ok(self
            .store
            .all()?
            .into_iter()
            .filter(|p| p.owner == owner)
            .collect())
    }
    pub fn get(&self, identity: usize, id: &str) -> Result<Project, Error> {
        self.store.get(self.owner(identity)?, id)
    }
    fn owner(&self, identity: usize) -> Result<&str, Error> {
        self.owners
            .get(identity)
            .map(String::as_str)
            .ok_or_else(|| Error::forbidden("EACCES"))
    }
    pub fn register(
        &self,
        state: &State,
        identity: usize,
        mut input: Register,
    ) -> Result<Project, Error> {
        if state.auth.client(identity).anonymous() {
            return Err(Error::forbidden("EACCES"));
        }
        let started = std::time::Instant::now();
        let mut audit = RegistrationAudit::new();
        let result=(||{
            let _node=state.files.shared()?;
            input.path=normalize(&input.path)?;
            let key=serde_json::json!([self.owner(identity)?,input.alias,input.path]).to_string();
            self.registrations.with(&key,||{
                audit.acquired(started.elapsed().as_millis());
                self.register_inner(state,identity,&mut input,&mut audit)
            })
        })();
        audit.complete(&result);
        result
    }
    fn register_inner(&self, state: &State, identity: usize, input: &mut Register, audit: &mut RegistrationAudit) -> Result<Project, Error> {
        input.path = normalize(&input.path)?;
        if let Some(project) = self
            .list(identity)?
            .into_iter()
            .find(|p| p.alias == input.alias && p.path == input.path)
        {
            let _project=self.lifecycle(&project.id).shared()?;
            authorize(state, identity, &project)?;
            audit.stage("metadata-scope", Some(&project.id));
            maintain_metadata(state, identity, &project)?;
            return Ok(project);
        }
        let (mut project,_claim) = self.new_registration(state, identity, input)?;
        self.ensure_registration_directory(state, identity, &project, input.create_directory, audit)?;
        audit.stage("directory-pin", Some(&project.id));
        capture(state, &mut project)?;
        validate(state, identity, &project, true)?;
        audit.stage("metadata-scope", Some(&project.id));
        maintain_metadata(state, identity, &project)?;
        audit.stage("catalog-persist", Some(&project.id));
        self.store.save(project, None)
    }
    fn new_registration<'a>(&'a self, state: &State, identity: usize, input: &Register) -> Result<(Project,super::identity::Claim<'a>), Error> {
        let claim=super::identity::reserve(&self.reserved_ids,||self.store.used_ids(),&input.name,input.id.as_deref())?;
        let id=claim.id.clone();
        Ok((Project {
            id,
            name: input.name.clone(),
            alias: input.alias.clone(),
            path: input.path.clone(),
            access: input.access.clone(),
            revision: 1,
            mounts: input.mounts.clone(),
            owner: state.auth.client(identity).id().into(),
            root_identity: String::new(),
            mount_identities: vec![],
            execution: Default::default(),
        },claim))
    }
    fn ensure_registration_directory(&self, state: &State, identity: usize, project: &Project, create: bool, audit: &mut RegistrationAudit) -> Result<(), Error> {
        audit.stage("directory-validation", Some(&project.id));
        validate(state, identity, project, false)?;
        let export = state.exports.get(&project.alias).ok_or_else(Error::invalid)?;
        match export.open_dir(&project.path) {
            Ok(_) => Ok(()),
            Err(error) if error.is_not_found() && create => {
                if project.path.is_empty() || !state.auth.client(identity).may_write(&project.alias) {
                    return Err(Error::forbidden("EROFS"));
                }
                // Create one child below an existing parent, never recursively create host paths.
                audit.stage("directory-create", Some(&project.id));
                export.apply(&Change::Mkdir {path: project.path.clone()}, || Ok(()))
            }
            Err(error) => Err(error),
        }
    }
    pub fn set_environment(&self, state: &State, identity: usize, id: &str, revision: u64,
        environment: super::execution::ProjectEnvironment) -> Result<Project, Error> {
        let mut project = self.get(identity, id)?;
        authorize(state, identity, &project)?;
        if project.revision != revision { return Err(Error::conflict("PROJECT_REVISION_CHANGED")); }
        if project.access != "rw" || !state.auth.client(identity).may_write(&project.alias) {
            return Err(Error::forbidden("EROFS"));
        }
        super::execution::resolve(&self.execution_profiles, &project.owner, &environment)?;
        project.execution = environment;
        project.revision += 1;
        self.store.save(project, Some(revision))
    }
    pub fn forget(&self, identity: usize, id: &str, revision: u64) -> Result<(), Error> {
        self.store.remove(self.owner(identity)?, id, revision)
    }
    pub fn configure(
        &self,
        state: &State,
        identity: usize,
        id: &str,
        revision: u64,
        name: String,
        mounts: Vec<Mount>,
    ) -> Result<Project, Error> {
        let mut project = self.get(identity, id)?;
        if project.revision != revision {
            return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
        }
        authorize(state, identity, &project)?;
        project.name = name;
        project.mounts = mounts;
        project.revision += 1;
        capture(state, &mut project)?;
        validate(state, identity, &project, true)?;
        maintain_metadata(state, identity, &project)?;
        self.store.save(project, Some(revision))
    }
    pub fn remount(
        &self,
        state: &State,
        identity: usize,
        id: &str,
        revision: u64,
        access: &str,
        directory: Option<&Value>,
    ) -> Result<Project, Error> {
        let previous=self.get(identity,id)?;
        let (alias,path)=match directory{
            Some(value)=>(value["alias"].as_str().ok_or_else(Error::invalid)?,normalize(value["path"].as_str().ok_or_else(Error::invalid)?)?),
            None=>(previous.alias.as_str(),previous.path.clone()),
        };
        let key=serde_json::json!([previous.owner,alias,path]).to_string();
        self.registrations.with(&key,||self.remount_inner(state,identity,id,revision,access,directory))
    }
    fn remount_inner(&self,state:&State,identity:usize,id:&str,revision:u64,access:&str,directory:Option<&Value>)->Result<Project,Error>{
        let mut project = self.get(identity, id)?;
        if project.revision != revision {
            return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
        }
        if !matches!(access, "ro" | "rw") {
            return Err(Error::invalid());
        }
        let previous=project.clone();
        if let Some(directory)=directory {readdress(&mut project,directory)?;}
        project.access = access.into();
        if directory.is_some()&&self.list(identity)?.iter().any(|other|other.id!=project.id&&other.alias==project.alias&&other.path==project.path) {
            return Err(Error::conflict("PROJECT_DIRECTORY_ALREADY_REGISTERED"));
        }
        validate(state, identity, &project, true)?;
        if previous.access == access && previous.alias==project.alias && previous.path==project.path {
            maintain_metadata(state, identity, &project)?;
            return Ok(project);
        }
        project.revision += 1;
        maintain_metadata(state, identity, &project)?;
        self.store.save(project, Some(revision))
    }
}
fn readdress(project:&mut Project,directory:&Value)->Result<(),Error> {
    let alias=directory["alias"].as_str().filter(|s|!s.is_empty()).ok_or_else(Error::invalid)?;
    let path=normalize(directory["path"].as_str().ok_or_else(Error::invalid)?)?;
    for mount in &mut project.mounts {
        if mount.alias!=project.alias {continue;}
        let suffix=if mount.path==project.path {Some("")} else if project.path.is_empty(){Some(mount.path.as_str())}
            else {mount.path.strip_prefix(&(project.path.clone()+"/"))};
        if let Some(suffix)=suffix {mount.alias=alias.into();mount.path=[path.as_str(),suffix].into_iter().filter(|s|!s.is_empty()).collect::<Vec<_>>().join("/");}
    }
    project.alias=alias.into();project.path=path;Ok(())
}

pub fn normalize(value: &str) -> Result<String, Error> {
    if value.starts_with('/') || value.contains(['\\', '\0']) {
        return Err(Error::invalid());
    }
    let path = value
        .split('/')
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    path::validate(&path)?;
    Ok(path)
}
pub fn mounts(project: &Project) -> Vec<Mount> {
    let mut mounts = vec![Mount {
        alias: project.alias.clone(),
        path: project.path.clone(),
        at: "/workspace".into(),
        access: project.access.clone(),
    }];
    mounts.extend(project.mounts.clone());
    mounts
}
pub fn authorize(state: &State, identity: usize, project: &Project) -> Result<(), Error> {
    validate(state, identity, &effective(state, identity, project), true)
}
pub fn effective(state: &State, identity: usize, project: &Project) -> Project {
    let client = state.auth.client(identity);
    let mut project = project.clone();
    if !client.may_write(&project.alias) {
        project.access = "ro".into();
    }
    for mount in &mut project.mounts {
        if !client.may_write(&mount.alias) {
            mount.access = "ro".into();
        }
    }
    project
}
fn validate(
    state: &State,
    identity: usize,
    project: &Project,
    existing: bool,
) -> Result<(), Error> {
    if project.owner != state.auth.client(identity).id()
        || project.name.trim().is_empty()
        || project.name.len() > 256
        || project.mounts.len() > 31
    {
        return Err(Error::invalid());
    }
    path::validate(&project.path)?;
    for mount in &project.mounts {
        if !mount.at.starts_with("/workspace/") {
            return Err(Error::forbidden("PROJECT_MOUNT_OUTSIDE_HOME"));
        }
        path::validate(&mount.path)?;
        let declarations = mounts(project);
        let parent = declarations
            .iter()
            .filter(|p| p.at.len() < mount.at.len() && mount.at.starts_with(&(p.at.clone() + "/")))
            .max_by_key(|p| p.at.len())
            .ok_or_else(Error::invalid)?;
        let target = [
            parent.path.as_str(),
            mount.at[parent.at.len()..].trim_start_matches('/'),
        ]
        .into_iter()
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("/");
        state
            .exports
            .get(&parent.alias)
            .ok_or_else(Error::invalid)?
            .open_dir(&target)
            .map_err(|_| Error::not_found("PROJECT_MOUNT_TARGET_REQUIRED"))?;

        if !state.auth.client(identity).may_read(&mount.alias) {
            return Err(Error::forbidden("EACCES"));
        }
        state
            .exports
            .get(&mount.alias)
            .ok_or_else(Error::invalid)?
            .open_dir(&mount.path)?;
    }
    let mut request = request(state, project, "/bin/true".into(), vec![], 1000);
    if !existing {
        request.cwd = "/workspace".into();
    }
    // Shared process policy validates attenuation, runtime shadowing and source overlap.
    process::policy::validate_mounts(state, identity, &request.mounts)?;
    if existing {
        process::policy::plan(state, identity, &request)?;
        let root = state
            .exports
            .get(&project.alias)
            .ok_or_else(Error::invalid)?
            .open_dir(&project.path)?;
        if !identity_matches(&root, &project.root_identity)?
            || project.mount_identities.len() != project.mounts.len()
        {
            return Err(Error::conflict("PROJECT_DIRECTORY_CHANGED"));
        }
        for (mount, expected) in project.mounts.iter().zip(&project.mount_identities) {
            let directory = state
                .exports
                .get(&mount.alias)
                .ok_or_else(Error::invalid)?
                .open_dir(&mount.path)?;
            if !identity_matches(&directory, expected)? {
                return Err(Error::conflict("PROJECT_DIRECTORY_CHANGED"));
            }
        }
    }
    Ok(())
}
pub fn identity_of(directory: &std::fs::File) -> Result<String, Error> {
    use std::os::unix::fs::MetadataExt;
    let metadata = directory.metadata()?;
    let created = metadata
        .created()
        .map_err(|_| Error::unsupported())?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::unsupported())?;
    Ok(format!(
        "{}:{}:{}",
        crate::fs::directory_identity::device(directory)?,
        metadata.ino(),
        created.as_nanos()
    ))
}
pub fn identity_matches(directory: &std::fs::File, expected: &str) -> Result<bool, Error> {
    Ok(crate::fs::directory_identity::matches(expected, &identity_of(directory)?))
}
fn capture(state: &State, project: &mut Project) -> Result<(), Error> {
    project.root_identity = identity_of(
        &state
            .exports
            .get(&project.alias)
            .ok_or_else(Error::invalid)?
            .open_dir(&project.path)?,
    )?;
    project.mount_identities = project
        .mounts
        .iter()
        .map(|mount| {
            identity_of(
                &state
                    .exports
                    .get(&mount.alias)
                    .ok_or_else(Error::invalid)?
                    .open_dir(&mount.path)?,
            )
        })
        .collect::<Result<_, _>>()?;
    Ok(())
}
pub fn pin(plan: &mut process::policy::Plan, project: &Project) -> Result<(), Error> {
    for mount in &mut plan.mounts {
        let identity = if mount.at == "/workspace" {
            project.root_identity.clone()
        } else {
            let index = project
                .mounts
                .iter()
                .position(|m| m.at == mount.at && m.alias == mount.alias && m.path == mount.path)
                .ok_or_else(Error::invalid)?;
            project
                .mount_identities
                .get(index)
                .ok_or_else(Error::invalid)?
                .clone()
        };
        mount.identity = Some(identity);
    }
    Ok(())
}
pub fn request(
    state: &State,
    project: &Project,
    command: String,
    args: Vec<String>,
    timeout_ms: u64,
) -> process::Request {
    process::Request {
        server_id: state.auth.server_id().unwrap_or("").into(),
        epoch: state.epoch.as_str().into(),
        request_id: String::new(),
        project_id: None,
        project_revision: None,
        read_only: false,
        command,
        args,
        cwd: "/workspace".into(),
        mounts: mounts(project),
        timeout_ms,
    }
}

fn maintain_metadata(state: &State, identity: usize, project: &Project) -> Result<(), Error> {
    if effective(state, identity, project).access != "rw" { return Ok(()); }
    let started = std::time::Instant::now();
    let export = state.exports.get(&project.alias).ok_or_else(Error::invalid)?;
    let owner = format!("{}:{}", state.auth.server_id().unwrap_or(""), project.owner);
    let value = serde_json::json!({"version":1,"id":project.id,"name":project.name,"alias":project.alias,"path":project.path});
    let result = export.initialize_metadata_scope(&project.path, &owner, &project.id, &value);
    crate::core::events::emit(if result.is_ok() {crate::core::events::Level::Info} else {crate::core::events::Level::Error},
        "project.metadata.prepare", serde_json::json!({"projectId":project.id,"stage":"directory-scope",
        "result":if result.is_ok() {"success"} else {"failure"},"code":result.as_ref().err().map(|e|e.code),"elapsedMs":started.elapsed().as_millis()}));
    result
}
