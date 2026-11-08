//! Explicit cooperative execution in the Cells host OS (including a dedicated VM).
use super::{
    model::{Isolation, Project},
    runtime::ProjectLauncher,
};
use crate::{
    app::State,
    core::error::Error,
    process::{policy::Plan, sandbox::Prepared},
};
use std::{
    fs::File,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::Stdio,
};

pub struct Native;
pub fn enabled(state: &State) -> bool {
    state
        .services
        .get::<super::ProjectService>()
        .is_some_and(|s| s.isolation == Isolation::None)
}
pub fn root(state: &State, project: &Project) -> Result<String, Error> {
    let directory = state
        .exports
        .get(&project.alias)
        .ok_or_else(Error::invalid)?
        .open_dir(&project.path)?;
    if !super::service::identity_matches(&directory, &project.root_identity)? {
        return Err(Error::conflict("PROJECT_DIRECTORY_CHANGED"));
    }
    physical(&directory)?
        .into_os_string()
        .into_string()
        .map_err(|_| Error::invalid())
}
pub fn physical(directory: &File) -> Result<PathBuf, Error> {
    Ok(std::fs::read_link(format!(
        "/proc/self/fd/{}",
        directory.as_raw_fd()
    ))?)
}
impl ProjectLauncher for Native {
    fn prepare_process(
        &self,
        plan: &Plan,
        lock: File,
        environment: Option<(
            &super::execution::ExecutionProfile,
            &super::execution::ProjectEnvironment,
        )>,
    ) -> Result<Prepared, Error> {
        let mut prepared = prepare(plan, lock)?;
        if let Some((profile, values)) = environment {
            super::execution::prepare_native(&mut prepared.command, Some(profile), values)?;
        }
        Ok(prepared)
    }
    fn prepare_harness(
        &self,
        plan: &Plan,
        lock: File,
        _home: &Path,
        _environment: Option<(
            &super::execution::ExecutionProfile,
            &super::execution::ProjectEnvironment,
        )>,
    ) -> Result<Prepared, Error> {
        prepare(plan, lock)
    }
}
fn prepare(plan:&Plan,lock:File)->Result<Prepared,Error> {
    let mut directories=pin_directories(plan)?;
    validate_mappings(plan,&directories)?;
    let cwd=working_directory(plan)?;
    let mut command=native_command(plan);
    child_context(&mut command,&cwd,&lock);
    directories.extend([cwd,lock]);Ok(Prepared {command,directories})
}
fn pin_directories(plan:&Plan)->Result<Vec<File>,Error> {
    plan.mounts.iter().map(|mount|{
        let file=mount.export.open_dir(&mount.path)?;
        if mount.identity.as_ref().map(|expected|super::service::identity_matches(&file,expected)).transpose()?.is_some_and(|matches|!matches) {
            return Err(Error::conflict("PROJECT_DIRECTORY_CHANGED"));
        }
        Ok(file)
    }).collect()
}
fn working_directory(plan:&Plan)->Result<File,Error> {
    let mount=plan.mounts.iter().filter(|m|plan.cwd==m.at||plan.cwd.starts_with(&(m.at.clone()+"/")))
        .max_by_key(|m|m.at.len()).ok_or_else(Error::invalid)?;
    let relative=plan.cwd.strip_prefix(&mount.at).ok_or_else(Error::invalid)?.trim_start_matches('/');
    mount.export.open_dir(&[mount.path.as_str(),relative].into_iter().filter(|s|!s.is_empty()).collect::<Vec<_>>().join("/"))
}
fn native_command(plan:&Plan)->tokio::process::Command {
    let mut command=tokio::process::Command::new(&plan.command);
    command.args(&plan.args).env_clear().env("PATH",std::env::var_os("PATH").unwrap_or_else(||"/usr/local/bin:/usr/bin:/bin".into()))
        .env("LANG","C.UTF-8").stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).process_group(0);
    for key in ["HOME","USER","LOGNAME"] {if let Some(value)=std::env::var_os(key){command.env(key,value);}}
    command
}
fn child_context(command:&mut tokio::process::Command,cwd:&File,lock:&File) {
    let cwd_fd=cwd.as_raw_fd();let lock_fd=lock.as_raw_fd();let parent=std::process::id() as libc::pid_t;
    // SAFETY: only async-signal-safe syscalls are used in the forked child.
    unsafe {command.pre_exec(move||{
        if libc::fchdir(cwd_fd)!=0||libc::fcntl(lock_fd,libc::F_SETFD,0)!=0||libc::prctl(libc::PR_SET_PDEATHSIG,libc::SIGKILL)!=0||libc::getppid()!=parent {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    });}
}
fn validate_mappings(plan: &Plan, directories: &[File]) -> Result<(), Error> {
    for (index, mount) in plan.mounts.iter().enumerate().skip(1) {
        let base = &plan.mounts[0];
        let suffix = mount
            .at
            .strip_prefix(&(base.at.clone() + "/"))
            .ok_or_else(|| Error::conflict("NATIVE_MOUNT_REMAP_UNSUPPORTED"))?;
        let target = base.export.open_dir(
            &[base.path.as_str(), suffix]
                .into_iter()
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("/"),
        )?;
        if super::service::identity_of(&target)?
            != super::service::identity_of(&directories[index])?
        {
            return Err(Error::conflict("NATIVE_MOUNT_REMAP_UNSUPPORTED"));
        }
    }
    Ok(())
}
