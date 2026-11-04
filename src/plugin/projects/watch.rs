//! Bounded inotify invalidation over pinned project directory capabilities.
use super::{model::Project, service};
use crate::{app::State, core::error::Error};
use rustix::fs::inotify::{self, CreateFlags, ReadFlags, WatchFlags};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::File,
    mem::MaybeUninit,
    os::fd::{AsRawFd, OwnedFd},
    sync::Arc,
    time::{Duration, Instant},
};
mod subscription;
mod tree;
pub(super) use subscription::subscribe;

struct Watch {
    generation: u64,
    roots: Vec<tree::Root>,
    fd: OwnedFd,
    directories: BTreeMap<i32, tree::Directory>,
    version: u64,
    truncated: bool,
    needs_rescan: bool,
}
fn authorized(state: &State, owner: usize, args: &Value) -> Result<Project, Error> {
    let id = args["projectId"].as_str().ok_or_else(Error::invalid)?;
    let project = state
        .services
        .get::<crate::projects::ProjectService>()
        .ok_or_else(Error::unsupported)?
        .get(owner, id)?;
    service::authorize(state, owner, &project)?;
    if args["revision"].as_u64() != Some(project.revision) {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }
    Ok(project)
}
fn create(state: &State, project: &Project, directory: Option<&str>) -> Result<(Watch, String), Error> {
    let roots = tree::scoped_roots(state, project, directory)?;
    let fd = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)?;
    let (directories, truncated) = tree::scan(&fd, &roots)?;
    let mut random = [0u8; 16];
    getrandom::getrandom(&mut random).map_err(|_| Error::internal())?;
    let token = random.iter().map(|b| format!("{b:02x}")).collect();
    Ok((
        Watch {
            generation: 0,
            roots,
            fd,
            directories,
            version: 0,
            truncated,
            needs_rescan: false,
        },
        token,
    ))
}
fn poll(watch: &mut Watch) -> Result<bool, Error> {
    let (changed, rescan, overflow) = drain(watch)?;
    if changed {
        watch.version += 1;
    }
    watch.needs_rescan |= rescan;
    if watch.needs_rescan {
        rebuild(watch)?;
    }
    Ok(overflow)
}
fn drain(watch: &Watch) -> Result<(bool, bool, bool), Error> {
    let mut buffer = [MaybeUninit::uninit(); 65536];
    let mut reader = inotify::Reader::new(&watch.fd, &mut buffer);
    let (mut changed, mut rescan, mut overflow) = (false, false, false);
    for _ in 0..8192 {
        let event = match reader.next() {
            Ok(event) => event,
            Err(rustix::io::Errno::WOULDBLOCK) => break,
            Err(error) => return Err(error.into()),
        };
        if event.events().contains(ReadFlags::QUEUE_OVERFLOW) {
            changed = true;
            rescan = true;
            overflow = true;
            continue;
        }
        let Some(directory) = watch.directories.get(&event.wd()) else {
            continue;
        };
        if !tree::visible(directory, event.file_name()) {
            continue;
        }
        changed = true;
        rescan |= event.events().intersects(
            ReadFlags::ISDIR | ReadFlags::DELETE_SELF | ReadFlags::MOVE_SELF | ReadFlags::IGNORED,
        );
    }
    Ok((changed, rescan, overflow))
}
fn rebuild(watch: &mut Watch) -> Result<(), Error> {
    let fd = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)?;
    let (directories, truncated) = tree::scan(&fd, &watch.roots)?;
    watch.fd = fd;
    watch.generation += 1;
    watch.directories = directories;
    watch.truncated = truncated;
    watch.needs_rescan = false;
    Ok(())
}
