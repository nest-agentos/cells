//! Coordinate only named resources; the kernel owns directory traversal and rename ordering.
use super::error::Error;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock, Weak},
};
#[derive(Default)]
pub(crate) struct PathLocks(Mutex<HashMap<String, Weak<RwLock<()>>>>);
impl PathLocks {
    pub(crate) fn with<T>(
        &self,
        paths: &[(&str, bool)],
        work: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut keys = BTreeMap::new();
        for (path, write) in paths {
            keys.entry((*path).to_owned())
                .and_modify(|prior| *prior |= *write)
                .or_insert(*write);
        }
        let resources = {
            let mut registry = self.0.lock().map_err(|_| Error::internal())?;
            registry.retain(|_, lock| lock.strong_count() > 0);
            keys.into_iter()
                .map(|(key, write)| {
                    let lock = registry
                        .get(&key)
                        .and_then(Weak::upgrade)
                        .unwrap_or_else(|| {
                            let lock = Arc::new(RwLock::new(()));
                            registry.insert(key, Arc::downgrade(&lock));
                            lock
                        });
                    (lock, write)
                })
                .collect::<Vec<_>>()
        };
        enum Guard<'a> {
            Read(#[allow(dead_code)] std::sync::RwLockReadGuard<'a, ()>),
            Write(#[allow(dead_code)] std::sync::RwLockWriteGuard<'a, ()>),
        }
        let _guards = resources
            .iter()
            .map(|(lock, write)| {
                if *write {
                    lock.write()
                        .map(Guard::Write)
                        .map_err(|_| Error::internal())
                } else {
                    lock.read().map(Guard::Read).map_err(|_| Error::internal())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        work()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_blocked_file_does_not_serialize_siblings_or_its_parent() {
        let locks = Arc::new(PathLocks::default());
        let (ready, entered) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let child = {
            let locks = locks.clone();
            std::thread::spawn(move || {
                locks
                    .with(&[("project/a", true)], || {
                        ready.send(()).unwrap();
                        released.recv().unwrap();
                        Ok(())
                    })
                    .unwrap()
            })
        };
        entered.recv().unwrap();
        locks.with(&[("project/b", true)], || Ok(())).unwrap();
        let (done, finished) = std::sync::mpsc::channel();
        let parent = {
            let locks = locks.clone();
            std::thread::spawn(move || {
                locks.with(&[("project", true)], || Ok(())).unwrap();
                done.send(()).unwrap();
            })
        };
        finished
            .recv_timeout(std::time::Duration::from_millis(200))
            .unwrap();
        release.send(()).unwrap();
        child.join().unwrap();
        parent.join().unwrap();
    }
}
