//! Shared commit/lease barrier. No transport credentials are stored in diagnostics.
use super::error::Error;
use axum::http::StatusCode;
use std::{
    cell::RefCell,
    sync::{Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant},
};

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Resource {
    pub entry: String,
    pub directory: Option<String>,
    pub ancestors: Vec<String>,
}
impl Resource {
    fn overlaps(&self, other: &Self) -> bool {
        self.entry == other.entry
            || (self.directory.is_some() && self.directory == other.directory)
            || self
                .directory
                .as_ref()
                .is_some_and(|id| other.ancestors.contains(id))
            || other
                .directory
                .as_ref()
                .is_some_and(|id| self.ancestors.contains(id))
    }
}
struct Lease {
    resource: Resource,
    token: String,
    owner: usize,
    operation: String,
    url: String,
    expires: Instant,
}
#[derive(Default)]
pub(crate) struct Registry {
    enabled: bool,
    unknown: bool,
    native: usize,
    leases: Vec<Lease>,
}
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
thread_local! { static TOKENS: RefCell<(usize, Vec<String>, Vec<usize>)> = const { RefCell::new((usize::MAX, Vec::new(), Vec::new())) }; }
pub(crate) fn credentials<T>(owner: usize, tokens: Vec<String>, work: impl FnOnce() -> T) -> T {
    credentials_for(owner, tokens, Vec::new(), work)
}
pub(crate) fn credentials_for<T>(
    owner: usize,
    tokens: Vec<String>,
    scopes: Vec<usize>,
    work: impl FnOnce() -> T,
) -> T {
    struct Reset((usize, Vec<String>, Vec<usize>));
    impl Drop for Reset {
        fn drop(&mut self) {
            TOKENS.with(|slot| {
                slot.replace(std::mem::take(&mut self.0));
            });
        }
    }
    let _reset = Reset(TOKENS.with(|slot| slot.replace((owner, tokens, scopes))));
    work()
}
pub(crate) fn registry() -> Result<MutexGuard<'static, Registry>, Error> {
    let mut registry = REGISTRY
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| Error::internal())?;
    registry.leases.retain(|lease| {
        if lease.expires > Instant::now() { return true; }
        super::events::emit(super::events::Level::Info,"file.lock.expired",serde_json::json!({"operation":"lock.expire","operationId":lease.operation,"stage":"lease","result":"success","code":"LOCK_EXPIRED","durationMs":0}));
        false
    });
    Ok(registry)
}
pub(crate) fn enable() -> Result<(), Error> {
    registry()?.enabled = true;
    Ok(())
}
pub(crate) fn poison() {
    if let Ok(mut registry) = registry() {
        registry.unknown = true;
    }
}
pub(crate) fn locked() -> Error {
    Error::new(StatusCode::LOCKED, "FILE_LOCK_CONFLICT")
}
impl Registry {
    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }
    pub(crate) fn check(&self, resources: &[Resource]) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }
        if self.unknown {
            return Err(Error::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "FILE_EXECUTION_UNKNOWN",
            ));
        }
        if self.native > 0 {
            return Err(Error::busy());
        }
        TOKENS.with(|slot| {
            let (owner, tokens, scopes) = &*slot.borrow();
            self.check_tokens(*owner, tokens, scopes, resources)?;
            if self.leases.iter().any(|lease| {
                lease.expires > Instant::now()
                    && resources
                        .iter()
                        .any(|resource| resource.overlaps(&lease.resource))
                    && !(lease.owner == *owner && tokens.contains(&lease.token))
            }) {
                return Err(locked());
            }
            Ok(())
        })
    }
    fn check_tokens(
        &self,
        owner: usize,
        tokens: &[String],
        scopes: &[usize],
        resources: &[Resource],
    ) -> Result<(), Error> {
        for (index, token) in tokens.iter().enumerate() {
            let lease = self
                .leases
                .iter()
                .find(|lease| {
                    lease.owner == owner && lease.token == *token && lease.expires > Instant::now()
                })
                .ok_or_else(Error::precondition_failed)?;
            if let Some(scope) = scopes.get(index) {
                if !resources
                    .get(*scope)
                    .is_some_and(|resource| resource.overlaps(&lease.resource))
                {
                    return Err(Error::precondition_failed());
                }
            }
        }
        Ok(())
    }
    pub(crate) fn lease(
        &mut self,
        resource: Resource,
        owner: usize,
        token: String,
        seconds: u64,
        operation: String,
        url: String,
    ) -> Result<(), Error> {
        if self.unknown {
            return Err(Error::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "FILE_EXECUTION_UNKNOWN",
            ));
        }
        if self.native > 0
            || self
                .leases
                .iter()
                .any(|lease| resource.overlaps(&lease.resource))
        {
            return Err(locked());
        }
        if self.leases.len() >= 4096 {
            return Err(Error::too_many("LOCK_LIMIT"));
        }
        self.leases.push(Lease {
            resource,
            owner,
            token,
            operation,
            url,
            expires: Instant::now() + Duration::from_secs(seconds),
        });
        Ok(())
    }
    pub(crate) fn refresh(
        &mut self,
        resource: &Resource,
        owner: usize,
        token: &str,
        seconds: u64,
    ) -> Result<(), Error> {
        let lease = self
            .leases
            .iter_mut()
            .find(|lease| {
                lease.resource.entry == resource.entry
                    && lease.owner == owner
                    && lease.token == token
            })
            .ok_or_else(|| Error::precondition_failed())?;
        lease.expires = Instant::now() + Duration::from_secs(seconds);
        Ok(())
    }
    pub(crate) fn release(
        &mut self,
        resource: &Resource,
        owner: usize,
        token: &str,
    ) -> Result<(), Error> {
        let index = self
            .leases
            .iter()
            .position(|lease| {
                lease.resource.entry == resource.entry
                    && lease.owner == owner
                    && lease.token == token
            })
            .ok_or_else(|| Error::conflict("LOCK_TOKEN_UNKNOWN"))?;
        self.leases.remove(index);
        Ok(())
    }
    pub(crate) fn removed(&mut self, resource: &Resource) {
        self.leases.retain(|lease| {
            lease.resource.entry != resource.entry
                && !resource.directory.as_ref().is_some_and(|id| {
                    lease.resource.directory.as_ref() == Some(id)
                        || lease.resource.ancestors.contains(id)
                })
        });
    }
    pub(crate) fn discovery(
        &self,
        resource: &Resource,
        owner: usize,
    ) -> Vec<(String, u64, String)> {
        self.leases
            .iter()
            .filter(|lease| lease.owner == owner && lease.resource.overlaps(resource))
            .map(|lease| {
                (
                    lease.token.clone(),
                    lease
                        .expires
                        .saturating_duration_since(Instant::now())
                        .as_secs(),
                    lease.url.clone(),
                )
            })
            .collect()
    }
    pub(crate) fn owns(&self, resource: &Resource, owner: usize, tokens: &[String]) -> bool {
        self.leases.iter().any(|lease| {
            lease.resource.overlaps(resource)
                && lease.expires > Instant::now()
                && lease.owner == owner
                && tokens.contains(&lease.token)
        })
    }
}
#[derive(Debug)]
pub(crate) struct Native;
pub(crate) fn native() -> Result<Native, Error> {
    let mut registry = registry()?;
    if registry.enabled && registry.unknown {
        return Err(Error::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "FILE_EXECUTION_UNKNOWN",
        ));
    }
    if registry.enabled && !registry.leases.is_empty() {
        super::events::emit(
            super::events::Level::Warn,
            "file.lock.native_rejected",
            serde_json::json!({"operation":"execution.admit","stage":"lease","result":"failure","code":"FILE_LOCK_CONFLICT","blockerOperationId":registry.leases.first().map(|lease|&lease.operation),"lockCount":registry.leases.len(),"durationMs":0}),
        );
        return Err(locked());
    }
    registry.native += 1;
    Ok(Native)
}
impl Drop for Native {
    fn drop(&mut self) {
        if let Ok(mut registry) = registry() {
            registry.native = registry.native.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn file(name: &str) -> Resource {
        Resource {
            entry: format!("1:1/{name}"),
            directory: None,
            ancestors: vec!["1:1".into()],
        }
    }
    fn directory() -> Resource {
        Resource {
            entry: "root:1:1".into(),
            directory: Some("1:1".into()),
            ancestors: vec!["1:1".into()],
        }
    }
    fn registry_with(resource: Resource) -> Registry {
        let mut registry = Registry {
            enabled: true,
            ..Default::default()
        };
        registry
            .lease(
                resource,
                1,
                "token".into(),
                60,
                "op".into(),
                "/dav/docs/a".into(),
            )
            .unwrap();
        registry
    }
    #[test]
    fn directory_leases_cover_aliases_and_children_without_locking_other_roots() {
        let registry = registry_with(directory());
        assert_eq!(
            registry.check(&[file("child")]).unwrap_err().status,
            StatusCode::LOCKED
        );
        let other = Resource {
            entry: "2:2/child".into(),
            directory: None,
            ancestors: vec!["2:2".into()],
        };
        assert!(registry.check(&[other]).is_ok());
        assert!(credentials(1, vec!["token".into()], || registry.check(&[file("child")])).is_ok());
        assert!(credentials(2, vec!["token".into()], || registry.check(&[file("child")])).is_err());
    }
    #[test]
    fn tagged_tokens_must_belong_to_the_tagged_resource() {
        let registry = registry_with(file("a"));
        assert!(
            credentials_for(1, vec!["token".into()], vec![0], || registry
                .check(&[file("a"), file("b")]))
            .is_ok()
        );
        assert_eq!(
            credentials_for(1, vec!["token".into()], vec![1], || registry
                .check(&[file("a"), file("b")]))
            .unwrap_err()
            .status,
            StatusCode::PRECONDITION_FAILED
        );
    }
    #[test]
    fn stale_tokens_and_unknown_execution_never_authorize_a_commit() {
        let mut registry = Registry {
            enabled: true,
            ..Default::default()
        };
        assert_eq!(
            credentials(1, vec!["stale".into()], || registry.check(&[file("a")]))
                .unwrap_err()
                .status,
            StatusCode::PRECONDITION_FAILED
        );
        registry.native = 1;
        assert_eq!(registry.check(&[file("a")]).unwrap_err().code, "EBUSY");
        registry.native = 0;
        registry.unknown = true;
        assert_eq!(
            registry.check(&[file("a")]).unwrap_err().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    #[test]
    fn expiration_during_a_commit_invalidates_the_submitted_token() {
        let mut registry = registry_with(file("a"));
        registry.leases[0].expires = Instant::now() - Duration::from_secs(1);
        assert_eq!(
            credentials(1, vec!["token".into()], || registry.check(&[file("a")]))
                .unwrap_err()
                .status,
            StatusCode::PRECONDITION_FAILED
        );
        assert!(registry.check(&[file("a")]).is_ok());
    }
    #[test]
    fn namespace_removal_keeps_ancestor_lease_and_retires_source_lease() {
        let mut registry = registry_with(directory());
        registry.removed(&file("a"));
        assert_eq!(registry.leases.len(), 1);
        registry.removed(&directory());
        assert!(registry.leases.is_empty());
    }
}
