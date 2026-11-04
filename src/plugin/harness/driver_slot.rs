//! Unload idle native runtimes while retaining the profile's operation receipts.
use super::{config::ProfileConfig, driver::HarnessDriver, HarnessPlugins};
use crate::{core::error::Error, projects::runtime::ProjectRuntime};
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

pub(super) struct DriverSlot {
    changed: tokio::sync::watch::Sender<u64>,
    current: Mutex<Option<Arc<dyn HarnessDriver>>>,
    touched: Mutex<Instant>,
    closed: AtomicBool,
    descriptor: Value,
    create: Box<dyn Fn() -> Result<Box<dyn HarnessDriver>, String> + Send + Sync>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::BoxFuture;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;
    struct Driver {
        idle: Arc<Mutex<Option<Instant>>>,
        stopped: Arc<AtomicUsize>,
    }
    impl HarnessDriver for Driver {
        fn descriptor(&self) -> Value {
            json!({})
        }
        fn read<'a>(&'a self, _: &'a str, _: Value) -> BoxFuture<'a, Result<Value, Error>> {
            Box::pin(async { Ok(json!({})) })
        }
        fn execute<'a>(&'a self, _: &'a str, _: &'a Value) -> BoxFuture<'a, Result<Value, Error>> {
            Box::pin(async { Ok(json!({})) })
        }
        fn stop(&self) {
            self.stopped.fetch_add(1, Ordering::SeqCst);
        }
        fn idle_since(&self) -> Option<Instant> {
            *self.idle.lock().unwrap()
        }
        fn drain(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }
    #[tokio::test]
    async fn unloads_only_idle_unborrowed_drivers_and_recreates_them_lazily() {
        let idle = Arc::new(Mutex::new(Some(Instant::now())));
        let stopped = Arc::new(AtomicUsize::new(0));
        let created = Arc::new(AtomicUsize::new(0));
        let create = {
            let (idle, stopped, created) = (idle.clone(), stopped.clone(), created.clone());
            Box::new(move || {
                created.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(Driver {
                    idle: idle.clone(),
                    stopped: stopped.clone(),
                }) as Box<dyn HarnessDriver>)
            })
        };
        let driver: Arc<dyn HarnessDriver> = create().unwrap().into();
        let slot = DriverSlot {
            changed: tokio::sync::watch::channel(0).0,
            current: Mutex::new(Some(driver)),
            touched: Mutex::new(Instant::now()),
            closed: AtomicBool::new(false),
            descriptor: json!({}),
            create,
        };
        let ttl = Duration::from_secs(300);
        let now = Instant::now();
        assert!(slot.idle(now + Duration::from_secs(299), ttl).is_none());
        let held = slot.get().unwrap();
        assert!(slot.idle(now + ttl + Duration::from_secs(1), ttl).is_none());
        drop(held);
        *idle.lock().unwrap() = None;
        assert!(slot.idle(now + ttl + Duration::from_secs(1), ttl).is_none());
        *idle.lock().unwrap() = Some(now);
        slot.idle(now + ttl + Duration::from_secs(1), ttl)
            .unwrap()
            .drain()
            .await;
        assert_eq!(stopped.load(Ordering::SeqCst), 1);
        assert_eq!(created.load(Ordering::SeqCst), 1);
        assert!(slot.get().is_ok());
        assert_eq!(created.load(Ordering::SeqCst), 2);
        slot.stop();
        slot.drain().await;
        assert!(slot.get().is_err());
    }
}
impl DriverSlot {
    pub fn new(
        plugins: HarnessPlugins,
        config: ProfileConfig,
        runtime: Option<Arc<ProjectRuntime>>,
    ) -> Result<Self, String> {
        let create = Box::new(move || plugins.create(config.clone(), runtime.clone()));
        let driver: Arc<dyn HarnessDriver> = create()?.into();
        Ok(Self {
            changed: tokio::sync::watch::channel(0).0,
            descriptor: driver.descriptor(),
            current: Mutex::new(Some(driver)),
            touched: Mutex::new(Instant::now()),
            closed: AtomicBool::new(false),
            create,
        })
    }
    pub fn descriptor(&self) -> Value {
        self.descriptor.clone()
    }
    pub fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub fn peek(&self) -> Option<Arc<dyn HarnessDriver>> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.current.lock().unwrap().clone()
    }
    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub fn get(&self) -> Result<Arc<dyn HarnessDriver>, Error> {
        let mut current = self.current.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::unavailable());
        }
        *self.touched.lock().unwrap() = Instant::now();
        if current.is_none() {
            *current = Some((self.create)().map_err(|_| Error::unavailable())?.into());
            self.changed.send_modify(|version| *version += 1);
        }
        Ok(current.as_ref().unwrap().clone())
    }
    pub fn idle(&self, now: Instant, ttl: Duration) -> Option<Arc<dyn HarnessDriver>> {
        let mut current = self.current.lock().unwrap();
        let driver = current.as_ref()?;
        let idle_since = driver.idle_since()?;
        if now.saturating_duration_since(*self.touched.lock().unwrap()) < ttl
            || now.saturating_duration_since(idle_since) < ttl
            || Arc::strong_count(driver) != 1
        {
            return None;
        }
        let driver = current.take()?;
        self.changed.send_modify(|version| *version += 1);
        driver.stop();
        Some(driver)
    }
    pub fn stop(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.send_modify(|version| *version += 1);
        if let Some(driver) = self.current.lock().unwrap().as_ref() {
            driver.stop();
        }
    }
    pub async fn drain(&self) {
        let driver = self.current.lock().unwrap().take();
        if let Some(driver) = driver {
            driver.drain().await;
        }
    }
    #[cfg(test)]
    pub fn testing(driver: Box<dyn HarnessDriver>) -> Self {
        Self {
            changed: tokio::sync::watch::channel(0).0,
            descriptor: driver.descriptor(),
            current: Mutex::new(Some(driver.into())),
            touched: Mutex::new(Instant::now()),
            closed: AtomicBool::new(false),
            create: Box::new(|| Err("Test driver cannot restart".into())),
        }
    }
}
