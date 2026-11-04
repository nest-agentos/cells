//! Reclaim completed follow subscriptions without stopping or archiving native sessions.
use super::*;
use crate::core::events::{emit, Level};
use std::time::{Duration, Instant};
type Observer = (String, Arc<Live>, tokio::task::JoinHandle<()>);

impl Dsh {
    pub(super) async fn reserve(
        &self,
        operation: &str,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Error> {
        if let Ok(slot) = self.slots.clone().try_acquire_owned() {
            return Ok(slot);
        }
        let started = Instant::now();
        let Some(observer) = self.retire_observer() else {
            self.reclaim_diagnostic(operation, None, started, "DSH_SESSION_LIMIT");
            return Err(Error::too_many("DSH_SESSION_LIMIT"));
        };
        self.release_observer(observer, operation, started).await?;
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::too_many("DSH_SESSION_LIMIT"))
    }

    async fn release_observer(
        &self,
        observer: Observer,
        operation: &str,
        started: Instant,
    ) -> Result<(), Error> {
        let (id, live, mut task) = observer;
        let confirmed = self.drain_observer(&mut task).await;
        drop(live);
        let code = if confirmed {
            "DSH_OBSERVER_RECLAIMED"
        } else {
            "DSH_SESSION_CLEANUP_PENDING"
        };
        self.reclaim_diagnostic(operation, Some(&id), started, code);
        if !confirmed {
            return Err(Error::too_many("DSH_SESSION_CLEANUP_PENDING"));
        }
        Ok(())
    }

    fn retire_observer(&self) -> Option<Observer> {
        let mut sessions = self.sessions.lock().unwrap();
        let (id, task) = sessions
            .iter()
            .find_map(|(id, live)| retire(live, id, &self.events).map(|task| (id.clone(), task)))?;
        Some((id.clone(), sessions.remove(&id)?, task))
    }

    async fn drain_observer(&self, task: &mut tokio::task::JoinHandle<()>) -> bool {
        let config = self.config.dsh.clone().unwrap_or_default();
        let timeout = Duration::from_millis(config.request_timeout_ms);
        matches!(tokio::time::timeout(timeout, task).await, Ok(Ok(())))
    }

    fn reclaim_diagnostic(&self, operation: &str, id: Option<&str>, started: Instant, code: &str) {
        let confirmed = code == "DSH_OBSERVER_RECLAIMED";
        emit(
            if confirmed { Level::Info } else { Level::Warn },
            "harness.session.capacity_reclaim",
            json!({"driver":"dsh","profileId":self.config.id,"operation":"harness.capacity.reclaim",
                "operationId":operation,"sessionId":id.filter(|id| crate::core::ids::is_identifier_within(id, 128)),"stage":"observer_drain",
                "outcome":if confirmed {"success"} else {"blocked"},"code":code,
                "durationMs":started.elapsed().as_millis(),"availableSlots":self.slots.available_permits(),
                "capacity":self.capacity_metadata()}),
        );
    }

    fn capacity_metadata(&self) -> Value {
        let sessions = self.sessions.lock().unwrap();
        let mut active = 0;
        let mut commands = 0;
        let mut borrowed = 0;
        let mut controls = 0;
        for live in sessions.values() {
            let cursor = live.cursor.lock().unwrap();
            active += usize::from(!cursor.turn.is_empty());
            commands +=
                usize::from(cursor.command_pending.is_some() || !cursor.commands.is_empty());
            borrowed += usize::from(Arc::strong_count(live) > 2);
            controls += usize::from(cursor.control_pending.is_some());
        }
        json!({"ownedObservers":sessions.len(),"activeTurns":active,"pendingCommands":commands,
            "borrowedObservers":borrowed,"pendingControls":controls,"pendingInteractions":self.events.lock().unwrap().requests.len()})
    }
}

fn retire(
    live: &Arc<Live>,
    id: &str,
    events: &Mutex<Events>,
) -> Option<tokio::task::JoinHandle<()>> {
    // The map and follow task are the only allowed owners. Any mutation or
    // incoming interaction retains a third reference until it settles.
    if Arc::strong_count(live) != 2 || live.stop.is_cancelled() {
        return None;
    }
    let cursor = live.cursor.lock().unwrap();
    let mut session = live.session.lock().unwrap();
    if cursor.busy()
        || session["status"] != "idle"
        || session["activeTurnId"].is_string()
        || !session["lastTurnId"].is_string()
        || !events.lock().unwrap().thread_idle(id)
    {
        return None;
    }
    let task = live.follow.lock().unwrap().take()?;
    session["owned"] = json!(false);
    live.stop.cancel();
    Some(task)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn driver() -> Dsh {
        Dsh::new(
            serde_json::from_value(json!({"id":"dsh","kind":"dsh","home":"/unused",
            "dsh":{"max_sessions":1,"request_timeout_ms":100}}))
            .unwrap(),
            None,
        )
    }
    fn observed(driver: &Dsh, gate: Option<Arc<tokio::sync::Notify>>) -> Arc<Live> {
        let live = Arc::new(Live {
            cursor: Mutex::new(protocol::Cursor::default()),
            session: Mutex::new(json!({"id":"s","status":"idle","lastTurnId":"finished"})),
            stop: CancellationToken::new(),
            _slot: driver.slots.clone().try_acquire_owned().unwrap(),
            follow: Mutex::new(None),
        });
        let retained = live.clone();
        *live.follow.lock().unwrap() = Some(tokio::spawn(async move {
            retained.stop.cancelled().await;
            if let Some(gate) = gate {
                gate.notified().await;
            }
            drop(retained);
        }));
        driver
            .sessions
            .lock()
            .unwrap()
            .insert("s".into(), live.clone());
        live
    }
    #[tokio::test]
    async fn capacity_protects_borrowers_unknown_turns_commands_and_interactions() {
        let driver = driver();
        let borrowed = observed(&driver, None);
        assert_eq!(
            driver.reserve("borrowed").await.err().unwrap().code,
            "DSH_SESSION_LIMIT"
        );
        drop(borrowed);
        {
            let sessions = driver.sessions.lock().unwrap();
            let live = sessions.get("s").unwrap();
            let mut cursor = live.cursor.lock().unwrap();
            cursor.turn = "unconfirmed".into();
        }
        assert_eq!(
            driver.reserve("unknown").await.err().unwrap().code,
            "DSH_SESSION_LIMIT"
        );
        driver.sessions.lock().unwrap()["s"]
            .cursor
            .lock()
            .unwrap()
            .turn
            .clear();
        driver.sessions.lock().unwrap()["s"]
            .cursor
            .lock()
            .unwrap()
            .command_pending = Some(super::command_admission::CommandAdmission::new(
            "unknown-command",
        ));
        assert!(driver.idle_since().is_none());
        assert_eq!(
            driver.reserve("command").await.err().unwrap().code,
            "DSH_SESSION_LIMIT"
        );
        driver.sessions.lock().unwrap()["s"]
            .cursor
            .lock()
            .unwrap()
            .command_pending = None;
        driver
            .events
            .lock()
            .unwrap()
            .push(json!({"id":"approval","method":"request","params":{"threadId":"s"}}))
            .unwrap();
        assert_eq!(
            driver.reserve("approval").await.err().unwrap().code,
            "DSH_SESSION_LIMIT"
        );
        driver
            .events
            .lock()
            .unwrap()
            .push(json!({"method":"serverRequest/resolved","params":{"requestId":"approval"}}))
            .unwrap();
        assert!(driver.reserve("settled").await.is_ok());
    }
    #[tokio::test]
    async fn unconfirmed_observer_shutdown_keeps_capacity_charged() {
        let driver = driver();
        let gate = Arc::new(tokio::sync::Notify::new());
        drop(observed(&driver, Some(gate.clone())));
        assert_eq!(
            driver.reserve("retire").await.err().unwrap().code,
            "DSH_SESSION_CLEANUP_PENDING"
        );
        assert_eq!(driver.slots.available_permits(), 0);
        gate.notify_one();
        for _ in 0..20 {
            if driver.slots.available_permits() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(driver.slots.available_permits(), 1);
        assert!(driver.reserve("next").await.is_ok());
    }
}
