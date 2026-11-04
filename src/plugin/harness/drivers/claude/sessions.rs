//! Only a session's own initialization may delay access to that session.
use super::process::Process;
use crate::core::error::Error;
use crate::harness::events::Events;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;

type Slot = Arc<AsyncMutex<Option<Arc<Process>>>>;
#[derive(Default)]
pub(super) struct Sessions(Mutex<BTreeMap<String, Slot>>);
enum Reservation {
    Ready(Slot),
    Retiring(Arc<Process>),
}

impl Sessions {
    pub async fn slot(&self, id: &str, events: &Mutex<Events>) -> Result<Slot, Error> {
        loop {
            match self.reserve(id, events)? {
                Reservation::Ready(slot) => return Ok(slot),
                Reservation::Retiring(process) => {
                    let started = std::time::Instant::now();
                    process.drain().await;
                    let confirmed = process.exited();
                    record_reclaim(id, &process, started.elapsed(), confirmed);
                    if !confirmed {
                        return Err(Error::too_many("CLAUDE_SESSION_CLEANUP_PENDING"));
                    }
                }
            }
        }
    }
    fn reserve(&self, id: &str, events: &Mutex<Events>) -> Result<Reservation, Error> {
        let mut sessions = self.0.lock().unwrap();
        sessions.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || slot.try_lock().map_or(true, |process| {
                    process.as_ref().is_some_and(|p| !p.exited())
                })
        });
        if !sessions.contains_key(id) && sessions.len() >= 16 {
            return retire_idle(&sessions, events);
        }
        Ok(Reservation::Ready(
            sessions.entry(id.into()).or_default().clone(),
        ))
    }
    pub async fn get(&self, id: &str) -> Option<Arc<Process>> {
        let slot = self.0.lock().unwrap().get(id).cloned()?;
        let process = slot.lock().await;
        process.as_ref().filter(|p| p.alive()).cloned()
    }
    pub fn live(&self) -> Vec<Arc<Process>> {
        let slots = self.0.lock().unwrap().values().cloned().collect::<Vec<_>>();
        slots
            .iter()
            .filter_map(|slot| {
                slot.try_lock()
                    .ok()
                    .and_then(|process| process.as_ref().filter(|p| p.alive()).cloned())
            })
            .collect()
    }
    pub async fn drain(&self) {
        let slots = std::mem::take(&mut *self.0.lock().unwrap());
        let mut processes = Vec::new();
        for slot in slots.into_values() {
            if let Some(process) = slot.lock().await.as_ref() {
                process.close();
                processes.push(process.clone());
            }
        }
        futures_util::future::join_all(processes.iter().map(|p| p.drain())).await;
    }
}
fn retire_idle(
    sessions: &BTreeMap<String, Slot>,
    events: &Mutex<Events>,
) -> Result<Reservation, Error> {
    let candidate = sessions
        .iter()
        .filter_map(|(id, slot)| reclaimable(slot, id, events).map(|age| (id.clone(), age)))
        .min_by_key(|(_, age)| *age);
    let (id, _) = candidate.ok_or_else(|| Error::too_many("CLAUDE_SESSION_LIMIT"))?;
    let slot = sessions
        .get(&id)
        .unwrap()
        .try_lock()
        .map_err(|_| Error::too_many("CLAUDE_SESSION_LIMIT"))?;
    let process = slot.as_ref().unwrap().clone();
    process.close();
    // Keep the retiring process charged to capacity until its exit is confirmed.
    Ok(Reservation::Retiring(process))
}
fn record_reclaim(id: &str, process: &Process, elapsed: std::time::Duration, confirmed: bool) {
    use crate::core::events::{emit, Level};
    emit(
        if confirmed { Level::Info } else { Level::Warn },
        "harness.session.capacity_reclaim",
        serde_json::json!({"driver":"claude","operation":"harness.capacity.reclaim","operationId":id,"stage":"drain",
            "sessionId":process.session.lock().unwrap()["id"],"outcome":if confirmed {"success"} else {"pending"},
            "durationMs":elapsed.as_millis(),"code":if confirmed {"IDLE_RECLAIMED"} else {"CLAUDE_SESSION_CLEANUP_PENDING"}}),
    );
}
fn reclaimable(slot: &Slot, id: &str, events: &Mutex<Events>) -> Option<i64> {
    if Arc::strong_count(slot) != 1 {
        return None;
    }
    let guard = slot.try_lock().ok()?;
    let process = guard.as_ref()?;
    if Arc::strong_count(process) != 1 || !process.alive() {
        return None;
    }
    let session = process.session.lock().unwrap();
    if session["status"] != "idle"
        || session["activeTurnId"].is_string()
        || !events.lock().unwrap().thread_idle(id)
    {
        return None;
    }
    Some(session["updatedAt"].as_i64().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn an_initializing_session_does_not_lock_other_slots_or_live_views() {
        let sessions = Sessions::default();
        let events = Mutex::new(Events::new());
        let slow = sessions.slot("slow", &events).await.unwrap();
        let _pending = slow.lock().await;
        let other = sessions.slot("other", &events).await.unwrap();
        assert!(other.try_lock().is_ok());
        assert!(sessions.live().is_empty());
        assert!(sessions.get("missing").await.is_none());
        assert!(Arc::ptr_eq(
            &slow,
            &sessions.slot("slow", &events).await.unwrap()
        ));
    }
    #[tokio::test]
    async fn capacity_counts_initializing_slots_and_reclaims_unused_reservations() {
        let sessions = Sessions::default();
        let events = Mutex::new(Events::new());
        let mut slots = Vec::new();
        for id in 0..16 {
            slots.push(sessions.slot(&id.to_string(), &events).await.unwrap());
        }
        assert_eq!(
            sessions.slot("overflow", &events).await.err().unwrap().code,
            "CLAUDE_SESSION_LIMIT"
        );
        drop(slots);
        assert!(sessions.slot("next", &events).await.is_ok());
    }

    #[tokio::test]
    async fn capacity_preserves_borrowed_slots_processes_and_unconfirmed_cleanup() {
        let sessions = Sessions::default();
        let events = Mutex::new(Events::new());
        let (process, exited) = Process::idle_fixture("idle");
        sessions
            .0
            .lock()
            .unwrap()
            .insert("idle".into(), Arc::new(AsyncMutex::new(Some(process))));
        let mut reservations = Vec::new();
        for id in 0..15 {
            reservations.push(sessions.slot(&id.to_string(), &events).await.unwrap());
        }
        let held_slot = sessions.0.lock().unwrap().get("idle").unwrap().clone();
        assert!(sessions.reserve("new", &events).is_err());
        drop(held_slot);
        let held_process = sessions.get("idle").await.unwrap();
        assert!(sessions.reserve("new", &events).is_err());
        drop(held_process);
        let Reservation::Retiring(retiring) = sessions.reserve("new", &events).unwrap() else {
            panic!("expected idle reclamation")
        };
        assert!(!retiring.alive());
        assert!(!retiring.exited());
        assert!(sessions.reserve("new", &events).is_err());
        exited.send(true).unwrap();
        assert!(matches!(
            sessions.reserve("new", &events).unwrap(),
            Reservation::Ready(_)
        ));
    }
}
