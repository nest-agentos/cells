//! Independent native turns retain one file-exclusion permit each.
use crate::core::{
    error::Error,
    gate::{ActivityPermit, FileGate},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

const LIMIT: usize = 128;
struct Active {
    operation: String,
    turn: Option<String>,
    completed: HashSet<String>,
    _permit: ActivityPermit,
}
#[derive(Default)]
pub(super) struct Activities(Mutex<HashMap<String, Active>>);
impl Activities {
    pub fn begin(&self, gate: &FileGate, session: &str, operation: &str) -> Result<(), Error> {
        let mut active = self.0.lock().map_err(|_| Error::internal())?;
        if active.contains_key(session) {
            return Err(Error::busy());
        }
        if active.len() >= LIMIT {
            return Err(Error::too_many("HARNESS_ACTIVE_LIMIT"));
        }
        active.insert(
            session.into(),
            Active {
                operation: operation.into(),
                turn: None,
                completed: HashSet::new(),
                _permit: gate.activity()?,
            },
        );
        Ok(())
    }
    pub fn bind(&self, session: &str, operation: &str, turn: &str) -> Option<ActivityPermit> {
        let mut active = self.0.lock().unwrap();
        let Some(current) = active.get_mut(session).filter(|a| a.operation == operation) else {
            return None;
        };
        current.turn = Some(turn.into());
        let finished = current.completed.contains(turn);
        current.completed.clear();
        if finished {
            return active.remove(session).map(|a| a._permit);
        }
        None
    }
    pub fn completed(&self, gate: &FileGate, session: &str, turn: &str) -> Option<ActivityPermit> {
        let mut active = self.0.lock().unwrap();
        let Some(current) = active.get_mut(session) else {
            return None;
        };
        match current.turn.as_deref() {
            Some(id) if id == turn => active.remove(session).map(|a| a._permit),
            Some(_) => None,
            None => {
                if current.completed.len() < LIMIT || current.completed.contains(turn) {
                    current.completed.insert(turn.into());
                } else {
                    gate.poison();
                }
                None
            }
        }
    }
    pub fn reject(&self, session: &str, operation: &str) -> Option<ActivityPermit> {
        let mut active = self.0.lock().unwrap();
        if !active
            .get(session)
            .is_some_and(|a| a.operation == operation)
        {
            return None;
        }
        active.remove(session).map(|a| a._permit)
    }
    pub fn stopped(&self, session: Option<&str>, clean: bool) -> (bool, Vec<ActivityPermit>) {
        let mut active = self.0.lock().unwrap();
        let affected = session.map_or(!active.is_empty(), |id| active.contains_key(id));
        let retired = if clean {
            if let Some(id) = session {
                active.remove(id).into_iter().map(|a| a._permit).collect()
            } else {
                active.drain().map(|(_, a)| a._permit).collect()
            }
        } else {
            Vec::new()
        };
        (affected, retired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_early_completion_remains_idempotent_at_the_buffer_limit() {
        let gate = FileGate::new();
        let active = Activities::default();
        active.begin(&gate, "session", "request").unwrap();
        for index in 0..LIMIT {
            drop(active.completed(&gate, "session", &format!("turn-{index}")));
        }
        drop(active.completed(&gate, "session", "turn-0"));
        assert!(!gate.is_poisoned());
        drop(active.bind("session", "request", "turn-0"));
        assert!(gate.shared().is_ok());
    }
    #[test]
    fn every_retirement_path_retains_exclusion_until_revision_invalidation_can_finish() {
        for path in ["bind", "completed", "reject", "stopped"] {
            let gate = FileGate::new();
            let active = Activities::default();
            active.begin(&gate, "session", "request").unwrap();
            let retired = match path {
                "bind" => {
                    drop(active.completed(&gate, "session", "turn"));
                    active
                        .bind("session", "request", "turn")
                        .into_iter()
                        .collect::<Vec<_>>()
                }
                "completed" => {
                    drop(active.bind("session", "request", "turn"));
                    active
                        .completed(&gate, "session", "turn")
                        .into_iter()
                        .collect()
                }
                "reject" => active.reject("session", "request").into_iter().collect(),
                _ => active.stopped(None, true).1,
            };
            assert_eq!(retired.len(), 1);
            assert!(gate.shared().is_err());
            assert!(gate.exclusive().is_err());
            drop(retired);
            assert!(gate.shared().is_ok());
        }
    }
    #[test]
    fn completing_one_turn_never_releases_another_session_or_its_successor() {
        let gate = FileGate::new();
        let active = Activities::default();
        active.begin(&gate, "a", "op-a").unwrap();
        drop(active.bind("a", "op-a", "turn-a"));
        active.begin(&gate, "b", "op-b").unwrap();
        drop(active.bind("b", "op-b", "turn-b"));
        assert_eq!(
            active.begin(&gate, "a", "duplicate").unwrap_err().code,
            "EBUSY"
        );
        assert!(active.completed(&gate, "a", "foreign").is_none());
        assert!(active.completed(&gate, "a", "turn-a").is_some());
        assert!(gate.shared().is_err());
        active.begin(&gate, "a", "next").unwrap();
        drop(active.bind("a", "next", "turn-next"));
        assert!(active.completed(&gate, "a", "turn-a").is_none());
        assert!(active.reject("a", "op-a").is_none());
        assert!(active.completed(&gate, "b", "turn-b").is_some());
        assert!(gate.shared().is_err());
        assert!(active.completed(&gate, "a", "turn-next").is_some());
        assert!(gate.shared().is_ok());
    }
    #[test]
    fn terminal_events_before_a_start_reply_are_bound_to_the_exact_returned_turn() {
        let gate = FileGate::new();
        let active = Activities::default();
        active.begin(&gate, "a", "operation").unwrap();
        drop(active.completed(&gate, "a", "stale"));
        drop(active.completed(&gate, "a", "actual"));
        assert!(gate.shared().is_err());
        assert!(active.bind("a", "old-operation", "actual").is_none());
        assert!(active.bind("a", "operation", "actual").is_some());
        assert!(gate.shared().is_ok());
        active.begin(&gate, "a", "second").unwrap();
        drop(active.completed(&gate, "a", "actual"));
        assert!(active.bind("a", "second", "new-turn").is_none());
        assert!(gate.shared().is_err());
        drop(active.completed(&gate, "a", "new-turn"));
        assert!(gate.shared().is_ok());
    }
    #[test]
    fn a_reaped_session_only_releases_its_own_permit_and_unknown_ownership_stays_held() {
        let gate = FileGate::new();
        let active = Activities::default();
        active.begin(&gate, "a", "op-a").unwrap();
        active.begin(&gate, "b", "op-b").unwrap();
        assert!(!active.stopped(Some("foreign"), true).0);
        assert!(active.stopped(Some("a"), true).0);
        assert!(gate.shared().is_err());
        assert!(active.stopped(Some("b"), false).0);
        assert!(gate.shared().is_err());
        assert!(active.stopped(None, true).0);
        assert!(gate.shared().is_ok());
    }
}
