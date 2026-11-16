//! Current turn responsibility outlives the bounded event replay buffer.
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};

const MAX_TERMINAL_HEADS: usize = 1024;

#[derive(Default)]
pub(super) struct RuntimeHeads {
    heads: HashMap<String, Value>,
    order: VecDeque<String>,
}

impl RuntimeHeads {
    pub fn get(&self, thread: &str) -> Value {
        self.heads.get(thread).cloned().unwrap_or_else(|| json!({}))
    }
    pub fn observe(&mut self, message: &Value) {
        let (Some(thread), Some(turn)) = (
            message["params"]["threadId"].as_str(),
            message["params"]["turn"]["id"].as_str(),
        ) else {
            return;
        };
        let method = message["method"].as_str();
        if !matches!(method, Some("turn/started" | "turn/completed")) {
            return;
        }
        let head = self.heads.entry(thread.into()).or_insert_with(|| json!({}));
        if method == Some("turn/started") {
            head["activeTurnId"] = json!(turn);
        } else {
            Self::finish(head, turn, message["params"]["turn"]["status"].as_str());
        }
        self.order.retain(|id| id != thread);
        self.order.push_back(thread.into());
        self.prune();
    }
    fn finish(head: &mut Value, turn: &str, status: Option<&str>) {
        if head["activeTurnId"] == turn {
            head.as_object_mut().unwrap().remove("activeTurnId");
        }
        head["lastTurnId"] = json!(turn);
        head["lastTurnResult"] = match status {
            Some("completed") => json!("completed"),
            Some("failed") => json!("failed"),
            Some("interrupted" | "cancelled") => json!("cancelled"),
            _ => Value::Null,
        };
    }
    fn prune(&mut self) {
        // Never evict an admitted active turn. Only settled heads have a retention limit.
        while self
            .heads
            .values()
            .filter(|head| head["activeTurnId"].is_null())
            .count()
            > MAX_TERMINAL_HEADS
        {
            let Some(index) = self
                .order
                .iter()
                .position(|id| self.heads[id]["activeTurnId"].is_null())
            else {
                break;
            };
            let id = self.order.remove(index).unwrap();
            self.heads.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn late_completion_does_not_clear_a_different_active_turn() {
        let mut heads = RuntimeHeads::default();
        heads.observe(
            &json!({"method":"turn/started","params":{"threadId":"s","turn":{"id":"new"}}}),
        );
        heads.observe(&json!({"method":"turn/completed","params":{"threadId":"s","turn":{"id":"old","status":"completed"}}}));
        assert_eq!(
            heads.get("s"),
            json!({"activeTurnId":"new","lastTurnId":"old","lastTurnResult":"completed"})
        );
    }
    #[test]
    fn bounds_terminal_retention_without_evicting_active_responsibility() {
        let mut heads = RuntimeHeads::default();
        heads.observe(&json!({"method":"turn/started","params":{"threadId":"active","turn":{"id":"running"}}}));
        for id in 0..=MAX_TERMINAL_HEADS {
            heads.observe(&json!({"method":"turn/completed","params":{"threadId":id.to_string(),"turn":{"id":"done","status":"completed"}}}));
        }
        assert_eq!(heads.heads.len(), MAX_TERMINAL_HEADS + 1);
        assert_eq!(heads.get("active")["activeTurnId"], "running");
        assert!(heads.get("0").as_object().unwrap().is_empty());
    }
}
