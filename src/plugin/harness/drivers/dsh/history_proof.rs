//! Derive prompt provenance only from a complete, ordered native turn boundary.
use super::protocol;
use serde_json::{json, Value};
use std::collections::BTreeMap;

struct Turn {
    number: u64,
    start: u64,
    input: Option<(String, String)>,
    inputs: usize,
}
impl Turn {
    fn observe(&mut self, event: &Value) {
        if event["data"]["source"]["kind"] != "user" {
            return;
        }
        self.inputs += 1;
        self.input = prompt(event);
    }
    fn terminal(self, event: &Value) -> Value {
        let mut fact = json!({"status": protocol::terminal_result(&event["data"]["reason"])});
        if let Some((id, digest)) = self.input.filter(|_| self.inputs == 1) {
            fact["submissionEvidence"] = json!({"version":1,"requestId":id,"promptSha256":digest,
                "startSequence":self.start,"endSequence":event["seq"]});
        }
        fact
    }
}

fn ordered(records: &[Value]) -> bool {
    let mut previous: Option<u64> = None;
    for row in records {
        let Some(seq) = row["event"]["seq"].as_u64() else {
            return false;
        };
        if previous.is_some_and(|prior| prior.checked_add(1) != Some(seq)) {
            return false;
        }
        previous = Some(seq);
    }
    true
}
fn prompt(event: &Value) -> Option<(String, String)> {
    if !protocol::surface_appends(&event["surfaceOp"]) {
        return None;
    }
    let data = &event["data"];
    let id = data["source"]["rpcId"]
        .as_str()
        .filter(|id| crate::core::ids::is_identifier_within(id, 128))?;
    let digest = super::admission::digest(&data["content"])?;
    Some((
        id.into(),
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    ))
}
fn advance(current: &mut Option<Turn>, event: &Value) -> Option<Turn> {
    match event["type"].as_str() {
        Some("turn/start") => {
            *current = event["data"]["turn"].as_u64().map(|number| Turn {
                number,
                start: event["seq"].as_u64().unwrap(),
                input: None,
                inputs: 0,
            })
        }
        Some("user/message") => {
            if let Some(turn) = current.as_mut() {
                turn.observe(event);
            }
        }
        Some("turn/end") => {
            return current
                .take()
                .filter(|turn| event["data"]["turn"].as_u64() == Some(turn.number))
        }
        _ => {}
    }
    None
}
fn facts(records: &[Value], session: &str) -> BTreeMap<String, Value> {
    let mut result = BTreeMap::new();
    if !ordered(records) {
        return result;
    }
    let mut current = None;
    for row in records {
        if let Some(turn) = advance(&mut current, &row["event"]) {
            let id = format!("{session}-{}", turn.number);
            if result.contains_key(&id) {
                return BTreeMap::new();
            }
            result.insert(id, turn.terminal(&row["event"]));
        }
    }
    result
}

/** Facts are a projection of the same bounded history page, never a second status store. */
pub(super) fn annotate(mut turns: Vec<Value>, records: &[Value], session: &str) -> Vec<Value> {
    let facts = facts(records, session);
    for turn in &mut turns {
        if let Some(fact) = turn["id"].as_str().and_then(|id| facts.get(id)) {
            turn["status"] = fact["status"].clone();
            if let Some(evidence) = fact.get("submissionEvidence") {
                turn["submissionEvidence"] = evidence.clone();
            }
        }
    }
    turns
}

#[cfg(test)]
mod tests {
    use super::*;
    fn records() -> Vec<Value> {
        [json!({"type":"turn/start","data":{"turn":1}}),
            json!({"type":"user/message","data":{"source":{"kind":"user","rpcId":"original"},"content":[{"type":"text","text":"frozen"}]}}),
            json!({"type":"turn/end","data":{"turn":1,"reason":{"kind":"completed"}}})]
            .into_iter().enumerate().map(|(seq, mut event)| {event["seq"] = json!(seq); json!({"event":event})}).collect()
    }
    #[test]
    fn only_complete_native_boundaries_provide_completion_evidence() {
        let source = records();
        let value = facts(&source, "s");
        assert_eq!(value["s-1"]["submissionEvidence"]["requestId"], "original");
        assert!(facts(&source[..2], "s").is_empty());
        assert!(facts(&source[1..], "s").is_empty());
        let mut gap = source.clone();
        gap[1]["event"]["seq"] = json!(9);
        assert!(facts(&gap, "s").is_empty());
        let mut foreign_end = source.clone();
        foreign_end[2]["event"]["data"]["turn"] = json!(2);
        assert!(facts(&foreign_end, "s").is_empty());
    }
    #[test]
    fn replacement_or_multiple_user_inputs_do_not_prove_a_single_submission() {
        let mut replaced = records();
        replaced[1]["event"]["surfaceOp"] = json!({"op":"replace"});
        assert!(facts(&replaced, "s")["s-1"]
            .get("submissionEvidence")
            .is_none());
        let mut multiple = records();
        multiple.insert(2, multiple[1].clone());
        for (seq, row) in multiple.iter_mut().enumerate() {
            row["event"]["seq"] = json!(seq);
        }
        assert!(facts(&multiple, "s")["s-1"]
            .get("submissionEvidence")
            .is_none());
    }
}
