//! One parsed-history budget shared by all matching native sessions.
use super::history::{self, Entry};
use crate::{
    core::{error::Error, search::KeywordQuery},
    harness::config::ProfileConfig,
};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
struct Budget {
    matches: Vec<Value>,
    bytes: usize,
    started: Instant,
    truncated: bool,
}
impl Budget {
    fn full(&self) -> bool {
        self.matches.len() >= 100
            || self.bytes >= 16 * 1024 * 1024
            || self.started.elapsed() >= Duration::from_secs(15)
    }
    fn record(&mut self, session: &Value, item: &Value, query: &KeywordQuery, prompt: bool) {
        let text = super::super::search::search_text(item, prompt);
        if !self.full() && query.matches(&text) {
            self.matches.push(json!({"sessionId":session["id"],"title":session["title"],"updatedAt":session["updatedAt"],"turnId":item["turnId"],"itemId":item["id"],"summary":text.chars().take(1000).collect::<String>()}));
        }
    }
}
pub fn collect(
    config: &ProfileConfig,
    runtime: Option<&crate::projects::runtime::ProjectRuntime>,
    entries: Vec<Entry>,
    query: &str,
    mode: &str,
) -> Result<Value, Error> {
    let mut budget = Budget {
        matches: Vec::new(),
        bytes: 0,
        started: Instant::now(),
        truncated: false,
    };
    let query = KeywordQuery::new(query);
    for entry in entries {
        if budget.full() {
            budget.truncated = true;
            break;
        }
        if mode != "title" {
            history(
                config,
                runtime,
                &entry,
                &query,
                mode == "prompt",
                &mut budget,
            )?;
        } else if entry.session["title"]
            .as_str()
            .is_some_and(|title| query.matches(title))
        {
            budget.matches.push(json!({"sessionId":entry.session["id"],"title":entry.session["title"],"updatedAt":entry.session["updatedAt"],"summary":entry.session["title"]}));
        }
    }
    Ok(
        json!({"matches":budget.matches,"truncated":budget.truncated || budget.full(),"nextCursor":null}),
    )
}
fn history(
    config: &ProfileConfig,
    runtime: Option<&crate::projects::runtime::ProjectRuntime>,
    entry: &Entry,
    query: &KeywordQuery,
    prompt: bool,
    budget: &mut Budget,
) -> Result<(), Error> {
    let mut cursor = Value::Null;
    for _ in 0..64 {
        if budget.full() {
            budget.truncated = true;
            return Ok(());
        }
        let page = history::read(config, runtime, entry, &json!({"cursor":cursor}))?;
        budget.bytes += page.to_string().len();
        for turn in page["turns"].as_array().unwrap() {
            for item in turn["items"].as_array().unwrap() {
                budget.record(&entry.session, item, query, prompt);
            }
        }
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            return Ok(());
        }
    }
    budget.truncated = true;
    Ok(())
}
