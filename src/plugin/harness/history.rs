//! Read-only fallback for native history APIs that are not implemented by a CLI version.
use crate::{core::error::Error, fs::Export};
use serde_json::{json, Value};
use std::{
    io::{Read, Seek, SeekFrom},
    path::Path,
};

/** File identity and change metadata only; never hash or expose transcript content. */
pub async fn revision(home: &Path, path: &str) -> Result<String, Error> {
    let home = home.to_owned();
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        use sha2::Digest;
        use std::os::unix::fs::MetadataExt;
        let stat = open_log(&home, &path)?.metadata()?;
        let identity = format!(
            "{}:{}:{}:{}:{}:{}:{}",
            stat.dev(),
            stat.ino(),
            stat.len(),
            stat.mtime(),
            stat.mtime_nsec(),
            stat.ctime(),
            stat.ctime_nsec()
        );
        Ok(format!("{:x}", sha2::Sha256::digest(identity.as_bytes())))
    })
    .await
    .map_err(|_| Error::internal())?
}

pub async fn read(
    home: &Path,
    path: &str,
    before: Option<u64>,
    summary: bool,
) -> Result<Value, Error> {
    let home = home.to_owned();
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || read_log(&home, &path, before, summary))
        .await
        .map_err(|_| Error::internal())?
}

fn relative_log<'a>(home: &Path, path: &'a str) -> Result<&'a str, Error> {
    let relative = Path::new(path)
        .strip_prefix(home)
        .map_err(|_| Error::forbidden("EACCES"))?;
    let relative = relative.to_str().ok_or_else(Error::invalid)?;
    if !(relative.starts_with("sessions/") || relative.starts_with("archived_sessions/"))
        || !relative.ends_with(".jsonl")
    {
        return Err(Error::forbidden("EACCES"));
    }
    Ok(relative)
}
pub(super) fn open_log(home: &Path, path: &str) -> Result<std::fs::File, Error> {
    let relative = relative_log(home, path)?;
    // Directory capabilities reject symlinks and traversal even if native metadata is stale.
    let export = Export::open(home)?;
    let file = export.open_dir("")?;
    let file = rustix::fs::openat2(
        &file,
        relative,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
        rustix::fs::ResolveFlags::BENEATH
            | rustix::fs::ResolveFlags::NO_SYMLINKS
            | rustix::fs::ResolveFlags::NO_MAGICLINKS,
    )?;
    Ok(std::fs::File::from(file))
}
fn read_log(home: &Path, path: &str, before: Option<u64>, summary: bool) -> Result<Value, Error> {
    let mut file = open_log(home, path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(Error::invalid());
    }
    let end = before.unwrap_or(metadata.len());
    if end > metadata.len() {
        return Err(Error::invalid());
    }
    let start = end.saturating_sub(8 * 1024 * 1024);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    (&mut file).take(end - start).read_to_end(&mut bytes)?;
    let first = complete_lines(&bytes, start)?
        .first()
        .map(|(at, _)| *at)
        .unwrap_or(start);
    let prior = prior_turn(&mut file, first)?;
    page(&bytes, start, prior, summary)
}
fn context(record: &Value) -> Option<String> {
    if record["type"] != "turn_context"
        && !(record["type"] == "event_msg" && record["payload"]["type"] == "task_started")
    {
        return None;
    }
    record["payload"]["turn_id"].as_str().map(str::to_owned)
}
fn prior_turn(file: &mut std::fs::File, mut end: u64) -> Result<Option<String>, Error> {
    // Search backwards using bounded buffers; large tool output does not allocate a large record.
    while end > 0 {
        let start = end.saturating_sub(256 * 1024);
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = Vec::new();
        (&mut *file).take(end - start).read_to_end(&mut bytes)?;
        let Ok(lines) = complete_lines(&bytes, start) else {
            return Ok(None);
        };
        let first = lines.first().map(|(at, _)| *at).unwrap_or(start);
        for (_, line) in lines.into_iter().rev() {
            let Ok(record) = serde_json::from_slice::<Value>(line) else {
                return Ok(None);
            };
            if !record.is_object() {
                return Ok(None);
            }
            if let Some(id) = context(&record) {
                return Ok(Some(id));
            }
        }
        if start == 0 {
            break;
        }
        end = first.min(end - 1);
    }
    Ok(None)
}
fn complete_lines(bytes: &[u8], start: u64) -> Result<Vec<(u64, &[u8])>, Error> {
    let skip = if start == 0 {
        0
    } else {
        bytes
            .iter()
            .position(|b| *b == b'\n')
            .map(|i| i + 1)
            .ok_or_else(|| Error::too_large("HARNESS_HISTORY_RECORD_TOO_LARGE"))?
    };
    let mut offset = start + skip as u64;
    Ok(bytes[skip..]
        .split_inclusive(|b| *b == b'\n')
        .map(|line| {
            let at = offset;
            offset += line.len() as u64;
            (at, line)
        })
        .filter(|(_, line)| line.ends_with(b"\n"))
        .collect())
}
struct Page {
    items: Vec<Value>,
    terminals: Vec<Value>,
    budget: usize,
    next: u64,
    recovery: Recovery,
}
#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Recovery {
    skipped_records: u64,
    skipped_bytes: u64,
    nul_records: u64,
    invalid_json_records: u64,
    invalid_record_records: u64,
    first_offset: Option<u64>,
    last_offset: Option<u64>,
}
impl Recovery {
    fn push(&mut self, at: u64, bytes: u64, kind: &str) {
        self.skipped_records += 1;
        self.skipped_bytes += bytes;
        match kind {
            "nul" => self.nul_records += 1,
            "json" => self.invalid_json_records += 1,
            _ => self.invalid_record_records += 1,
        }
        self.first_offset = Some(at);
        self.last_offset.get_or_insert(at);
    }
}
enum Entry {
    Item(Value),
    Terminal(Value),
    Corrupt { bytes: u64, kind: &'static str },
}
impl Page {
    fn push(&mut self, at: u64, entry: Option<Entry>) -> Result<bool, Error> {
        match entry {
            Some(Entry::Corrupt { bytes, kind }) => self.recovery.push(at, bytes, kind),
            Some(Entry::Item(value)) => {
                if !self.push_value(value, false)? {
                    return Ok(false);
                }
            }
            Some(Entry::Terminal(value)) => {
                if !self.push_value(value, true)? {
                    return Ok(false);
                }
            }
            None => (),
        }
        self.next = at;
        Ok(true)
    }
    fn push_value(&mut self, value: Value, terminal: bool) -> Result<bool, Error> {
        let size = value.to_string().len();
        if size > 2 * 1024 * 1024 {
            return Err(Error::too_large("HARNESS_HISTORY_ITEM_TOO_LARGE"));
        }
        if size > self.budget || self.items.len() + self.terminals.len() >= 100 {
            return Ok(false);
        }
        self.budget -= size;
        if terminal {
            self.terminals.push(value);
        } else {
            self.items.push(value);
        }
        Ok(true)
    }
}
fn parse_record(line: &[u8]) -> Result<Value, Entry> {
    match serde_json::from_slice::<Value>(line) {
        Ok(record) if record.is_object() => Ok(record),
        result => {
            let kind = if line.contains(&0) {
                "nul"
            } else if result.is_err() {
                "json"
            } else {
                "record"
            };
            Err(Entry::Corrupt {
                bytes: line.len() as u64,
                kind,
            })
        }
    }
}
fn entries(
    lines: Vec<(u64, &[u8])>,
    mut turn: Option<String>,
    summary: bool,
) -> Vec<(u64, Option<Entry>)> {
    let mut entries = Vec::with_capacity(lines.len());
    for (at, line) in lines {
        let record = match parse_record(line) {
            Ok(record) => record,
            Err(corrupt) => {
                entries.push((at, Some(corrupt)));
                // Lost context must not associate later messages with an earlier turn.
                turn = None;
                continue;
            }
        };
        if let Some(id) = context(&record) {
            turn = Some(id);
        }
        entries.push((at, project_entry(&record, at, turn.as_deref(), summary)));
    }
    entries
}
fn project_entry(record: &Value, at: u64, turn: Option<&str>, summary: bool) -> Option<Entry> {
    let item = record_item(record, at, turn).and_then(|entry| {
        if summary {
            super::presentation::item(&entry)
        } else {
            Some(entry)
        }
    });
    terminal(record)
        .map(Entry::Terminal)
        .or_else(|| item.map(Entry::Item))
}
/** Only an explicit native lifecycle record with its own turn identity proves termination. */
fn terminal(record: &Value) -> Option<Value> {
    if record["type"] != "event_msg" {
        return None;
    }
    let payload = &record["payload"];
    let status = match payload["type"].as_str()? {
        "task_complete" => "completed",
        "turn_aborted" => "interrupted",
        _ => return None,
    };
    let id = payload["turn_id"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 1024)?;
    Some(json!({"id":id,"status":status,"items":[]}))
}
fn record_item(record: &Value, at: u64, turn: Option<&str>) -> Option<Value> {
    let mut entry = item(record)?;
    entry["id"] = record["payload"]["id"]
        .as_str()
        .map(|id| json!(id))
        .unwrap_or_else(|| json!(format!("rollout:{at}")));
    entry["timestamp"] = super::presentation::time_ms(&record["timestamp"]);
    if entry["type"] == "userMessage" {
        entry["submittedAt"] = entry["timestamp"].clone();
    }
    if entry["type"] == "agentMessage" || entry["type"] == "reasoning" {
        entry["completedAt"] = entry["timestamp"].clone();
        entry["status"] = json!("completed");
    }
    entry["turnId"] = json!(record["payload"]["turn_id"]
        .as_str()
        .or(record["payload"]["internal_chat_message_metadata_passthrough"]["turn_id"].as_str())
        .or(turn));
    Some(entry)
}
fn page(bytes: &[u8], start: u64, turn: Option<String>, summary: bool) -> Result<Value, Error> {
    let entries = entries(complete_lines(bytes, start)?, turn, summary);
    let mut page = Page {
        items: Vec::new(),
        terminals: Vec::new(),
        budget: 2 * 1024 * 1024,
        next: entries.first().map(|(at, _)| *at).unwrap_or(start),
        recovery: Recovery::default(),
    };
    for (at, entry) in entries.into_iter().rev() {
        if !page.push(at, entry)? {
            break;
        }
    }
    page.items.reverse();
    page.terminals.reverse();
    let mut turns = vec![json!({"items":page.items})];
    turns.extend(page.terminals);
    let mut result = json!({"turns":turns,"nextCursor":if page.next > 0 {Some(page.next.to_string())} else {None}});
    if page.recovery.skipped_records > 0 {
        result["recovery"] = serde_json::to_value(&page.recovery).map_err(|_| Error::internal())?;
    }
    Ok(result)
}

fn item(record: &Value) -> Option<Value> {
    if record["type"] != "response_item" {
        return None;
    }
    let payload = &record["payload"];
    match payload["type"].as_str()? {
        "message" => {
            let role = payload["role"].as_str()?;
            if !matches!(role, "user" | "assistant") {
                return None;
            }
            Some(message_item(payload, role))
        }
        "function_call" | "custom_tool_call" => Some(
            json!({"type":"toolCall","name":payload["name"],"arguments":payload["arguments"],"input":payload["input"]}),
        ),
        "function_call_output" | "custom_tool_call_output" => {
            Some(json!({"type":"toolOutput","text":payload["output"]}))
        }
        "reasoning" => Some(json!({"type":"reasoning","summary":payload["summary"]})),
        _ => None,
    }
}

fn message_item(payload: &Value, role: &str) -> Value {
    let mut item = json!({"type":if role=="user" {"userMessage"} else {"agentMessage"},"content":payload["content"],"phase":payload["phase"]});
    let kinds = &payload["internal_chat_message_metadata_passthrough"]["content_item_kinds"];
    if role == "user"
        && kinds.as_array().is_some_and(|kinds| {
            payload["content"].as_array().is_some_and(|content| {
                kinds.len() == content.len() && kinds.iter().all(Value::is_string)
            })
        })
    {
        item["contentItemKinds"] = kinds.clone();
    }
    item
}

#[cfg(test)]
mod tests {
    use super::*;
    fn records(records: &[Value]) -> String {
        records.iter().map(|record| format!("{record}\n")).collect()
    }
    #[test]
    fn damaged_records_preserve_valid_history_and_reset_turn_context() {
        let head = records(&[
            json!({"type":"turn_context","payload":{"turn_id":"old"}}),
            json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"text":"before"}]}}),
        ]);
        let tail = records(&[
            json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"text":"after"}]}}),
            json!({"type":"turn_context","payload":{"turn_id":"new"}}),
            json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"text":"next"}]}}),
            json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"explicit"}}),
        ]);
        let bytes = format!("{head}{}\n{{broken\n[]\n{tail}", "\0".repeat(286));
        let result = page(bytes.as_bytes(), 0, None, true).unwrap();
        let items = result["turns"][0]["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["turnId"], "old");
        assert!(items[1]["turnId"].is_null());
        assert_eq!(items[2]["turnId"], "new");
        assert_eq!(result["turns"][1]["id"], "explicit");
        assert_eq!(result["recovery"]["skippedRecords"], 3);
        assert_eq!(result["recovery"]["nulRecords"], 1);
        assert_eq!(result["recovery"]["invalidJsonRecords"], 1);
        assert_eq!(result["recovery"]["invalidRecordRecords"], 1);
        assert_eq!(result["recovery"]["firstOffset"], head.len());
        assert_eq!(result["recovery"]["skippedBytes"], 298);
        assert!(result["nextCursor"].is_null());
    }
    #[test]
    fn recovery_pages_advance_without_modifying_the_log() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join("sessions")).unwrap();
        let path = home.path().join("sessions/recovery.jsonl");
        let mut original = String::new();
        for i in 0..205 {
            if i == 50 || i == 150 {
                original.push_str("\0\0\n");
            }
            original.push_str(&records(&[json!({"type":"response_item","payload":{
                "type":"message","id":format!("item-{i}"),"role":"assistant","content":[{"text":"valid"}]}})]));
        }
        original.push_str("{unfinished");
        std::fs::write(&path, &original).unwrap();
        let mut before = None;
        let mut ids = std::collections::HashSet::new();
        let mut skipped = 0;
        loop {
            let result = read_log(home.path(), path.to_str().unwrap(), before, true).unwrap();
            for item in result["turns"][0]["items"].as_array().unwrap() {
                assert!(ids.insert(item["id"].as_str().unwrap().to_owned()));
            }
            skipped += result["recovery"]["skippedRecords"].as_u64().unwrap_or(0);
            let Some(cursor) = result["nextCursor"].as_str() else {
                break;
            };
            let next = cursor.parse::<u64>().unwrap();
            assert!(before.is_none_or(|previous| next < previous));
            before = Some(next);
        }
        assert_eq!(ids.len(), 205);
        assert_eq!(skipped, 2);
        assert_eq!(std::fs::read(&path).unwrap(), original.as_bytes());
    }
    /// Opt-in read-only verification against a real authorized rollout, without logging content.
    #[test]
    #[ignore = "requires CELLS_HISTORY_RECOVERY_HOME and CELLS_HISTORY_RECOVERY_LOG"]
    fn recovery_reads_real_rollout_without_writing() {
        let home = std::env::var("CELLS_HISTORY_RECOVERY_HOME").unwrap();
        let path = std::env::var("CELLS_HISTORY_RECOVERY_LOG").unwrap();
        let original = std::fs::read(&path).unwrap();
        let mut before = None;
        let mut items = 0;
        let mut skipped = 0;
        let mut skipped_bytes = 0;
        loop {
            let result = read_log(Path::new(&home), &path, before, true).unwrap();
            items += result["turns"][0]["items"].as_array().unwrap().len();
            skipped += result["recovery"]["skippedRecords"].as_u64().unwrap_or(0);
            skipped_bytes += result["recovery"]["skippedBytes"].as_u64().unwrap_or(0);
            let Some(cursor) = result["nextCursor"].as_str() else {
                break;
            };
            let next = cursor.parse::<u64>().unwrap();
            assert!(before.is_none_or(|previous| next < previous));
            before = Some(next);
        }
        assert!(items > 0);
        assert!(skipped > 0);
        assert!(
            std::fs::read(&path).unwrap() == original,
            "source changed during read-only verification"
        );
        println!("history_recovery items={items} skipped_records={skipped} skipped_bytes={skipped_bytes} source_unchanged=true");
    }
    #[test]
    fn prior_context_does_not_cross_damage() {
        let mut file = tempfile::tempfile().unwrap();
        let bytes = records(&[json!({"type":"turn_context","payload":{"turn_id":"old"}})]);
        use std::io::Write;
        file.write_all(format!("{bytes}\0\n").as_bytes()).unwrap();
        let end = file.metadata().unwrap().len();
        assert_eq!(prior_turn(&mut file, end).unwrap(), None);
    }
    #[test]
    fn lifecycle_terminals_keep_exact_identity_without_inventing_completion() {
        let bytes = records(&[
            json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"unfinished"}}),
            json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"completed"}}),
            json!({"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"cancelled"}}),
            json!({"type":"event_msg","payload":{"type":"turn_aborted"}}),
            json!({"type":"event_msg","payload":{"type":"error","turn_id":"unfinished"}}),
            json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"text":"Done"}]}}),
        ]);
        for summary in [false, true] {
            let result = page(bytes.as_bytes(), 0, None, summary).unwrap();
            let turns = result["turns"].as_array().unwrap();
            assert_eq!(turns.len(), 3);
            assert_eq!(
                turns[1],
                json!({"id":"completed","status":"completed","items":[]})
            );
            assert_eq!(
                turns[2],
                json!({"id":"cancelled","status":"interrupted","items":[]})
            );
            assert!(turns.iter().all(|turn| turn["id"] != "unfinished"));
        }
    }
    #[test]
    fn terminal_only_pages_remain_bounded_and_do_not_skip_older_results() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join("sessions")).unwrap();
        let path = home.path().join("sessions/session.jsonl");
        let bytes = records(
            &(0..205)
                .map(|i| {
                    json!({"type":"event_msg","payload":{
            "type":"task_complete","turn_id":format!("turn-{i}")}})
                })
                .collect::<Vec<_>>(),
        );
        std::fs::write(&path, bytes).unwrap();
        let mut cursor = None;
        let mut ids = std::collections::HashSet::new();
        loop {
            let result = read_log(home.path(), path.to_str().unwrap(), cursor, true).unwrap();
            let turns = result["turns"].as_array().unwrap();
            assert!(turns.len() <= 101);
            for turn in turns.iter().skip(1) {
                assert!(ids.insert(turn["id"].as_str().unwrap().to_owned()));
            }
            match result["nextCursor"].as_str() {
                Some(next) => cursor = Some(next.parse().unwrap()),
                None => break,
            }
        }
        assert_eq!(ids.len(), 205);
    }
    #[test]
    fn user_provenance_and_native_turn_survive_history_projection() {
        let record = json!({"type":"response_item","payload":{"id":"context","type":"message","role":"user",
            "content":[{"type":"input_text","text":"project rules"},{"type":"input_text","text":"actual prompt"}],
            "internal_chat_message_metadata_passthrough":{"turn_id":"native-turn","content_item_kinds":["agents_md.instructions","user.text"]}}});
        let item = record_item(&record, 0, Some("older-turn")).unwrap();
        assert_eq!(item["turnId"], "native-turn");
        assert_eq!(
            item["contentItemKinds"],
            json!(["agents_md.instructions", "user.text"])
        );
        assert_eq!(
            super::super::presentation::item(&item).unwrap()["content"],
            record["payload"]["content"]
        );
    }
    #[test]
    fn malformed_or_absent_provenance_never_classifies_prompt_text() {
        let mut record = json!({"type":"response_item","payload":{"type":"message","role":"user",
            "content":[{"text":"# AGENTS.md instructions for /project"}]}});
        assert!(item(&record).unwrap().get("contentItemKinds").is_none());
        for kinds in [
            json!([]),
            json!([12]),
            json!(["agents_md.instructions", "user.text"]),
        ] {
            record["payload"]["internal_chat_message_metadata_passthrough"] =
                json!({"content_item_kinds":kinds});
            assert!(item(&record).unwrap().get("contentItemKinds").is_none());
        }
    }
    fn message(index: usize, text: &str) -> String {
        format!(
            "{}\n",
            json!({"type":"response_item","payload":{"type":"message","role":if index % 2 == 0 {"user"} else {"assistant"},"content":[{"type":"text","text":text}]}})
        )
    }
    #[test]
    fn large_history_pages_preserve_all_roles_and_stable_ids_without_duplicates() {
        let home = tempfile::tempdir().unwrap();
        let folder = home.path().join("sessions");
        std::fs::create_dir(&folder).unwrap();
        let path = folder.join("session.jsonl");
        let text = "x".repeat(90_000);
        let records = (0..220).map(|i| message(i, &text)).collect::<String>();
        std::fs::write(&path, records + "{\"unfinished\":").unwrap();
        let mut cursor = None;
        let mut ids = std::collections::HashSet::new();
        let mut count = 0;
        loop {
            let page = read_log(home.path(), path.to_str().unwrap(), cursor, false).unwrap();
            for item in page["turns"][0]["items"].as_array().unwrap() {
                assert!(ids.insert(item["id"].as_str().unwrap().to_owned()));
                count += 1;
                assert!(matches!(
                    item["type"].as_str(),
                    Some("userMessage" | "agentMessage")
                ));
            }
            match page["nextCursor"].as_str() {
                Some(next) => cursor = Some(next.parse().unwrap()),
                None => break,
            }
        }
        assert_eq!(count, 220);
    }
    #[test]
    fn history_rejects_outside_paths_symlinks_and_invalid_cursors() {
        let home = tempfile::tempdir().unwrap();
        let folder = home.path().join("sessions");
        std::fs::create_dir(&folder).unwrap();
        let path = folder.join("session.jsonl");
        std::fs::write(&path, message(0, "visible")).unwrap();
        assert!(read_log(home.path(), "/tmp/other.jsonl", None, false).is_err());
        assert!(read_log(home.path(), path.to_str().unwrap(), Some(u64::MAX), false).is_err());
        let link = folder.join("link.jsonl");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(read_log(home.path(), link.to_str().unwrap(), None, false).is_err());
    }
    #[test]
    fn summary_preserves_turns_times_request_arrays_and_custom_tools_without_large_outputs() {
        let records = [
            json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"first"}}),
            json!({"type":"response_item","timestamp":"2026-10-08T11:22:45Z","payload":{"type":"message","role":"user","content":[{"text":"Request one"},{"text":"Request two"}]}}),
            json!({"type":"response_item","payload":{"type":"custom_tool_call","id":"patch","name":"apply_patch","input":"*** Begin Patch\n*** Update File: src/main.ts\n@@\n-private\n+content\n*** End Patch"}}),
            json!({"type":"response_item","payload":{"type":"custom_tool_call_output","output":"x".repeat(3 * 1024 * 1024)}}),
            json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"text":"Answer one"},{"text":"Answer two"}]}}),
            json!({"type":"turn_context","payload":{"turn_id":"second"}}),
            json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"text":"Next request"}]}}),
        ];
        let bytes = records
            .iter()
            .map(|record| format!("{record}\n"))
            .collect::<String>();
        let result = page(bytes.as_bytes(), 0, None, true).unwrap();
        let items = result["turns"][0]["items"].as_array().unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0]["turnId"], "first");
        assert_eq!(items[0]["timestamp"], 1791458565000i64);
        assert_eq!(items[0]["content"].as_array().unwrap().len(), 2);
        assert_eq!(items[1]["id"], "patch");
        assert_eq!(items[1]["paths"], json!(["src/main.ts"]));
        assert_eq!(items[1]["operation"], "write");
        assert!(items[1].get("input").is_none());
        assert_eq!(items[2]["content"].as_array().unwrap().len(), 2);
        assert_eq!(items[3]["turnId"], "second");
        assert!(result["turns"][0].get("id").is_none());
        assert!(result["nextCursor"].is_null());
    }
    #[test]
    fn pages_keep_the_turn_context_before_the_bounded_read_window() {
        let home = tempfile::tempdir().unwrap();
        let folder = home.path().join("sessions");
        std::fs::create_dir(&folder).unwrap();
        let path = folder.join("session.jsonl");
        let context = format!(
            "{}\n",
            json!({"type":"turn_context","payload":{"turn_id":"native-turn"}})
        );
        let text = "x".repeat(90_000);
        let records = context + &(0..110).map(|i| message(i, &text)).collect::<String>();
        std::fs::write(&path, records).unwrap();
        let result = read_log(home.path(), path.to_str().unwrap(), None, true).unwrap();
        assert!(result["nextCursor"].is_string());
        assert!(result["turns"][0]["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["turnId"] == "native-turn"));
    }
    #[test]
    fn compact_pages_reach_the_first_request_after_many_exec_tools() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join("sessions")).unwrap();
        let path = home.path().join("sessions/session.jsonl");
        let mut records = message(0, "First request");
        for index in 0..230 {
            records.push_str(&format!("{}\n", json!({"type":"response_item","payload":{
                "id":format!("exec-{index}"),"type":"function_call","name":"exec",
                "arguments":json!({"cmd":"pnpm typecheck\ncat details","workdir":"/project"}).to_string()}})));
        }
        std::fs::write(&path, records + &message(1, "Last answer")).unwrap();
        let mut cursor = None;
        let mut messages = Vec::new();
        loop {
            let page = read_log(home.path(), path.to_str().unwrap(), cursor, true).unwrap();
            messages.splice(
                0..0,
                page["turns"][0]["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .cloned(),
            );
            match page["nextCursor"].as_str() {
                Some(next) => cursor = Some(next.parse().unwrap()),
                None => break,
            }
        }
        assert_eq!(messages.len(), 232);
        assert_eq!(messages[0]["content"][0]["text"], "First request");
        assert_eq!(
            messages.last().unwrap()["content"][0]["text"],
            "Last answer"
        );
        assert!(messages[1..231]
            .iter()
            .all(|item| item["commandPreview"] == "pnpm typecheck"));
    }
}
