//! A claimed item is never resent. Unknown admission is reconciled by its original identity.
use super::*;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
pub(crate) fn ensure(state: &Arc<State>, binding: &Binding) {
    let key = json!([
        binding.project_id,
        binding.owner,
        binding.profile_id,
        binding.session_id
    ])
    .to_string();
    let Some(service) = state.services.get::<super::super::ProjectService>() else {
        return;
    };
    if !store::owns(&service.channels, binding).unwrap_or(false) {
        return;
    }
    if !service.channels.4.lock().unwrap().insert(key.clone()) {
        return;
    }
    let state = Arc::downgrade(state);
    let binding = binding.clone();
    crate::core::trace::spawn(async move {
        let mut events = None;
        let mut last_error = None;
        loop {
            let Some(host) = state.upgrade() else {
                break;
            };
            if host.event_stop.is_cancelled()
                || host
                    .services
                    .get::<crate::harness::Harnesses>()
                    .is_none_or(|h| h.is_closed())
            {
                break;
            }
            let Some(identity) = host
                .auth
                .clients()
                .iter()
                .position(|c| c.id() == binding.owner)
            else {
                break;
            };
            let args = base(&binding);
            if harness_recovery::authorize(&host, identity, &args).is_err() {
                break;
            }
            let result = advance(&host, identity, &binding).await;
            if let Err(error) = result {
                if last_error != Some(error.code) {
                    crate::core::events::emit(
                        crate::core::events::Level::Warn,
                        "harness.queue.worker",
                        json!({"operation":"harness.queue","stage":"advance","sessionId":binding.session_id,"result":"deferred","code":error.code}),
                    );
                    last_error = Some(error.code);
                }
            } else {
                last_error = None;
            }
            let channels = &host
                .services
                .require::<super::super::ProjectService>()
                .unwrap()
                .channels;
            let keep = harness_recovery::store::load(channels, &binding)
                .ok()
                .is_some_and(|(_, r)| has_work(&r));
            if !keep {
                break;
            }
            if events.is_none() {
                events = host
                    .services
                    .require::<crate::harness::Harnesses>()
                    .unwrap()
                    .subscribe(&host, identity, base(&binding))
                    .await
                    .ok();
            }
            let stop = host.event_stop.clone();
            drop(host);
            tokio::select! {
                _=stop.cancelled()=>break,
                _=tokio::time::sleep(std::time::Duration::from_secs(1))=>{},
                event=async {if let Some(events)=events.as_mut(){events.next().await}else{std::future::pending().await}}=>{
                    match event{Some(Ok(frame))=>{if let Some(host)=state.upgrade(){
                        let started=std::time::Instant::now();
                        if let Err(error)=observe(&host,&binding,&frame).await {
                            crate::core::events::emit(crate::core::events::Level::Warn,"harness.queue.worker",json!({"operation":"harness.queue","stage":"observe","sessionId":binding.session_id,"result":"deferred","code":error.code,"durationMs":started.elapsed().as_millis()}));
                            events=None;
                        }
                    }},_=>events=None}
                }
            }
        }
        if let Some(host) = state.upgrade() {
            if let Some(service) = host.services.get::<super::super::ProjectService>() {
                service.channels.4.lock().unwrap().remove(&key);
                restart_registered(&host, &service.channels, &binding);
            }
        }
    });
}
// A refreshed grant may replace an exiting worker before its marker is removed.
fn restart_registered(state: &Arc<State>, channels: &Channels, previous: &Binding) {
    if state.event_stop.is_cancelled()
        || state
            .services
            .get::<crate::harness::Harnesses>()
            .is_none_or(|h| h.is_closed())
    {
        return;
    }
    let Some(identity) = state
        .auth
        .clients()
        .iter()
        .position(|c| c.id() == previous.owner)
    else {
        return;
    };
    let Ok(bindings) = harness_recovery::store::bindings(channels) else {
        return;
    };
    for binding in bindings {
        if binding.project_id != previous.project_id
            || binding.owner != previous.owner
            || binding.profile_id != previous.profile_id
            || binding.session_id != previous.session_id
        {
            continue;
        }
        if harness_recovery::authorize(state, identity, &base(&binding)).is_err() {
            continue;
        }
        if harness_recovery::store::load(channels, &binding)
            .ok()
            .is_some_and(|(_, r)| has_work(&r))
        {
            ensure(state, &binding);
            break;
        }
    }
}
fn base(binding: &Binding) -> Value {
    json!({"projectId":binding.project_id,"revision":binding.revision,"profileId":binding.profile_id,"sessionId":binding.session_id,"namespace":binding.namespace})
}
async fn native(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    name: &str,
    extra: Value,
) -> Result<Value, Error> {
    let mut args = base(binding);
    for (k, v) in extra.as_object().ok_or_else(Error::invalid)? {
        args[k] = v.clone();
    }
    state
        .services
        .require::<crate::harness::Harnesses>()?
        .project_call(state, identity, name, args)
        .await
}
pub(super) async fn advance(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
) -> Result<(), Error> {
    let channels = &state
        .services
        .require::<super::super::ProjectService>()?
        .channels;
    if !store::owns(channels, binding)? {
        return Ok(());
    }
    let (_, record) = harness_recovery::store::load(channels, binding)?;
    if blocked(&record) {
        return Ok(());
    }
    let item = record["instructionQueue"]["items"][0].clone();
    if item.is_null() || item["state"] == "failed" {
        return Ok(());
    }
    crate::core::trace::scope(
        crate::core::trace::resume(&item["traceContext"]),
        advance_item(state, identity, binding, &record, item),
    )
    .await
}
async fn advance_item(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    record: &Value,
    item: Value,
) -> Result<(), Error> {
    let session = native(state, identity, binding, "harness_session_info", json!({})).await?
        ["session"]
        .clone();
    if item["state"] != "pending" {
        return reconcile(state, identity, binding, &item, &session, &record["instructionQueue"]).await;
    }
    if record["instructionQueue"]["paused"] == true {
        return Ok(());
    }
    let active = session["activeTurnId"].as_str();
    if session["statusDetails"]["activeFlags"]
        .as_array()
        .is_some_and(|r| {
            r.iter()
                .any(|r| r == "waitingOnApproval" || r == "waitingOnUserInput")
        })
    {
        return Ok(());
    }
    if active.is_some() {
        return supplement(state, identity, binding, record, &session).await;
    }
    if !["idle", "notLoaded"].contains(&session["status"].as_str().unwrap_or("")) {
        return Ok(());
    }
    let epoch = state
        .services
        .require::<crate::harness::Harnesses>()?
        .profiles()["epoch"]
        .clone();
    let mut claimed = item.clone();
    claimed["traceContext"] = crate::core::trace::capture();
    claimed["state"] = json!("dispatching");
    claimed["dispatchedAt"] = json!(now());
    claimed["queueEpoch"] = epoch.clone();
    claimed["preparation"] = json!(if session["owned"] == true {
        "submit"
    } else {
        "resume"
    });
    update(state, binding, &item, |queue, index| {
        queue["items"][index] = claimed.clone();
        stamp_receipt(queue, &claimed);
        Ok(())
    })
    .await?;
    if claimed["preparation"] == "resume" {
        let receipt = native(
            state,
            identity,
            binding,
            "harness_resume",
            json!({"epoch":epoch,"requestId":preparation_id(&claimed)}),
        )
        .await?;
        if receipt["outcome"] != "committed" {
            return admit(state, binding, &claimed, &receipt, true).await;
        }
        claimed["preparation"] = json!("submit");
        update(state, binding, &claimed, |q, i| {
            q["items"][i]["preparation"] = json!("submit");
            Ok(())
        })
        .await?;
    }
    dispatch(state, identity, binding, &claimed).await
}
fn has_work(record: &Value) -> bool {
    let queue = &record["instructionQueue"];
    queue["items"].as_array().is_some_and(|rows| {
        rows.iter()
            .any(|r| ["dispatching", "uncertain"].contains(&r["state"].as_str().unwrap_or("")))
            || queue["paused"] != true && rows.iter().any(|r| r["state"] == "pending")
    })
}
fn blocked(record: &Value) -> bool {
    record["pending"].is_object()
        || record["responses"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
        || record["controls"].as_array().is_some_and(|r| {
            r.iter().any(|r| {
                r["receipt"] == "pending"
                    || r["receipt"] == "unknown"
                    || r["receipt"] == "accepted" && r["terminal"].is_null()
            })
        })
}
fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn attempt(item: &Value) -> &str {
    item["attemptId"]
        .as_str()
        .or_else(|| item["id"].as_str())
        .unwrap_or("")
}
fn preparation_id(item: &Value) -> String {
    format!("prepare-{:x}", Sha256::digest(attempt(item).as_bytes()))
}
async fn dispatch(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    item: &Value,
) -> Result<(), Error> {
    commands::validate(item)?;
    let prompt = item["text"].as_str().unwrap();
    let prompt = if prompt.trim().is_empty() {
        format!(
            "Attachments: {}",
            item["attachments"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|r| r["name"].as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        prompt.into()
    };
    let mut extra = json!({"epoch":item["queueEpoch"],"requestId":attempt(item),"prompt":prompt,"attachments":item["attachments"]});
    if !item["selection"].is_null() {
        extra["selection"] = item["selection"].clone();
    }
    let name = if !item["steerTurnId"].is_null() {
        extra["turnId"] = item["steerTurnId"].clone();
        "harness_steer"
    } else if !item["commandKind"].is_null() {
        let text = item["text"]
            .as_str()
            .unwrap()
            .trim_start()
            .strip_prefix('/')
            .ok_or_else(Error::invalid)?;
        let (name, args) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
        extra["outputMode"] = json!("reference");
        extra["command"] = json!(name);
        extra["arguments"] = json!(args.trim_start());
        extra["expectedCommand"] =
            json!({"id":item["commandId"],"revision":item["commandRevision"]});
        extra.as_object_mut().unwrap().remove("prompt");
        extra.as_object_mut().unwrap().remove("attachments");
        "harness_command"
    } else {
        "harness_turn"
    };
    match native(state, identity, binding, name, extra).await {
        Ok(receipt) => admit(state, binding, item, &receipt, false).await,
        Err(error) => {
            let definite = error.outcome == Some("not-committed");
            let receipt =
                json!({"outcome":if definite{"not-committed"}else{"unknown"},"code":error.code});
            admit(state, binding, item, &receipt, false).await
        }
    }
}
async fn admit(
    state: &Arc<State>,
    binding: &Binding,
    item: &Value,
    receipt: &Value,
    preparing: bool,
) -> Result<(), Error> {
    update(state, binding, item, |queue, index| {
        apply_admission(queue, index, item, receipt, preparing)
    })
    .await
}
fn apply_admission(
    queue: &mut Value,
    index: usize,
    item: &Value,
    receipt: &Value,
    preparing: bool,
) -> Result<(), Error> {
    if receipt["outcome"] != "committed" {
        reject_admission(queue, index, receipt);
        return Ok(());
    }
    if preparing {
        return Ok(());
    }
    let result = &receipt["result"];
    let mut data = receipt_data(result);
    if !valid_native_result(result) {
        queue["paused"] = json!(true);
        queue["items"][index]["receipt"] = json!({"resultError":{"code":"HARNESS_RESULT_INVALID"}});
        queue["items"][index]["error"] = json!("HARNESS_RESULT_INVALID");
        return Ok(());
    }
    remember_command(queue, item, result)?;
    if !item["steerTurnId"].is_null()
        && data["steeredFrom"]
            .as_str()
            .or_else(|| data["turnId"].as_str())
            != item["steerTurnId"].as_str()
    {
        data["resultError"] = json!({"code":"HARNESS_STEER_TARGET_CHANGED"});
        queue["paused"] = json!(true);
    }
    let row = &mut queue["items"][index];
    row["receipt"] = data;
    row["state"] = json!("dispatching");
    row["acceptedAt"] = json!(now());
    row.as_object_mut().unwrap().remove("error");
    let current = row.clone();
    stamp_receipt(queue, &current);
    Ok(())
}
fn reject_admission(queue: &mut Value, index: usize, receipt: &Value) {
    queue["paused"] = json!(true);
    queue["items"][index]["state"] = json!(if receipt["outcome"] == "not-committed" {
        "failed"
    } else {
        "uncertain"
    });
    queue["items"][index]["error"] = json!(receipt["code"]
        .as_str()
        .unwrap_or("HARNESS_RECEIPT_UNKNOWN"));
}
fn valid_native_result(result: &Value) -> bool {
    let valid = |value: &Value| {
        value.is_null()
            || value
                .as_str()
                .is_some_and(|id| !id.is_empty() && id.len() <= 1024)
    };
    ["turnId", "commandId", "targetTurnId"]
        .iter()
        .all(|key| valid(&result[key]))
        && (result["turnId"].is_string() || result["commandId"].is_string())
        && (result["status"].is_null()
            || result["status"] == "running"
            || result["status"] == "completed")
        && (result["failed"].is_null() || result["failed"].is_boolean())
        && (result["output"].is_null() || result["output"].is_string())
}

fn receipt_data(result: &Value) -> Value {
    let mut data = json!({});
    for (source, target) in [
        ("turnId", "turnId"),
        ("targetTurnId", "steeredFrom"),
        ("commandId", "commandId"),
        ("status", "commandStatus"),
        ("failed", "failed"),
    ] {
        if let Some(value) = result.get(source).filter(|v| !v.is_null()) {
            data[target] = value.clone();
        }
    }
    data
}
fn remember_command(queue: &mut Value, item: &Value, result: &Value) -> Result<(), Error> {
    if !result["commandId"].is_string() {
        return Ok(());
    }
    let mut command = result.clone();
    if !command["output"].is_string() {
        command["output"] = json!("");
    }
    command["operationId"] = json!(attempt(item));
    command["operationEpoch"] = item["queueEpoch"].clone();
    if !queue["commandResults"].is_array() {
        queue["commandResults"] = json!([]);
    }
    let rows = queue["commandResults"].as_array_mut().unwrap();
    if let Some(existing) = rows
        .iter_mut()
        .find(|r| r["commandId"] == command["commandId"])
    {
        *existing = command;
    } else {
        rows.push(command);
    }
    trim_commands(rows)
}
fn trim_commands(rows: &mut Vec<Value>) -> Result<(), Error> {
    while rows.len() > 64 {
        let index = rows
            .iter()
            .position(|r| r["status"] != "running" && r["resultError"].is_null())
            .ok_or_else(|| Error::too_large("HARNESS_COMMAND_REPORT_LIMIT"))?;
        rows.remove(index);
    }
    Ok(())
}

fn stamp_receipt(queue: &mut Value, item: &Value) {
    if let Some(row) = queue["receipts"]
        .as_array_mut()
        .and_then(|r| r.iter_mut().find(|r| r["id"] == item["id"]))
    {
        for key in ["dispatchedAt", "acceptedAt"] {
            if let Some(value) = item.get(key) {
                row[key] = value.clone();
            }
        }
        row["attemptId"] = json!(attempt(item));
        for key in ["turnId", "commandId"] {
            if let Some(value) = item["receipt"].get(key) {
                row[key] = value.clone();
            }
        }
        if !item["steerTurnId"].is_null() {
            row["kind"] = json!("steer");
        }
    }
}
async fn update<F>(
    state: &Arc<State>,
    binding: &Binding,
    expected: &Value,
    change: F,
) -> Result<(), Error>
where
    F: FnOnce(&mut Value, usize) -> Result<(), Error>,
{
    let started = std::time::Instant::now();
    let channels = &state
        .services
        .require::<super::super::ProjectService>()?
        .channels;
    let _gate = harness_recovery::locks::session_lock(state, binding, "queue.update").await?;
    let (raw, mut record) = harness_recovery::store::load(channels, binding)?;
    let queue = &mut record["instructionQueue"];
    let index = queue["items"]
        .as_array()
        .and_then(|r| {
            r.iter()
                .position(|r| r["id"] == expected["id"] && attempt(r) == attempt(expected))
        })
        .ok_or_else(|| Error::conflict("HARNESS_QUEUE_ATTEMPT_CHANGED"))?;
    change(queue, index)?;
    commands::bump(queue)?;
    let result = store::save(channels, binding, &raw, &record);
    crate::core::trace::sync_scope(
        crate::core::trace::resume(&expected["traceContext"]),
        || {
            crate::core::events::emit(
                crate::core::events::Level::Info,
                "harness.queue.transition",
                json!({"operation":"harness.queue","operationId":attempt(expected),"sessionId":binding.session_id,"stage":"persist_transition","result":if result.is_ok(){"success"}else{"failure"},"revision":record["instructionQueue"]["revision"],"count":record["instructionQueue"]["items"].as_array().map(Vec::len),"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}),
            );
        },
    );
    result
}
async fn reconcile(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    item: &Value,
    session: &Value,
    queue: &Value,
) -> Result<(), Error> {
    if item["preparation"] == "resume" && !item["receipt"].is_object() {
        return reconcile_preparation(state, identity, binding, item).await;
    }
    if !item["receipt"].is_object() {
        let current_epoch = state
            .services
            .require::<crate::harness::Harnesses>()?
            .profiles()["epoch"]
            .clone();
        if item["queueEpoch"] != current_epoch {
            if item["commandKind"].is_null() && item["steerTurnId"].is_null() {
                let mut args = base(binding);
                args["requestId"] = json!(attempt(item));
                if let Some(turn) = state
                    .services
                    .require::<crate::harness::Harnesses>()?
                    .admitted_project_turn(state, identity, args)
                    .await?
                {
                    return admit(
                        state,
                        binding,
                        item,
                        &json!({"outcome":"committed","result":{"turnId":turn}}),
                        false,
                    )
                    .await;
                }
            }
            return Ok(());
        }
        let receipt = native(
            state,
            identity,
            binding,
            "harness_operation",
            json!({"epoch":current_epoch,"requestId":attempt(item)}),
        )
        .await?;
        return admit(state, binding, item, &receipt, false).await;
    }
    if item["receipt"]["resultError"].is_object() {
        return reconcile_result(state, identity, binding, item).await;
    }
    if let Some(turn) = item["receipt"]["turnId"].as_str() {
        if session["executionRecovery"]["state"] == "detached"
            && harness_recovery::decision::restart_matches(session, item, turn)
        {
            let _native_guard = state
                .services
                .require::<crate::harness::Harnesses>()?
                .recovery_guard(state, identity, base(binding))
                .await?;
            return harness_recovery::startup::reconcile(state, identity, binding).await;
        }
    }
    if !item["steerTurnId"].is_null() && !item["receipt"]["resultError"].is_object() {
        return update(state, binding, item, |q, i| {
            q["items"].as_array_mut().unwrap().remove(i);
            Ok(())
        })
        .await;
    }
    if session["activeTurnId"].as_str().is_some() {
        return supplement_current(state, identity, binding, session).await;
    }
    if item["receipt"]["resultError"].is_object() {
        return Ok(());
    }
    let turn = item["receipt"]["turnId"].as_str();
    let mut result = item["terminal"]["result"].as_str().map(str::to_owned);
    if let Some(turn) = turn {
        if session["lastTurnId"] == turn {
            result = session["lastTurnResult"].as_str().map(str::to_owned);
        }
        if result.is_none() {
            let history = harness_recovery::history(state, identity, binding).await?;
            result = history["turns"]
                .as_array()
                .and_then(|r| r.iter().find(|r| r["id"] == turn))
                .and_then(|r| r["status"].as_str())
                .map(str::to_owned);
        }
        if !result
            .as_deref()
            .is_some_and(|r| ["completed", "failed", "cancelled", "interrupted"].contains(&r))
        {
            return Ok(());
        }
    } else if item["receipt"]["commandStatus"] != "completed" {
        return settle_legacy_command(state, binding, item, queue).await;
    }
    let failed =
        result.as_deref().is_some_and(|r| r != "completed") || item["receipt"]["failed"] == true;
    update(state, binding, item, |q, i| {
        q["items"].as_array_mut().unwrap().remove(i);
        if failed {
            q["paused"] = json!(true);
        }
        Ok(())
    })
    .await
}

/// Before explicit command status, synchronous results returned a command ID and output.
fn legacy_completed_command(queue: &Value, item: &Value) -> Option<usize> {
    let receipt = &item["receipt"];
    let id = receipt["commandId"].as_str()?;
    if item["commandKind"] != "action"
        || item["state"] != "dispatching"
        || item["queueEpoch"].as_str().is_none()
        || !receipt["turnId"].is_null()
        || !receipt["commandStatus"].is_null()
        || !receipt["resultError"].is_null()
    {
        return None;
    }
    queue["commandResults"].as_array()?.iter().position(|row| {
        row["commandId"] == id
            && row["operationId"] == attempt(item)
            && row["operationEpoch"] == item["queueEpoch"]
            && row["turnId"].is_null()
            && row["status"].is_null()
            && row["resultError"].is_null()
            && row["output"].as_str().is_some_and(|output| !output.is_empty())
    })
}

async fn settle_legacy_command(
    state: &Arc<State>,
    binding: &Binding,
    item: &Value,
    queue: &Value,
) -> Result<(), Error> {
    if legacy_completed_command(queue, item).is_none() {
        return Ok(());
    }
    let started = std::time::Instant::now();
    let result = update(state, binding, item, |queue, index| {
        let current = &queue["items"][index];
        let command = legacy_completed_command(queue, current)
            .ok_or_else(|| Error::conflict("HARNESS_QUEUE_ATTEMPT_CHANGED"))?;
        let failed = current["receipt"]["failed"] == true
            || queue["commandResults"][command]["failed"] == true;
        queue["commandResults"][command]["status"] = json!("completed");
        queue["items"].as_array_mut().unwrap().remove(index);
        if failed {
            queue["paused"] = json!(true);
        }
        Ok(())
    }).await;
    crate::core::events::emit(crate::core::events::Level::Info, "harness.queue.reconcile",
        json!({"operation":"harness.queue","operationId":attempt(item),"sessionId":binding.session_id,"stage":"legacy_command_receipt",
            "result":if result.is_ok() {"success"} else {"failure"},"code":result.as_ref().err().map(|e|e.code),"durationMs":started.elapsed().as_millis()}));
    result
}
async fn reconcile_result(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    item: &Value,
) -> Result<(), Error> {
    let epoch = state
        .services
        .require::<crate::harness::Harnesses>()?
        .profiles()["epoch"]
        .clone();
    if epoch != item["queueEpoch"]
        || now() - item["lastResultCheckAt"].as_i64().unwrap_or(0) < 15000
    {
        return Ok(());
    }
    update(state, binding, item, |q, i| {
        q["items"][i]["lastResultCheckAt"] = json!(now());
        Ok(())
    })
    .await?;
    let receipt = native(
        state,
        identity,
        binding,
        "harness_operation",
        json!({"epoch":epoch,"requestId":attempt(item)}),
    )
    .await?;
    if receipt["outcome"] == "committed" && valid_native_result(&receipt["result"]) {
        admit(state, binding, item, &receipt, false).await?;
    }
    Ok(())
}

async fn reconcile_preparation(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    item: &Value,
) -> Result<(), Error> {
    let epoch = state
        .services
        .require::<crate::harness::Harnesses>()?
        .profiles()["epoch"]
        .clone();
    if epoch != item["queueEpoch"] {
        return admit(
            state,
            binding,
            item,
            &json!({"outcome":"not-committed","code":"HARNESS_PREPARATION_INTERRUPTED"}),
            true,
        )
        .await;
    }
    let receipt = native(
        state,
        identity,
        binding,
        "harness_operation",
        json!({"epoch":epoch,"requestId":preparation_id(item)}),
    )
    .await?;
    if receipt["outcome"] != "committed" {
        return admit(state, binding, item, &receipt, true).await;
    }
    let mut ready = item.clone();
    ready["preparation"] = json!("submit");
    update(state, binding, item, |q, i| {
        q["items"][i]["preparation"] = json!("submit");
        Ok(())
    })
    .await?;
    dispatch(state, identity, binding, &ready).await
}
async fn supplement_current(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    session: &Value,
) -> Result<(), Error> {
    let channels = &state
        .services
        .require::<super::super::ProjectService>()?
        .channels;
    let (_, record) = harness_recovery::store::load(channels, binding)?;
    supplement(state, identity, binding, &record, session).await
}
async fn supplement(
    state: &Arc<State>,
    identity: usize,
    binding: &Binding,
    record: &Value,
    session: &Value,
) -> Result<(), Error> {
    if record["instructionQueue"]["paused"] == true {
        return Ok(());
    }
    let Some(turn) = session["activeTurnId"].as_str() else {
        return Ok(());
    };
    let items = record["instructionQueue"]["items"]
        .as_array()
        .ok_or_else(Error::invalid)?;
    if let Some(existing) = items.iter().find(|r| !r["steerTurnId"].is_null()) {
        return Box::pin(reconcile(state, identity, binding, existing, session, &record["instructionQueue"])).await;
    }
    let Some(index) = items.iter().position(|r| r["state"] == "pending") else {
        return Ok(());
    };
    let item = &items[index];
    if item["allowSteer"] != true
        || !item["selection"].is_null()
        || !item["commandKind"].is_null()
        || items[..index]
            .iter()
            .any(|r| r["state"] != "dispatching" || r["receipt"]["turnId"] != turn)
    {
        return Ok(());
    }
    let mut claimed = item.clone();
    claimed["traceContext"] = crate::core::trace::capture();
    claimed["state"] = json!("dispatching");
    claimed["dispatchedAt"] = json!(now());
    claimed["steerTurnId"] = json!(turn);
    claimed["queueEpoch"] = state
        .services
        .require::<crate::harness::Harnesses>()?
        .profiles()["epoch"]
        .clone();
    update(state, binding, item, |q, i| {
        q["items"][i] = claimed.clone();
        stamp_receipt(q, &claimed);
        Ok(())
    })
    .await?;
    dispatch(state, identity, binding, &claimed).await
}
async fn observe(state: &Arc<State>, binding: &Binding, frame: &Value) -> Result<(), Error> {
    for row in frame["events"].as_array().into_iter().flatten() {
        let message = &row["message"];
        if message["method"] != "harness/command/updated" {
            continue;
        }
        let params = &message["params"];
        if params["threadId"] != binding.session_id {
            continue;
        }
        let channels = &state
            .services
            .require::<super::super::ProjectService>()?
            .channels;
        let (_, record) = harness_recovery::store::load(channels, binding)?;
        let Some(item) = record["instructionQueue"]["items"]
            .as_array()
            .and_then(|r| {
                r.iter()
                    .find(|r| r["receipt"]["commandId"] == params["commandId"])
            })
        else {
            continue;
        };
        update(state, binding, item, |q, i| {
            for key in ["turnId", "failed"] {
                if let Some(value) = params.get(key) {
                    q["items"][i]["receipt"][key] = value.clone();
                }
            }
            q["items"][i]["receipt"]["commandStatus"] = params["status"].clone();
            if let Some(command) = q["commandResults"]
                .as_array_mut()
                .and_then(|r| r.iter_mut().find(|r| r["commandId"] == params["commandId"]))
            {
                for key in ["turnId", "failed", "status"] {
                    if let Some(value) = params.get(key) {
                        command[key] = value.clone();
                    }
                }
            }
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciles_legacy_output_only_for_the_original_synchronous_command() {
        let item = json!({"id":"input","attemptId":"attempt","queueEpoch":"epoch",
            "state":"dispatching","commandKind":"action","receipt":{"commandId":"command"}});
        let result = json!({"commandId":"command","operationEpoch":"epoch","operationId":"attempt","output":"completed output"});
        assert_eq!(legacy_completed_command(&json!({"commandResults":[result]}), &item), Some(0));
        for (key, value) in [("operationId",json!("different")), ("operationEpoch",json!("changed")),
            ("status",json!("running")), ("output",json!("")), ("turnId",json!("turn"))] {
            let mut changed = result.clone();
            changed[key] = value;
            assert_eq!(legacy_completed_command(&json!({"commandResults":[changed]}), &item), None);
        }
    }
}
