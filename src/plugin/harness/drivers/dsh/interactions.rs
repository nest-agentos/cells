//! Scope DSH event waterfalls to attached sessions and answer through their owner.
use super::{transport::Client, Live};
use crate::{
    core::error::Error,
    harness::{approval_policy::{self, Driver}, events::Events, service::string},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
type Sessions = Arc<Mutex<BTreeMap<String, Arc<Live>>>>;

pub async fn start(
    client: Arc<Client>,
    sessions: Sessions,
    events: Arc<Mutex<Events>>,
) -> Result<(), Error> {
    let mut sub = client.subscribe("$events", json!({}))?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(25), sub.next())
        .await
        .map_err(|_| Error::timed_out())??;
    if ready["type"] != "ready" {
        return Err(Error::invalid());
    }
    let client_id = string(&ready, "clientId")?.to_owned();
    tokio::spawn(async move {
        while let Ok(frame) = sub.next().await {
            if handle(&client, &client_id, &sessions, &events, frame)
                .await
                .is_err()
            {
                break;
            }
        }
        client.stop.cancel();
    });
    Ok(())
}
async fn handle(
    client: &Arc<Client>,
    client_id: &str,
    sessions: &Sessions,
    events: &Arc<Mutex<Events>>,
    frame: Value,
) -> Result<(), Error> {
    match frame["type"].as_str() {
        Some("waterfall") => waterfall(client, client_id, sessions, events, &frame).await?,
        Some("cancel") => events.lock().unwrap().push(
            json!({"method":"serverRequest/resolved","params":{"requestId":frame["eventId"]}}),
        )?,
        Some("emit") => emitted(sessions, events, &frame)?,
        _ => (),
    }
    Ok(())
}
async fn waterfall(
    client: &Arc<Client>,
    client_id: &str,
    sessions: &Sessions,
    events: &Arc<Mutex<Events>>,
    frame: &Value,
) -> Result<(), Error> {
    let id = string(frame, "eventId")?;
    let session = frame["agentId"].as_str().unwrap_or("");
    let live = sessions.lock().unwrap().get(session).cloned();
    let method = match frame["event"].as_str() {
        Some("approval/request") if live.is_some() => "item/commandExecution/requestApproval",
        Some("user-questions/request") if live.is_some() => "item/tool/requestUserInput",
        _ => return delegate(client, client_id, id),
    };
    let live = live.unwrap();
    let turn = live.cursor.lock().unwrap().turn.clone();
    let mut params = json!({"threadId":session,"turnId":turn,"questions":frame["request"]["questions"],
        "reason":frame["request"]["displayReason"],"toolName":frame["request"]["toolName"],"decisions":["accept","decline"]});
    let eligible = frame["event"] == "approval/request" && params["toolName"].as_str().is_some_and(approval_policy::native_tool);
    if eligible {
        let decision = {
            let _approval = client.approval_admission.lock().await;
            approval_policy::consider(&events.lock().unwrap(), Driver::Dsh, session, false)
        };
        if let Some(decision) = decision {
            let started = std::time::Instant::now();
            let result = client.call("$events/result", json!({"clientId":client_id,"eventId":id,
                "outcome":approval_outcome(decision.allows())})).await;
            approval_policy::record(&json!(id), decision, started, &result);
            return Ok(());
        }
        approval_policy::offer_for_request(&events.lock().unwrap(), &mut params);
    }
    events.lock().unwrap().push(json!({"id":id,"method":method,"params":params,
        "dshClientId":client_id,"dshEvent":frame["event"]}))
}
fn approval_outcome(allow: bool) -> Value {
    json!({"kind":"result","value":if allow {"allowed-once"} else {"rejected"}})
}

fn delegate(client: &Arc<Client>, client_id: &str, id: &str) -> Result<(), Error> {
    let client = client.clone();
    let args = json!({"clientId":client_id,"eventId":id,"outcome":{"kind":"next"}});
    tokio::spawn(async move {
        if client.call("$events/result", args).await.is_err() {
            client.stop.cancel();
        }
    });
    Ok(())
}
fn emitted(sessions: &Sessions, events: &Arc<Mutex<Events>>, frame: &Value) -> Result<(), Error> {
    let id = frame["args"][0].as_str().unwrap_or("");
    let live = sessions.lock().unwrap().get(id).cloned();
    let Some(live) = live else {
        return Ok(());
    };
    if frame["event"] == "api-session/status" && frame["args"][1].is_boolean() {
        live.session.lock().unwrap()["status"] = json!(if frame["args"][1] == true {
            "active"
        } else {
            "idle"
        });
    }
    if frame["event"] == "api-session/error" {
        fail_session(id, &live, events)?;
    }
    Ok(())
}
fn fail_session(id: &str, live: &Live, events: &Mutex<Events>) -> Result<(), Error> {
    events
        .lock()
        .unwrap()
        .push(json!({"method":"error","params":{"threadId":id,
        "error":{"message":"DSH session failed","code":"DSH_SESSION_ERROR"},"willRetry":false}}))?;
    let turn = std::mem::take(&mut live.cursor.lock().unwrap().turn);
    if turn.is_empty() {
        return Ok(());
    }
    let mut session = live.session.lock().unwrap();
    session["status"] = json!("idle");
    session["activeTurnId"] = Value::Null;
    session["lastTurnResult"] = json!("failed");
    drop(session);
    events.lock().unwrap().push(json!({"method":"turn/completed","params":{"threadId":id,"turn":{"id":turn,"status":"failed"}}}))
}
pub async fn respond(
    client: &Client,
    sessions: &Sessions,
    events: &Arc<Mutex<Events>>,
    args: &Value,
) -> Result<Value, Error> {
    let id = string(args, "nativeRequestId")?;
    let request = pending(events, id)?;
    if !sessions
        .lock()
        .unwrap()
        .contains_key(string(&request["params"], "threadId")?)
    {
        return Err(Error::forbidden("EACCES"));
    }
    let _approval = if args["response"]["decision"] == approval_policy::ALLOW_SESSION {
        Some(client.approval_admission.lock().await)
    } else { None };
    let revision = if _approval.is_some() {
        Some(approval_policy::grant_revision(&events.lock().unwrap(), &request)?)
    } else { None };
    let outcome = if revision.is_some() { approval_outcome(true) } else { response(&request, &args["response"])? };
    client
        .call(
            "$events/result",
            json!({"clientId":request["dshClientId"],"eventId":id,"outcome":outcome}),
        )
        .await?;
    let mut events = events.lock().unwrap();
    if let Some(revision) = revision {
        events.approval_grants.insert(string(&request["params"], "threadId")?.into(), revision);
    }
    events.push(json!({"method":"serverRequest/resolved","params":{"requestId":id}}))?;
    Ok(json!({}))
}
fn response(request: &Value, response: &Value) -> Result<Value, Error> {
    if request["dshEvent"] == "approval/request" {
        return match string(response, "decision")? {
            "accept" => Ok(approval_outcome(true)),
            "decline" | "cancel" => Ok(approval_outcome(false)),
            _ => Err(Error::invalid()),
        };
    }
    let answers = response["answers"]
        .as_object()
        .ok_or_else(Error::invalid)?
        .iter()
        .map(|(id, value)| {
            let selected = value["answers"]
                .as_array()
                .filter(|items| items.iter().all(Value::is_string))
                .ok_or_else(Error::invalid)?;
            Ok(json!({"id":id,"selected":selected}))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(json!({"kind":"result","value":{"answers":answers}}))
}

fn pending(events: &Mutex<Events>, id: &str) -> Result<Value, Error> {
    events
        .lock()
        .unwrap()
        .requests
        .get(&json!(id).to_string())
        .cloned()
        .ok_or_else(|| Error::not_found("ENOENT"))
}
