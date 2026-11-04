//! Authorize workspace membership before exposing DSH session metadata or records.
use super::{protocol, transport::Client, Dsh};
use crate::{
    core::error::Error,
    harness::{config::WorkspaceConfig, service::string},
};
use serde_json::{json, Value};

impl Dsh {
    pub(super) async fn workspace(
        &self,
        client: &Client,
        workspace: &WorkspaceConfig,
    ) -> Result<String, Error> {
        let baseline = client.first("workspace/follow", json!({})).await?;
        if baseline["type"] != "baseline" {
            return Err(Error::invalid());
        }
        if let Some(row) = baseline["value"]["items"].as_array().and_then(|rows| {
            rows.iter()
                .find(|row| row["path"].as_str() == workspace.path.to_str())
        }) {
            return Ok(string(row, "workspaceId")?.to_owned());
        }
        let result = client
            .call(
                "workspace/create",
                json!({"request":{"path":workspace.path}}),
            )
            .await?;
        Ok(string(&result["workspace"], "workspaceId")?.to_owned())
    }
    async fn catalog(&self, client: &Client) -> Result<Vec<Value>, Error> {
        let (rows, baseline) = tokio::try_join!(
            client.call("session/list", json!({"_request":{}})),
            client.first("workspace/follow", json!({}))
        )?;
        if baseline["type"] != "baseline" {
            return Err(Error::invalid());
        }
        let rows = rows["items"].as_array().ok_or_else(Error::invalid)?;
        let workspaces = baseline["value"]["items"]
            .as_array()
            .ok_or_else(Error::invalid)?;
        let archived = baseline["value"]["archivedSessionIds"].as_array();
        Ok(rows
            .iter()
            .filter(|row| self.visible(row, workspaces))
            .map(|row| {
                protocol::session(
                    row,
                    self.sessions
                        .lock()
                        .unwrap()
                        .contains_key(row["sessionId"].as_str().unwrap_or("")),
                    archived.is_some_and(|ids| ids.contains(&row["sessionId"])),
                )
            })
            .collect())
    }

    fn authorized(&self, cwd: &str) -> bool {
        if let Some(runtime) = &self.runtime {
            return runtime.resolve_cwd(cwd).is_ok();
        }
        let path = std::path::Path::new(cwd);
        !path
            .components()
            .any(|p| p == std::path::Component::ParentDir)
            && self
                .config
                .workspaces
                .iter()
                .any(|w| path.starts_with(&w.path))
    }
    fn visible(&self, row: &Value, workspaces: &[Value]) -> bool {
        let Some(cwd) = row["cwd"].as_str() else {
            return false;
        };
        self.authorized(cwd)
            && workspaces.iter().any(|workspace| {
                workspace["path"].as_str().is_some_and(|path| {
                    self.runtime.as_ref().map_or_else(
                        || {
                            self.config
                                .workspaces
                                .iter()
                                .any(|w| Some(path) == w.path.to_str())
                        },
                        |runtime| runtime.resolve_cwd(path).is_ok(),
                    )
                }) && workspace["sessionIds"]
                    .as_array()
                    .is_some_and(|ids| ids.contains(&row["sessionId"]))
            })
    }

    pub(super) async fn summary(&self, client: &Client, id: &str) -> Result<Value, Error> {
        let mut session = self
            .catalog(client)
            .await?
            .into_iter()
            .find(|s| s["id"] == id)
            .ok_or_else(|| Error::not_found("ENOENT"))?;
        session["interruptible"] = json!(false);
        if let Some(live) = self.sessions.lock().unwrap().get(id) {
            let cursor = live.cursor.lock().unwrap();
            session["interruptible"] = json!(
                !cursor.turn.is_empty()
                    && cursor.native_turn.is_some()
                    && cursor.control_pending.is_none()
            );
            let current = live.session.lock().unwrap();
            for key in ["activeTurnId", "lastTurnResult", "lastTurnId", "owned"] {
                session[key] = current[key].clone();
            }
        }
        Ok(session)
    }
    pub(super) async fn read_native(&self, name: &str, args: Value) -> Result<Value, Error> {
        let client = self.client().await?;
        match name {
            "harness_query" => self.query(&client, &args).await,
            "harness_models" => self.models(&client, &args).await,
            "harness_commands" => self.commands(&client, &args).await,
            "harness_sessions" => self.list(&client, &args).await,
            "harness_session_info" => {
                Ok(json!({"session":self.summary(&client,string(&args,"sessionId")?).await?}))
            }
            "harness_session_read" => self.history(&client, &args).await,
            "harness_session_search" => {
                let client = &*client;
                super::super::search::search_with(
                    &args,
                    |input| Box::pin(async move { self.list(client, &input).await }),
                    |input| Box::pin(async move { self.history(client, &input).await }),
                )
                .await
            }
            _ => Err(Error::unsupported()),
        }
    }
    async fn list(&self, client: &Client, args: &Value) -> Result<Value, Error> {
        let mut sessions = self.catalog(client).await?;
        sessions.retain(|s| {
            s["archived"].as_bool() == Some(args["archived"].as_bool().unwrap_or(false))
        });
        sessions.sort_by(|a, b| {
            b["updatedAt"]
                .as_i64()
                .cmp(&a["updatedAt"].as_i64())
                .then(a["id"].as_str().cmp(&b["id"].as_str()))
        });
        let offset = offset(args)?;
        let limit = args["limit"].as_u64().unwrap_or(100).clamp(1, 100) as usize;
        if offset > sessions.len() {
            return Err(Error::invalid());
        }
        let end = (offset + limit).min(sessions.len());
        Ok(
            json!({"sessions":sessions[offset..end],"nextCursor":if end<sessions.len(){json!(end.to_string())}else{Value::Null}}),
        )
    }

    async fn history(&self, client: &Client, args: &Value) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let session = self.summary(client, id).await?;
        let (records, has_more, through) = history_page(client, id, &args["cursor"]).await?;
        let mut items = protocol::history_records(&records, id);
        if args["toolDetail"] == "summary" {
            items = items
                .iter()
                .filter_map(super::super::presentation::item)
                .collect();
        }
        let before = records
            .iter()
            .filter_map(|r| r["event"]["seq"].as_i64())
            .min();
        if has_more && before.is_none() {
            return Err(Error::invalid());
        }
        Ok(
            json!({"session":session,"turns":super::history_proof::annotate(protocol::turns(items), &records, id),
            "nextCursor":if has_more{json!(format!("{through}:{}",before.unwrap()))}else{Value::Null}}),
        )
    }

    pub(super) async fn manage(
        &self,
        client: &Client,
        name: &str,
        args: &Value,
    ) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let mut session = self.summary(client, id).await?;
        let _owned = self.sessions.lock().unwrap().get(id).cloned();
        if name == "harness_rename" {
            rename(client, id, &mut session, args).await?;
        } else {
            self.archive(client, id, &mut session).await?;
        }
        Ok(json!({"session":session}))
    }
    async fn archive(&self, client: &Client, id: &str, session: &mut Value) -> Result<(), Error> {
        if session["status"] == "active" {
            return Err(Error::busy());
        }
        let result = client
            .call(
                "workspace/archiveSession",
                json!({"request":{"sessionId":id,"stopActivity":false}}),
            )
            .await?;
        if !result["archivedSessionIds"]
            .as_array()
            .is_some_and(|ids| ids.iter().any(|value| value == id))
        {
            return Err(Error::unavailable());
        }
        session["archived"] = json!(true);
        session["archiveAllowed"] = json!(false);
        session["owned"] = json!(false);
        if let Some(live) = self.sessions.lock().unwrap().remove(id) {
            live.stop.cancel();
        }
        Ok(())
    }
}
fn offset(args: &Value) -> Result<usize, Error> {
    args["cursor"]
        .as_str()
        .map(|v| v.parse().map_err(|_| Error::invalid()))
        .transpose()
        .map(|v| v.unwrap_or(0))
}

async fn history_page(
    client: &Client,
    id: &str,
    cursor: &Value,
) -> Result<(Vec<Value>, bool, i64), Error> {
    let address = json!({"kind":"session","sessionId":id});
    let (page, through) = if let Some(cursor) = cursor.as_str() {
        let (through, before) = parse_cursor(cursor)?;
        (client.call("session/page",json!({"request":{"address":address,"throughSeq":through,"beforeSeq":before,"maxMessages":80}})).await?,through)
    } else {
        let snapshot = client
            .first(
                "session/follow",
                json!({"request":{"address":address,"maxMessages":80}}),
            )
            .await?;
        if snapshot["type"] != "snapshot" {
            return Err(Error::invalid());
        }
        let through = snapshot["cursor"].as_i64().ok_or_else(Error::invalid)?;
        (snapshot, through)
    };
    let records = page["records"]
        .as_array()
        .ok_or_else(Error::invalid)?
        .clone();
    Ok((records, page["hasMore"] == true, through))
}
fn parse_cursor(cursor: &str) -> Result<(i64, i64), Error> {
    let (through, before) = cursor.split_once(':').ok_or_else(Error::invalid)?;
    let through = through.parse::<i64>().map_err(|_| Error::invalid())?;
    let before = before.parse::<i64>().map_err(|_| Error::invalid())?;
    if through < 0 || before < 0 || before > through.saturating_add(1) {
        return Err(Error::invalid());
    }
    Ok((through, before))
}

async fn rename(client: &Client, id: &str, session: &mut Value, args: &Value) -> Result<(), Error> {
    let title = string(args, "name")?.trim();
    if title.is_empty() || title.len() > 512 || title.chars().any(char::is_control) {
        return Err(Error::invalid());
    }
    let result = client
        .call(
            "session/rename",
            json!({"request":{"sessionId":id,"title":title}}),
        )
        .await?;
    session["title"] = result["title"]
        .as_str()
        .map(|value| json!(value))
        .ok_or_else(Error::unavailable)?;
    Ok(())
}
