//! Attachment and creation of native DSH sessions.
use super::*;
impl Dsh {
    pub(super) async fn attach(
        &self,
        client: &Arc<transport::Client>,
        session: Value,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
        operation: &str,
    ) -> Result<Value, Error> {
        let id = string(&session, "id")?.to_owned();
        if session["archived"] == true {
            return Err(Error::forbidden("HARNESS_SESSION_ARCHIVED"));
        }
        if let Some(live) = self.sessions.lock().unwrap().get(&id) {
            return Ok(json!({"session":live.session.lock().unwrap().clone()}));
        }
        let slot = match slot {
            Some(slot) => slot,
            None => self.reserve(operation).await?,
        };
        let (subscription, snapshot) = self.observe(client, &id).await?;
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(live) = sessions.get(&id) {
            return Ok(json!({"session":live.session.lock().unwrap().clone()}));
        }
        let live = Arc::new(tracked(session, &snapshot, slot, &id));
        for message in protocol::baseline(&snapshot, &mut live.cursor.lock().unwrap(), &id) {
            self.events.lock().unwrap().push(message)?;
        }
        let session = live.session.lock().unwrap().clone();
        sessions.insert(id.clone(), live.clone());
        *live.follow.lock().unwrap() = Some(follow(
            id,
            live.clone(),
            subscription,
            self.events.clone(),
            client.clone(),
        ));
        Ok(json!({"session":session}))
    }
    async fn observe(
        &self,
        client: &transport::Client,
        id: &str,
    ) -> Result<(transport::Subscription, Value), Error> {
        let mut subscription = client.subscribe(
            "session/follow",
            json!({"request":{
            "address":{"kind":"session","sessionId":id},"maxMessages":1,"assistantStream":true}}),
        )?;
        let options = self.config.dsh.clone().unwrap_or_default();
        let snapshot = tokio::time::timeout(
            std::time::Duration::from_millis(options.request_timeout_ms),
            subscription.next(),
        )
        .await
        .map_err(|_| Error::timed_out())??;
        if snapshot["type"] != "snapshot" {
            return Err(Error::invalid());
        }
        Ok((subscription, snapshot))
    }

    pub(super) fn owned(&self, id: &str) -> Result<Arc<Live>, Error> {
        self.sessions
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| Error::forbidden("HARNESS_SESSION_NOT_OWNED"))
    }

    pub(super) async fn create(
        &self,
        client: &Arc<transport::Client>,
        args: &Value,
    ) -> Result<Value, Error> {
        let operation = string(args, "requestId")?;
        let slot = self.reserve(operation).await?;
        let workspace = self
            .config
            .workspaces
            .iter()
            .find(|w| Some(w.id.as_str()) == args["workspaceId"].as_str())
            .ok_or_else(Error::invalid)?;
        let id = self
            .native_create(client, workspace, string(args, "requestId")?)
            .await?;
        let session = json!({"id":id,"cwd":workspace.path,"title":"","status":"idle","updatedAt":null,"owned":false,"resumable":true,"archived":false});
        self.attach(client, session, Some(slot), operation)
            .await
            .map_err(|_| Error::unavailable())
    }
    async fn native_create(
        &self,
        client: &transport::Client,
        workspace: &WorkspaceConfig,
        operation: &str,
    ) -> Result<String, Error> {
        use sha2::{Digest, Sha256};
        let host_workspace = self.workspace(client, workspace).await?;
        let expected = format!(
            "pi-{:x}",
            Sha256::digest(
                json!([self.config.id, workspace.path, operation])
                    .to_string()
                    .as_bytes()
            )
        );
        let result = client
            .call(
                "session/create",
                json!({"request":{"workspaceId":host_workspace,"sessionId":expected}}),
            )
            .await?;
        let id = result["sessionId"]
            .as_str()
            .filter(|id| *id == expected)
            .ok_or_else(Error::unavailable)?;
        Ok(id.to_owned())
    }
}

fn tracked(
    mut session: Value,
    snapshot: &Value,
    slot: tokio::sync::OwnedSemaphorePermit,
    id: &str,
) -> Live {
    session["owned"] = json!(true);
    let active = if session["status"] == "active" {
        format!("{id}-active")
    } else {
        String::new()
    };
    if !active.is_empty() {
        session["activeTurnId"] = json!(active);
    }
    let native_turn = snapshot_turn(snapshot);
    Live {
        cursor: Mutex::new(protocol::Cursor {
            seq: snapshot["cursor"].as_u64(),
            turn: active,
            native_turn,
            observed_native: native_turn,
            ..Default::default()
        }),
        session: Mutex::new(session),
        stop: CancellationToken::new(),
        _slot: slot,
        follow: Mutex::new(None),
    }
}

fn snapshot_turn(snapshot: &Value) -> Option<u64> {
    snapshot["records"]
        .as_array()?
        .iter()
        .rev()
        .find(|row| row["event"]["type"] == "turn/start")?["event"]["data"]["turn"]
        .as_u64()
}
