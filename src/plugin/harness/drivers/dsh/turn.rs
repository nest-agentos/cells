//! Prompt admission and lifecycle controls; native turns remain asynchronous.
use super::*;
impl Dsh {
    async fn turn(
        &self,
        client: &Arc<transport::Client>,
        args: &Value,
        dispatched: &mut bool,
    ) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let live = self.owned(id)?;
        let prompt = validate_prompt(args)?;
        let operation = string(args, "requestId")?;
        let selection = self.prepare_model_selection(client, args).await?;
        self.begin(id, operation, prompt, &live)?;
        *dispatched = true;
        let result = self.submit_turn(client, args, selection).await;
        self.admission(id, operation, &live, result)
    }
    async fn submit_turn(
        &self,
        client: &transport::Client,
        args: &Value,
        selection: Option<Value>,
    ) -> Result<Value, Error> {
        if let Some(request) = selection {
            client
                .call("session/selectModel", json!({"request":request}))
                .await?;
        }
        client
            .call(
                "session/prompt",
                json!({"request":{"sessionId":args["sessionId"],"requestId":args["requestId"],
            "mode":"queue","content":[{"type":"text","text":args["prompt"]}]}}),
            )
            .await
    }
    fn begin(&self, id: &str, operation: &str, prompt: &str, live: &Live) -> Result<(), Error> {
        let mut cursor = live.cursor.lock().unwrap();
        admission::require_ready(&cursor)?;
        if cursor.busy() || live.session.lock().unwrap()["status"] == "active" {
            return Err(Error::busy());
        }
        if let Some(runtime) = &self.runtime {
            runtime.begin(id, operation)?;
            runtime.bind_turn(id, operation, operation);
        }
        cursor.turn = operation.into();
        cursor.native_turn = None;
        cursor.observed_native = None;
        cursor.expected_request = Some(operation.into());
        cursor.admission = Some(admission::PromptAdmission::new(operation, prompt));
        live.session.lock().unwrap()["activeTurnId"] = json!(operation);
        Ok(())
    }
    fn admission(
        &self,
        id: &str,
        operation: &str,
        live: &Live,
        result: Result<Value, Error>,
    ) -> Result<Value, Error> {
        if let Some(result) = live
            .cursor
            .lock()
            .unwrap()
            .admission
            .as_ref()
            .filter(|admission| admission.id == operation)
            .and_then(|admission| admission.result())
        {
            return result;
        }
        match result {
            Ok(value) if value["accepted"] == true => Ok(json!({"turnId":operation})),
            Ok(_) => Err(Error::unavailable()),
            Err(error) => {
                self.reject_prompt(id, operation, live, &error);
                Err(error)
            }
        }
    }

    fn reject_prompt(&self, id: &str, operation: &str, live: &Live, error: &Error) {
        let mut cursor = live.cursor.lock().unwrap();
        if !cursor
            .admission
            .as_ref()
            .is_some_and(|value| value.id == operation)
        {
            return;
        }
        if let Some(runtime) = &self.runtime {
            runtime.submission_failed(id, operation, error);
        }
        if !matches!(error.code, "EIO" | "ETIMEDOUT") {
            if cursor.turn == operation {
                cursor.turn.clear();
                cursor.expected_request = None;
                live.session.lock().unwrap()["activeTurnId"] = Value::Null;
            }
        }
    }

    pub(super) async fn execute_native(
        &self,
        name: &str,
        args: &Value,
        dispatched: &mut bool,
    ) -> Result<Value, Error> {
        let client = self.client().await?;
        match name {
            "harness_turn" => return self.turn(&client, args, dispatched).await,
            "harness_command" => return self.command(&client, args, dispatched).await,
            _ => *dispatched = true,
        }
        match name {
            "harness_create" => self.create(&client, args).await,
            "harness_resume" => {
                self.attach(
                    &client,
                    self.summary(&client, string(args, "sessionId")?).await?,
                    None,
                    string(args, "requestId")?,
                )
                .await
            }
            "harness_interrupt" => self.interrupt(&client, args).await,
            "harness_respond" => {
                interactions::respond(&client, &self.sessions, &self.events, args).await
            }
            "harness_rename" | "harness_archive" => self.manage(&client, name, args).await,
            _ => Err(Error::unsupported()),
        }
    }
    async fn interrupt(&self, client: &transport::Client, args: &Value) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let live = self.owned(id)?;
        self.begin_interrupt(&live, args)?;
        let result = client
            .call("session/cancel", json!({"request":{"sessionId":id}}))
            .await?;
        if result["accepted"] != true {
            return Err(Error::busy());
        }
        Ok(json!({}))
    }
    fn begin_interrupt(&self, live: &Live, args: &Value) -> Result<(), Error> {
        let mut cursor = live.cursor.lock().unwrap();
        if cursor.control_pending.is_some() {
            return Err(Error::conflict("DSH_CONTROL_RESULT_PENDING"));
        }
        if cursor.turn != string(args, "turnId")? {
            return Err(Error::conflict("HARNESS_TURN_CHANGED"));
        }
        if cursor.native_turn.is_none() {
            return Err(Error::conflict("DSH_TURN_NOT_IDENTIFIED"));
        }
        cursor.control_pending = Some(string(args, "requestId")?.into());
        Ok(())
    }
}

fn validate_prompt(args: &Value) -> Result<&str, Error> {
    let prompt = string(args, "prompt")?;
    if prompt.is_empty() || prompt.len() > 128 * 1024 {
        return Err(Error::invalid());
    }
    if args["attachments"]
        .as_array()
        .is_some_and(|files| !files.is_empty())
    {
        return Err(Error::unsupported());
    }
    Ok(prompt)
}
