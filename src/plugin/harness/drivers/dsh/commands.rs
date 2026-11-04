use super::*;

impl Dsh {
    pub(super) async fn query(
        &self,
        client: &transport::Client,
        args: &Value,
    ) -> Result<Value, Error> {
        let catalog = query_catalog("DSH_SESSION_NOT_ATTACHED");
        let name = super::super::commands::verify_query(args, &catalog)?;
        let id = string(args, "sessionId")?;
        match name {
            "status" | "session:status" => {
                super::super::commands::status(args, &self.summary(client, id).await?)
            }
            "session:models" => {
                super::super::commands::result(args, self.models(client, args).await?)
            }
            _ => Err(Error::unsupported()),
        }
    }
    pub(super) async fn commands(
        &self,
        client: &transport::Client,
        args: &Value,
    ) -> Result<Value, Error> {
        let Some(id) = args["sessionId"].as_str() else {
            return Ok(query_catalog("DSH_SESSION_REQUIRED"));
        };
        let session = self.summary(client, id).await?;
        // DSH's Agent lookup may resume a cold Session. Discovery must never claim its writer.
        if session["archived"] == true {
            return Ok(query_catalog("HARNESS_SESSION_ARCHIVED"));
        }
        let _owned = self.sessions.lock().unwrap().get(id).cloned();
        if _owned.is_none() {
            return Ok(query_catalog("DSH_SESSION_NOT_ATTACHED"));
        }
        let rows = match client.call("commands/list", json!({"agentId":id})).await {
            Ok(rows) => rows,
            Err(error) if error.status == axum::http::StatusCode::CONFLICT => {
                return Ok(query_catalog(error.code))
            }
            Err(error) => return Err(error),
        };
        native_catalog(rows)
    }
    pub(super) async fn command(
        &self,
        client: &transport::Client,
        args: &Value,
        dispatched: &mut bool,
    ) -> Result<Value, Error> {
        string(args, "sessionId")?;
        let name = string(args, "command")?;
        let commands = self.commands(client, args).await?;
        let descriptor = commands["commands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == name)
            .ok_or_else(Error::unsupported)?;
        super::super::commands::verify(args, descriptor)?;
        if descriptor["execution"] == "query" {
            return self.query(client, args).await;
        }
        if args.get("selection").is_some() {
            return Err(Error::unsupported());
        }
        self.serial_command(client, args, dispatched).await
    }
    async fn serial_command(
        &self,
        client: &transport::Client,
        args: &Value,
        dispatched: &mut bool,
    ) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let name = string(args, "command")?;
        let live = self.owned(id)?;
        let input = super::super::commands::arguments(args)?;
        let operation = string(args, "requestId")?;
        self.begin_command(id, operation, &live)?;
        *dispatched = true;
        let result = self.run_command(client, id, name, input).await;
        self.finish_command(operation, &live, result)
    }
    async fn run_command(
        &self,
        client: &transport::Client,
        id: &str,
        name: &str,
        input: &str,
    ) -> Result<Value, Error> {
        let options = self.config.dsh.clone().unwrap_or_default();
        let timeout = std::time::Duration::from_millis(options.command_timeout_ms);
        client.call_with_timeout("commands/execute", json!({"agentId":id,"line":format!("/{name} {input}").trim_end(),"submittedAttachments":[]}), timeout).await
            .and_then(validate_execution).map(|result| json!({"commandId":result["commandId"],
                "output":result["result"]["text"].as_str().unwrap_or(name),"failed":result["result"]["kind"]=="error","status":"completed"}))
    }

    fn begin_command(&self, id: &str, operation: &str, live: &Live) -> Result<(), Error> {
        let mut cursor = live.cursor.lock().unwrap();
        admission::require_ready(&cursor)?;
        if cursor.busy() || live.session.lock().unwrap()["status"] == "active" {
            return Err(Error::busy());
        }
        if let Some(runtime) = &self.runtime {
            runtime.begin(id, operation)?;
        }
        cursor.command_pending = Some(command_admission::CommandAdmission::new(operation));
        Ok(())
    }

    fn finish_command(
        &self,
        operation: &str,
        live: &Live,
        result: Result<Value, Error>,
    ) -> Result<Value, Error> {
        let mut cursor = live.cursor.lock().unwrap();
        let admission = cursor
            .command_pending
            .as_mut()
            .filter(|value| value.id == operation)
            .ok_or_else(|| Error::conflict("DSH_COMMAND_EVIDENCE_MISMATCH"))?;
        if let Ok(value) = &result {
            admission.record(value.clone())?;
        }
        result
    }
}

fn query_catalog(code: &str) -> Value {
    json!({"commands":query_commands(json!([])), "observation":{"complete":false,"code":code}})
}
fn query_commands(mut commands: Value) -> Value {
    commands
        .as_array_mut()
        .unwrap()
        .retain(|row| row["name"] != "session:models");
    commands
        .as_array_mut()
        .unwrap()
        .push(super::super::commands::query(
            "session:models",
            "Read this session's model selection",
        ));
    super::super::commands::with_status(commands)
}
fn native_catalog(rows: Value) -> Result<Value, Error> {
    let rows = rows
        .as_array()
        .filter(|rows| rows.len() <= 256)
        .ok_or_else(Error::invalid)?;
    let mut rows = rows.clone();
    for row in &mut rows {
        row["argumentHint"] = row["input"]["hint"].clone();
    }
    let mut commands = super::super::commands::normalize(&json!(rows));
    for row in commands.as_array_mut().unwrap() {
        row["kind"] = json!("action");
        row["execution"] = json!("serial");
    }
    Ok(json!({"commands":query_commands(commands), "observation":{"complete":true}}))
}

fn validate_execution(value: Value) -> Result<Value, Error> {
    if value["commandId"].is_string()
        && matches!(value["result"]["kind"].as_str(), Some("success" | "error"))
    {
        Ok(value)
    } else {
        Err(Error::unavailable())
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    fn fixture() -> (Dsh, Arc<Live>) {
        let driver = Dsh::new(
            serde_json::from_value(json!({"id":"dsh","kind":"dsh","home":"/unused"})).unwrap(),
            None,
        );
        let live = Arc::new(Live {
            cursor: Mutex::default(),
            session: Mutex::new(json!({"id":"s","status":"idle"})),
            stop: CancellationToken::new(),
            _slot: driver.slots.clone().try_acquire_owned().unwrap(),
            follow: Mutex::default(),
        });
        driver
            .sessions
            .lock()
            .unwrap()
            .insert("s".into(), live.clone());
        (driver, live)
    }
    fn output(id: &str) -> Value {
        json!({"commandId":id,"output":"private command result","failed":false,"status":"completed"})
    }
    fn assert_unconfirmed_receipts(driver: &Dsh, live: &Live, result: &Value) {
        for receipt in [
            json!({"requestId":"original","outcome":"unknown"}),
            json!({"requestId":"original","outcome":"not-committed"}),
            json!({"requestId":"foreign","outcome":"committed","result":result}),
            json!({"requestId":"original","outcome":"committed","result":output("foreign")}),
        ] {
            driver.operation_settled("original", &receipt);
            assert!(live.cursor.lock().unwrap().command_pending.is_some());
        }
    }

    #[test]
    fn original_response_remains_reconcilable_and_blocks_dispatch_until_a_matching_receipt_is_confirmed(
    ) {
        let (driver, live) = fixture();
        driver.begin_command("s", "original", &live).unwrap();
        let result = driver
            .finish_command("original", &live, Ok(output("native")))
            .unwrap();
        assert_eq!(driver.command_evidence("original"), Some(result.clone()));
        assert!(driver.command_evidence("foreign").is_none());
        assert_eq!(
            driver.begin_command("s", "next", &live).err().unwrap().code,
            "DSH_COMMAND_RESULT_PENDING"
        );
        assert!(driver.idle_since().is_none());
        assert_unconfirmed_receipts(&driver, &live, &result);
        driver.operation_settled(
            "original",
            &json!({"requestId":"original","outcome":"committed","result":result}),
        );
        assert!(driver.command_evidence("original").is_none());
        driver.begin_command("s", "next", &live).unwrap();
        driver.operation_settled(
            "original",
            &json!({"requestId":"original","outcome":"committed","result":result}),
        );
        let cursor = live.cursor.lock().unwrap();
        assert_eq!(cursor.command_pending.as_ref().unwrap().id, "next");
    }

    #[test]
    fn unknown_transport_never_uses_matching_log_events_as_original_command_evidence() {
        let (driver, live) = fixture();
        driver.begin_command("s", "unknown", &live).unwrap();
        assert_eq!(
            driver
                .finish_command("unknown", &live, Err(Error::timed_out()))
                .err()
                .unwrap()
                .code,
            "ETIMEDOUT"
        );
        protocol::records(
            &[
                json!({"event":{"seq":1,"type":"command/done","data":{"commandId":"native","name":"compact","kind":"success","text":"same text"}}}),
            ],
            &mut live.cursor.lock().unwrap(),
            "s",
        );
        assert!(driver.command_evidence("unknown").is_none());
        assert_eq!(
            driver.begin_command("s", "next", &live).err().unwrap().code,
            "DSH_COMMAND_RESULT_PENDING"
        );
        driver.operation_settled(
            "unknown",
            &json!({"requestId":"unknown","outcome":"unknown"}),
        );
        assert!(live.cursor.lock().unwrap().command_pending.is_some());
    }

    #[test]
    fn confirmed_rejection_releases_only_the_matching_admission_and_conflicting_native_evidence_is_rejected(
    ) {
        let (driver, live) = fixture();
        driver.begin_command("s", "rejected", &live).unwrap();
        assert!(driver
            .finish_command("rejected", &live, Err(Error::busy()))
            .is_err());
        assert!(live.cursor.lock().unwrap().command_pending.is_some());
        driver.operation_settled(
            "rejected",
            &json!({"requestId":"rejected","outcome":"not-committed"}),
        );
        driver.begin_command("s", "accepted", &live).unwrap();
        driver
            .finish_command("accepted", &live, Ok(output("native")))
            .unwrap();
        assert_eq!(
            driver
                .finish_command("accepted", &live, Ok(output("foreign")))
                .err()
                .unwrap()
                .code,
            "DSH_COMMAND_EVIDENCE_MISMATCH"
        );
        assert_eq!(driver.command_evidence("accepted"), Some(output("native")));
    }
}
