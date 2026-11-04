use super::super::commands as descriptors;
use super::*;
use sha2::{Digest, Sha256};

fn skill_descriptor(skill: &Value, skills: &[Value]) -> Value {
    let name = skill["name"].as_str().unwrap();
    let identity = format!(
        "{:x}",
        Sha256::digest(skill["path"].as_str().unwrap().as_bytes())
    );
    let duplicate = skills
        .iter()
        .filter(|row| row["name"] == name)
        .take(2)
        .count()
        > 1;
    let command = if duplicate || name.len() > 122 {
        format!(
            "skill:{}:{}",
            name.chars().take(96).collect::<String>(),
            &identity[..16]
        )
    } else {
        format!("skill:{name}")
    };
    json!({"name":command,"id":identity,"description":skill["description"].as_str().unwrap_or(""),"kind":"prompt","argumentHint":"[instructions]"})
}

fn base_commands() -> Vec<Value> {
    vec![
        json!({"name":"compact","description":"Compact this conversation's context","argumentHint":"","kind":"action"}),
        json!({"name":"review","description":"Review working changes or the supplied instructions","argumentHint":"[instructions]","kind":"action"}),
        descriptors::query(
            "models",
            "Read available models and this session's selection",
        ),
        descriptors::query("mcp", "Read MCP server connection status"),
        descriptors::query("skills", "Read available skills"),
    ]
}

impl Codex {
    pub(super) async fn query(&self, bridge: &Bridge, args: &Value) -> Result<Value, Error> {
        let catalog = json!({"commands":descriptors::with_status(json!(base_commands()))});
        let name = descriptors::verify_query(args, &catalog)?;
        self.query_command(bridge, args, name)
            .await?
            .ok_or_else(Error::unsupported)
    }
    pub(super) async fn commands(&self, bridge: &Bridge, args: &Value) -> Result<Value, Error> {
        let mut rows = base_commands();
        match self.skills(bridge, args).await {
            Ok(skills) => rows.extend(skills.iter().map(|skill| skill_descriptor(skill, &skills))),
            Err(error) if error.code == "ECAPABILITY" => (),
            Err(error) => return Err(error),
        }
        Ok(json!({"commands":descriptors::with_status(json!(rows))}))
    }
    async fn skills(&self, bridge: &Bridge, args: &Value) -> Result<Vec<Value>, Error> {
        let cwd = if let Some(id) = args["sessionId"].as_str() {
            self.summary(bridge, id).await?["cwd"].clone()
        } else {
            json!(
                self.config
                    .workspaces
                    .first()
                    .ok_or_else(Error::unsupported)?
                    .path
            )
        };
        let result = bridge
            .call("skills/list", json!({"cwds":[cwd],"forceReload":true}))
            .await?;
        let groups = result["data"].as_array().ok_or_else(Error::unsupported)?;
        Ok(groups
            .iter()
            .flat_map(|group| group["skills"].as_array().into_iter().flatten())
            .filter(|skill| {
                skill["enabled"] != false
                    && skill["path"].is_string()
                    && skill["name"].as_str().is_some_and(descriptors::valid_name)
            })
            .take(240)
            .cloned()
            .collect())
    }
    async fn query_command(
        &self,
        bridge: &Bridge,
        args: &Value,
        name: &str,
    ) -> Result<Option<Value>, Error> {
        let value = match name {
            "status" | "session:status" => {
                let thread = self.summary(bridge, string(args, "sessionId")?).await?;
                let mut session = self.session(&thread);
                session["modelSelection"] = self
                    .selections
                    .lock()
                    .await
                    .get(string(args, "sessionId")?)
                    .cloned()
                    .unwrap_or(Value::Null);
                return descriptors::status(args, &session).map(Some);
            }
            "models" => self.models(bridge, args).await?,
            "skills" => json!(self
                .skills(bridge, args)
                .await?
                .iter()
                .map(|skill| json!({"name":skill["name"],"description":skill["description"]}))
                .collect::<Vec<_>>()),
            "mcp" => self.mcp_status(bridge).await?,
            _ => return Ok(None),
        };
        descriptors::result(args, value).map(Some)
    }
    async fn mcp_status(&self, bridge: &Bridge) -> Result<Value, Error> {
        let result = bridge
            .call("mcpServerStatus/list", json!({"limit":100}))
            .await?;
        let rows = result["data"].as_array().ok_or_else(Error::unsupported)?;
        Ok(json!(rows.iter().map(|row| json!({"name":row["name"],"authStatus":row["authStatus"],"toolCount":row["tools"].as_object().map(|tools|tools.len())})).collect::<Vec<_>>()))
    }
    pub(super) async fn command(&self, bridge: &Bridge, args: &Value) -> Result<Value, Error> {
        let name = string(args, "command")?;
        let catalog = self.commands(bridge, args).await?;
        let descriptor = catalog["commands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == name)
            .ok_or_else(Error::unsupported)?;
        descriptors::verify(args, descriptor)?;
        if let Some(result) = self.query_command(bridge, args, name).await? {
            return Ok(result);
        }
        let id = string(args, "sessionId")?;
        self.require_owned(id).await?;
        if self.summary(bridge, id).await?["status"]["type"] == "active" {
            return Err(Error::busy());
        }
        let input = descriptors::arguments(args)?;
        if name.starts_with("skill:") {
            return self.skill(bridge, args, name, input).await;
        }
        if args.get("selection").is_some() {
            return Err(Error::unsupported());
        }
        match name {
            "compact" if input.trim().is_empty() => self.compact(bridge, args).await,
            "review" => self.review(bridge, args, input).await,
            _ => Err(Error::unsupported()),
        }
    }
    async fn compact(&self, bridge: &Bridge, args: &Value) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let operation = string(args, "requestId")?;
        if let Some(runtime) = &self.runtime {
            runtime.begin(id, operation)?;
        }
        if let Err(error) = bridge
            .events
            .lock()
            .unwrap()
            .register_command(id, operation)
        {
            if let Some(runtime) = &self.runtime {
                runtime.reject(id, operation);
            }
            return Err(error);
        }
        match bridge
            .persistent_call("thread/compact/start", json!({"threadId":id}))
            .await
        {
            Ok(_) => Ok(json!({"commandId":operation,"status":"running"})),
            Err(error) => {
                if let Some(runtime) = &self.runtime {
                    runtime.submission_failed(id, operation, &error);
                }
                if !matches!(error.code, "EIO" | "ETIMEDOUT") {
                    bridge.events.lock().unwrap().forget_command(id, operation);
                }
                Err(error)
            }
        }
    }
    async fn skill(
        &self,
        bridge: &Bridge,
        args: &Value,
        name: &str,
        input: &str,
    ) -> Result<Value, Error> {
        let skills = self.skills(bridge, args).await?;
        let skill = skills
            .iter()
            .find(|skill| skill_descriptor(skill, &skills)["name"] == name)
            .ok_or_else(Error::unsupported)?;
        let descriptor = descriptors::normalize(&json!([skill_descriptor(skill, &skills)]));
        descriptors::verify(args, &descriptor[0])?;
        self.start_input(bridge,args,json!([{"type":"skill","name":skill["name"],"path":skill["path"]},{"type":"text","text":input}])).await
    }
    async fn review(&self, bridge: &Bridge, args: &Value, input: &str) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let operation = string(args, "requestId")?;
        if let Some(runtime) = &self.runtime {
            runtime.begin(id, operation)?;
        }
        let target = if input.trim().is_empty() {
            json!({"type":"uncommittedChanges"})
        } else {
            json!({"type":"custom","instructions":input})
        };
        let result = bridge
            .persistent_call(
                "review/start",
                json!({"threadId":id,"delivery":"inline","target":target}),
            )
            .await;
        if let (Err(error), Some(runtime)) = (&result, &self.runtime) {
            runtime.submission_failed(id, operation, error);
        }
        let result = result?;
        let turn = string(&result["turn"], "id").map_err(|_| Error::internal())?;
        if let Some(runtime) = &self.runtime {
            runtime.bind_turn(id, operation, turn);
        }
        Ok(json!({"turnId":turn}))
    }
}
