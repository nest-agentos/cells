//! SDK commands and explicit session controls; terminal navigation belongs to the host.
use super::*;
use super::super::approval_policy::{self, Driver};
const MODES: &[&str] = &["manual", "plan", "acceptEdits", "dontAsk"];
const NAVIGATION: &[&str] = &["clear", "reset", "new", "resume", "fork", "exit", "quit"];

pub(super) fn catalog(raw: &Value) -> Value {
    let raw = raw
        .as_array()
        .into_iter()
        .flatten()
        .map(|entry| {
            let mut row = if let Some(name) = entry.as_str() {
                json!({"name":name})
            } else {
                entry.clone()
            };
            if row["name"] == "compact" {
                row["kind"] = json!("action");
            }
            row
        })
        .collect::<Vec<_>>();
    let mut rows = super::super::commands::with_aliases(&json!(raw))
        .as_array()
        .unwrap()
        .clone();
    rows.retain(|row| {
        !NAVIGATION.contains(&row["name"].as_str().unwrap_or("")) && row["name"] != "permissions" && row["name"] != "permissions:mode" && row["name"] != "permissions:reset"
    });
    rows.truncate(247);
    rows.push(approval_policy::reset_command());
    rows.push(json!({"name":"permissions:mode","description":"Change this session's tool permission mode",
        "argumentHint":"[manual|plan|acceptEdits|dontAsk|reset-reads]","kind":"action",
        "argumentChoices":MODES.iter().map(|mode|json!({"value":mode,"label":mode})).collect::<Vec<_>>()}));
    for (name, description) in [
        ("permissions", "Read this session's permission state"),
        ("session:models", "Read this session's model selection"),
        ("session:mcp", "Read MCP connection state"),
        ("session:context", "Read context window usage"),
    ] {
        rows.retain(|row| row["name"] != name);
        rows.push(super::super::commands::query(name, description));
    }
    super::super::commands::with_status(json!(rows))
}
pub(super) fn refresh(prior: &Value, raw: &Value) -> Value {
    let mut rows = prior.as_array().unwrap().clone();
    for row in super::super::commands::with_aliases(raw)
        .as_array()
        .unwrap()
    {
        if !rows.iter().any(|old| old["name"] == row["name"]) {
            rows.push(row.clone());
        }
    }
    catalog(&json!(rows))
}
impl Claude {
    pub(super) async fn query(&self, args: &Value) -> Result<Value, Error> {
        let catalog = json!({"commands":catalog(&json!([]))});
        let name = super::super::commands::verify_query(args, &catalog)?;
        self.query_command(args, name).await
    }
    pub(super) async fn commands(&self, args: &Value) -> Result<Value, Error> {
        if let Some(id) = args["sessionId"].as_str() {
            self.session(id).await?;
            if let Some(process) = self.sessions.get(id).await {
                return Ok(json!({"commands":process.commands.lock().unwrap().clone()}));
            }
        }
        Ok(json!({"commands":self.discover(args, true).await?["commands"]}))
    }
    pub(super) async fn command(&self, args: &Value) -> Result<Value, Error> {
        let name = string(args, "command")?;
        let catalog = self.commands(args).await?;
        let descriptor = catalog["commands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == name)
            .ok_or_else(Error::unsupported)?;
        super::super::commands::verify(args, descriptor)?;
        if descriptor["execution"] == "query" {
            return self.query_command(args, name).await;
        }
        let process = self.owned(string(args, "sessionId")?).await?;
        if !process
            .commands
            .lock()
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == name)
        {
            return Err(Error::unsupported());
        }
        let input = super::super::commands::arguments(args)?;
        if name == "permissions:reset" {
            let result = super::super::commands::result(args, json!({"sessionGrant":false,"rememberedReadCount":0}))?;
            process.clear_read_grants(true).await?;
            return Ok(result);
        }
        if name == "permissions:mode" {
            return self.permissions(&process, args, input.trim()).await;
        }
        let mut turn = args.clone();
        turn["prompt"] = json!(format!("/{name} {input}").trim_end());
        self.turn(&turn).await
    }
    async fn query_command(&self, args: &Value, name: &str) -> Result<Value, Error> {
        let id = string(args, "sessionId")?;
        let value = match name {
            "status" | "session:status" => {
                return super::super::commands::status(args, &self.session(id).await?)
            }
            "permissions" => {
                let session = self.session(id).await?;
                let mut status = approval_policy::status(&self.events.lock().unwrap(), Driver::Claude, id);
                status["mode"] = session["permissionMode"].clone();
                status["rememberedReadCount"] = session["rememberedReadCount"].clone();
                status["reset"] = json!("/permissions:reset");
                status
            }
            "session:models" => self.models(args).await?,
            "session:mcp" => {
                let value = self
                    .owned(id)
                    .await?
                    .control(json!({"subtype":"mcp_status"}))
                    .await?;
                json!(value["mcpServers"].as_array().ok_or_else(Error::unsupported)?.iter().map(|row|json!({"name":row["name"],"status":row["status"],"toolCount":row["tools"].as_array().map(|tools|tools.len())})).collect::<Vec<_>>())
            }
            "session:context" => {
                let source = self
                    .owned(id)
                    .await?
                    .control(json!({"subtype":"get_context_usage"}))
                    .await?;
                if !source["totalTokens"].is_number() || !source["maxTokens"].is_number() {
                    return Err(Error::unsupported());
                }
                let mut value = json!({});
                for key in [
                    "totalTokens",
                    "maxTokens",
                    "percentage",
                    "model",
                    "isAutoCompactEnabled",
                ] {
                    value[key] = source[key].clone();
                }
                value["categories"] = json!(source["categories"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|row| json!({"name":row["name"],"tokens":row["tokens"]}))
                    .collect::<Vec<_>>());
                value
            }
            _ => return Err(Error::unsupported()),
        };
        super::super::commands::result(args, value)
    }
    async fn permissions(
        &self,
        process: &process::Process,
        args: &Value,
        mode: &str,
    ) -> Result<Value, Error> {
        if args.get("selection").is_some() {
            return Err(Error::unsupported());
        }
        if process.session.lock().unwrap()["status"] == "active" {
            return Err(Error::busy());
        }
        if mode == "reset-reads" {
            process.clear_read_grants(false).await?;
        } else if !mode.is_empty() {
            if !MODES.contains(&mode) {
                return Err(Error::invalid());
            }
            process
                .control(json!({"subtype":"set_permission_mode","mode":mode}))
                .await?;
            process.session.lock().unwrap()["permissionMode"] = json!(mode);
        }
        let current = process.session.lock().unwrap()["permissionMode"]
            .as_str()
            .unwrap_or("manual")
            .to_owned();
        Ok(
            json!({"commandId":args["requestId"],"status":"completed","output":format!("Permission mode: {current}\n/permissions:mode [{} | reset-reads]\nSession only; project authorization still applies.", MODES.join(" | "))}),
        )
    }
}
