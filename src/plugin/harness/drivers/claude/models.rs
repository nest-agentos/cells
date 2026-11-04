use super::*;

fn model(row: &Value) -> Result<Value, Error> {
    let efforts = if row["supportsEffort"] == true {
        row["supportedEffortLevels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str())
            .map(|id| json!({"id":id,"label":id}))
            .collect()
    } else {
        Vec::new()
    };
    let mut model = super::super::models::model(
        string(row, "value")?,
        string(row, "displayName")?,
        efforts,
        None,
    );
    if row["resolvedModel"].is_string() {
        model["resolvedModel"] = row["resolvedModel"].clone();
    }
    Ok(model)
}
pub(super) fn catalog(rows: &Value) -> Result<Value, Error> {
    let rows = rows
        .as_array()
        .filter(|rows| rows.len() <= 512)
        .ok_or_else(Error::invalid)?;
    let models = rows.iter().map(model).collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"models":models}))
}
impl Claude {
    pub(super) async fn models(&self, args: &Value) -> Result<Value, Error> {
        if let Some(id) = args["sessionId"].as_str() {
            self.session(id).await?;
            if let Some(process) = self.sessions.get(id).await {
                return Ok(process.models.lock().unwrap().clone());
            }
        }
        Ok(self.discover(args, false).await?["models"].clone())
    }
    pub(super) async fn select_model(
        &self,
        process: &process::Process,
        args: &Value,
    ) -> Result<(), Error> {
        let catalog = process.models.lock().unwrap().clone();
        let Some(selected) = super::super::models::selection(args, &catalog)? else {
            return Ok(());
        };
        process
            .control(json!({"subtype":"set_model","model":selected["modelId"]}))
            .await?;
        // Only session-scoped settings are writable through this port.
        process.control(json!({"subtype":"apply_flag_settings","settings":{"effortLevel":selected.get("reasoningEffort").cloned().unwrap_or(Value::Null)}})).await?;
        process.models.lock().unwrap()["current"] = selected.clone();
        Ok(())
    }
}
