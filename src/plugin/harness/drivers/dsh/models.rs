use super::*;

fn identity(provider: &str, model: &str) -> String {
    json!([provider, model]).to_string()
}
fn selection(value: &Value) -> Value {
    let Some(provider) = value["provider"].as_str() else {
        return Value::Null;
    };
    let Some(model) = value["model"].as_str() else {
        return Value::Null;
    };
    let mut result = json!({"modelId":identity(provider,model)});
    if value["reasoningEffort"].is_string() {
        result["reasoningEffort"] = value["reasoningEffort"].clone();
    }
    result
}
fn model(provider: &str, group: &str, row: &Value) -> Result<Value, Error> {
    let efforts = row["reasoning"]["efforts"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|e| json!({"id":e["id"],"label":e["name"]}))
        .collect();
    Ok(super::super::models::model(
        &identity(provider, string(row, "id")?),
        &format!("{} · {}", group, string(row, "name")?),
        efforts,
        row["reasoning"]["defaultEffort"].as_str(),
    ))
}
fn catalog(native: &Value) -> Result<Vec<Value>, Error> {
    let mut models = Vec::new();
    for group in native["groups"].as_array().ok_or_else(Error::invalid)? {
        let provider = string(group, "id")?;
        if !native["routableProviders"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row == provider))
        {
            continue;
        }
        for row in group["models"].as_array().ok_or_else(Error::invalid)? {
            models.push(model(provider, string(group, "name")?, row)?);
            if models.len() > 512 {
                return Err(Error::invalid());
            }
        }
    }
    Ok(models)
}
impl Dsh {
    pub(super) async fn models(
        &self,
        client: &transport::Client,
        args: &Value,
    ) -> Result<Value, Error> {
        let current = if let Some(id) = args["sessionId"].as_str() {
            let session = self.summary(client, id).await?;
            let state = &session["modelSelection"];
            selection(if state["next"].is_object() {
                &state["next"]
            } else {
                &state["lastUsed"]
            })
        } else {
            Value::Null
        };
        let native = client.call("session/modelCatalog", json!({})).await?;
        let models = catalog(&native)?;
        Ok(json!({"models":models,"default":selection(&native["default"]),"current":current}))
    }
    pub(super) async fn prepare_model_selection(
        &self,
        client: &transport::Client,
        args: &Value,
    ) -> Result<Option<Value>, Error> {
        if args.get("selection").is_none() {
            return Ok(None);
        }
        let catalog = self.models(client, args).await?;
        let selected =
            super::super::models::selection(args, &catalog)?.ok_or_else(Error::invalid)?;
        let pair: Vec<String> =
            serde_json::from_str(string(selected, "modelId")?).map_err(|_| Error::invalid())?;
        if pair.len() != 2 {
            return Err(Error::invalid());
        }
        let mut request = json!({"sessionId":args["sessionId"],"provider":pair[0],"model":pair[1]});
        if selected["reasoningEffort"].is_string() {
            request["reasoningEffort"] = selected["reasoningEffort"].clone();
        }
        Ok(Some(request))
    }
}
