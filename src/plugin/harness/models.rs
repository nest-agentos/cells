//! Presentation metadata and strict selection validation shared by native adapters.
use crate::core::error::Error;
use serde_json::{json, Value};

pub fn model(id: &str, label: &str, efforts: Vec<Value>, default: Option<&str>) -> Value {
    let default = default.filter(|id| efforts.iter().any(|effort| effort["id"] == *id));
    json!({"id":id,"label":label,"reasoningEfforts":efforts,"defaultReasoningEffort":default})
}

pub fn selection<'a>(args: &'a Value, catalog: &Value) -> Result<Option<&'a Value>, Error> {
    let Some(value) = args.get("selection") else {
        return Ok(None);
    };
    let fields = value.as_object().ok_or_else(Error::invalid)?;
    if fields
        .keys()
        .any(|key| !["modelId", "reasoningEffort"].contains(&key.as_str()))
    {
        return Err(Error::invalid());
    }
    let id = value["modelId"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 1024)
        .ok_or_else(Error::invalid)?;
    let row = catalog["models"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["id"] == id))
        .ok_or_else(Error::unsupported)?;
    validate_effort(value, row)?;
    Ok(Some(value))
}

fn validate_effort(value: &Value, row: &Value) -> Result<(), Error> {
    if let Some(effort) = value.get("reasoningEffort") {
        let effort = effort.as_str().ok_or_else(Error::invalid)?;
        if !row["reasoningEfforts"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == effort))
        {
            return Err(Error::unsupported());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn non_reasoning_models_do_not_advertise_an_unselectable_default_effort() {
        let plain = model("plain", "Plain", Vec::new(), Some("none"));
        assert!(plain["reasoningEfforts"].as_array().unwrap().is_empty());
        assert!(plain["defaultReasoningEffort"].is_null());
        let reasoning = model(
            "reasoning",
            "Reasoning",
            vec![json!({"id":"high","label":"High"})],
            Some("high"),
        );
        assert_eq!(reasoning["defaultReasoningEffort"], "high");
    }
}
