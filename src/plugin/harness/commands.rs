//! Bounded native command descriptors; discovery never grants execution authority.
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn arguments(args: &Value) -> Result<&str, crate::core::error::Error> {
    match args.get("arguments") {
        None => Ok(""),
        Some(Value::String(input)) if input.len() <= 128 * 1024 => Ok(input),
        _ => Err(crate::core::error::Error::invalid()),
    }
}

pub fn normalize(value: &Value) -> Value {
    let mut names = std::collections::BTreeSet::new();
    let commands = value.as_array().into_iter().flatten().take(256).filter_map(|entry| {
        let name = entry.as_str().or(entry["name"].as_str())?.trim_start_matches('/');
        if !valid_name(name) || !names.insert(name.to_owned()) { return None; }
        let mut row = json!({"name":name,"description":entry["description"].as_str().unwrap_or("").chars().take(2048).collect::<String>(),
            "argumentHint":entry["argumentHint"].as_str().unwrap_or("").chars().take(256).collect::<String>(),
            "kind":if entry["kind"] == "action" {"action"} else {"prompt"},
            "execution":if entry["execution"] == "query" {"query"} else {"serial"}});
        if let Some(id) = entry["definitionId"].as_str().or(entry["id"].as_str()) {
            row["id"] = json!(id.chars().take(256).collect::<String>());
        }
        row["revision"] = json!(format!("{:x}", Sha256::digest(row.to_string().as_bytes())));
        Some(row)
    }).collect::<Vec<_>>();
    json!(commands)
}

pub fn query(name: &str, description: &str) -> Value {
    json!({"name":name,"description":description,"argumentHint":"","kind":"action","execution":"query"})
}
pub fn verify_query<'a>(
    args: &'a Value,
    catalog: &Value,
) -> Result<&'a str, crate::core::error::Error> {
    let invalid = crate::core::error::Error::invalid;
    let id = args["requestId"].as_str().ok_or_else(invalid)?;
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(invalid());
    }
    let name = args["command"].as_str().ok_or_else(invalid)?;
    let row = catalog["commands"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| {
            row["name"] == name
                && row["execution"] == "query"
                && row["kind"] == "action"
                && row["disabled"] != true
        })
        .ok_or_else(crate::core::error::Error::unsupported)?;
    verify(args, row)?;
    Ok(name)
}
pub fn verify(args: &Value, descriptor: &Value) -> Result<(), crate::core::error::Error> {
    if descriptor["execution"] == "query"
        && (!arguments(args)?.trim().is_empty() || args.get("selection").is_some())
    {
        return Err(crate::core::error::Error::invalid());
    }
    let Some(expected) = args.get("expectedCommand") else {
        return Ok(());
    };
    for key in ["id", "revision"] {
        if !expected[key].is_null() && expected[key] != descriptor[key] {
            return Err(crate::core::error::Error::conflict(
                "HARNESS_COMMAND_CHANGED",
            ));
        }
    }
    Ok(())
}
pub fn with_status(rows: Value) -> Value {
    let mut rows = rows.as_array().cloned().unwrap_or_default();
    rows.retain(|row| row["name"] != "session:status");
    rows.truncate(254);
    if !rows.iter().any(|row| row["name"] == "status") {
        rows.push(query("status", "Read this session's current runtime state"));
    }
    rows.push(query(
        "session:status",
        "Read this session's current runtime state and observation scope",
    ));
    normalize(&json!(rows))
}
pub fn result(args: &Value, value: Value) -> Result<Value, crate::core::error::Error> {
    if !arguments(args)?.trim().is_empty() || args.get("selection").is_some() {
        return Err(crate::core::error::Error::invalid());
    }
    let output =
        serde_json::to_string_pretty(&value).map_err(|_| crate::core::error::Error::internal())?;
    if output.len() > 128 * 1024 {
        return Err(crate::core::error::Error::too_large(
            "HARNESS_COMMAND_OUTPUT_LIMIT",
        ));
    }
    Ok(json!({"commandId":args["requestId"],"output":output,"status":"completed"}))
}
pub fn status(args: &Value, session: &Value) -> Result<Value, crate::core::error::Error> {
    let mut value = json!({"scope":if session["owned"] == true {"controlled"} else {"external"},
        "loaded":session.get("loaded").cloned().unwrap_or_else(|| if session["status"] == "notLoaded" {json!(false)} else if session["owned"] == true {json!(true)} else {Value::Null}), "observation":"native-runtime"});
    for key in [
        "id",
        "status",
        "statusDetails",
        "activeTurnId",
        "lastTurnResult",
        "lastTurnId",
        "owned",
        "resumable",
        "updatedAt",
        "modelSelection",
        "permissionMode",
    ] {
        if !session[key].is_null() {
            value[key] = session[key].clone();
        }
    }
    result(args, value)
}
/// SDK aliases are executable names, not extra model prompts or UI actions.
pub fn with_aliases(value: &Value) -> Value {
    let mut entries = Vec::new();
    for entry in value.as_array().into_iter().flatten().take(256) {
        entries.push(entry.clone());
        for alias in entry["aliases"].as_array().into_iter().flatten().take(32) {
            if let Some(name) = alias.as_str().filter(|name| valid_name(name)) {
                let mut row = entry.clone();
                row["name"] = json!(name);
                entries.push(row);
            }
        }
    }
    normalize(&json!(entries))
}
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || "_:-.".contains(c))
}
