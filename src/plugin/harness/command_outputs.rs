//! Bounded views and scoped byte reads of output retained in the original receipt.
use crate::core::error::Error;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const MAX_CHUNK: usize = 64 * 1024;

pub(super) fn prepare(mut receipt: Value) -> Value {
    if receipt["outcome"] == "committed" {
        if let Some(output) = receipt["result"]["output"]
            .as_str()
            .filter(|text| text.len() > MAX_CHUNK)
        {
            let saved = metadata(&Value::Null, output);
            receipt["_commandOutput"] = saved;
        }
    }
    receipt
}

pub(super) fn reference_requested(args: &Value) -> Result<bool, Error> {
    match args.get("outputMode") {
        None => Ok(false),
        Some(value) if value == "reference" => Ok(true),
        _ => Err(Error::new(
            axum::http::StatusCode::BAD_REQUEST,
            "HARNESS_COMMAND_OUTPUT_MODE_INVALID",
        )),
    }
}
pub(super) fn project(mut receipt: Value, epoch: &str, referenced: bool) -> Value {
    if !referenced || receipt["outcome"] != "committed" {
        return public_receipt(receipt);
    }
    let Some(output) = receipt["result"]["output"]
        .as_str()
        .filter(|value| value.len() > MAX_CHUNK)
    else {
        return public_receipt(receipt);
    };
    let mut end = MAX_CHUNK;
    while !output.is_char_boundary(end) {
        end -= 1;
    }
    let preview = output[..end].to_owned();
    let reference = reference(
        epoch,
        receipt["requestId"].as_str().unwrap_or(""),
        output,
        &receipt["_commandOutput"],
    );
    receipt["result"]["output"] = json!(preview);
    receipt["result"]["outputRef"] = reference;
    public_receipt(receipt)
}
pub(super) fn public_receipt(mut receipt: Value) -> Value {
    if let Some(value) = receipt.as_object_mut() {
        value.remove("_commandOutput");
    }
    receipt
}

pub(super) fn chunk(receipt: Value, args: &Value, epoch: &str) -> Result<Value, Error> {
    if receipt["outcome"] != "committed" {
        return Err(Error::conflict("HARNESS_COMMAND_OUTPUT_PENDING"));
    }
    let output = receipt["result"]["output"].as_str().ok_or_else(missing)?;
    let id = receipt["requestId"].as_str().ok_or_else(Error::invalid)?;
    if args["requestId"] != id {
        return Err(Error::conflict("HARNESS_COMMAND_OUTPUT_CHANGED"));
    }
    let reference = reference(epoch, id, output, &receipt["_commandOutput"]);
    if args["sha256"] != reference["sha256"] || args["bytes"] != reference["bytes"] {
        return Err(Error::conflict("HARNESS_COMMAND_OUTPUT_CHANGED"));
    }
    let offset = integer(args, "offset", 0, output.len())?;
    let limit = integer(args, "limit", 1, MAX_CHUNK)?;
    let end = offset.saturating_add(limit).min(output.len());
    Ok(
        json!({"ref":reference,"offset":offset,"data":STANDARD.encode(&output.as_bytes()[offset..end]),
        "nextOffset":if end < output.len() {json!(end)} else {Value::Null}}),
    )
}

fn reference(epoch: &str, id: &str, output: &str, saved: &Value) -> Value {
    let identity = metadata(saved, output);
    json!({"version":1,"epoch":epoch,"requestId":id,"bytes":output.len(),
        "sha256":identity["sha256"],"mimeType":"text/plain"})
}
fn metadata(saved: &Value, output: &str) -> Value {
    if saved["bytes"].as_u64() == Some(output.len() as u64)
        && saved["sha256"].as_str().is_some_and(|sha| {
            sha.len() == 64
                && sha
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
    {
        return saved.clone();
    }
    json!({"bytes":output.len(),"sha256":format!("{:x}",Sha256::digest(output.as_bytes()))})
}
fn integer(args: &Value, key: &str, minimum: usize, maximum: usize) -> Result<usize, Error> {
    args[key]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| (minimum..=maximum).contains(value))
        .ok_or_else(Error::invalid)
}
pub(super) fn missing() -> Error {
    Error::not_found("HARNESS_COMMAND_OUTPUT_NOT_FOUND")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn receipt(output: &str) -> Value {
        json!({"outcome":"committed","requestId":"original","result":{"commandId":"native","output":output,"status":"completed","failed":true}})
    }
    #[test]
    fn large_unicode_output_has_a_valid_preview_and_byte_chunks_reconstruct_the_original() {
        let text = "界😀".repeat(40_000);
        let raw = receipt(&text);
        let projected = project(raw.clone(), "epoch", true);
        let preview = projected["result"]["output"].as_str().unwrap();
        assert!(preview.len() <= MAX_CHUNK);
        assert!(text.starts_with(preview));
        assert_eq!(projected["result"]["failed"], true);
        assert_eq!(raw["result"]["output"], text);
        let mut args = projected["result"]["outputRef"].clone();
        args["offset"] = json!(0);
        args["limit"] = json!(MAX_CHUNK - 1);
        let mut bytes = Vec::new();
        loop {
            let result = chunk(raw.clone(), &args, "epoch").unwrap();
            bytes.extend(STANDARD.decode(result["data"].as_str().unwrap()).unwrap());
            if result["nextOffset"].is_null() {
                break;
            }
            args["offset"] = result["nextOffset"].clone();
        }
        assert_eq!(String::from_utf8(bytes).unwrap(), text);
    }
    #[test]
    fn small_output_and_unknown_receipts_remain_unchanged() {
        for raw in [
            receipt("small"),
            receipt(&"x".repeat(MAX_CHUNK)),
            json!({"outcome":"unknown","requestId":"original"}),
        ] {
            assert_eq!(project(raw.clone(), "epoch", true), raw);
        }
    }
    #[test]
    fn commit_metadata_keeps_the_native_result_unchanged_and_is_not_exposed_in_receipt_views() {
        let output = "raw body".repeat(20_000);
        let original = receipt(&output);
        let prepared = prepare(original.clone());
        assert_eq!(project(prepared.clone(), "epoch", false), original);
        assert_eq!(prepared["result"], original["result"]);
        assert_eq!(prepared["_commandOutput"]["bytes"], output.len());
        let view = project(prepared.clone(), "epoch", true);
        assert!(view.get("_commandOutput").is_none());
        assert_eq!(
            view["result"]["outputRef"]["sha256"],
            prepared["_commandOutput"]["sha256"]
        );
    }
    #[test]
    fn identity_and_range_failures_do_not_change_the_original_receipt() {
        let raw = receipt("content");
        let mut args = reference("epoch", "original", "content", &Value::Null);
        args["offset"] = json!(0);
        args["limit"] = json!(4);
        for (key, value) in [
            ("bytes", json!(99)),
            ("sha256", json!("0".repeat(64))),
            ("offset", json!(-1)),
            ("offset", json!(99)),
            ("limit", json!(0)),
            ("limit", json!(MAX_CHUNK + 1)),
            ("limit", json!(1.5)),
        ] {
            let mut invalid = args.clone();
            invalid[key] = value;
            assert!(chunk(raw.clone(), &invalid, "epoch").is_err());
        }
        assert_eq!(raw, receipt("content"));
    }
}
