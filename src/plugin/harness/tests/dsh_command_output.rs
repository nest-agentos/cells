//! Large native responses retain their original receipt while returning bounded previews.
use super::{fixture, mutate, service};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use sha2::{Digest, Sha256};

#[tokio::test]
async fn large_unicode_result_keeps_native_failure_and_original_callback_ownership() {
    let root = tempfile::tempdir().unwrap();
    let host = fixture::Host::new(root.path()).await;
    let service = service(root.path(), &host.url);
    mutate(
        &service,
        "harness_resume",
        "output-resume",
        json!({"sessionId":"existing"}),
    )
    .await;
    let text = "界😀".repeat(40_000);
    *host.state.command_result.lock().unwrap() =
        Some(json!({"commandId":"large-native","result":{"kind":"error","text":text}}));
    let args = json!({"sessionId":"existing","command":"compact","outputMode":"reference"});
    let receipt = mutate(&service, "harness_command", "large-output", args.clone()).await;
    assert_eq!(receipt["outcome"], "committed");
    assert_eq!(receipt["result"]["failed"], true);
    let result = &receipt["result"];
    let preview = result["output"].as_str().unwrap();
    assert!(preview.len() <= 65536);
    assert!(text.starts_with(preview));
    let reference = &result["outputRef"];
    assert_eq!(reference["bytes"], text.len());
    assert_eq!(
        reference["sha256"],
        format!("{:x}", Sha256::digest(text.as_bytes()))
    );
    assert_eq!(
        mutate(&service, "harness_command", "large-output", args.clone()).await,
        receipt
    );
    assert_eq!(
        service
            .call(
                "harness_operation",
                json!({"profileId":"dsh","epoch":"epoch","requestId":"large-output","outputMode":"reference"})
            )
            .await
            .unwrap(),
        receipt
    );
    let mut read = reference.clone();
    read["profileId"] = json!("dsh");
    read["offset"] = json!(1);
    read["limit"] = json!(65536);
    let chunk = service.call("harness_command_output", read).await.unwrap();
    assert_eq!(
        STANDARD.decode(chunk["data"].as_str().unwrap()).unwrap(),
        &text.as_bytes()[1..65537]
    );
    *host.state.command_result.lock().unwrap() = None;
    assert_eq!(
        mutate(&service, "harness_command", "after-large", args).await["outcome"],
        "committed"
    );
    assert_eq!(
        host.state
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["method"] == "commands/execute")
            .count(),
        2
    );
    assert_eq!(
        service.profiles()["profiles"][0]["capabilities"]["commandOutputs"],
        true
    );
    service.close().await;
}
