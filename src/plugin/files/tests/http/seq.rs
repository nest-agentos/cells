use crate::common::{app_export, body_json, request};
use axum::{body::Body, http::StatusCode};
use cells::fs::Export;
use serde_json::{json,Value};
use tower::ServiceExt;

#[tokio::test]
async fn seq_transactions_remain_atomic_during_unrelated_native_and_command_execution() {
    let root=tempfile::tempdir().unwrap();
    let state=crate::common::state(Export::exclusive(root.path()).unwrap(),None,vec!["docs".into()],vec!["docs".into()]);
    let _native=state.files.activity_for(Some("other-session"),Some("other-turn")).unwrap();
    let _command=state.files.command_now("other-command").unwrap();
    let server=crate::common::router_for(state);
    let transaction=|id:&str,revision:Value,value:&str|request("/v1/fs/docs/seq/transaction").method("POST")
        .header("content-type","application/json").header("x-operation-id",id)
        .body(Body::from(json!({"path":"info.seq","expectedRevision":revision,"changes":[{"action":"set","key":"name","value":value}]}).to_string())).unwrap();
    let first=server.clone().oneshot(transaction("first",Value::Null,"original")).await.unwrap();
    assert_eq!(first.status(),StatusCode::OK);assert_eq!(body_json(first).await["outcome"],"committed");
    let snapshot=server.clone().oneshot(request("/v1/fs/docs/seq/snapshot").method("POST").header("content-type","application/json")
        .body(Body::from(r#"{"path":"info.seq"}"#)).unwrap()).await.unwrap();
    let revision=body_json(snapshot).await["revision"].clone();assert!(revision.is_string());
    let (left,right)=tokio::join!(server.clone().oneshot(transaction("left",revision.clone(),"left")),server.oneshot(transaction("right",revision,"right")));
    let codes=[left.unwrap().status(),right.unwrap().status()];
    assert_eq!(codes.iter().filter(|&&code|code==StatusCode::OK).count(),1);
    assert_eq!(codes.iter().filter(|&&code|code==StatusCode::PRECONDITION_FAILED).count(),1);
}

#[tokio::test]
async fn seq_routes_enforce_authorization_and_write_permissions() {
    let root = tempfile::tempdir().unwrap();
    let server = app_export(Export::exclusive(root.path()).unwrap());
    let update = json!({"path":"info.seq","expectedRevision":null,"changes":[{"action":"set","key":"name","value":"Project"}]}).to_string();
    let response = server
        .clone()
        .oneshot(
            request("/v1/fs/docs/seq/transaction")
                .method("POST")
                .header("content-type", "application/json")
                .header("x-operation-id", "seq-create")
                .body(Body::from(update.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_json(response).await["outcome"], "committed");
    let response = server
        .clone()
        .oneshot(
            request("/v1/fs/docs/seq/snapshot")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"path":"info.seq"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(body_json(response).await["entries"][0]["value"], "Project");
    let readonly = app_export(Export::open(root.path()).unwrap());
    let response = readonly
        .oneshot(
            request("/v1/fs/docs/seq/transaction")
                .method("POST")
                .header("content-type", "application/json")
                .header("x-operation-id", "readonly")
                .body(Body::from(update))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = server
        .oneshot(
            request("/v1/fs/unknown/seq/snapshot")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"path":"info.seq"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn selective_queries_and_body_routes_enforce_project_access() {
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let server = app_export(Export::exclusive(root.path()).unwrap());
    let text = "immutable text";
    let hash = format!("{:x}", Sha256::digest(text.as_bytes()));
    for (route, input, operation) in [
        (
            "body/put",
            json!({"path":"info.seq","sha256":hash,"text":text}),
            "body-write",
        ),
        (
            "body",
            json!({"path":"info.seq","sha256":hash}),
            "body-read",
        ),
        (
            "query",
            json!({"path":"missing.seq","selection":{"keys":["only-this"]}}),
            "key-read",
        ),
    ] {
        let response = server
            .clone()
            .oneshot(
                request(&format!("/v1/fs/docs/seq/{route}"))
                    .method("POST")
                    .header("content-type", "application/json")
                    .header("x-operation-id", operation)
                    .body(Body::from(input.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        if route == "body" {
            assert_eq!(body["text"], text);
        }
        if route == "query" {
            assert_eq!(body["entries"], json!([]));
        }
        let response = server
            .clone()
            .oneshot(
                request(&format!("/v1/fs/unknown/seq/{route}"))
                    .method("POST")
                    .header("content-type", "application/json")
                    .header("x-operation-id", "forbidden")
                    .body(Body::from(input.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    let readonly = app_export(Export::open(root.path()).unwrap());
    let response = readonly
        .oneshot(
            request("/v1/fs/docs/seq/body/put")
                .method("POST")
                .header("content-type", "application/json")
                .header("x-operation-id", "readonly-body")
                .body(Body::from(
                    json!({"path":"info.seq","sha256":hash,"text":text}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}
