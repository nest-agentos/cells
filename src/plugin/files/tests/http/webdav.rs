use crate::common::{body_bytes, body_json, mutate_request, request, router_for, SECRET};
use axum::{
    body::Body,
    http::{HeaderValue, StatusCode},
    response::Response,
    Router,
};
use cells::{app::State, config::Config};
use std::sync::Arc;
use tower::ServiceExt;

const LOCK: &str = r#"<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>"#;

fn fixture() -> (tempfile::TempDir, Arc<State>, Router) {
    let root = tempfile::tempdir().unwrap();
    let docs = root.path().join("docs");
    let legacy = root.path().join("legacy");
    std::fs::create_dir(&docs).unwrap();
    std::fs::create_dir(&legacy).unwrap();
    let config: Config = toml::from_str(&format!(
        r#"
listen = "127.0.0.1:0"
execution = false
token = "{SECRET}"
[plugin_options.files]
webdav = true
[[exports]]
alias = "docs"
path = {docs:?}
access = "rw"
[[exports]]
alias = "legacy"
path = {legacy:?}
access = "rw"
webdav_locks = true
"#
    ))
    .unwrap();
    let state = State::from_config(&config).unwrap();
    let app = router_for(state.clone());
    (root, state, app)
}

async fn call(
    app: &Router,
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Response {
    let mut req = request(url).method(method);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    app.clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap()
}

async fn revision(app: &Router, url: &str) -> HeaderValue {
    let response = call(app, "GET", url, &[], "").await;
    assert_eq!(response.status(), StatusCode::OK);
    response.headers()["etag"].clone()
}

#[tokio::test]
async fn conditional_export_advertises_versions_and_rejects_lock_requests() {
    let (_root, _state, app) = fixture();
    let options = call(&app, "OPTIONS", "/dav/docs/", &[], "").await;
    assert_eq!(options.headers()["dav"], "1");
    assert_eq!(
        options.headers()["x-cells-dav-structural-conditions"],
        "if-match"
    );
    assert!(!options.headers()["allow"]
        .to_str()
        .unwrap()
        .contains("LOCK"));
    let properties = call(&app, "PROPFIND", "/dav/docs/", &[("depth", "0")], "").await;
    assert_eq!(properties.status(), StatusCode::MULTI_STATUS);
    let properties = String::from_utf8(body_bytes(properties).await).unwrap();
    assert!(!properties.contains("<D:lockentry>"));
    let reply = call(&app, "LOCK", "/dav/docs/no-file", &[("depth", "0")], LOCK).await;
    assert_eq!(reply.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body_json(reply).await["code"], "DAV_LOCK_DISABLED");
    assert_eq!(
        call(&app, "GET", "/dav/docs/no-file", &[], "")
            .await
            .status(),
        404
    );
}

#[tokio::test]
async fn active_native_turn_does_not_block_decision_directory_or_conditional_dav_writes() {
    let (_root, state, app) = fixture();
    let activity = state
        .files
        .activity_for(Some("session-a"), Some("turn-a"))
        .unwrap();
    let mkdir = app
        .clone()
        .oneshot(mutate_request(
            "decision-start",
            r#"{"action":"mkdir","path":"decision"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(mkdir.status(), StatusCode::OK);
    let note = "/dav/docs/decision/note";
    assert_eq!(
        call(&app, "PUT", note, &[("if-none-match", "*")], "start")
            .await
            .status(),
        201
    );
    let original = revision(&app, note).await;
    let condition = [("if-match", original.to_str().unwrap())];
    let (one, two) = tokio::join!(
        call(&app, "PUT", note, &condition, "one"),
        call(&app, "PUT", note, &condition, "two"),
    );
    let mut statuses = [one.status().as_u16(), two.status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [204, 412]);
    let legacy = call(&app, "LOCK", "/dav/legacy/file", &[("depth", "0")], LOCK).await;
    assert_eq!(legacy.status(), StatusCode::LOCKED);
    drop(activity);
}

#[tokio::test]
async fn structural_changes_require_source_version_and_do_not_replace_destinations() {
    let (_root, _state, app) = fixture();
    let note = "/dav/docs/note";
    for url in [note, "/dav/docs/occupied"] {
        assert_eq!(
            call(&app, "PUT", url, &[("if-none-match", "*")], "original")
                .await
                .status(),
            201
        );
    }
    assert_eq!(call(&app, "DELETE", note, &[], "").await.status(), 428);
    let current = revision(&app, note).await;
    let source = current.to_str().unwrap();
    let move_note = |to, etag| [("destination", to), ("if-match", etag)];
    assert_eq!(
        call(
            &app,
            "MOVE",
            note,
            &move_note("/dav/docs/renamed", "\"stale\""),
            ""
        )
        .await
        .status(),
        412
    );
    assert_eq!(
        call(
            &app,
            "MOVE",
            note,
            &move_note("/dav/docs/occupied", source),
            ""
        )
        .await
        .status(),
        412
    );
    let overwrite = [
        ("destination", "/dav/docs/occupied"),
        ("if-match", source),
        ("overwrite", "T"),
    ];
    assert_eq!(call(&app, "MOVE", note, &overwrite, "").await.status(), 428);
    assert_eq!(
        call(
            &app,
            "MOVE",
            note,
            &move_note("/dav/docs/renamed", source),
            ""
        )
        .await
        .status(),
        201
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            "/dav/docs/renamed",
            &[("if-match", "\"stale\"")],
            ""
        )
        .await
        .status(),
        412
    );
    let renamed = revision(&app, "/dav/docs/renamed").await;
    assert_eq!(
        call(
            &app,
            "DELETE",
            "/dav/docs/renamed",
            &[("if-match", renamed.to_str().unwrap())],
            ""
        )
        .await
        .status(),
        204
    );
    assert_eq!(
        body_bytes(call(&app, "GET", "/dav/docs/occupied", &[], "").await).await,
        b"original"
    );
}

#[tokio::test]
async fn opted_in_leases_protect_the_target_without_blocking_other_file_exports() {
    let (_root, state, app) = fixture();
    let options = call(&app, "OPTIONS", "/dav/legacy/", &[], "").await;
    assert_eq!(options.headers()["dav"], "1, 2");
    let locked = call(&app, "LOCK", "/dav/legacy/note", &[("depth", "0")], LOCK).await;
    assert_eq!(locked.status(), StatusCode::CREATED);
    let token = locked.headers()["lock-token"].to_str().unwrap();
    assert!(state.files.activity().is_err());
    let (_another_root, other, _other_app) = fixture();
    assert!(other.files.activity().is_ok());
    assert_eq!(
        call(&app, "MKCOL", "/dav/docs/independent", &[], "")
            .await
            .status(),
        201
    );
    let current = revision(&app, "/dav/legacy/note").await;
    let response = call(
        &app,
        "PUT",
        "/v1/fs/legacy/content?path=note",
        &[
            ("if-match", current.to_str().unwrap()),
            ("x-operation-id", "native-replace"),
        ],
        "wrong",
    )
    .await;
    assert_eq!(response.status(), StatusCode::LOCKED);
    assert_eq!(
        call(
            &app,
            "PUT",
            "/dav/legacy/note",
            &[("if", &format!("({token})"))],
            "owner"
        )
        .await
        .status(),
        204
    );
    assert_eq!(
        call(
            &app,
            "UNLOCK",
            "/dav/legacy/note",
            &[("lock-token", token)],
            ""
        )
        .await
        .status(),
        204
    );
    assert!(state.files.activity().is_ok());
}
