#[path = "../../common/tests/support/mod.rs"]
mod common;
use axum::{body::Body, http::StatusCode};
use cells::{
    app::{Application, State},
    auth::{Auth, Client},
    core::error::Error,
    fs::Exports,
    plugin::files::source::*,
    plugins::{PluginRegistry, PluginSettings},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tower::ServiceExt;

struct MemorySource {
    calls: Arc<AtomicUsize>,
}
fn stat() -> Stat {
    Stat {
        kind: "file",
        size: 6,
        modified_at: 0,
        created_at: 0,
        revision: Some("\"memory-v1\"".into()),
        metadata: Default::default(),
        source_identity: None,
    }
}
impl FileSource for MemorySource {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            strong_revision: true,
            ..Default::default()
        }
    }
    fn stat<'a>(
        &'a self,
        paths: Vec<String>,
        _: ReadFields,
        _: ReadContext,
    ) -> SourceFuture<'a, Vec<Result<Option<Stat>, Error>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if paths.iter().any(|path| path == "mismatch") {
                return Ok(vec![]);
            }
            Ok(paths
                .into_iter()
                .map(|path| {
                    assert!(!path.contains(".."));
                    if path == "absent" {
                        Ok(None)
                    } else {
                        Ok(Some(stat()))
                    }
                })
                .collect())
        })
    }
    fn list<'a>(&'a self, _: String, _: ReadFields, _: ReadContext) -> SourceFuture<'a, Listing> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(Listing {
                entries: vec![Entry {
                    name: "a.txt".into(),
                    stat: stat(),
                }],
                warnings: vec![],
            })
        })
    }
    fn open<'a>(&'a self, path: String, _: ReadContext) -> SourceFuture<'a, OpenContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if path == "slow" {
                std::future::pending::<()>().await;
            }
            if path == "failed" {
                return Err(Error::unavailable());
            }
            Ok(OpenContent {
                size: 6,
                revision: Some(if path == "invalid-revision" {
                    "invalid\nrevision".into()
                } else {
                    "\"memory-v1\"".into()
                }),
                content: Box::new(MemoryContent(if path == "truncated" {
                    b"abc"
                } else {
                    b"abcdef"
                })),
            })
        })
    }
}
struct MemoryContent(&'static [u8]);
impl Content for MemoryContent {
    fn read(self: Box<Self>, offset: u64, length: u64) -> SourceFuture<'static, Reader> {
        Box::pin(async move {
            use tokio::io::AsyncReadExt;
            Ok(Box::pin(
                std::io::Cursor::new(self.0.get(offset as usize..).unwrap_or_default().to_vec())
                    .take(length),
            ) as Reader)
        })
    }
}
fn application(allowed: bool) -> (Application, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut sources = Sources::default();
    sources
        .register(
            "remote",
            Arc::new(MemorySource {
                calls: calls.clone(),
            }),
        )
        .unwrap();
    let auth = Auth::new(
        None,
        vec![Client::new(
            common::SECRET.into(),
            None,
            if allowed {
                vec!["remote".into()]
            } else {
                vec![]
            },
            vec![],
        )],
    );
    let mut host = State::host(auth, Exports::default()).unwrap();
    host.services.insert(Arc::new(sources)).unwrap();
    // The product catalog supplies HTTP adapters; an isolated host may reuse them explicitly.
    let mut registry = PluginRegistry::empty();
    registry
        .register(Arc::new(cells::plugin::files::Plugin))
        .unwrap();
    let mut routes = cells::http::HttpRoutes::default();
    routes
        .register("files", cells::plugin::files::http::routes())
        .unwrap();
    host.services.insert(Arc::new(routes)).unwrap();
    (
        Application::assemble(host, registry, &PluginSettings::default()).unwrap(),
        calls,
    )
}
#[tokio::test]
async fn existing_file_protocol_reads_a_non_posix_source_without_local_exports() {
    let (app, calls) = application(true);
    let router = app.router().unwrap();
    let response = router
        .clone()
        .oneshot(
            common::request("/v1/fs/remote/content?path=a.txt")
                .header("range", "bytes=1-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["etag"], "\"memory-v1\"");
    assert_eq!(common::body_bytes(response).await, b"bcd");
    let response = router
        .clone()
        .oneshot(
            common::request("/v1/fs/remote/stat")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"paths":["a.txt","../escape","absent"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let results = common::body_json(response).await;
    assert_eq!(results["results"][0]["stat"]["size"], 6);
    assert_eq!(results["results"][1]["error"], "EINVAL");
    assert!(results["results"][2]["stat"].is_null());
    let response = router
        .clone()
        .oneshot(
            common::request("/v1/fs/remote/entries?path=")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        common::body_json(response).await["entries"][0]["name"],
        "a.txt"
    );
    let response = router
        .oneshot(common::request("/v1/exports").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let exports = common::body_json(response).await;
    assert_eq!(exports["exports"][0]["access"], "ro");
    assert_eq!(exports["exports"][0]["metadata"], false);
    assert_eq!(exports["exports"][0]["strongRevision"], true);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    app.shutdown().await.unwrap();
}
#[tokio::test]
async fn remote_sources_enforce_grants_paths_preconditions_and_deadlines() {
    let (denied, calls) = application(false);
    let response = denied
        .router()
        .unwrap()
        .oneshot(
            common::request("/v1/fs/remote/content?path=a.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (app, _) = application(true);
    for (path, status) in [("../escape", 400), ("slow", 504), ("failed", 503)] {
        let response = app
            .router()
            .unwrap()
            .oneshot(
                common::request(&format!("/v1/fs/remote/content?path={path}"))
                    .header("x-timeout-ms", "10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
    }
    let response = app
        .router()
        .unwrap()
        .oneshot(
            common::request("/v1/fs/remote/content?path=a.txt")
                .header("if-match", "\"old\"")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    let response = app
        .router()
        .unwrap()
        .oneshot(
            common::request("/v1/fs/remote/mutate")
                .method("POST")
                .header("content-type", "application/json")
                .header("x-operation-id", "remote-write")
                .body(Body::from(r#"{"action":"mkdir","path":"new"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    app.shutdown().await.unwrap();
    denied.shutdown().await.unwrap();
}
#[tokio::test]
async fn streaming_retains_lifecycle_ownership_until_the_body_is_dropped() {
    let (app, _) = application(true);
    let response = app
        .router()
        .unwrap()
        .oneshot(
            common::request("/v1/fs/remote/content?path=a.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(10),
        app.state().files.drain()
    )
    .await
    .is_err());
    drop(response);
    tokio::time::timeout(std::time::Duration::from_secs(1), app.state().files.drain())
        .await
        .unwrap();
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_truncated_source_fails_the_body_and_releases_its_lease() {
    use http_body_util::BodyExt;
    let (app, _) = application(true);
    let response = app
        .router()
        .unwrap()
        .oneshot(
            common::request("/v1/fs/remote/content?path=truncated")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.into_body().collect().await.is_err());
    tokio::time::timeout(std::time::Duration::from_secs(1), app.state().files.drain())
        .await
        .unwrap();
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_source_cannot_return_a_misleading_partial_stat_batch() {
    let (app, _) = application(true);
    let response = app
        .router()
        .unwrap()
        .oneshot(
            common::request("/v1/fs/remote/stat")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"paths":["mismatch"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        common::body_json(response).await["code"],
        "FILE_SOURCE_RESULT_MISMATCH"
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_source_validators_return_a_diagnostic_instead_of_panicking() {
    let (app, _) = application(true);
    let response = app
        .router()
        .unwrap()
        .oneshot(
            common::request("/v1/fs/remote/content?path=invalid-revision")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        common::body_json(response).await["code"],
        "FILE_SOURCE_INVALID_REVISION"
    );
    app.shutdown().await.unwrap();
}
