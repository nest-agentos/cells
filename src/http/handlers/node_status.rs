use crate::{app::State, core::error::Error, http::access};
use axum::{extract::State as AxumState, http::HeaderMap, Json};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn status(
    AxumState(state): AxumState<Arc<State>>,
    headers: HeaderMap,
) -> Result<Json<Value>, Error> {
    let identity = access::identity(&state, &headers)?;
    let client = state.auth.client(identity);
    if client.anonymous() {
        return Err(Error::forbidden("NODE_STATUS_REQUIRES_AUTHENTICATION"));
    }
    let exports = state
        .exports
        .iter()
        .filter(|(alias, _)| client.may_read(alias))
        .take(256)
        .map(|(alias, export)| (alias.clone(), export.clone()))
        .collect::<Vec<_>>();
    let mut value = state
        .workers
        .run_ungated(
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            move |_| {
                let mut value = crate::core::node_status::sample();
                value["exports"] = json!(exports
                    .iter()
                    .map(|(alias, export)| export.space(alias))
                    .collect::<Vec<_>>());
                Ok(value)
            },
        )
        .await?;
    value["version"] = json!(1);
    value["harness"] = if let Some(harness) = state.services.get::<crate::harness::Harnesses>() {
        harness.node_activity(client).await
    } else {
        json!({"activeSessions":0,"loadedRuntimes":0})
    };
    Ok(Json(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        auth::{Auth, Client},
        fs::{Export, Exports},
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use std::collections::BTreeMap;
    use tower::ServiceExt;
    #[tokio::test]
    async fn statistics_only_include_readable_exports_and_never_paths_or_secrets() {
        let allowed = tempfile::tempdir().unwrap();
        let hidden = tempfile::tempdir().unwrap();
        let exports = Exports::new(BTreeMap::from([
            (
                "allowed".into(),
                Arc::new(Export::open(allowed.path()).unwrap()),
            ),
            (
                "hidden".into(),
                Arc::new(Export::open(hidden.path()).unwrap()),
            ),
        ]));
        let client = Client::new(
            "private-statistics-test-key".into(),
            None,
            vec!["allowed".into()],
            vec![],
        );
        let state =
            Arc::new(State::host(Auth::new(Some("node".into()), vec![client]), exports).unwrap());
        let router = crate::router(state, &[]).unwrap();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v1/node/status")
                    .header("authorization", "Bearer private-statistics-test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["exports"].as_array().unwrap().len(), 1);
        assert_eq!(value["exports"][0]["alias"], "allowed");
        assert_eq!(value["harness"]["activeSessions"], 0);
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains("private-statistics-test-key"));
        assert!(!text.contains(allowed.path().to_str().unwrap()));
    }
    #[tokio::test]
    async fn anonymous_and_invalid_credentials_cannot_read_system_statistics() {
        let state = Arc::new(
            State::host(
                Auth::new(Some("node".into()), vec![Client::public(vec![])]),
                Exports::default(),
            )
            .unwrap(),
        );
        let router = crate::router(state, &[]).unwrap();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/node/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v1/node/status")
                    .header("authorization", "Bearer invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
