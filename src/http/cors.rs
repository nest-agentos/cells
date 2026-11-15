//! CORS preflight handling must not consume a protocol's ordinary OPTIONS request.
use axum::{
    body::Body,
    http::{header::ACCESS_CONTROL_REQUEST_METHOD, Method, Request},
    Router,
};
use tower::{service_fn, ServiceBuilder, ServiceExt};
use tower_http::cors::CorsLayer;

pub(super) fn protocol_options(application: Router, cors: CorsLayer) -> Router {
    let regular = application.clone().layer(cors.clone());
    // The CORS layer sees a normal request; routing/authentication still see OPTIONS.
    let options =
        ServiceBuilder::new()
            .layer(cors)
            .service(service_fn(move |mut request: Request<Body>| {
                *request.method_mut() = Method::OPTIONS;
                application.clone().oneshot(request)
            }));
    Router::new().fallback_service(service_fn(move |mut request: Request<Body>| {
        let protocol = request.method() == Method::OPTIONS
            && !request
                .headers()
                .contains_key(ACCESS_CONTROL_REQUEST_METHOD);
        let options = options.clone();
        let regular = regular.clone();
        async move {
            if protocol {
                *request.method_mut() = Method::GET;
                options.oneshot(request).await
            } else {
                regular.oneshot(request).await
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app::State,
        auth::{Auth, Client},
        fs::{Export, Exports},
        plugin::files::webdav::Options,
    };
    use std::{collections::BTreeMap, sync::Arc};
    const SECRET: &str = "test-secret-at-least-24-bytes";
    const ORIGIN: &str = "https://nestos.example";
    fn application(enabled: bool) -> (Router, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let auth = Auth::new(
            Some("test-node".into()),
            vec![Client::new(
                SECRET.into(),
                None,
                vec!["docs".into()],
                vec![],
            )],
        );
        let exports = Exports::new(BTreeMap::from([(
            "docs".into(),
            Arc::new(Export::open(directory.path()).unwrap()),
        )]));
        let mut state = State::with_secrets(auth, exports, [42; 32], "epoch".into());
        Arc::get_mut(&mut state).unwrap().allowed_origins = vec![ORIGIN.into()];
        Arc::get_mut(&mut state)
            .unwrap()
            .services
            .insert(Arc::new(Options { webdav: enabled }))
            .unwrap();
        (
            crate::http::router(state, &[ORIGIN.into()]).unwrap(),
            directory,
        )
    }
    #[tokio::test]
    async fn protocol_options_reaches_plugin_and_keeps_authentication_and_cors() {
        let (app, _directory) = application(true);
        let request = Request::builder()
            .uri("/dav/docs/")
            .method("OPTIONS")
            .header("origin", ORIGIN)
            .header("authorization", format!("Bearer {SECRET}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["dav"], "1, 2");
        assert!(response.headers()["allow"]
            .to_str()
            .unwrap()
            .contains("PROPFIND"));
        assert_eq!(response.headers()["access-control-allow-origin"], ORIGIN);
        assert!(response.headers()["access-control-expose-headers"]
            .to_str()
            .unwrap()
            .contains("lock-token"));
        let rejected = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/dav/docs/")
                    .method("OPTIONS")
                    .header("origin", "https://blocked.example")
                    .header("authorization", format!("Bearer {SECRET}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), 403);
        assert!(!rejected
            .headers()
            .contains_key("access-control-allow-origin"));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dav/docs/")
                    .method("OPTIONS")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
    }
    #[tokio::test]
    async fn real_preflight_uses_plugin_transport_declarations() {
        let request = Request::builder()
            .uri("/dav/docs/")
            .method("OPTIONS")
            .header("origin", ORIGIN)
            .header("access-control-request-method", "PROPFIND")
            .header("access-control-request-headers", "authorization,depth")
            .body(Body::empty())
            .unwrap();
        let (app, _directory) = application(true);
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);
        assert!(response.headers()["access-control-allow-methods"]
            .to_str()
            .unwrap()
            .contains("PROPFIND"));
        assert!(response.headers()["access-control-allow-headers"]
            .to_str()
            .unwrap()
            .contains("depth"));
        assert!(!response.headers().contains_key("dav"));
    }
    #[tokio::test]
    async fn disabled_protocol_is_not_falsely_reported_as_supported() {
        let request = Request::builder()
            .uri("/dav/docs/")
            .method("OPTIONS")
            .header("authorization", format!("Bearer {SECRET}"))
            .body(Body::empty())
            .unwrap();
        let (app, _directory) = application(false);
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 404);
        assert!(!response.headers().contains_key("dav"));
    }
}
