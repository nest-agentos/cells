//! Bounded correlation metadata, never bodies, URL queries or file paths.
use crate::core::{
    events::{emit, Level},
    trace,
};
use axum::{
    extract::{MatchedPath, Request},
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
pub async fn failures(request: Request, next: Next) -> Response {
    let context = {
        let read = |name| request.headers().get(name).and_then(|v| v.to_str().ok());
        trace::fields(
            read("traceparent"),
            read("x-nestos-browser-session-id"),
            read("x-parent-request-id"),
        )
    };
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|v| v.as_str().to_owned())
        .unwrap_or_else(|| "resource".into());
    trace::scope(context.clone(),async move {
        let started=std::time::Instant::now();let mut response=next.run(request).await;
        for (header,key) in [("x-trace-id","traceId"),("x-request-id","requestId")] {
            if let Some(value)=context.get(key).and_then(|v|v.as_str()).and_then(|v|HeaderValue::from_str(v).ok()) {response.headers_mut().insert(HeaderName::from_static(header),value);}
        }
        let failed=response.status().is_client_error()||response.status().is_server_error();
        emit(if failed {Level::Warn}else{Level::Info},if failed {"http.failed"}else{"http.complete"},
            serde_json::json!({"method":method,"route":route,"stage":"response.headers","status":response.status().as_u16(),
            "code":response.extensions().get::<crate::core::error::Error>().map(|e|e.code),"elapsedMs":started.elapsed().as_millis()}));
        response
    }).await
}
