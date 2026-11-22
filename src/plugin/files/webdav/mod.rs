//! Opt-in DAV adapter over shared file capabilities and operation receipts.
mod conditions;
mod locking;
mod properties;
mod writing;
use crate::{app::State, core::error::Error, http::access};
use axum::{
    body::Body,
    extract::{Request, State as AxumState},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct ReceiptOutcome {
    pub code: String,
    pub outcome: String,
}
#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Options {
    pub webdav: bool,
}
pub(crate) fn routes() -> Router<Arc<State>> {
    Router::new()
        .route("/dav/:alias", any(handle))
        .route("/dav/:alias/", any(handle))
        .route("/dav/:alias/*path", any(handle))
}
pub(super) fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, Error> {
    headers
        .get(name)
        .map(|value| value.to_str().map_err(|_| Error::invalid()))
        .transpose()
}
pub(super) fn location(raw: &str) -> Result<(String, String), Error> {
    if raw.contains('?') || raw.contains('#') {
        return Err(Error::invalid());
    }
    let raw = raw.strip_prefix("/dav/").ok_or_else(Error::invalid)?;
    let (alias, path) = raw.split_once('/').unwrap_or((raw, ""));
    if !crate::core::ids::is_identifier(alias) {
        return Err(Error::invalid());
    }
    // Decode once; encoded separators and NUL must not change URL hierarchy.
    if path.to_ascii_lowercase().contains("%2f") || path.to_ascii_lowercase().contains("%5c") {
        return Err(Error::invalid());
    }
    for (index, byte) in path.bytes().enumerate() {
        if byte == b'%'
            && !path
                .as_bytes()
                .get(index + 1..index + 3)
                .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(Error::invalid());
        }
    }
    if path.ends_with("//") {
        return Err(Error::invalid());
    }
    let path = percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| Error::invalid())?
        .trim_end_matches('/')
        .to_owned();
    crate::fs::path::validate(&path)?;
    Ok((alias.to_owned(), path))
}
async fn handle(AxumState(state): AxumState<Arc<State>>, request: Request) -> Response {
    let started = std::time::Instant::now();
    let method = request.method().clone();
    let id = match header(request.headers(), "x-operation-id") {
        Ok(Some(id)) => id.to_owned(),
        Ok(None) => match crate::app::random_epoch() {
            Ok(random) => format!("dav-{random}"),
            Err(_) => return Error::internal().into_response(),
        },
        Err(error) => return error.into_response(),
    };
    if !crate::core::ids::is_identifier_within(&id, 128) {
        return Error::invalid().into_response();
    }
    let result = dispatch(state, request, &id).await;
    let (status, code, outcome) = match &result {
        Ok(response) => {
            let receipt = response.extensions().get::<ReceiptOutcome>();
            (
                response.status(),
                receipt.map_or("OK", |receipt| receipt.code.as_str()),
                receipt.map_or("completed", |receipt| receipt.outcome.as_str()),
            )
        }
        Err(error) => (
            error.status,
            error.code,
            error.outcome.unwrap_or("not-committed"),
        ),
    };
    crate::core::events::emit(
        if status.is_success() {
            crate::core::events::Level::Info
        } else {
            crate::core::events::Level::Warn
        },
        "webdav.finished",
        serde_json::json!({"operation":method.as_str(),"operationId":id,"stage":"response","result":if status.is_success(){"success"}else{"failure"},"code":code,"outcome":outcome,"durationMs":started.elapsed().as_millis() as u64}),
    );
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    if let Ok(id) = id.parse() {
        response.headers_mut().insert("x-operation-id", id);
    }
    response
}
async fn dispatch(state: Arc<State>, request: Request, id: &str) -> Result<Response, Error> {
    if !state
        .services
        .get::<Options>()
        .is_some_and(|options| options.webdav)
    {
        return Err(Error::not_found("WEBDAV_DISABLED"));
    }
    if !crate::core::ids::is_identifier_within(id, 128) {
        return Err(Error::invalid());
    }
    let (parts, body) = request.into_parts();
    let (alias, path) = location(
        parts
            .uri
            .path_and_query()
            .ok_or_else(Error::invalid)?
            .as_str(),
    )?;
    let headers = parts.headers;
    access::origin(&state, &headers)?;
    let (identity, export) = access::export(&state, &headers, &alias)?;
    let method = parts.method.as_str();
    if matches!(method, "GET" | "HEAD" | "PROPFIND" | "OPTIONS") && headers.contains_key("if") {
        return Err(Error::new(StatusCode::BAD_REQUEST, "DAV_IF_UNSUPPORTED"));
    }
    let write = !matches!(method, "OPTIONS" | "PROPFIND" | "GET" | "HEAD");
    let guards = access::authorize_path(&state, &headers, &alias, &path, write, false)?;
    if write {
        access::writable_export(&state, &headers, &alias)?;
    }
    match method {
        "OPTIONS" => Ok(options(&export)),
        "PROPFIND" => properties::find(state, export, alias, path, headers, body, guards).await,
        "GET" | "HEAD" => {
            let response = super::http::content::content(
                AxumState(state),
                axum::extract::Path(alias),
                headers,
                axum::extract::Query(crate::http::query::ListQuery {
                    path,
                    cursor: None,
                    fields: Default::default(),
                }),
            )
            .await?;
            if method == "HEAD" {
                let (parts, _) = response.into_parts();
                Ok(Response::from_parts(parts, Body::empty()))
            } else {
                Ok(response)
            }
        }
        "LOCK" | "UNLOCK" if export.webdav_locks => {
            locking::run(
                state,
                export,
                alias,
                path,
                identity,
                id.to_owned(),
                method == "UNLOCK",
                headers,
                body,
                guards,
            )
            .await
        }
        "LOCK" | "UNLOCK" => {
            let error = Error {
                outcome: Some("not-committed"),
                ..Error::new(StatusCode::METHOD_NOT_ALLOWED, "DAV_LOCK_DISABLED")
            };
            let mut response = error.into_response();
            response.headers_mut().insert("allow", allow(false).parse().unwrap());
            response.extensions_mut().insert(ReceiptOutcome {
                code: error.code.into(),
                outcome: "not-committed".into(),
            });
            Ok(response)
        }
        "PUT" | "MKCOL" | "MOVE" | "DELETE" => {
            writing::run(
                state,
                export,
                alias,
                path,
                identity,
                method.to_owned(),
                headers,
                body,
                id.to_owned(),
                guards,
            )
            .await
        }
        _ => Err(Error::new(
            StatusCode::NOT_IMPLEMENTED,
            "DAV_METHOD_UNSUPPORTED",
        )),
    }
}
fn allow(locks: bool) -> &'static str {
    if locks {
        "OPTIONS, PROPFIND, GET, HEAD, PUT, MKCOL, MOVE, DELETE, LOCK, UNLOCK"
    } else {
        "OPTIONS, PROPFIND, GET, HEAD, PUT, MKCOL, MOVE, DELETE"
    }
}
fn options(export: &crate::fs::Export) -> Response {
    let mut response = Response::builder()
        .header("allow", allow(export.webdav_locks))
        .header("dav", if export.webdav_locks { "1, 2" } else { "1" });
    if !export.webdav_locks {
        response = response.header("x-cells-dav-structural-conditions", "if-match");
    }
    response.body(Body::empty()).unwrap()
}
pub(super) fn xml(status: StatusCode, body: String) -> Response {
    (
        status,
        [("content-type", "application/xml; charset=utf-8")],
        body,
    )
        .into_response()
}
pub(super) async fn small_body(
    body: Body,
    deadline: tokio::time::Instant,
) -> Result<String, Error> {
    let bytes = tokio::time::timeout_at(deadline, axum::body::to_bytes(body, 64 * 1024))
        .await
        .map_err(|_| Error::timed_out())?
        .map_err(|_| Error::too_large("DAV_XML_LIMIT"))?;
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::invalid())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paths_decode_once_and_preserve_export_boundaries() {
        assert_eq!(
            location("/dav/docs/a%20b").unwrap(),
            ("docs".into(), "a b".into())
        );
        assert_eq!(location("/dav/docs/%252F").unwrap().1, "%2F");
        for path in [
            "/dav/docs/%2Fetc",
            "/dav/docs/%2e%2e/a",
            "/dav/docs/.nestos/a",
            "/dav/docs/a?query=1",
            "/dav/docs/%zz",
            "/dav/docs/a//",
        ] {
            assert!(location(path).is_err(), "{path}");
        }
    }
}
