//! `GET /v1/fs/:alias/content` — streaming read with byte-range support.
//!
//! The stream retains node shutdown ownership and the originating project's
//! directory fence. Unrelated commands and projects continue concurrently.


use crate::{
    app::State,
    core::error::Error,
    http::{access, query::ListQuery, range},
};
use axum::{
    body::Body,
    extract::{Path, Query, State as AxumState},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use std::sync::Arc;

/// Bytes read per stream item.
const CHUNK_BYTES: u64 = 64 * 1024;

pub async fn content(
    AxumState(state): AxumState<Arc<State>>,
    Path(alias): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Response, Error> {
    let deadline = access::deadline(&headers)?;
    crate::core::gate_read::retry(deadline, || content_once(
        AxumState(state.clone()), Path(alias.clone()), headers.clone(), Query(query.clone()),
    )).await
}

async fn content_once(
    AxumState(state): AxumState<Arc<State>>,
    Path(alias): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Response, Error> {
    let identity = access::identity(&state, &headers)?;
    state.auth.authorize(identity, &alias)?;
    crate::fs::path::validate(&query.path)?;
    let _project_guard =
        access::authorize_path(&state, &headers, &alias, &query.path, false, false)?;
    let deadline = access::deadline(&headers)?;
    let reading = super::super::reading::Reading::authorize(&state, identity, &alias, _project_guard, deadline)?;
    let opened = reading.run("open", reading.source.open(query.path, reading.context.clone())).await?;
    let revision = opened.revision;
    let size = opened.size;

    // A read-only export has no validator, so a client-supplied `If-Match`
    // cannot be evaluated and must not turn every read into a 412.
    if let Some(revision) = revision.as_deref() {
        if let Some(expected) = headers.get("if-match") {
            if expected.to_str().unwrap_or("") != revision {
                return Err(Error::precondition_failed());
            }
        }
    }
    if let Some(condition) = header(&headers, "if-none-match") {
        if condition != "*" && (!condition.starts_with('"') || !condition.ends_with('"') || condition.contains(',')) { return Err(Error::invalid()); }
        if condition == "*" || revision.as_deref() == Some(condition) {
            let mut response = Response::builder().status(StatusCode::NOT_MODIFIED);
            if let Some(revision) = revision { response = response.header("etag", revision); }
            return response.body(Body::empty()).map_err(|_| Error::internal());
        }
    }
    let range_header = if if_range_matches(&headers, revision.as_deref()) {
        header(&headers, "range")
    } else {
        None
    };
    let window = match range::resolve(range_header, size) {
        Ok(window) => window,
        Err(error) if error.is_range_unsatisfiable() => {
            return Ok(Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(
                    "content-range",
                    range::ByteRange::unsatisfiable_header(size),
                )
                .body(Body::empty())
                .expect("static response"));
        }
        Err(error) => return Err(error),
    };

    let file = reading.run("read-window", opened.content.read(window.start, window.length)).await?;
    // Retain lifecycle ownership until the body is consumed or dropped.
    let stream = futures_util::stream::try_unfold(
        (file, window.length, reading),
        move |(mut file, remaining, reading)| async move {
            if remaining == 0 {
                return Ok::<_, std::io::Error>(None);
            }
            let mut data = vec![0; remaining.min(CHUNK_BYTES) as usize];
            let read = reading.chunk(&mut file, &mut data).await?;
            data.truncate(read);
            Ok(Some((data, (file, remaining - read as u64, reading))))
        },
    );
    let mut response = Response::builder()
        .status(if window.partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header("content-type", "application/octet-stream")
        .header("content-length", window.length)
        .header("accept-ranges", "bytes");
    if let Some(revision) = revision {
        response = response.header("etag", revision);
    }
    if window.partial {
        response = response.header("content-range", window.content_range(size));
    }
    response.body(Body::from_stream(stream)).map_err(|_| {
        Error::new(Error::internal().status, "FILE_SOURCE_INVALID_REVISION")
    })
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// `If-Range` matches only when the server has a validator and it is equal.
fn if_range_matches(headers: &HeaderMap, revision: Option<&str>) -> bool {
    match (revision, headers.get("if-range")) {
        (Some(revision), Some(value)) => value.to_str().unwrap_or("") == revision,
        (None, Some(_)) => false,
        (_, None) => true,
    }
}
