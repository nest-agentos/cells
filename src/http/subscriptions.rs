//! Authenticated, bounded SSE multiplexing over plugin-owned streams.
use crate::{
    app::State,
    core::{
        error::Error,
        events::{emit, Level},
    },
    plugin::common::EventStream,
};
use axum::{
    extract::{DefaultBodyLimit, State as AxumState},
    http::HeaderMap,
    response::{
        sse::{Event, KeepAlive},
        Sse,
    },
    routing::post,
    Json, Router,
};
use futures_util::{
    stream::{self, SelectAll},
    StreamExt,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u8,
    subscriptions: Vec<Subscription>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    id: String,
    plugin: String,
    args: Value,
}

pub fn routes() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/events", post(subscribe))
        .layer(DefaultBodyLimit::max(65536))
        .layer(axum::middleware::map_response(
            |mut response: axum::response::Response| async move {
                response
                    .headers_mut()
                    .insert("x-accel-buffering", "no".parse().unwrap());
                response
            },
        ))
}
pub fn capability() -> Value {
    json!({"version":1,"endpoint":"/v1/events","transport":"sse"})
}

type Output = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Event, Infallible>> + Send>>;
async fn subscribe(
    AxumState(state): AxumState<Arc<State>>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Result<Sse<Output>, Error> {
    let identity = super::access::identity(&state, &headers)?;
    super::access::origin(&state, &headers)?;
    validate(&request)?;
    let permit = state
        .event_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::too_many("EVENT_CONNECTION_LIMIT"))?;
    let operation = match headers.get("x-operation-id") {
        Some(value) => value
            .to_str()
            .ok()
            .filter(|value| crate::core::ids::is_identifier_within(value, 128))
            .ok_or_else(Error::invalid)?
            .to_owned(),
        None => crate::app::random_epoch().map_err(|_| Error::internal())?,
    };
    let guard = Guard {
        _permit: permit,
        operation,
        started: Instant::now(),
        frames: 0,
    };
    let sources = tokio::time::timeout(
        Duration::from_secs(30),
        prepare(&state, identity, &request, &guard.operation),
    )
    .await
    .map_err(|_| Error::timed_out())?;
    emit(
        Level::Info,
        "events.opened",
        json!({"operationId":guard.operation,"identity":identity,"subscriptions":request.subscriptions.len(),"stage":"stream"}),
    );
    Ok(response(sources, guard, state.event_stop.clone()))
}
async fn prepare(
    state: &Arc<State>,
    identity: usize,
    request: &Request,
    operation: &str,
) -> SelectAll<EventStream> {
    let mut sources = SelectAll::<EventStream>::new();
    for subscription in &request.subscriptions {
        let source = match tokio::time::timeout(
            Duration::from_secs(10),
            state.plugins.subscribe(
                state,
                identity,
                &subscription.plugin,
                subscription.args.clone(),
            ),
        )
        .await
        .unwrap_or_else(|_| Err(Error::timed_out()))
        {
            Ok(source) => source,
            Err(error) => {
                emit(
                    Level::Warn,
                    "events.rejected",
                    json!({"operationId":operation,"stage":"subscribe","code":error.code,"identity":identity,"subscriptionId":subscription.id}),
                );
                Box::pin(stream::once(async move { Err(error) })) as EventStream
            }
        };
        sources.push(envelope(
            source,
            subscription.id.clone(),
            operation.to_owned(),
        ));
    }
    sources
}
fn response(
    sources: SelectAll<EventStream>,
    guard: Guard,
    stop: tokio_util::sync::CancellationToken,
) -> Sse<Output> {
    let output = stream::unfold(
        (sources, guard, stop),
        |(mut sources, mut guard, stop)| async move {
            let value = tokio::select! {_ = stop.cancelled() => return None, value = sources.next() => value?};
            guard.frames += 1;
            let frame = Event::default()
                .event("update")
                .json_data(value.unwrap_or_else(|e| json!({"error":{"code":e.code}})))
                .ok()?;
            Some((Ok(frame), (sources, guard, stop)))
        },
    );
    Sse::new(Box::pin(output) as Output).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    )
}
fn validate(request: &Request) -> Result<(), Error> {
    let mut ids = BTreeSet::new();
    if request.version != 1 || request.subscriptions.is_empty() || request.subscriptions.len() > 32
    {
        return Err(Error::invalid());
    }
    for subscription in &request.subscriptions {
        if !crate::core::ids::is_identifier_within(&subscription.id, 128)
            || !ids.insert(&subscription.id)
            || !crate::core::ids::is_identifier_within(&subscription.plugin, 64)
            || !subscription.args.is_object()
        {
            return Err(Error::invalid());
        }
    }
    Ok(())
}
fn envelope(source: EventStream, id: String, operation: String) -> EventStream {
    Box::pin(stream::unfold(
        (source, id, false, operation),
        |(mut source, id, ended, operation)| async move {
            if ended {
                return None;
            }
            let next = source.next().await;
            let (value, ended) = match next {
                Some(Ok(data)) if data.to_string().len() <= 8 * 1024 * 1024 => {
                    (json!({"id":id,"data":data}), false)
                }
                Some(Ok(_)) => (
                    json!({"id":id,"error":{"code":"EVENT_FRAME_LIMIT","status":413}}),
                    true,
                ),
                Some(Err(error)) => {
                    emit(
                        Level::Warn,
                        "events.source_failed",
                        json!({"operationId":operation,"subscriptionId":id,"stage":"source","code":error.code,"status":error.status.as_u16()}),
                    );
                    (
                        json!({"id":id,"error":{"code":error.code,"status":error.status.as_u16()}}),
                        true,
                    )
                }
                None => (
                    json!({"id":id,"error":{"code":"EVENT_SOURCE_CLOSED","status":503}}),
                    true,
                ),
            };
            Some((Ok(value), (source, id, ended, operation)))
        },
    ))
}
struct Guard {
    _permit: tokio::sync::OwnedSemaphorePermit,
    operation: String,
    started: Instant,
    frames: u64,
}
impl Drop for Guard {
    fn drop(&mut self) {
        emit(
            Level::Info,
            "events.closed",
            json!({"operationId":self.operation,"stage":"stream","frames":self.frames,"elapsedMs":self.started.elapsed().as_millis()}),
        );
    }
}
