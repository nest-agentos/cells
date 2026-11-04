//! DSH 0.2 Remote HTTP receipts and multiplexed, bounded WebSocket subscriptions.
use super::config::DshConfig;
use crate::core::error::Error;
use futures_util::{SinkExt, StreamExt};
use reqwest::{cookie::CookieStore, Url};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
use tokio_util::sync::CancellationToken;

const MAX_BYTES: usize = 8 * 1024 * 1024;
type Listeners = Arc<Mutex<HashMap<String, mpsc::Sender<Queued>>>>;
struct Queued {
    value: Option<Result<Value, Error>>,
    bytes: usize,
    budget: Arc<AtomicUsize>,
}
impl Drop for Queued {
    fn drop(&mut self) {
        self.budget.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
pub struct Client {
    http: reqwest::Client,
    base: Url,
    timeout: Duration,
    sender: mpsc::Sender<Value>,
    listeners: Listeners,
    pub stop: CancellationToken,
    requests: tokio::sync::Semaphore,
}
pub struct Subscription {
    id: String,
    receiver: mpsc::Receiver<Queued>,
    sender: mpsc::Sender<Value>,
    listeners: Listeners,
}
impl Drop for Subscription {
    fn drop(&mut self) {
        self.listeners.lock().unwrap().remove(&self.id);
        let _ = self
            .sender
            .try_send(json!({"type":"cancel","streamId":self.id}));
    }
}
impl Subscription {
    pub async fn next(&mut self) -> Result<Value, Error> {
        let mut queued = self.receiver.recv().await.ok_or_else(Error::unavailable)?;
        queued.value.take().unwrap()
    }
}
impl Client {
    pub async fn connect(
        base: Url,
        token: Option<String>,
        config: &DshConfig,
        stop: CancellationToken,
    ) -> Result<Arc<Self>, Error> {
        let timeout = Duration::from_millis(config.request_timeout_ms);
        let jar = Arc::new(reqwest::cookie::Jar::default());
        let http = http_client(&base, jar.clone(), timeout)?;
        if let Some(token) = token {
            authenticate(&http, &base, &token, &jar).await?;
        }
        let websocket = websocket(&base, &jar, timeout).await?;
        let (sender, receiver) = mpsc::channel(128);
        let client = Arc::new(Self {
            http,
            base,
            timeout,
            sender,
            listeners: Arc::default(),
            stop,
            requests: tokio::sync::Semaphore::new(128),
        });
        spawn_mux(
            websocket,
            receiver,
            client.listeners.clone(),
            client.stop.clone(),
        );
        Ok(client)
    }
    pub async fn call(&self, endpoint: &str, args: Value) -> Result<Value, Error> {
        self.call_with_timeout(endpoint, args, self.timeout).await
    }
    pub async fn call_with_timeout(
        &self,
        endpoint: &str,
        args: Value,
        timeout: Duration,
    ) -> Result<Value, Error> {
        let started = Instant::now();
        let _permit = self
            .requests
            .try_acquire()
            .map_err(|_| Error::too_many("DSH_RPC_BUSY"))?;
        let rpc_id = id();
        match self.post(endpoint, args, &rpc_id, timeout).await {
            Ok(response) => finish_reply(response, endpoint, &rpc_id, started).await,
            Err(error) => {
                report_rpc_failure(endpoint, &rpc_id, 0, None, error, started);
                Err(error)
            }
        }
    }
    async fn post(
        &self,
        endpoint: &str,
        args: Value,
        rpc_id: &str,
        timeout: Duration,
    ) -> Result<reqwest::Response, Error> {
        let url = self
            .base
            .join(&format!("api/{endpoint}"))
            .map_err(|_| Error::invalid())?;
        let request = self
            .http
            .post(url)
            .timeout(timeout)
            .json(&json!({"type":"client-request",
            "rpcId":rpc_id,"method":endpoint,"payload":{"args":args}}))
            .send();
        tokio::select! {
            _ = self.stop.cancelled() => Err(Error::unavailable()),
            response = request => response.map_err(network_error),
        }
    }

    pub fn subscribe(&self, endpoint: &str, args: Value) -> Result<Subscription, Error> {
        if self.stop.is_cancelled() {
            return Err(Error::unavailable());
        }
        let id = id();
        let (sender, receiver) = mpsc::channel(128);
        self.listeners.lock().unwrap().insert(id.clone(), sender);
        if self
            .sender
            .try_send(json!({"type":"open","streamId":id,"endpoint":endpoint,
            "payload":{"args":args}}))
            .is_err()
        {
            self.listeners.lock().unwrap().remove(&id);
            return Err(Error::too_many("DSH_STREAM_BUSY"));
        }
        Ok(Subscription {
            id,
            receiver,
            sender: self.sender.clone(),
            listeners: self.listeners.clone(),
        })
    }
    pub async fn first(&self, endpoint: &str, args: Value) -> Result<Value, Error> {
        let mut sub = self.subscribe(endpoint, args)?;
        tokio::time::timeout(self.timeout, sub.next())
            .await
            .map_err(|_| Error::timed_out())?
    }
}
async fn finish_reply(
    response: reqwest::Response,
    endpoint: &str,
    rpc_id: &str,
    started: Instant,
) -> Result<Value, Error> {
    let status = response.status().as_u16();
    let body = bounded_json(response).await;
    let result = match &body {
        Ok(body) => decode_http_reply(status, body, rpc_id),
        Err(_) => Err(if (200..300).contains(&status) {
            Error::unavailable()
        } else {
            status_error(status)
        }),
    };
    if let Err(error) = result {
        report_rpc_failure(endpoint, rpc_id, status, body.as_ref().ok(), error, started);
    }
    result
}
async fn bounded_json(response: reqwest::Response) -> Result<Value, Error> {
    if response
        .content_length()
        .is_some_and(|size| size > MAX_BYTES as u64)
    {
        return Err(Error::too_large("EFBIG"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(network_error)?;
        if bytes.len() + chunk.len() > MAX_BYTES {
            return Err(Error::too_large("EFBIG"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::unavailable())
}
fn spawn_mux<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    commands: mpsc::Receiver<Value>,
    listeners: Listeners,
    stop: CancellationToken,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sink, stream) = socket.split();
    tokio::spawn(write_mux(sink, commands, stop.clone()));
    tokio::spawn(read_mux(stream, listeners, stop));
}
async fn write_mux<S>(mut sink: S, mut commands: mpsc::Receiver<Value>, stop: CancellationToken)
where
    S: futures_util::Sink<Message> + Unpin,
{
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    loop {
        let message = tokio::select! {
            _=stop.cancelled()=>break,
            _=heartbeat.tick()=>Message::Ping(Vec::new().into()),
            value=commands.recv()=>match value {Some(value)=>Message::Text(value.to_string().into()),None=>break},
        };
        tokio::select! {
            _=stop.cancelled()=>break,
            result=sink.send(message)=>if result.is_err(){break;},
        }
    }
    stop.cancel();
}
async fn read_mux<S>(mut stream: S, listeners: Listeners, stop: CancellationToken)
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let budget = Arc::new(AtomicUsize::new(0));
    loop {
        let message = tokio::select! {
            _=stop.cancelled()=>break,
            message=tokio::time::timeout(Duration::from_secs(45),stream.next())=>message.ok().flatten(),
        };
        match message {
            Some(Ok(Message::Text(text))) => {
                let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                    break;
                };
                if dispatch(&listeners, frame, &budget, text.len()).is_err() {
                    break;
                }
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => (),
            _ => break,
        }
    }
    stop.cancel();
    listeners.lock().unwrap().clear();
}

fn dispatch(
    listeners: &Listeners,
    frame: Value,
    budget: &Arc<AtomicUsize>,
    bytes: usize,
) -> Result<(), Error> {
    let id = frame["streamId"].as_str().ok_or_else(Error::invalid)?;
    let mut listeners = listeners.lock().unwrap();
    let Some(sender) = listeners.get(id) else {
        return Ok(());
    };
    let result = match frame["type"].as_str() {
        Some("item") => Ok(frame["value"].clone()),
        Some("error") => Err(remote_error(&frame["error"])),
        Some("end") => Err(Error::unavailable()),
        _ => return Err(Error::invalid()),
    };
    enqueue(sender, result, budget, bytes)?;
    if frame["type"] != "item" {
        listeners.remove(id);
    }
    Ok(())
}
pub fn id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("random unavailable");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn network_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::timed_out()
    } else {
        Error::unavailable()
    }
}
fn status_error(status: u16) -> Error {
    match status {
        401 => Error::unauthenticated(),
        403 => Error::forbidden("EACCES"),
        404 => Error::not_found("ENOENT"),
        409 => Error::conflict("DSH_RPC_CONFLICT"),
        429 => Error::too_many("DSH_BUSY"),
        _ => Error::unavailable(),
    }
}
fn remote_error(error: &Value) -> Error {
    match error["code"].as_str().unwrap_or("") {
        "gateway/method-unavailable" => Error::unsupported(),
        "session/not-found" | "workspace/not-found" => Error::not_found("ENOENT"),
        "session/busy" => Error::conflict("DSH_SESSION_BUSY"),
        "session/agent-busy" => Error::conflict("DSH_AGENT_BUSY"),
        "session/writer-held" => Error::conflict("DSH_SESSION_WRITER_HELD"),
        "workspace/session-active" => Error::conflict("DSH_WORKSPACE_SESSION_ACTIVE"),
        "gateway/bad-request" | "session/title-invalid" => Error::invalid(),
        _ => Error::unavailable(),
    }
}

fn http_client(
    base: &Url,
    jar: Arc<reqwest::cookie::Jar>,
    timeout: Duration,
) -> Result<reqwest::Client, Error> {
    let mut builder = reqwest::Client::builder()
        .cookie_provider(jar)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout);
    if matches!(base.host_str(), Some("127.0.0.1" | "localhost" | "::1")) {
        builder = builder.no_proxy();
    }
    builder.build().map_err(|_| Error::unavailable())
}
async fn authenticate(
    http: &reqwest::Client,
    base: &Url,
    token: &str,
    jar: &reqwest::cookie::Jar,
) -> Result<(), Error> {
    let mut url = base.clone();
    url.query_pairs_mut().append_pair("token", token);
    let response = http.get(url).send().await.map_err(network_error)?;
    if response.status() != reqwest::StatusCode::SEE_OTHER || jar.cookies(base).is_none() {
        return Err(Error::unauthenticated());
    }
    Ok(())
}
async fn websocket(
    base: &Url,
    jar: &reqwest::cookie::Jar,
    timeout: Duration,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Error,
> {
    let request = websocket_request(base, jar)?;
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(MAX_BYTES))
        .max_frame_size(Some(MAX_BYTES));
    tokio::time::timeout(
        timeout,
        tokio_tungstenite::connect_async_with_config(request, Some(config), false),
    )
    .await
    .map_err(|_| Error::timed_out())?
    .map(|value| value.0)
    .map_err(|error| match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            status_error(response.status().as_u16())
        }
        _ => Error::unavailable(),
    })
}

fn decode_http_reply(status: u16, body: &Value, rpc_id: &str) -> Result<Value, Error> {
    if !(200..300).contains(&status) {
        if status == 409
            && body["type"] == "server-response"
            && body["rpcId"] == rpc_id
            && body["result"]["ok"] == false
        {
            return Err(remote_error(&body["result"]["error"]));
        }
        return Err(status_error(status));
    }
    decode_reply(body, rpc_id)
}
fn report_rpc_failure(
    endpoint: &str,
    rpc_id: &str,
    status: u16,
    body: Option<&Value>,
    error: Error,
    started: Instant,
) {
    let native_code = body
        .filter(|value| value["rpcId"] == rpc_id)
        .and_then(|value| value["result"]["error"]["code"].as_str())
        .filter(|value| {
            value.len() <= 128
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"/_-".contains(&b))
        });
    crate::core::events::emit(
        crate::core::events::Level::Warn,
        "harness.dsh.rpc_failed",
        json!({"operation":"harness.dsh.rpc","operationId":rpc_id,"stage":endpoint,"outcome":"failure",
        "code":error.code,"nativeCode":native_code,"httpStatus":status,"durationMs":started.elapsed().as_millis()}),
    );
}

fn decode_reply(body: &Value, rpc_id: &str) -> Result<Value, Error> {
    if body["type"] != "server-response" || body["rpcId"] != rpc_id {
        return Err(Error::unavailable());
    }
    if body["result"]["ok"] != true {
        return Err(remote_error(&body["result"]["error"]));
    }
    Ok(body["result"]["value"].clone())
}

fn enqueue(
    sender: &mpsc::Sender<Queued>,
    value: Result<Value, Error>,
    budget: &Arc<AtomicUsize>,
    bytes: usize,
) -> Result<(), Error> {
    if budget
        .fetch_add(bytes, Ordering::Relaxed)
        .saturating_add(bytes)
        > MAX_BYTES
    {
        budget.fetch_sub(bytes, Ordering::Relaxed);
        return Err(Error::too_large("EFBIG"));
    }
    let queued = Queued {
        value: Some(value),
        bytes,
        budget: budget.clone(),
    };
    sender
        .try_send(queued)
        .map_err(|_| Error::too_many("DSH_STREAM_OVERFLOW"))
}

fn websocket_request(
    base: &Url,
    jar: &reqwest::cookie::Jar,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, Error> {
    let mut url = base.join("api/remote.mux").map_err(|_| Error::invalid())?;
    url.set_scheme(if base.scheme() == "https" {
        "wss"
    } else {
        "ws"
    })
    .unwrap();
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| Error::invalid())?;
    if let Some(cookie) = jar.cookies(base) {
        request.headers_mut().insert("cookie", cookie);
    }
    Ok(request)
}

#[cfg(test)]
mod reply_tests {
    use super::*;
    #[test]
    fn preserves_native_conflict_categories_without_accepting_failed_http_successes() {
        for (native, expected) in [
            ("session/busy", "DSH_SESSION_BUSY"),
            ("session/writer-held", "DSH_SESSION_WRITER_HELD"),
            ("session/agent-busy", "DSH_AGENT_BUSY"),
            ("workspace/session-active", "DSH_WORKSPACE_SESSION_ACTIVE"),
        ] {
            let body = json!({"type":"server-response","rpcId":"r","result":{"ok":false,"error":{"code":native,"message":"private error text"}}});
            for status in [200, 409] {
                assert_eq!(
                    decode_http_reply(status, &body, "r").err().unwrap().code,
                    expected
                );
            }
            assert_eq!(
                decode_http_reply(401, &body, "r").err().unwrap().code,
                "EACCES"
            );
            assert_eq!(
                decode_http_reply(403, &body, "r").err().unwrap().code,
                "EACCES"
            );
        }
        let success = json!({"type":"server-response","rpcId":"r","result":{"ok":true,"value":{}}});
        assert_eq!(
            decode_http_reply(409, &success, "r").err().unwrap().code,
            "DSH_RPC_CONFLICT"
        );
        assert_eq!(
            decode_http_reply(200, &success, "another")
                .err()
                .unwrap()
                .code,
            "EIO"
        );
        assert_eq!(decode_http_reply(200, &success, "r").unwrap(), json!({}));
    }
}
