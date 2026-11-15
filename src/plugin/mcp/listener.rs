//! Node-local transport ownership; scope tokens and business snapshots remain unchanged.
use crate::{app::State, plugin::common::PluginFailure};
use std::{net::SocketAddr, sync::{Arc,atomic::{AtomicBool,Ordering},RwLock}, time::Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

type Running=(CancellationToken,tokio::task::JoinHandle<Result<(),std::io::Error>>);
pub struct Listener {
    address: SocketAddr,
    enabled:AtomicBool,
    endpoint:RwLock<Option<String>>,
    running: Mutex<Option<Running>>,
}
impl Listener {
    pub fn new(address: SocketAddr) -> Self {Self {address, enabled:AtomicBool::new(false),endpoint:RwLock::new(None),running: Mutex::new(None)}}
    pub fn require(&self){self.enabled.store(true,Ordering::Release);}
    pub fn endpoint(&self)->Result<String,PluginFailure> {
        self.endpoint.read().map_err(|_|PluginFailure::new("MCP_LISTENER_UNAVAILABLE","Internal endpoint unavailable"))?.clone().ok_or_else(||PluginFailure::new("MCP_LISTENER_UNAVAILABLE","Internal listener not ready"))
    }
    pub async fn start(&self, state: &Arc<State>) -> Result<(), PluginFailure> {
        if !self.enabled.load(Ordering::Acquire){return Ok(());}
        let mut running=self.running.lock().await;if running.is_some(){return Ok(());}
        let started=Instant::now();
        let listener=tokio::net::TcpListener::bind(self.address).await.map_err(|error| {
            crate::core::events::emit(crate::core::events::Level::Error,"mcp.listener.failed",serde_json::json!({"operation":"mcp.listen","operationId":std::process::id(),"stage":"bind","result":"failure","code":"MCP_LISTEN_FAILED","address":self.address.to_string(),"errorKind":format!("{:?}",error.kind()),"durationMs":started.elapsed().as_millis() as u64}));
            PluginFailure::new("MCP_LISTEN_FAILED",format!("Cannot bind internal MCP listener {} ({:?})",self.address,error.kind()))
        })?;
        let address=listener.local_addr().map_err(|_|PluginFailure::new("MCP_LISTEN_FAILED","Cannot inspect internal listener"))?;
        let endpoint=format!("http://{address}/v1/channel/mcp");
        *self.endpoint.write().map_err(|_|PluginFailure::new("MCP_LISTENER_UNAVAILABLE","Internal endpoint unavailable"))?=Some(endpoint.clone());
        let router=crate::http::internal_mcp(state.clone(),super::http_routes::routes());
        let stop=CancellationToken::new();let cancelled=stop.clone();
        let task=tokio::spawn(async move {axum::serve(listener,router).with_graceful_shutdown(cancelled.cancelled_owned()).await});
        *running=Some((stop,task));
        crate::core::events::emit(crate::core::events::Level::Info,"mcp.listener.ready",serde_json::json!({"operation":"mcp.listen","operationId":std::process::id(),"stage":"serve","result":"success","endpoint":endpoint,"durationMs":started.elapsed().as_millis() as u64}));
        Ok(())
    }
    pub async fn stop(&self)->Result<(),PluginFailure> {
        let Some((stop,mut task))=self.running.lock().await.take() else {return Ok(());};
        stop.cancel();
        if tokio::time::timeout(std::time::Duration::from_secs(5),&mut task).await.is_err(){task.abort();let _=task.await;}
        *self.endpoint.write().map_err(|_|PluginFailure::new("MCP_LISTENER_UNAVAILABLE","Internal endpoint unavailable"))?=None;
        Ok(())
    }
}
pub fn address(config:&dyn crate::plugin::common::PluginConfiguration)->Result<SocketAddr,String> {
    #[derive(serde::Deserialize)] #[serde(deny_unknown_fields)] struct Options {listen:Option<SocketAddr>}
    let options=match config.option("mcp"){Some(value)=>value.clone().try_into::<Options>().map_err(|_|"Invalid MCP listener options")?,None=>Options {listen:None}};
    let address=options.listen.unwrap_or_else(||"127.0.0.1:8788".parse().unwrap());
    if !address.ip().is_loopback(){return Err("Internal MCP listener must bind a loopback address".into());}
    Ok(address)
}
