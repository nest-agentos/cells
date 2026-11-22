use super::{
    conditions::Conditions,
    header,
    properties::{escape, href},
};
use crate::{
    app::State,
    core::{error::Error, file_constraints},
    fs::{upload, Export},
    http::access,
};
use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

struct LockPlan {
    export: Arc<Export>,
    path: String,
    owner: usize,
    operation_id: String,
    token: String,
    seconds: u64,
    refresh: bool,
    unlock: bool,
    depth_zero: bool,
    conditions: Conditions,
    url: String,
    _guards: Vec<OwnedRwLockReadGuard<()>>,
}
pub(super) fn active(token: &str, seconds: u64, url: &str) -> String {
    format!("<D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>infinity</D:depth><D:timeout>Second-{seconds}</D:timeout><D:locktoken><D:href>{}</D:href></D:locktoken><D:lockroot><D:href>{}</D:href></D:lockroot></D:activelock>",escape(token),escape(url))
}
pub(super) async fn run(
    state: Arc<State>,
    export: Arc<Export>,
    alias: String,
    path: String,
    owner: usize,
    operation_id: String,
    unlock: bool,
    headers: HeaderMap,
    body: Body,
    guards: Vec<OwnedRwLockReadGuard<()>>,
) -> Result<Response, Error> {
    let deadline = access::deadline(&headers)?;
    let body = super::small_body(body, deadline).await?;
    let plan = LockPlan::prepare(
        export,
        &alias,
        path,
        owner,
        operation_id.clone(),
        unlock,
        &headers,
        &body,
        guards,
    )?;
    let gate = state.files.shared()?;
    let operation = state
        .services
        .require::<crate::operations::Operations>()?
        .register(owner, &alias, &operation_id)?;
    let tracked = operation.clone();
    let seconds = plan.seconds;
    crate::core::trace::spawn(async move {
        let started = std::time::Instant::now();
        let result = execute(state, plan, gate, tracked.clone(), deadline).await;
        finished(&operation_id, unlock, started, &result);
        tracked.finish(result);
    });
    let (status, receipt) = operation.wait(deadline).await;
    response(status, receipt, unlock, seconds)
}
impl LockPlan {
    fn prepare(
        export: Arc<Export>,
        alias: &str,
        path: String,
        owner: usize,
        operation_id: String,
        unlock: bool,
        headers: &HeaderMap,
        body: &str,
        guards: Vec<OwnedRwLockReadGuard<()>>,
    ) -> Result<Self, Error> {
        let conditions = Conditions::parse(headers)?;
        conditions.validate_tags(alias, &[&path])?;
        let refresh = !unlock && body.is_empty();
        let token = token(headers, body, unlock, refresh, &conditions)?;
        let seconds = seconds(headers)?;
        let depth_zero = match header(headers, "depth")? {
            None | Some("infinity") => false,
            Some("0") => true,
            _ => return Err(Error::invalid()),
        };
        let url = href(alias, &path, false);
        Ok(Self {
            export,
            path,
            owner,
            operation_id,
            token,
            seconds,
            refresh,
            unlock,
            depth_zero,
            conditions,
            url,
            _guards: guards,
        })
    }
    fn apply(
        &self,
        operation: &crate::operations::Operation,
        deadline: tokio::time::Instant,
    ) -> Result<serde_json::Value, Error> {
        operation.checkpoint(deadline)?;
        let mut registry = self.export.lease_registry()?.write()?;
        let resource = self.export.resource(&self.path)?;
        operation.checkpoint(deadline)?;
        self.conditions.check(&self.export, &self.path)?;
        let created = if self.unlock {
            registry.release(&resource, self.owner, &self.token)?;
            false
        } else if self.refresh {
            registry.refresh(&resource, self.owner, &self.token, self.seconds)?;
            false
        } else {
            if self.depth_zero && resource.directory.is_some() {
                return Err(Error::unsupported());
            }
            let created = self.export.stat_inner(&self.path)?.is_none();
            registry.lease(
                resource.clone(),
                self.owner,
                self.token.clone(),
                self.seconds,
                self.operation_id.clone(),
                self.url.clone(),
            )?;
            drop(registry);
            if created {
                self.create(&resource, || operation.checkpoint(deadline))?;
            }
            return Ok(self.receipt(created));
        };
        Ok(self.receipt(created))
    }
    fn create(
        &self,
        resource: &file_constraints::Resource,
        checkpoint: impl Fn() -> Result<(), Error>,
    ) -> Result<(), Error> {
        let result = file_constraints::credentials(self.owner, vec![self.token.clone()], || {
            let mut upload = self.export.stage(&self.path)?;
            upload.track_operation(&self.operation_id);
            upload::commit(&self.export, &self.path, upload, None, checkpoint)
        });
        if let Err(error) = result {
            self.export.lease_registry()?.write()?.release(resource, self.owner, &self.token)?;
            return Err(error);
        }
        Ok(())
    }
    fn receipt(&self, created: bool) -> serde_json::Value {
        serde_json::json!({"created":created,"url":self.url,"token":self.token})
    }
}
fn seconds(headers: &HeaderMap) -> Result<u64, Error> {
    match header(headers, "timeout")? {
        None | Some("Infinite") => Ok(600),
        Some(value) => value
            .strip_prefix("Second-")
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(|value| value.min(3600))
            .ok_or_else(Error::invalid),
    }
}
fn token(
    headers: &HeaderMap,
    body: &str,
    unlock: bool,
    refresh: bool,
    conditions: &Conditions,
) -> Result<String, Error> {
    if unlock {
        if !body.is_empty() || !conditions.tokens.is_empty() {
            return Err(Error::invalid());
        }
        return header(headers, "lock-token")?
            .and_then(|value| value.strip_prefix('<'))
            .and_then(|value| value.strip_suffix('>'))
            .map(str::to_owned)
            .ok_or_else(Error::invalid);
    }
    if refresh {
        if conditions.tokens.len() != 1 {
            return Err(Error::invalid());
        }
        return Ok(conditions.tokens[0].clone());
    }
    if !conditions.tokens.is_empty() {
        return Err(Error::invalid());
    }
    let document = roxmltree::Document::parse(body).map_err(|_| Error::invalid())?;
    let root = document.root_element();
    if !root.has_tag_name(("DAV:", "lockinfo")) {
        return Err(Error::invalid());
    }
    for (container, value) in [("lockscope", "exclusive"), ("locktype", "write")] {
        let parent = root
            .children()
            .find(|node| node.has_tag_name(("DAV:", container)))
            .ok_or_else(Error::invalid)?;
        let children: Vec<_> = parent.children().filter(|node| node.is_element()).collect();
        if children.len() != 1 || !children[0].has_tag_name(("DAV:", value)) {
            return Err(Error::unsupported());
        }
    }
    Ok(format!(
        "opaquelocktoken:{}",
        crate::app::random_epoch().map_err(|_| Error::internal())?
    ))
}
fn finished(
    id: &str,
    unlock: bool,
    started: std::time::Instant,
    result: &Result<serde_json::Value, Error>,
) {
    crate::core::events::emit(
        if result.is_ok() {
            crate::core::events::Level::Info
        } else {
            crate::core::events::Level::Warn
        },
        "webdav.lock.finished",
        serde_json::json!({"operation":if unlock{"UNLOCK"}else{"LOCK"},"operationId":id,"stage":"lease","result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|error|error.code),"outcome":result.as_ref().err().and_then(|error|error.outcome).unwrap_or(if result.is_ok(){"committed"}else{"not-committed"}),"durationMs":started.elapsed().as_millis() as u64}),
    );
}
fn response(
    status: StatusCode,
    receipt: serde_json::Value,
    unlock: bool,
    seconds: u64,
) -> Result<Response, Error> {
    if !status.is_success() {
        let outcome = super::ReceiptOutcome {
            code: receipt["code"].as_str().unwrap_or("EIO").to_owned(),
            outcome: receipt["outcome"].as_str().unwrap_or("unknown").to_owned(),
        };
        let mut response = (status, axum::Json(receipt)).into_response();
        response.extensions_mut().insert(outcome);
        return Ok(response);
    }
    if unlock {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    let result = &receipt["result"];
    let token = result["token"].as_str().ok_or_else(Error::internal)?;
    let url = result["url"].as_str().ok_or_else(Error::internal)?;
    let mut response = super::xml(if result["created"]==true{StatusCode::CREATED}else{StatusCode::OK},format!("<?xml version=\"1.0\"?><D:prop xmlns:D=\"DAV:\"><D:lockdiscovery>{}</D:lockdiscovery></D:prop>",active(token,seconds,url)));
    response.headers_mut().insert(
        "lock-token",
        format!("<{token}>")
            .parse()
            .map_err(|_| Error::internal())?,
    );
    Ok(response)
}

async fn execute(
    state: Arc<State>,
    plan: LockPlan,
    gate: OwnedRwLockReadGuard<()>,
    operation: Arc<crate::operations::Operation>,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value, Error> {
    let permit = state.workers.acquire(deadline).await?;
    let context = crate::core::trace::current();
    tokio::task::spawn_blocking(move || {
        crate::core::trace::sync_scope(context, || {
            let (_permit, _gate) = (permit, gate);
            plan.apply(&operation, deadline)
        })
    })
    .await
    .map_err(|_| Error::unknown("FILE_LOCK_WORKER_LOST"))?
}
