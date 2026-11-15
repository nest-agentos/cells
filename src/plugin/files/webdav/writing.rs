use super::{conditions::Conditions, header, location};
use crate::{
    app::State,
    core::{error::Error, file_constraints},
    fs::{mutation::Change, Export},
    http::access,
    operations::Operation,
    plugin::files::writing::{Replacement, WriteCondition},
};
use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{cell::Cell, sync::Arc};
use tokio::{sync::OwnedRwLockReadGuard, time::Instant};

type Guards = Vec<OwnedRwLockReadGuard<()>>;
struct Input {
    state: Arc<State>,
    export: Arc<Export>,
    alias: String,
    path: String,
    owner: usize,
    method: String,
    headers: HeaderMap,
    id: String,
    guards: Guards,
}
struct Plan {
    input: Input,
    conditions: Conditions,
    change: Option<Change>,
    overwrite: bool,
    scopes: Vec<usize>,
    body: Option<Body>,
    deadline: Instant,
}

pub(super) async fn run(
    state: Arc<State>,
    export: Arc<Export>,
    alias: String,
    path: String,
    owner: usize,
    method: String,
    headers: HeaderMap,
    body: Body,
    id: String,
    guards: Guards,
) -> Result<Response, Error> {
    let input = Input {
        state,
        export,
        alias,
        path,
        owner,
        method,
        headers,
        id,
        guards,
    };
    input.prepare(body).await?.submit().await
}
impl Input {
    async fn prepare(mut self, body: Body) -> Result<Plan, Error> {
        if self.path.is_empty() {
            return Err(Error::forbidden("DAV_ROOT_IMMUTABLE"));
        }
        let deadline = access::deadline(&self.headers)?;
        let conditions = Conditions::parse(&self.headers)?;
        let (change, overwrite) = self.change()?;
        let mut paths = vec![self.path.as_str()];
        if let Some(Change::Rename { to, .. }) = &change {
            paths.push(to);
        }
        conditions.validate_tags(&self.alias, &paths)?;
        let scopes = conditions.scopes(&paths);
        let body = accept_body(&self.method, body, deadline).await?;
        if body.is_some()
            && conditions.revision.is_none()
            && !conditions.create
            && conditions.tokens.is_empty()
        {
            return Err(Error::precondition_required(
                "DAV_LOCK_OR_REVISION_REQUIRED",
            ));
        }
        Ok(Plan {
            input: self,
            conditions,
            change,
            overwrite,
            scopes,
            body,
            deadline,
        })
    }
    fn change(&mut self) -> Result<(Option<Change>, bool), Error> {
        let path = self.path.clone();
        Ok(match self.method.as_str() {
            "PUT" => (None, false),
            "MKCOL" => (Some(Change::Mkdir { path }), false),
            "DELETE" => {
                if header(&self.headers, "depth")?.is_some_and(|value| value != "infinity") {
                    return Err(Error::invalid());
                }
                (Some(Change::Remove { path }), false)
            }
            "MOVE" => self.movement()?,
            _ => return Err(Error::unsupported()),
        })
    }
    fn movement(&mut self) -> Result<(Option<Change>, bool), Error> {
        let raw = header(&self.headers, "destination")?.ok_or_else(Error::invalid)?;
        let uri: axum::http::Uri = raw.parse().map_err(|_| Error::invalid())?;
        if uri.authority().is_some_and(|authority| {
            Some(authority.as_str()) != header(&self.headers, "host").ok().flatten()
        }) {
            return Err(Error::forbidden("DAV_CROSS_ORIGIN_MOVE"));
        }
        let (alias, to) = location(uri.path_and_query().ok_or_else(Error::invalid)?.as_str())?;
        if alias != self.alias {
            return Err(Error::new(StatusCode::BAD_GATEWAY, "DAV_CROSS_EXPORT_MOVE"));
        }
        if to.is_empty() || to == self.path || to.starts_with(&format!("{}/", self.path)) {
            return Err(Error::invalid());
        }
        self.guards.extend(access::authorize_path(
            &self.state,
            &self.headers,
            &self.alias,
            &to,
            true,
            false,
        )?);
        let overwrite = match header(&self.headers, "overwrite")? {
            None | Some("T") => true,
            Some("F") => false,
            _ => return Err(Error::invalid()),
        };
        Ok((
            Some(Change::Rename {
                path: self.path.clone(),
                to,
            }),
            overwrite,
        ))
    }
}
async fn accept_body(method: &str, body: Body, deadline: Instant) -> Result<Option<Body>, Error> {
    if method == "PUT" {
        return Ok(Some(body));
    }
    if !super::small_body(body, deadline).await?.is_empty() {
        return Err(Error::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "DAV_BODY_UNSUPPORTED",
        ));
    }
    Ok(None)
}
impl Plan {
    async fn submit(self) -> Result<Response, Error> {
        let gate = self.input.state.files.shared()?;
        let operation = self
            .input
            .state
            .services
            .require::<crate::operations::Operations>()?
            .register(self.input.owner, &self.input.alias, &self.input.id)?;
        let tracked = operation.clone();
        let deadline = self.deadline;
        crate::core::trace::spawn(async move {
            let started = std::time::Instant::now();
            let (id, method) = (self.input.id.clone(), self.input.method.clone());
            let result = if self.body.is_some() {
                self.upload(gate, tracked.clone()).await
            } else {
                self.mutate(gate, tracked.clone()).await
            };
            finished(&id, &method, started, &result);
            tracked.finish(result);
        });
        let (status, receipt) = operation.wait(deadline).await;
        response(status, receipt)
    }
    async fn upload(
        mut self,
        gate: OwnedRwLockReadGuard<()>,
        operation: Arc<Operation>,
    ) -> Result<Value, Error> {
        let condition = WriteCondition::Authorized {
            owner: self.input.owner,
            tokens: self.conditions.tokens,
            expected: self.conditions.revision,
            create: self.conditions.create,
            scopes: self.scopes,
        };
        let body = self.body.take().ok_or_else(Error::internal)?;
        let input = self.input;
        let replacement = Replacement {
            state: input.state,
            export: input.export,
            path: input.path,
            condition,
            deadline: self.deadline,
            operation,
            operation_id: input.id,
            gate,
            project_guards: input.guards,
        };
        let stream = body.into_data_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|_| Error::new(StatusCode::BAD_REQUEST, "UPLOAD_DISCONNECTED"))
        });
        replacement
            .run(Box::pin(stream))
            .await
            .map(|stat| json!({"status":if self.conditions.create{201}else{204},"stat":stat}))
    }
    async fn mutate(
        self,
        gate: OwnedRwLockReadGuard<()>,
        operation: Arc<Operation>,
    ) -> Result<Value, Error> {
        let permit = self.input.state.workers.acquire(self.deadline).await?;
        let context = crate::core::trace::current();
        tokio::task::spawn_blocking(move || {
            crate::core::trace::sync_scope(context, || {
                let (_permit, _gate) = (permit, gate);
                file_constraints::credentials_for(
                    self.input.owner,
                    self.conditions.tokens.clone(),
                    self.scopes.clone(),
                    || self.apply(&operation),
                )
            })
        })
        .await
        .unwrap_or_else(|_| Err(Error::unknown("FILE_COMMIT_WORKER_LOST")))
    }
    fn apply(&self, operation: &Operation) -> Result<Value, Error> {
        let change = self.change.as_ref().ok_or_else(Error::internal)?;
        let status = Cell::new(if matches!(change, Change::Mkdir { .. }) {
            201
        } else {
            204
        });
        self.input
            .export
            .apply_checked(change, self.overwrite, || {
                operation.checkpoint(self.deadline)?;
                self.conditions
                    .check(&self.input.export, &self.input.path)?;
                if let Change::Rename { to, .. } = change {
                    status.set(if self.input.export.stat_inner(to)?.is_some() {
                        204
                    } else {
                        201
                    });
                }
                Ok(())
            })
            .map_err(|error| structural_error(change, self.overwrite, error))?;
        Ok(json!({"status":status.get()}))
    }
}
fn structural_error(change: &Change, overwrite: bool, error: Error) -> Error {
    match (change, error.code, overwrite) {
        (Change::Rename { .. }, "EEXIST", false) => Error::precondition_failed(),
        (Change::Mkdir { .. }, "EEXIST", _) => Error::new(StatusCode::METHOD_NOT_ALLOWED, "EEXIST"),
        _ => error,
    }
}
fn finished(id: &str, method: &str, started: std::time::Instant, result: &Result<Value, Error>) {
    crate::core::events::emit(
        if result.is_ok() {
            crate::core::events::Level::Info
        } else {
            crate::core::events::Level::Warn
        },
        "webdav.commit.finished",
        json!({"operation":method,"operationId":id,"stage":"commit","result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|error|error.code),"outcome":result.as_ref().err().and_then(|error|error.outcome).unwrap_or(if result.is_ok(){"committed"}else{"not-committed"}),"durationMs":started.elapsed().as_millis() as u64}),
    );
}
fn response(status: StatusCode, receipt: Value) -> Result<Response, Error> {
    if !status.is_success() {
        let outcome = super::ReceiptOutcome {
            code: receipt["code"].as_str().unwrap_or("EIO").to_owned(),
            outcome: receipt["outcome"].as_str().unwrap_or("unknown").to_owned(),
        };
        let mut response = (status, axum::Json(receipt)).into_response();
        response.extensions_mut().insert(outcome);
        return Ok(response);
    }
    let status = receipt["result"]["status"].as_u64().unwrap_or(204) as u16;
    let mut response = StatusCode::from_u16(status)
        .map_err(|_| Error::internal())?
        .into_response();
    if let Some(revision) = receipt["result"]["stat"]["revision"].as_str() {
        response
            .headers_mut()
            .insert("etag", revision.parse().map_err(|_| Error::internal())?);
    }
    Ok(response)
}
