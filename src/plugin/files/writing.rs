//! Shared streaming stage/commit service used by file protocol adapters.
use crate::{
    app::State,
    core::error::Error,
    fs::{upload, Export},
    operations::Operation,
};
use futures_util::{Stream, StreamExt};
use serde_json::{json, Value};
use std::{pin::Pin, sync::Arc};
use tokio::{io::AsyncWriteExt, sync::OwnedRwLockReadGuard, time::Instant};
pub(crate) type UploadStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send>>;
/// Largest accepted upload body.
const MAX_UPLOAD_BYTES: usize = 256 * 1024 * 1024;

/// What the client asserted about the current state of the target.
pub(crate) enum WriteCondition {
    /// `If-None-Match: *`
    Create,
    Authorized {
        owner: usize,
        tokens: Vec<String>,
        expected: Option<String>,
        create: bool,
        scopes: Vec<usize>,
    },
    /// `If-Match: <revision>`
    Replace(String),
}

/// One in-flight replacement: everything the body writer and the commit need.
pub(crate) struct Replacement {
    pub(crate) state: Arc<State>,
    pub(crate) export: Arc<Export>,
    pub(crate) path: String,
    pub(crate) condition: WriteCondition,
    pub(crate) deadline: Instant,
    pub(crate) operation: Arc<Operation>,
    pub(crate) operation_id: String,
    pub(crate) gate: OwnedRwLockReadGuard<()>,
    pub(crate) project_guards: Vec<OwnedRwLockReadGuard<()>>,
}

impl Replacement {
    /// Stream the request body into a staging file, then publish it atomically.
    pub(crate) async fn run(self, mut stream: UploadStream) -> Result<Value, Error> {
        let Self {
            state,
            export,
            path,
            condition,
            deadline,
            operation,
            operation_id,
            gate,
            project_guards,
        } = self;
        let mut audit = UploadAudit {
            id: operation_id,
            started: std::time::Instant::now(),
            upload_id: None,
        };
        let permit = state
            .workers
            .acquire(deadline)
            .await
            .inspect_err(|error| audit.failed("worker", error))?;
        operation.checkpoint(deadline)?;
        let staging = export.clone();
        let staged_path = path.clone();
        let context = crate::core::trace::current();
        let (mut staged, gate, project_guards) = tokio::task::spawn_blocking(move || {
            crate::core::trace::sync_scope(context, || {
                staging
                    .stage(&staged_path)
                    .map(|staged| (staged, gate, project_guards))
            })
        })
        .await
        .map_err(|_| Error::internal())
        .and_then(|result| result)
        .inspect_err(|error| audit.failed("stage", error))?;
        staged.track_operation(&audit.id);
        audit.upload_id = Some(staged.journal_id());
        audit.event("stage", None);

        let mut writer = tokio::fs::File::from_std(staged.writer()?);
        let mut size = 0usize;
        loop {
            operation.checkpoint(deadline)?;
            let chunk = tokio::select! {
                _ = operation.token().cancelled() => return Err(Error::cancelled()),
                chunk = tokio::time::timeout_at(deadline, stream.next()) => {
                    chunk.map_err(|_| Error::timed_out()).inspect_err(|error|audit.failed("body-read",error))?
                }
            };
            let Some(chunk) = chunk else { break };
            let chunk = chunk.inspect_err(|error| audit.failed("body-read", error))?;
            size += chunk.len();
            if size > MAX_UPLOAD_BYTES {
                let error = Error::too_large("UPLOAD_LIMIT");
                audit.failed("body-limit", &error);
                return Err(error);
            }
            tokio::time::timeout_at(deadline, writer.write_all(&chunk))
                .await
                .map_err(|_| Error::timed_out())
                .and_then(|result| result.map_err(Error::from))
                .inspect_err(|error| audit.failed("stage-write", error))?;
        }
        writer
            .flush()
            .await
            .map_err(Error::from)
            .inspect_err(|error| audit.failed("stage-flush", error))?;
        audit.event("body-complete", None);
        drop(writer);

        let context = crate::core::trace::current();
        let result = tokio::task::spawn_blocking(move || {
            crate::core::trace::sync_scope(context, || {
                let _permit = permit;
                let _gate = gate;
                let _project_guards = project_guards;
                let checkpoint = || operation.checkpoint(deadline);
                match condition {
                    WriteCondition::Create => {
                        upload::commit(&export, &path, staged, None, checkpoint)
                    }
                    WriteCondition::Replace(expected) => {
                        upload::commit(&export, &path, staged, Some(&expected), checkpoint)
                    }
                    WriteCondition::Authorized {
                        owner,
                        tokens,
                        expected,
                        create,
                        scopes,
                    } => crate::core::file_constraints::credentials_for(
                        owner,
                        tokens.clone(),
                        scopes,
                        || {
                            if expected.is_some() || create {
                                upload::commit(
                                    &export,
                                    &path,
                                    staged,
                                    expected.as_deref(),
                                    checkpoint,
                                )
                            } else {
                                upload::commit_locked(
                                    &export, &path, staged, owner, &tokens, checkpoint,
                                )
                            }
                        },
                    ),
                }
            })
        })
        .await
        .map_err(|_| Error::unknown("FILE_COMMIT_WORKER_LOST"))
        .and_then(|result| result);
        audit.event("commit", result.as_ref().err());
        result.map(|stat| json!(stat))
    }
}

struct UploadAudit {
    upload_id: Option<i64>,
    id: String,
    started: std::time::Instant,
}
impl UploadAudit {
    fn failed(&self, stage: &'static str, error: &Error) {
        self.event(stage, Some(error));
    }
    fn event(&self, stage: &'static str, error: Option<&Error>) {
        crate::core::events::emit(
            if error.is_some() {
                crate::core::events::Level::Warn
            } else {
                crate::core::events::Level::Info
            },
            "file.upload.stage",
            json!({"operation":"file.upload","operationId":self.id,"uploadId":self.upload_id,"stage":stage,"result":if error.is_some(){"failure"}else{"success"},"code":error.map(|error|error.code),"outcome":error.and_then(|error|error.outcome).unwrap_or(if stage=="commit" && error.is_none(){"committed"}else{"not-committed"}),"durationMs":self.started.elapsed().as_millis() as u64}),
        );
    }
}
