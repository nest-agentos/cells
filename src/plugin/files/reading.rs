//! Read coordination: grants, path policy, lifecycle fences, deadlines and diagnostics.
use super::source::{FileSource, ReadContext, SourceFuture, Sources};
use crate::{app::State, core::error::Error};
use std::sync::Arc;

pub(crate) struct ReadLease {
    _gate: tokio::sync::OwnedRwLockReadGuard<()>,
    _policies: Vec<tokio::sync::OwnedRwLockReadGuard<()>>,
}
pub(crate) struct Reading {
    pub source: Arc<dyn FileSource>,
    pub context: ReadContext,
    alias: String,
    operation_id: String,
}
impl Reading {
    pub fn authorize(
        state: &State,
        identity: usize,
        alias: &str,
        policies: Vec<tokio::sync::OwnedRwLockReadGuard<()>>,
        deadline: tokio::time::Instant,
    ) -> Result<Self, Error> {
        state.auth.authorize(identity, alias)?;
        let source = state
            .services
            .require::<Sources>()?
            .get(alias)
            .ok_or_else(|| Error::forbidden("EACCES"))?
            .clone();
        Ok(Self {
            source,
            context: read_context(state, policies, deadline)?,
            alias: alias.into(),
            operation_id: crate::app::random_epoch().map_err(|_| Error::internal())?,
        })
    }
    pub async fn stat(
        &self,
        paths: Vec<String>,
        fields: super::source::ReadFields,
    ) -> Result<Vec<Result<Option<super::source::Stat>, Error>>, Error> {
        // Keep invalid paths as per-item errors and never dispatch them to a provider.
        let valid = paths
            .iter()
            .filter(|p| super::fs::path::validate(p).is_ok())
            .cloned()
            .collect::<Vec<_>>();
        let expected = valid.len();
        let results = self
            .run(
                "stat",
                self.source.stat(valid, fields, self.context.clone()),
            )
            .await?;
        self.merge_stats(&paths, expected, results)
    }
    fn merge_stats(
        &self,
        paths: &[String],
        expected: usize,
        results: Vec<Result<Option<super::source::Stat>, Error>>,
    ) -> Result<Vec<Result<Option<super::source::Stat>, Error>>, Error> {
        if results.len() != expected {
            let error = Error::new(Error::internal().status, "FILE_SOURCE_RESULT_MISMATCH");
            self.failure("stat-results", error.code, 0);
            return Err(error);
        }
        let mut results = results.into_iter();
        paths
            .iter()
            .map(|path| match super::fs::path::validate(path) {
                Err(error) => Ok(Err(error)),
                Ok(()) => results.next().ok_or_else(Error::internal),
            })
            .collect()
    }

    pub async fn chunk(
        &self,
        reader: &mut super::source::Reader,
        data: &mut [u8],
    ) -> Result<usize, std::io::Error> {
        use tokio::io::AsyncReadExt;
        let started = std::time::Instant::now();
        let result = tokio::time::timeout_at(self.context.deadline, reader.read(data))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read deadline"))
            .and_then(|result| result)
            .and_then(nonempty);
        if let Err(error) = &result {
            let code = match error.kind() {
                std::io::ErrorKind::TimedOut => "FILE_SOURCE_READ_TIMEOUT",
                std::io::ErrorKind::UnexpectedEof => "FILE_SOURCE_TRUNCATED",
                _ => "FILE_SOURCE_READ_FAILED",
            };
            self.failure("transfer", code, started.elapsed().as_millis());
        }
        result
    }
    fn failure(&self, stage: &str, code: &str, elapsed_ms: u128) {
        crate::core::events::emit(
            crate::core::events::Level::Warn,
            "files.source",
            serde_json::json!({"operationId":self.operation_id,"operation":"files.read","stage":stage,
                "alias":self.alias,"result":"failure","code":code,"elapsedMs":elapsed_ms}),
        );
    }

    pub async fn run<T>(
        &self,
        stage: &'static str,
        future: SourceFuture<'_, T>,
    ) -> Result<T, Error> {
        let started = std::time::Instant::now();
        let result = tokio::time::timeout_at(self.context.deadline, future)
            .await
            .unwrap_or_else(|_| Err(Error::timed_out()));
        crate::core::events::emit(
            if result.is_ok() {
                crate::core::events::Level::Debug
            } else {
                crate::core::events::Level::Warn
            },
            "files.source",
            serde_json::json!({"operationId":self.operation_id,"operation":"files.read","stage":stage,
                "alias":self.alias,"result":if result.is_ok(){"success"}else{"failure"},
                "code":result.as_ref().err().map(|e|e.code),"elapsedMs":started.elapsed().as_millis()}),
        );
        result
    }
}

fn nonempty(read: usize) -> Result<usize, std::io::Error> {
    if read == 0 {
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "content changed while streaming",
        ))
    } else {
        Ok(read)
    }
}

fn read_context(
    state: &State,
    policies: Vec<tokio::sync::OwnedRwLockReadGuard<()>>,
    deadline: tokio::time::Instant,
) -> Result<ReadContext, Error> {
    Ok(ReadContext {
        deadline,
        workers: state.workers.clone(),
        lease: Arc::new(ReadLease {
            _gate: state.files.read_only()?,
            _policies: policies,
        }),
    })
}
