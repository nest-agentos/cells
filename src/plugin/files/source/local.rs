//! POSIX implementation of the file-source read port.
use super::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
pub struct LocalSource(pub Arc<Export>);
impl FileSource for LocalSource {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            write: self.0.writable(),
            metadata: true,
            strong_revision: self.0.writable(),
            local_directory: true,
            seq: true,
        }
    }
    fn local_export(&self) -> Option<Arc<Export>> {
        Some(self.0.clone())
    }
    fn stat<'a>(
        &'a self,
        paths: Vec<String>,
        fields: ReadFields,
        context: ReadContext,
    ) -> SourceFuture<'a, Vec<Result<Option<Stat>, Error>>> {
        let export = self.0.clone();
        Box::pin(async move {
            let lease = context.retain();
            context
                .workers
                .run_ungated(context.deadline, move |cancel| {
                    let _lease = lease;
                    let mut results = Vec::with_capacity(paths.len());
                    for path in paths {
                        if cancel.is_cancelled() {
                            return Err(Error::cancelled());
                        }
                        results.push(export.stat_fields(&path, fields));
                    }
                    Ok(results)
                })
                .await
        })
    }
    fn list<'a>(
        &'a self,
        path: String,
        fields: ReadFields,
        context: ReadContext,
    ) -> SourceFuture<'a, Listing> {
        let export = self.0.clone();
        Box::pin(async move {
            let lease = context.retain();
            context
                .workers
                .run_ungated(context.deadline, move |cancel| {
                    let _lease = lease;
                    export.list_fields(&path, &cancel, fields)
                })
                .await
        })
    }
    fn open<'a>(&'a self, path: String, context: ReadContext) -> SourceFuture<'a, OpenContent> {
        let export = self.0.clone();
        Box::pin(async move {
            let lease = context.retain();
            context
                .workers
                .run_ungated(context.deadline, move |_| {
                    let (file, revision) = export.read(&path)?;
                    let size = file.metadata()?.len();
                    Ok(OpenContent {
                        size,
                        revision,
                        content: Box::new(LocalContent {
                            file,
                            _lease: lease,
                        }),
                    })
                })
                .await
        })
    }
}
struct LocalContent {
    file: std::fs::File,
    _lease: Arc<super::super::reading::ReadLease>,
}
impl Content for LocalContent {
    fn read(self: Box<Self>, offset: u64, length: u64) -> SourceFuture<'static, Reader> {
        Box::pin(async move {
            let mut file = tokio::fs::File::from_std(self.file);
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            // The coordinator keeps its lease through consumption of the returned reader.
            Ok(Box::pin(file.take(length)) as Reader)
        })
    }
}
