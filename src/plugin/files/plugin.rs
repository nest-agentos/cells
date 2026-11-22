use crate::{app::State, plugins::NodePlugin};

use std::sync::Arc;

pub struct Files;
impl NodePlugin for Files {
    fn http_methods(&self) -> &'static [&'static str] {
        &["HEAD", "PROPFIND", "MKCOL", "MOVE", "DELETE", "LOCK", "UNLOCK"]
    }
    fn request_headers(&self) -> &'static [&'static str] {
        &["depth", "destination", "overwrite", "lock-token", "timeout", "if"]
    }
    fn response_headers(&self) -> &'static [&'static str] {
        &["dav", "allow", "lock-token", "timeout", "x-cells-dav-structural-conditions"]
    }
    fn capabilities(
        &self,
        state: &State,
        identity: usize,
        enabled: bool,
    ) -> crate::plugin::common::Capabilities {
        super::contribution::capabilities(state, identity, enabled)
    }
    fn directory_fields(&self) -> &'static [&'static str] {
        &["exports"]
    }
    fn directories(
        &self,
        state: &State,
        identity: usize,
        _cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<serde_json::Map<String, serde_json::Value>, crate::core::error::Error> {
        Ok(crate::plugin::common::object(
            serde_json::json!({"exports":super::directories::exports(state,state.auth.client(identity))}),
        ))
    }
    fn id(&self) -> &'static str {
        "files"
    }
    fn configure(
        &self,
        config: &dyn crate::plugin::common::PluginConfiguration,
        state: &mut State,
    ) -> Result<(), String> {
        let options: super::webdav::Options = config.option("files").cloned().map(|value| value.try_into()).transpose().map_err(|_| "Invalid files options")?.unwrap_or_default();
        if options.webdav {
            state.exports.enable_webdav_locks(&mut state.files)?;
        }
        state.services.insert(Arc::new(options))?;
        super::source::Sources::install(state)?;
        state
            .services
            .insert(Arc::new(super::operations::Operations::default()))
    }
    fn stop<'a>(&'a self, state: &'a Arc<State>) -> crate::plugin::common::PluginFuture<'a> {
        Box::pin(async move {
            if let Some(service) = state.services.get::<super::operations::Operations>() {
                service.cancel_all();
            }
            Ok(())
        })
    }

}
