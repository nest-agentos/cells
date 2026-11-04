//! Read SDK metadata without resuming a cold session or sending a user prompt.
use super::*;

impl Claude {
    async fn discovery_cwd(&self, args: &Value) -> Result<String, Error> {
        if let Some(id) = args["sessionId"].as_str() {
            return Ok(string(&self.session(id).await?, "cwd")?.into());
        }
        let workspace = match args["workspaceId"].as_str() {
            Some(id) => self.config.workspaces.iter().find(|w| w.id == id),
            None => self.config.workspaces.first(),
        }
        .ok_or_else(Error::unsupported)?;
        Ok(workspace.path.to_string_lossy().into())
    }
    pub(super) async fn discover(&self, args: &Value, fresh: bool) -> Result<Value, Error> {
        let cwd = self.discovery_cwd(args).await?;
        let mut cache = self.discovery.lock().await;
        // Commands can change on disk; cache only stable model metadata.
        if !fresh {
            if let Some(value) = cache.get(&cwd) {
                return Ok(value.clone());
            }
        }
        let process = process::Process::start(
            &self.config,
            self.runtime.clone(),
            json!({"id":uuid()?,"cwd":cwd,"status":"idle","catalogOnly":true}),
            None,
            Arc::new(Mutex::new(Events::new())),
        )
        .await?;
        let value = json!({"models":process.models.lock().unwrap().clone(),
            "commands":process.commands.lock().unwrap().clone()});
        process.drain().await;
        cache.insert(cwd, value.clone());
        Ok(value)
    }
}
