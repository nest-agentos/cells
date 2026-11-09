//! Private per-Agent routing into the common Cells MCP 2.0 channel.
use crate::core::error::Error;
use serde_json::{json, Value};
use std::{path::Path, sync::Mutex};
pub(super) struct Bindings {
    directory: tempfile::TempDir,
    values: Mutex<serde_json::Map<String, Value>>,
}
impl Bindings {
    pub fn new(home: &Path) -> Result<Self, Error> {
        let directory = tempfile::Builder::new()
            .prefix(".cells-channel-")
            .tempdir_in(home)?;
        std::fs::write(
            directory.path().join("channel.mjs"),
            include_str!("channel.mjs"),
        )?;
        let value = Self {
            directory,
            values: Mutex::new(serde_json::Map::new()),
        };
        value.save(&json!({}))?;
        Ok(value)
    }
    pub fn patch(&self) -> Value {
        json!({"insert":[{"id":"cells-session-channel","name":self.directory.path().join("channel.mjs"),"config":{
            "bindings":self.directory.path().join("bindings.json"),"tools":crate::projects::channel::definitions()}}]})
    }
    pub fn bind(&self, id: &str, config: Option<&Value>) -> Result<(), Error> {
        let mut values = self.values.lock().map_err(|_| Error::internal())?;
        if let Some(server) = config.and_then(|c| c.get("mcp_servers.cells_session")) {
            values.insert(
                id.into(),
                json!({"url":server["url"],"headers":server["http_headers"]}),
            );
        } else {
            values.remove(id);
        }
        self.save(&Value::Object(values.clone()))
    }
    fn save(&self, value: &Value) -> Result<(), Error> {
        use std::io::Write;
        let mut file = tempfile::NamedTempFile::new_in(self.directory.path())?;
        file.write_all(value.to_string().as_bytes())?;
        file.flush()?;
        file.persist(self.directory.path().join("bindings.json"))
            .map_err(|_| Error::internal())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bindings_rotate_remove_and_remain_private() {
        use std::os::unix::fs::PermissionsExt;
        let home = tempfile::tempdir().unwrap();
        let bindings = Bindings::new(home.path()).unwrap();
        let path = bindings.directory.path().join("bindings.json");
        let config = |token: &str| json!({"mcp_servers.cells_session":{"url":"http://localhost/v1/channel/mcp","http_headers":{"Authorization":token}}});
        bindings
            .bind("first", Some(&config("first-token")))
            .unwrap();
        bindings
            .bind("second", Some(&config("second-token")))
            .unwrap();
        bindings.bind("first", Some(&config("rotated"))).unwrap();
        let read = || serde_json::from_slice::<Value>(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(read()["first"]["headers"]["Authorization"], "rotated");
        assert_eq!(read()["second"]["headers"]["Authorization"], "second-token");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        bindings.bind("first", None).unwrap();
        assert!(read().get("first").is_none());
        drop(bindings);
        assert!(!path.exists());
    }
}
