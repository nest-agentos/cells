//! Persistent site access for a registered, origin-bound tool pair.
use super::*;

pub(super) fn site_registration(args: &Value) -> Result<Option<Value>, Error> {
    let Some(site) = args.get("site") else { return Ok(None); };
    let origin = reqwest::Url::parse(site["origin"].as_str().ok_or_else(Error::invalid)?)
        .map_err(|_| Error::invalid())?;
    let read = site["readTool"].as_str().ok_or_else(Error::invalid)?;
    let write = site["writeTool"].as_str().ok_or_else(Error::invalid)?;
    let tools = args["tools"].as_array().ok_or_else(Error::invalid)?;
    if origin.scheme() != "https" || origin.path() != "/" || !origin.username().is_empty()
        || origin.password().is_some() || origin.query().is_some() || origin.fragment().is_some()
        || read == write || tools.len() != 2 || !tools.iter().any(|tool| tool == read)
        || !tools.iter().any(|tool| tool == write)
    {
        return Err(Error::invalid());
    }
    Ok(Some(json!({"origin":origin.origin().ascii_serialization(),"readTool":read,"writeTool":write})))
}

impl Channels {
    pub fn site_approval(&self, owner: &str, session: &str, server: &str, tool: &str) -> Result<Option<Value>, Error> {
        self.6.owner(owner, |db| {
            let Some((revision, value)) = site_row(db, owner, server)? else { return Ok(None); };
            if value["enabled"] != true || value["sessionId"] != session {
                return Ok(None);
            }
            let site = &value["site"];
            let level = if site["readTool"] == tool { "read" }
                else if site["writeTool"] == tool { "write" }
                else { return Ok(None); };
            let allowed = value["siteGrant"] == "read-write" || (level == "read" && value["siteGrant"] == "read");
            Ok(Some(json!({"origin":site["origin"],"revision":revision,"level":level,"allowed":allowed})))
        })
    }

    pub fn grant_site(&self, owner: &str, session: &str, server: &str, revision: u64, grant: &str) -> Result<(), Error> {
        self.update_site_grant(owner, server, revision, Some(session), Some(grant))
    }

    pub fn revoke_site(&self, owner: &str, args: &Value) -> Result<Value, Error> {
        let id = identifier(args, "id")?;
        let revision = args["revision"].as_u64().ok_or_else(Error::invalid)?;
        self.update_site_grant(owner, id, revision, None, None)?;
        Ok(json!({"id":id,"revision":revision+1,"revoked":true}))
    }

    fn update_site_grant(&self, owner: &str, server: &str, revision: u64, session: Option<&str>, grant: Option<&str>) -> Result<(), Error> {
        self.6.owner(owner, |connection| {
            let db = connection.transaction().map_err(storage)?;
            let (current, mut value) = site_row(&db, owner, server)?.ok_or_else(|| Error::not_found("AGENT_SERVER_NOT_FOUND"))?;
            if current != revision || !value["site"].is_object()
                || session.is_some_and(|id| value["sessionId"] != id || value["enabled"] != true) {
                return Err(Error::conflict("AGENT_SERVER_CHANGED"));
            }
            match grant {
                Some("read" | "read-write") => value["siteGrant"] = json!(grant),
                None => { value.as_object_mut().unwrap().remove("siteGrant"); },
                _ => return Err(Error::invalid()),
            }
            db.execute("UPDATE agent_servers SET revision=?3,body=?4 WHERE owner=?1 AND id=?2",
                params![owner,server,revision+1,value.to_string()]).map_err(storage)?;
            db.commit().map_err(storage)
        })
    }
}

fn site_row(db: &rusqlite::Connection, owner: &str, server: &str) -> Result<Option<(u64, Value)>, Error> {
    let row: Option<(u64, String)> = db.query_row("SELECT revision,body FROM agent_servers WHERE owner=?1 AND id=?2",
        params![owner,server], |row| Ok((row.get(0)?,row.get(1)?))).optional().map_err(storage)?;
    row.map(|(revision, body)| Ok((revision, serde_json::from_str(&body).map_err(storage)?))).transpose()
}

#[cfg(test)]
#[test]
fn site_decisions_are_bound_and_revocable() {
    let root = tempfile::tempdir().unwrap();
    let store = Channels::open(root.path()).unwrap();
    let mut registration = json!({"id":"browser","name":"Browser","url":"http://127.0.0.1:18887/mcp",
        "revision":0,"enabled":true,"sessionId":"s","tools":["read","operate"],
        "site":{"origin":"https://example.test:2000","readTool":"read","writeTool":"operate"}});
    store.server_save("owner", &registration).unwrap();
    assert!(store.site_approval("other", "s", "browser", "read").unwrap().is_none());
    assert!(store.site_approval("owner", "other", "browser", "read").unwrap().is_none());
    assert!(store.site_approval("owner", "s", "browser", "unlisted").unwrap().is_none());
    store.grant_site("owner", "s", "browser", 1, "read").unwrap();
    assert_eq!(store.site_approval("owner", "s", "browser", "read").unwrap().unwrap()["allowed"], true);
    assert_eq!(store.site_approval("owner", "s", "browser", "operate").unwrap().unwrap()["allowed"], false);
    assert!(store.grant_site("owner", "s", "browser", 1, "read-write").is_err());
    store.grant_site("owner", "s", "browser", 2, "read-write").unwrap();
    assert_eq!(store.site_approval("owner", "s", "browser", "operate").unwrap().unwrap()["allowed"], true);
    store.revoke_site("owner", &json!({"id":"browser","revision":3})).unwrap();
    assert_eq!(store.site_approval("owner", "s", "browser", "read").unwrap().unwrap()["allowed"], false);
    store.grant_site("owner", "s", "browser", 4, "read-write").unwrap();
    registration["revision"] = json!(5);
    registration["site"]["origin"] = json!("https://example.test:3000");
    store.server_save("owner", &registration).unwrap();
    assert_eq!(store.site_approval("owner", "s", "browser", "operate").unwrap().unwrap()["allowed"], false);
}
