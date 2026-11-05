//! Owner-scoped MCP clients for native Harnesses. Management credentials never enter this registry.
use super::*;
impl Channels {
    pub fn server_list(&self,owner:&str)->Result<Value,Error> {
        let db=self.0.lock().map_err(storage)?;
        let mut stmt=db.prepare("SELECT id,revision,body FROM agent_servers WHERE owner=?1 ORDER BY id").map_err(storage)?;
        let rows=stmt.query_map([owner],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?,r.get::<_,String>(2)?))).map_err(storage)?;
        let mut servers=Vec::new();
        for row in rows {let (id,revision,body)=row.map_err(storage)?;let mut value:Value=serde_json::from_str(&body).map_err(storage)?;value["id"]=json!(id);value["revision"]=json!(revision);value["hasCredential"]=json!(value.get("credential").is_some());value.as_object_mut().unwrap().remove("credential");servers.push(value);}
        Ok(json!({"servers":servers}))
    }
    pub fn server_save(&self,owner:&str,args:&Value)->Result<Value,Error> {
        let id=identifier(args,"id")?;
        if id=="cells_session"||id.len()>64{return Err(Error::invalid());}
        let name=args["name"].as_str().filter(|s|!s.trim().is_empty()&&s.len()<=256).ok_or_else(Error::invalid)?;
        let url=reqwest::Url::parse(args["url"].as_str().ok_or_else(Error::invalid)?).map_err(|_|Error::invalid())?;
        if !["https","http"].contains(&url.scheme())||!url.username().is_empty()||url.password().is_some()||url.query().is_some()||url.fragment().is_some(){return Err(Error::invalid());}
        let tools=args["tools"].as_array().filter(|t|!t.is_empty()&&t.len()<=128&&t.iter().all(|v|v.as_str().is_some_and(|s|s.len()<=128&&!s.is_empty()&&s.bytes().all(|b|b.is_ascii_alphanumeric()||b"_.-".contains(&b))))).ok_or_else(Error::invalid)?;
        let expected=args["revision"].as_u64().ok_or_else(Error::invalid)?;
        let mut connection=self.0.lock().map_err(storage)?;let db=connection.transaction().map_err(storage)?;
        let previous:Option<(u64,String)>=db.query_row("SELECT revision,body FROM agent_servers WHERE owner=?1 AND id=?2",params![owner,id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(storage)?;
        if previous.as_ref().map(|p|p.0).unwrap_or(0)!=expected{return Err(Error::conflict("AGENT_SERVER_CHANGED"));}
        let mut value=json!({"name":name,"url":url.as_str(),"tools":tools,"enabled":args["enabled"].as_bool().ok_or_else(Error::invalid)?});
        let old=previous.map(|p|serde_json::from_str::<Value>(&p.1)).transpose().map_err(storage)?;
        if let Some(credential)=args.get("credential").or(old.as_ref().and_then(|v|v.get("credential"))) {
            let credential=credential.as_str().filter(|s|s.len()<=8192&&!s.contains(['\r','\n'])).ok_or_else(Error::invalid)?;
            if !credential.is_empty(){value["credential"]=json!(credential);}
        }
        db.execute("INSERT INTO agent_servers(owner,id,revision,body) VALUES(?1,?2,?3,?4) ON CONFLICT(owner,id) DO UPDATE SET revision=excluded.revision,body=excluded.body",params![owner,id,expected+1,value.to_string()]).map_err(storage)?;
        db.commit().map_err(storage)?;Ok(json!({"id":id,"revision":expected+1,"appliesTo":"next-native-context"}))
    }
    pub fn server_remove(&self,owner:&str,args:&Value)->Result<Value,Error> {
        let id=identifier(args,"id")?;let revision=args["revision"].as_u64().ok_or_else(Error::invalid)?;
        let count=self.0.lock().map_err(storage)?.execute("DELETE FROM agent_servers WHERE owner=?1 AND id=?2 AND revision=?3",params![owner,id,revision]).map_err(storage)?;
        if count!=1{return Err(Error::conflict("AGENT_SERVER_CHANGED"));}Ok(json!({"removed":true,"appliesTo":"next-native-context"}))
    }
    pub(super) fn agent_config(&self,owner:&str,scoped:Option<Value>)->Result<Option<Value>,Error> {
        let db=self.0.lock().map_err(storage)?;let mut stmt=db.prepare("SELECT id,body FROM agent_servers WHERE owner=?1 ORDER BY id").map_err(storage)?;
        let rows=stmt.query_map([owner],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(storage)?;
        let mut config=scoped.unwrap_or(json!({}));
        for row in rows {let (id,raw)=row.map_err(storage)?;let value:Value=serde_json::from_str(&raw).map_err(storage)?;
            if value["enabled"]!=true{continue;}
            let mut server=json!({"url":value["url"],"enabled_tools":value["tools"],"required":true});
            if let Some(secret)=value["credential"].as_str(){server["http_headers"]=json!({"Authorization":format!("Bearer {secret}")});}
            config[format!("mcp_servers.{id}")]=server;
        }
        if config.as_object().unwrap().is_empty(){return Ok(None);}
        config["features.mcp_2026_07_28"]=json!(true);Ok(Some(config))
    }
}
