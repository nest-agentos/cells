//! Immutable request/response records for host-resolved operations.
use super::*;
use sha2::{Digest,Sha256};
fn digest(raw:&str)->String {format!("{:x}",Sha256::digest(raw.as_bytes()))}
pub(super) fn request(db:&rusqlite::Transaction,project:&str,scope:&str,args:&Value)->Result<Value,Error> {
    let id=identifier(args,"id")?;
    let method=args["method"].as_str().filter(|s|s.len()<=128&&!s.is_empty()).ok_or_else(Error::invalid)?;
    if !args["params"].is_object() {return Err(Error::invalid());}
    let raw=serde_json_canonicalizer::to_string(&json!({"id":id,"scopeId":scope,"method":method,"params":args["params"]})).map_err(storage)?;
    if raw.len()>65536 {return Err(Error::too_large("CHANNEL_REQUEST_LIMIT"));}
    let existing:Option<String>=db.query_row("SELECT request FROM exchanges WHERE project=?1 AND scope=?2 AND id=?3",params![project,scope,id],|r|r.get(0)).optional().map_err(storage)?;
    if let Some(existing)=existing {if existing!=raw {return Err(Error::conflict("CHANNEL_REQUEST_CHANGED"));}}
    else {
        let count:u64=db.query_row("SELECT COUNT(*) FROM exchanges WHERE project=?1 AND scope=?2",params![project,scope],|r|r.get(0)).map_err(storage)?;
        if count>=128 {return Err(Error::too_many("CHANNEL_REQUEST_LIMIT"));}
        db.execute("INSERT INTO exchanges(project,scope,id,request) VALUES(?1,?2,?3,?4)",params![project,scope,id,raw]).map_err(storage)?;
    }
    response(db,project,scope,args)
}
pub(super) fn response(db:&Connection,project:&str,scope:&str,args:&Value)->Result<Value,Error> {
    let id=identifier(args,"id")?;
    let raw:Option<Option<String>>=db.query_row("SELECT response FROM exchanges WHERE project=?1 AND scope=?2 AND id=?3",params![project,scope,id],|r|r.get(0)).optional().map_err(storage)?;
    match raw {Some(Some(raw))=>serde_json::from_str(&raw).map_err(storage),Some(None)=>Ok(json!({"id":id,"scopeId":scope,"status":"pending","mustYield":true})),None=>Err(Error::not_found("CHANNEL_REQUEST_UNKNOWN"))}
}
pub(super) fn list(db:&Connection,project:&str,scope:&str)->Result<Value,Error> {
    let mut statement=db.prepare("SELECT request,response FROM exchanges WHERE project=?1 AND scope=?2 ORDER BY seq LIMIT 128").map_err(storage)?;
    let rows=statement.query_map(params![project,scope],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?))).map_err(storage)?;
    let mut result=Vec::new();
    for row in rows {let (request,response)=row.map_err(storage)?;result.push(json!({"request":serde_json::from_str::<Value>(&request).map_err(storage)?,"requestDigest":digest(&request),"response":response.map(|s|serde_json::from_str::<Value>(&s)).transpose().map_err(storage)?}));}
    Ok(json!({"requests":result}))
}
pub(super) fn respond(db:&Connection,project:&str,scope:&str,args:&Value)->Result<Value,Error> {
    let id=identifier(args,"id")?;
    let (request,previous):(String,Option<String>)=db.query_row("SELECT request,response FROM exchanges WHERE project=?1 AND scope=?2 AND id=?3",params![project,scope,id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(storage)?.ok_or_else(||Error::not_found("CHANNEL_REQUEST_UNKNOWN"))?;
    if args["requestDigest"].as_str()!=Some(&digest(&request)) {return Err(Error::conflict("CHANNEL_REQUEST_CHANGED"));}
    let status=args["status"].as_str().filter(|s|["ok","error"].contains(s)).ok_or_else(Error::invalid)?;
    let result=json!({"id":id,"scopeId":scope,"requestDigest":digest(&request),"status":status,"payload":args["payload"],"error":args["error"]});
    let raw=serde_json_canonicalizer::to_string(&result).map_err(storage)?;if raw.len()>65536 {return Err(Error::too_large("CHANNEL_RESPONSE_LIMIT"));}
    if let Some(previous)=previous {if previous!=raw {return Err(Error::conflict("CHANNEL_RESPONSE_CHANGED"));}}
    else {db.execute("UPDATE exchanges SET response=?4 WHERE project=?1 AND scope=?2 AND id=?3",params![project,scope,id,raw]).map_err(storage)?;}
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_hash_matches_ecmascript_numbers_and_utf16_key_order() {
        let fixture:Value=serde_json::from_str(include_str!("../tests/channel-canonical.json")).unwrap();
        let encoded=serde_json_canonicalizer::to_string(&fixture["value"]).unwrap();
        assert_eq!(encoded,fixture["canonical"].as_str().unwrap());
        assert_eq!(digest(&encoded),fixture["sha256"].as_str().unwrap());
    }
}
