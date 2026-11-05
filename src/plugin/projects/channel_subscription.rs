//! Channel invalidations contain no messages and never acquire the workspace file gate.
use crate::{app::State,core::error::Error,plugin::common::EventStream};
use serde_json::{Value,json};
use std::sync::Arc;
use futures_util::stream;
fn version(state:&State,owner:usize,args:&Value)->Result<String,Error> {
    let service=state.services.require::<super::ProjectService>()?;
    let project=service.get(owner,args["projectId"].as_str().ok_or_else(Error::invalid)?)?;
    super::service::authorize(state,owner,&project)?;
    if args["revision"].as_u64()!=Some(project.revision) {return Err(Error::conflict("PROJECT_REVISION_CHANGED"));}
    service.channels.revision(&project.id,args["channelId"].as_str().ok_or_else(Error::invalid)?)
}
pub async fn subscribe(state:Arc<State>,owner:usize,args:Value)->Result<EventStream,Error> {
    let service=state.services.require::<super::ProjectService>()?;
    let permit=service.watch_slots.clone().try_acquire_owned().map_err(|_|Error::too_many("CHANNEL_WATCH_LIMIT"))?;
    let receiver=service.channels.watch();version(&state,owner,&args)?;
    Ok(Box::pin(stream::unfold((state,owner,args,receiver,None,permit),|(state,owner,args,mut receiver,mut previous,permit)|async move {
        loop {
            let current=version(&state,owner,&args);
            match current {
                Err(error)=>return Some((Err(error),(state,owner,args,receiver,previous,permit))),
                Ok(current) if previous.as_ref()!=Some(&current)=>{
                    previous=Some(current.clone());
                    return Some((Ok(json!({"version":current,"scopeId":args["channelId"]})),(state,owner,args,receiver,previous,permit)));
                },
                _=>(),
            }
            tokio::select! {result=receiver.changed()=>{if result.is_err(){return None;}},_=tokio::time::sleep(std::time::Duration::from_secs(15))=>{}}
        }
    })))
}
