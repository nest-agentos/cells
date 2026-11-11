//! A bounded admission handoff, never a retry of native execution.
use super::{error::Error,gate::{ActivityPermit,FileGate}};
use tokio::time::{Duration,Instant};
impl FileGate {
    pub async fn admit_activity(&self,operation:&str,budget_ms:u64)->Result<ActivityPermit,Error> {
        let started=Instant::now();let deadline=started+Duration::from_millis(budget_ms.min(3000));
        let mut audit=AdmissionAudit{operation:operation.into(),started,trace:super::trace::current(),finished:false};
        let result=tokio::time::timeout_at(deadline,async {
            let _admission=self.native_admission.lock().await;
            self.check()?;
            if let Some(permit)=self.join_activity()?{return Ok(permit);}
            // Transfer the writer permit atomically; no native input has been sent.
            let previous=match self.inner.clone().try_write_owned(){Ok(guard)=>guard,Err(_)=>{
                let mut fields=self.owners.fields();fields.as_object_mut().unwrap().extend(serde_json::json!({
                    "operation":"file.gate.acquire","operationId":operation,"stage":"native-admission","result":"waiting","durationMs":0}).as_object().unwrap().clone());
                super::events::emit(super::events::Level::Info,"file.gate.native_wait",fields);
                self.inner.clone().write_owned().await
            }};
            self.retain_activity(previous)
        }).await.unwrap_or_else(|_|Err(Error::conflict("NATIVE_WORKSPACE_BUSY")));
        audit.complete(&result,budget_ms,self.owners.fields());
        result
    }
}

struct AdmissionAudit {operation:String,started:Instant,trace:serde_json::Map<String,serde_json::Value>,finished:bool}
impl AdmissionAudit {
    fn complete<T>(&mut self,result:&Result<T,Error>,budget:u64,mut fields:serde_json::Value) {
        self.finished=true;fields.as_object_mut().unwrap().extend(serde_json::json!({
            "operation":"file.gate.acquire","operationId":self.operation,"stage":"native-admission",
            "result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|e|e.code),
            "executionStarted":false,"durationMs":self.started.elapsed().as_millis(),"budgetMs":budget.min(3000)}).as_object().unwrap().clone());
        super::events::emit(if result.is_ok(){super::events::Level::Info}else{super::events::Level::Warn},"file.gate.native_admission",fields);
    }
}
impl Drop for AdmissionAudit {
    fn drop(&mut self){if !self.finished {
        let fields=serde_json::json!({"operation":"file.gate.acquire","operationId":self.operation,"stage":"native-admission",
            "result":"cancelled","executionStarted":false,"durationMs":self.started.elapsed().as_millis()});
        super::trace::sync_scope(self.trace.clone(),||super::events::emit(super::events::Level::Info,"file.gate.native_cancelled",fields));
    }}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn short_writer_handoff_joins_parallel_native_owners_without_replaying_work() {
        let gate=FileGate::new();let writer=gate.command("prepare",1000).await.unwrap();
        let mut a=Box::pin(gate.admit_activity("native-a",1000));
        let mut b=Box::pin(gate.admit_activity("native-b",1000));
        tokio::select!{biased;_= &mut a=>panic!("writer still held"),_=tokio::task::yield_now()=>{}}
        tokio::select!{biased;_= &mut b=>panic!("writer still held"),_=tokio::task::yield_now()=>{}}
        drop(writer);
        let (a,b)=tokio::time::timeout(Duration::from_millis(100),async{(a.await.unwrap(),b.await.unwrap())}).await.unwrap();
        assert!(gate.read_only().is_ok());assert!(gate.shared().is_err());drop(a);
        assert!(gate.shared().is_err());drop(b);assert!(gate.exclusive().is_ok());
    }
    #[tokio::test]
    async fn timeout_cancellation_and_poisoning_never_leak_or_admit_an_unknown_owner() {
        let gate=FileGate::new();let writer=gate.command("long",1000).await.unwrap();
        assert_eq!(gate.admit_activity("timeout",5).await.unwrap_err().code,"NATIVE_WORKSPACE_BUSY");
        let mut pending=Box::pin(gate.admit_activity("cancelled",1000));
        tokio::select!{biased;_= &mut pending=>panic!("writer still held"),_=tokio::task::yield_now()=>{}}
        drop(pending);drop(writer);assert!(gate.exclusive().is_ok());
        let writer=gate.command("poison",1000).await.unwrap();
        let mut pending=Box::pin(gate.admit_activity("unknown",1000));
        tokio::select!{biased;_= &mut pending=>panic!("writer still held"),_=tokio::task::yield_now()=>{}}
        gate.poison();drop(writer);assert_eq!(pending.await.unwrap_err().code,"EIO");
    }
}
