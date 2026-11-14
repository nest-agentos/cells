//! Safe lifecycle ownership metadata, without command arguments or file content.
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);
#[derive(Clone, Default, Debug)]
pub(super) struct Owners(Arc<Mutex<BTreeMap<u64, Owner>>>);
#[derive(Debug)]
struct Owner {
    operation_id: Option<String>,
    session_id: Option<String>,
    request_id: Option<String>,
    kind: &'static str,
    pid: Option<u32>,
    started: Instant,
    trace: serde_json::Map<String, Value>,
}
#[derive(Debug)]
pub(super) struct OwnerGuard {
    owners: Owners,
    id: u64,
}
impl Owners {
    pub fn enter(&self, kind: &'static str, operation_id: Option<&str>) -> OwnerGuard {
        let trace = super::trace::current();
        let id = NEXT_OWNER.fetch_add(1, Ordering::Relaxed);
        self.0.lock().unwrap().insert(
            id,
            Owner {
                operation_id: operation_id.map(str::to_owned),
                session_id: None,
                request_id: trace
                    .get("requestId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                kind,
                pid: None,
                started: Instant::now(),
                trace,
            },
        );
        OwnerGuard {
            owners: self.clone(),
            id,
        }
    }
    pub fn fields(&self) -> Value {
        let owners = self.0.lock().unwrap();
        let fields = |owner: &Owner| {
            json!({"blockerOperationId":owner.operation_id,"blockerSessionId":owner.session_id,
            "blockerRequestId":owner.request_id,"blockerTraceId":owner.trace.get("traceId"),"blockerSpanId":owner.trace.get("spanId"),
            "blockerKind":owner.kind,"blockerPid":owner.pid,"heldMs":owner.started.elapsed().as_millis()})
        };
        match owners.len() {
            0 => json!({"blockerKind":"file-operation"}),
            1 => fields(owners.values().next().unwrap()),
            _ => {
                json!({"blockerKind":"lifecycle-owners","blockerCount":owners.len(),"blockers":owners.values().take(16).map(fields).collect::<Vec<_>>()})
            }
        }
    }
}
impl OwnerGuard {
    pub fn process_started(&self, pid: Option<u32>) {
        if let Some(owner) = self.owners.0.lock().unwrap().get_mut(&self.id) {
            owner.pid = pid;
        }
    }
    pub fn session_started(&self, session: Option<&str>) {
        if let Some(owner) = self.owners.0.lock().unwrap().get_mut(&self.id) {
            owner.session_id = session.map(str::to_owned);
        }
    }
}
impl Drop for OwnerGuard {
    fn drop(&mut self) {
        let owner = self.owners.0.lock().unwrap().remove(&self.id);
        if let Some(owner) = owner {
            let fields = json!({"operation":"file.lifecycle.release","operationId":owner.operation_id,"sessionId":owner.session_id,
                "stage":"release","result":"success","holderKind":owner.kind,"pid":owner.pid,"heldMs":owner.started.elapsed().as_millis()});
            super::trace::sync_scope(owner.trace, || {
                super::events::emit(super::events::Level::Info, "file.gate.released", fields)
            });
        }
    }
}
