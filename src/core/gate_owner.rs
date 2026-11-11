//! Safe ownership metadata; never store command arguments, paths or file content.
use serde_json::{json, Value};
use std::{sync::{Arc, Mutex}, time::Instant};

#[derive(Clone, Default, Debug)]
pub(super) struct Owners(Arc<Mutex<Option<Owner>>>);

#[derive(Debug)]
struct Owner {
    operation_id: Option<String>,
    request_id: Option<String>,
    kind: &'static str,
    pid: Option<u32>,
    started: Instant,
}

#[derive(Debug)]
pub(super) struct OwnerGuard(Owners);

impl Owners {
    pub fn enter(&self, kind: &'static str, operation_id: Option<&str>) -> OwnerGuard {
        let context = super::trace::current();
        *self.0.lock().unwrap() = Some(Owner {
            operation_id: operation_id.map(str::to_owned),
            request_id: context.get("requestId").and_then(Value::as_str).map(str::to_owned),
            kind, pid: None, started: Instant::now(),
        });
        OwnerGuard(self.clone())
    }

    pub fn fields(&self) -> Value {
        let owner = self.0.lock().unwrap();
        match owner.as_ref() {
            Some(owner) => json!({"blockerOperationId":owner.operation_id,
                "blockerRequestId":owner.request_id,"blockerKind":owner.kind,"blockerPid":owner.pid,
                "heldMs":owner.started.elapsed().as_millis()}),
            None => json!({"blockerKind":"native-or-file-operation"}),
        }
    }
}

impl OwnerGuard {
    pub fn process_started(&self, pid: Option<u32>) {
        if let Some(owner) = self.0.0.lock().unwrap().as_mut() { owner.pid = pid; }
    }
}

impl Drop for OwnerGuard {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = None;
    }
}
