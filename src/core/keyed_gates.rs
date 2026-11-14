//! Resource lifecycle gates; the registry is never held while a resource is in use.
use super::gate::FileGate;
use std::{collections::HashMap, sync::Mutex};
#[derive(Default)]
pub(crate) struct KeyedGates(Mutex<HashMap<String, FileGate>>);
impl KeyedGates {
    pub(crate) fn get(&self, key: &str) -> FileGate {
        let mut entries = self.0.lock().unwrap_or_else(|error| error.into_inner());
        // Owned permits and runtime handles retain the inner Arc. Unknown ownership is never evicted.
        if entries.len() >= 1024 {
            entries.retain(|_, gate| !gate.idle());
        }
        entries
            .entry(key.into())
            .or_insert_with(|| FileGate::named(key))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_reclamation_preserves_owned_file_permits_and_unknown_resources() {
        let gates = KeyedGates::default();
        let held = gates.get("active").shared().unwrap();
        gates.get("unknown").poison();
        for index in 0..1100 {
            drop(gates.get(&format!("idle-{index}")));
        }
        assert!(gates.get("active").exclusive().is_err());
        assert_eq!(gates.get("unknown").shared().unwrap_err().code, "EIO");
        assert!(gates.get("independent").exclusive().is_ok());
        drop(held);
        assert!(gates.get("active").exclusive().is_ok());
    }
    #[test]
    fn execution_cleanup_poison_is_local_and_all_native_dependencies_stay_owned() {
        let node = FileGate::new();
        let a = FileGate::named("a");
        let b = FileGate::named("b");
        let process = node
            .command_now("process")
            .unwrap()
            .with_resource(a.command_now("process").unwrap());
        process.process_started(Some(42));
        assert_eq!(a.owners.fields()["blockerPid"], 42);
        process.poison();
        drop(process);
        assert!(node.shared().is_ok());
        assert!(b.shared().is_ok());
        assert!(a.shared().is_err());
        let a = FileGate::named("primary");
        let c = FileGate::named("context");
        let native = node
            .activity()
            .unwrap()
            .with_resource(a.activity().unwrap())
            .with_resource(c.activity().unwrap());
        assert!(a.exclusive().is_err());
        assert!(c.exclusive().is_err());
        assert!(b.exclusive().is_ok());
        drop(native);
        assert!(a.exclusive().is_ok());
        assert!(c.exclusive().is_ok());
    }
}
