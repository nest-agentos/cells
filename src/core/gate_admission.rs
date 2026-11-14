//! Native admission shares lifecycle ownership; it never waits for unrelated execution.
use super::{
    error::Error,
    gate::{ActivityPermit, FileGate},
};
impl FileGate {
    pub async fn admit_activity(
        &self,
        operation: &str,
        _budget_ms: u64,
    ) -> Result<ActivityPermit, Error> {
        let result = self.activity_for(None, Some(operation));
        super::events::emit(
            super::events::Level::Info,
            "file.gate.native_admission",
            serde_json::json!({
            "operation":"file.lifecycle.admit","operationId":operation,"stage":"native-admission","gateMode":"shared",
            "result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|error|error.code),
            "executionStarted":false,"durationMs":0}),
        );
        result
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn admits_independent_turns_while_commands_and_file_operations_are_active() {
        let gate = FileGate::new();
        let _command = gate.command_now("prepare-other").unwrap();
        let _file = gate.shared().unwrap();
        let a = gate.admit_activity("native-a", 1).await.unwrap();
        let b = gate.admit_activity("native-b", 1).await.unwrap();
        assert!(gate.shared().is_ok());
        drop((a, b));
        assert!(gate.command_now("another").is_ok());
    }
    #[tokio::test]
    async fn topology_rejection_is_uncommitted_and_poisoning_remains_closed() {
        let gate = FileGate::new();
        let topology = gate.exclusive().unwrap();
        assert_eq!(
            gate.admit_activity("blocked", 1).await.unwrap_err().outcome,
            Some("not-committed")
        );
        drop(topology);
        gate.poison();
        assert_eq!(
            gate.admit_activity("unknown", 1).await.unwrap_err().code,
            "EIO"
        );
    }
}
