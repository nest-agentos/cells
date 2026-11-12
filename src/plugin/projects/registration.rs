//! Safe registration stage diagnostics; no paths, input bodies or credentials.
use super::model::Project;
use crate::core::error::Error;

pub(super) struct RegistrationAudit { started: std::time::Instant, stage: &'static str, project: Option<String>, completed: bool }
impl RegistrationAudit {
    pub(super) fn new() -> Self {
        let mut audit = Self {started:std::time::Instant::now(),stage:"catalog-admission",project:None,completed:false};
        audit.stage("catalog-admission",None); audit
    }
    pub(super) fn stage(&mut self, stage: &'static str, project: Option<&str>) {
        self.stage=stage;self.project=project.map(str::to_owned);
        crate::core::events::emit(crate::core::events::Level::Info,"project.registration.stage",serde_json::json!({
            "operation":"projects.register","stage":stage,"projectId":self.project,"result":"started",
            "durationMs":self.started.elapsed().as_millis()}));
    }
    pub(super) fn acquired(&self, wait_ms: u128) {
        crate::core::events::emit(crate::core::events::Level::Info,"project.registration.admitted",serde_json::json!({
            "operation":"projects.register","stage":"catalog-admission","result":"success","lockScope":"project-catalog",
            "waitMs":wait_ms,"durationMs":self.started.elapsed().as_millis()}));
    }
    pub(super) fn complete(&mut self, result: &Result<Project, Error>) {
        self.completed=true;
        crate::core::events::emit(if result.is_ok(){crate::core::events::Level::Info}else{crate::core::events::Level::Warn},
            "project.registration.completed",serde_json::json!({"operation":"projects.register","stage":self.stage,
                "result":if result.is_ok(){"success"}else{"failure"},"code":result.as_ref().err().map(|error|error.code),
                "projectId":self.project,"lockScope":"project-catalog","durationMs":self.started.elapsed().as_millis()}));
    }
}
impl Drop for RegistrationAudit {
    fn drop(&mut self) { if !self.completed {
        crate::core::events::emit(crate::core::events::Level::Warn,"project.registration.aborted",serde_json::json!({
            "operation":"projects.register","stage":self.stage,"result":"failure","code":"REGISTRATION_ABORTED",
            "projectId":self.project,"durationMs":self.started.elapsed().as_millis()}));
    }}
}
