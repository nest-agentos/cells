//! Match project defaults and live session grants before presenting a native request.
use crate::projects::execution::HarnessApproval;
use super::events::Events;
use serde_json::{json, Value};
use std::time::Instant;

#[derive(Clone, Copy)]
pub enum Driver { Claude, Codex, Dsh }

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Decision { ProjectDeny, ProjectAllow, SessionAllow, RepeatedRead }

impl Decision {
    pub fn allows(self) -> bool { self != Self::ProjectDeny }
    pub fn source(self) -> &'static str {
        match self {
            Self::ProjectDeny | Self::ProjectAllow => "project-default",
            Self::SessionAllow => "session-grant",
            Self::RepeatedRead => "repeated-read",
        }
    }
}

pub fn native_tool(tool: &str) -> bool {
    !tool.is_empty() && !tool.starts_with("mcp__")
        && !matches!(tool, "AskUserQuestion" | "EnterPlanMode" | "ExitPlanMode")
}

pub const ALLOW_SESSION: &str = "allowAllForSession";

fn current(events: &Events, driver: Driver) -> Option<(u64, HarnessApproval)> {
    let (revision, defaults) = events.runtime.as_ref()?.approval_policy().ok()?;
    let policy = match driver {
        Driver::Claude => defaults.claude,
        Driver::Codex => defaults.codex,
        Driver::Dsh => defaults.dsh,
    };
    Some((revision, policy))
}

pub fn status(events: &Events, driver: Driver, session: &str) -> Value {
    let policy = current(events, driver);
    json!({"projectDefault":policy.map(|(_, default)| default),
        "sessionGrant":policy.is_some_and(|(revision, _)| events.approval_grants.get(session) == Some(&revision)),
        "pendingApprovals":super::commands::pending_approvals(events, session)})
}

pub fn reset_command() -> Value {
    json!({"name":"permissions:reset","kind":"action","description":"Clear Cells grants for this session"})
}

pub fn reset(events: &mut Events, session: &str, args: &Value) -> Result<Value, crate::core::error::Error> {
    let result = super::commands::result(args, json!({"sessionGrant":false}))?;
    events.approval_grants.remove(session);
    Ok(result)
}

pub fn consider(events: &Events, driver: Driver, session: &str, read: bool) -> Option<Decision> {
    let (revision, policy) = current(events, driver)?;
    let session_granted = events.approval_grants.get(session) == Some(&revision);
    decide(policy, read, session_granted)
}

fn decide(policy: HarnessApproval, read: bool, session_granted: bool) -> Option<Decision> {
    if policy == HarnessApproval::Deny { return Some(Decision::ProjectDeny); }
    if session_granted { return Some(Decision::SessionAllow); }
    if policy == HarnessApproval::AllowAll || (read && policy == HarnessApproval::AllowRead) {
        return Some(Decision::ProjectAllow);
    }
    None
}

pub fn grant_revision(events: &Events, request: &Value) -> Result<u64, crate::core::error::Error> {
    use crate::core::error::Error;
    let options = request["params"]["decisions"].as_array().ok_or_else(Error::invalid)?;
    if !options.iter().any(|option| option == ALLOW_SESSION) {
        return Err(Error::invalid());
    }
    if events.approval_grants.len() >= 256 {
        return Err(Error::too_many("HARNESS_APPROVAL_GRANT_LIMIT"));
    }
    let (revision, _) = events.runtime.as_ref().ok_or_else(Error::unsupported)?.approval_policy()?;
    if request["params"]["approvalPolicyRevision"].as_u64() != Some(revision) {
        return Err(Error::conflict("HARNESS_APPROVAL_POLICY_CHANGED"));
    }
    Ok(revision)
}

pub fn offer_for_request(events: &Events, params: &mut Value) {
    let Some((revision, _)) = events.runtime.as_ref().and_then(|runtime| runtime.approval_policy().ok()) else {
        return;
    };
    if events.approval_grants.len() < 256 {
        if let Some(options) = params["decisions"].as_array_mut() {
            options.push(json!(ALLOW_SESSION));
            params["approvalPolicyRevision"] = json!(revision);
        }
    }
}

pub fn record(id: &Value, decision: Decision, started: Instant, result: &Result<Value, crate::core::error::Error>) {
    use crate::core::events::{emit, Level};
    emit(if result.is_ok() { Level::Info } else { Level::Warn }, "harness.approval.automatic",
        json!({"operation":"harness.approval.automatic","operationId":id,"stage":"dispatch",
            "decision":if decision.allows() {"allow"} else {"deny"},"decisionSource":decision.source(),
            "result":if result.is_ok() {"success"} else {"failure"},
            "code":result.as_ref().err().map(|error| error.code),"durationMs":started.elapsed().as_millis()}));
}

pub fn codex_request(events: &Events, bridge: &super::bridge::Bridge, request: &mut Value) -> bool {
    if !matches!(request["method"].as_str(),
        Some("item/commandExecution/requestApproval" | "item/fileChange/requestApproval")) {
        return false;
    }
    let id = request["id"].clone();
    if !id.is_string() && !id.is_number() { return false; }
    let params = &mut request["params"];
    if let Some(params) = params.as_object_mut() { params.remove("approvalPolicyRevision"); }
    let Some(session) = params["threadId"].as_str().filter(|id| bridge.owns_approval_session(id)) else {
        return false;
    };
    let offered = params.get("availableDecisions").or_else(|| params.get("decisions"))
        .cloned().unwrap_or_else(|| json!(["accept","acceptForSession","decline","cancel"]));
    let has = |decision: &str| offered.as_array().is_some_and(|options| options.iter().any(|option| option == decision));
    if let Some(decision) = consider(events, Driver::Codex, session, false) {
        let native = if decision.allows() { "accept" } else { "decline" };
        if has(native) && bridge.reply_automatically(&id, json!({"decision":native}), decision) {
            return true;
        }
    }
    if has("accept") {
        params["decisions"] = offered;
        offer_for_request(events, params);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_defaults_bound_session_and_read_grants() {
        for (policy, read, session_granted, result) in [
            (HarnessApproval::Ask, true, false, None),
            (HarnessApproval::Ask, false, true, Some(Decision::SessionAllow)),
            (HarnessApproval::Deny, true, true, Some(Decision::ProjectDeny)),
            (HarnessApproval::AllowRead, true, false, Some(Decision::ProjectAllow)),
            (HarnessApproval::AllowRead, false, false, None),
            (HarnessApproval::AllowAll, false, false, Some(Decision::ProjectAllow)),
        ] {
            assert_eq!(decide(policy, read, session_granted), result);
        }
        assert!(!native_tool("mcp__browser__navigate"));
        assert!(!native_tool("AskUserQuestion"));
        assert!(!native_tool("ExitPlanMode"));
        assert!(native_tool("Bash"));
    }
}
