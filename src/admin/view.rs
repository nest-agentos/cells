//! Sanitized configured-policy read models; credentials and runtime execution are never exposed.
use super::AccessCommand;
use crate::{
    auth::Auth,
    config::{exports, model::Access, Config},
};
use serde_json::{json, Value};

pub(super) fn read(config: &Config, auth: &Auth, command: AccessCommand) -> Result<Value, String> {
    match command {
        AccessCommand::List => summary(config, auth),
        AccessCommand::Validate => {
            Ok(json!({"valid":true,"scope":"configured-policy","policy":summary(config,auth)?}))
        }
        AccessCommand::Check {
            principal,
            alias,
            operation,
        } => check(config, auth, &principal, &alias, &operation),
        _ => Err("Expected a read-only policy command".into()),
    }
}
pub(super) fn summary(config: &Config, auth: &Auth) -> Result<Value, String> {
    let mut directories = Vec::new();
    for (index, export) in config.exports.iter().enumerate() {
        let alias = exports::alias(export, index)?;
        let permissions: Vec<Value> = auth
            .clients()
            .iter()
            .map(|c| {
                json!({"principal":c.id(),
            "read":c.may_read(&alias),"write":c.may_write(&alias) && export.access==Access::Rw,
            "execute":config.execution && c.may_execute(&alias)})
            })
            .collect();
        directories.push(json!({"alias":alias,"path":export.path,"access":if export.access==Access::Rw {"rw"} else {"ro"},
            "public":export.public,"permissions":permissions}));
    }
    let principals: Vec<Value> = auth.clients().iter().map(|c| {
        let reference = if c.legacy() { config.token_env.as_ref().or(config.password_env.as_ref()) }
            else { config.principals.iter().find(|p| p.id==c.id()).and_then(|p| p.token_env.as_ref().or(p.password_env.as_ref())) };
        json!({"id":c.id(),"authentication":if c.anonymous(){"anonymous"}else if c.username().is_some(){"basic"}else{"bearer"},
            "username":c.username(),"credentialSource":if let Some(name)=reference {json!({"environment":name})}else{json!({"inline":!c.anonymous()})}})
    }).collect();
    Ok(
        json!({"scope":"configured-policy","mode":if config.principals.is_empty(){"legacy"}else{"principals"},
        "serverId":config.server_id,"execution":config.execution,"principals":principals,"exports":directories}),
    )
}
fn check(
    config: &Config,
    auth: &Auth,
    principal: &str,
    alias: &str,
    operation: &str,
) -> Result<Value, String> {
    let directory = config
        .exports
        .iter()
        .enumerate()
        .find_map(|(i, e)| (exports::alias(e, i).ok().as_deref() == Some(alias)).then_some(e))
        .ok_or("Unknown export alias")?;
    if !matches!(operation, "read" | "write" | "execute") {
        return Err("Operation must be read, write or execute".into());
    }
    let client = auth.clients().iter().find(|c| c.id() == principal);
    if client.is_none() && principal != "anonymous" {
        return Err("Unknown principal".into());
    }
    let allowed = client.is_some_and(|c| {
        c.may_read(alias)
            && match operation {
                "read" => true,
                "write" => c.may_write(alias) && directory.access == Access::Rw,
                _ => config.execution && c.may_execute(alias),
            }
    });
    Ok(
        json!({"scope":"configured-policy","runtimeVerified":false,"principal":principal,"export":alias,"operation":operation,"allowed":allowed}),
    )
}
