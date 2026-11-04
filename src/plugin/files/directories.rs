//! Authorized logical directory descriptions, without host paths or file contents.
use crate::{app::State, auth::Client, process::Mount};
use serde_json::{json, Value};

pub(crate) fn exports(state: &State, client: &Client) -> Vec<Value> {
    client
        .exports()
        .iter()
        .map(|alias| export(state, client, alias))
        .collect()
}

fn export(state: &State, client: &Client, alias: &str) -> Value {
    let writable = state
        .exports
        .get(alias)
        .is_some_and(|export| export.writable());
    let access = if writable && client.may_write(alias) {
        "rw"
    } else {
        "ro"
    };
    let mut value = directory(
        state,
        client,
        &Mount {
            alias: alias.into(),
            path: String::new(),
            at: String::new(),
            access: access.into(),
        },
    );
    value.as_object_mut().unwrap().remove("at");
    value["nameSemantics"] = json!("source");
    value["strongRevision"] = json!(writable);
    value["metadata"] = json!(true);
    value["authorization"] = client.authorization();
    value
}

pub(crate) fn directory(state: &State, client: &Client, mount: &Mount) -> Value {
    let writable = mount.access == "rw"
        && client.may_write(&mount.alias)
        && state
            .exports
            .get(&mount.alias)
            .is_some_and(|export| export.writable());
    let virtual_path = format!("/{}/{}", mount.alias, mount.path)
        .trim_end_matches('/')
        .to_owned();
    json!({"alias":mount.alias,"path":mount.path,"virtualPath":virtual_path,"at":mount.at,
        "access":if writable {"rw"} else {"ro"},
        "capabilities":{"fileRead":true,"fileWrite":writable,
            "run":state.services.get::<crate::process::Execution>().is_some_and(|service| service.ready()) && client.may_execute(&mount.alias)}})
}
