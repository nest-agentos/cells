use crate::{app::State, core::error::Error, http::access, plugin::common::object};
use axum::{extract::State as AxumState, http::HeaderMap, Json};
use serde_json::{json, Value};
use std::sync::Arc;
pub async fn capabilities(
    AxumState(state): AxumState<Arc<State>>,
    headers: HeaderMap,
) -> Result<Json<Value>, Error> {
    let identity = access::identity(&state, &headers)?;
    let base = object(
        json!({"processEpoch":state.epoch,"version":1,"serverId":state.auth.server_id(),
        "authorization":state.auth.client(identity).authorization(),"plugins":state.plugins.statuses(),"terminal":{"pty":false},"events":crate::http::subscriptions::capability()}),
    );
    let mut http = state.plugins.capabilities(&state, identity, base.clone(), true)?;
    let mut node = state.plugins.capabilities(&state, identity, base, false)?;
    node["httpEndpoint"] = json!("./");
    node["api"] = json!({"version":1,"endpoint":"v1/api/"});
    http["node"] = node;
    Ok(Json(http))
}
