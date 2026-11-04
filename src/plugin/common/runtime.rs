use super::{NodePlugin, PluginFailure, PluginPlan};
use crate::{
    app::State,
    core::events::{emit, Level},
};
use serde_json::json;
use std::{sync::Arc, time::Instant};
use tokio::sync::Mutex;

enum Phase {
    New,
    Starting,
    Running,
    Stopping,
    Stopped,
}
struct Lifecycle {
    phase: Phase,
    pending: Vec<Arc<dyn NodePlugin>>,
}

/// Retains cleanup ownership across cancelled startup and shutdown futures.
pub struct PluginRuntime {
    lifecycle: Mutex<Lifecycle>,
}
impl Default for PluginRuntime {
    fn default() -> Self {
        Self {
            lifecycle: Mutex::new(Lifecycle {
                phase: Phase::New,
                pending: vec![],
            }),
        }
    }
}
impl PluginRuntime {
    pub async fn start(&self, plan: &PluginPlan, state: &Arc<State>) -> Result<(), String> {
        let mut lifecycle = self.lifecycle.lock().await;
        match lifecycle.phase {
            Phase::Running => return Ok(()),
            Phase::New => lifecycle.phase = Phase::Starting,
            _ => {
                return Err(
                    "Node plugin runtime is stopped or interrupted; shut it down before disposal"
                        .into(),
                )
            }
        }
        for plugin in &plan.ordered {
            lifecycle.pending.push(plugin.clone());
            if let Err(error) = hook(plugin, state, "start").await {
                let cleanup = unwind(&mut lifecycle, state).await;
                return Err(format!(
                    "{error}{}",
                    cleanup
                        .err()
                        .map(|e| format!("; cleanup: {e}"))
                        .unwrap_or_default()
                ));
            }
        }
        lifecycle.phase = Phase::Running;
        Ok(())
    }
    pub async fn stop(&self, state: &Arc<State>) -> Result<(), String> {
        let mut lifecycle = self.lifecycle.lock().await;
        if matches!(lifecycle.phase, Phase::New) {
            lifecycle.pending = state.plugins.ordered.clone();
        }
        unwind(&mut lifecycle, state).await
    }
}
async fn unwind(lifecycle: &mut Lifecycle, state: &Arc<State>) -> Result<(), String> {
    lifecycle.phase = Phase::Stopping;
    let mut errors = vec![];
    while let Some(plugin) = lifecycle.pending.last().cloned() {
        if let Err(error) = hook(&plugin, state, "stop").await {
            errors.push(error);
        }
        lifecycle.pending.pop();
    }
    lifecycle.phase = Phase::Stopped;
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}
async fn hook(plugin: &Arc<dyn NodePlugin>, state: &Arc<State>, phase: &str) -> Result<(), String> {
    let started = Instant::now();
    emit(
        Level::Debug,
        "plugin.lifecycle.started",
        json!({"plugin":plugin.id(),"phase":phase,"operationId":state.epoch.as_str()}),
    );
    let result = invoke(plugin, state, phase).await;
    emit(
        if result.is_ok() {
            Level::Info
        } else {
            Level::Error
        },
        "plugin.lifecycle",
        json!({"plugin":plugin.id(),"phase":phase,"operationId":state.epoch.as_str(),"elapsedMs":started.elapsed().as_millis(),
        "outcome":if result.is_ok(){"completed"}else{"failed"},"code":result.as_ref().err().map(|error|error.code)}),
    );
    result.map_err(|error| format!("Plugin {} {phase} failed: {error}", plugin.id()))
}

async fn invoke(
    plugin: &Arc<dyn NodePlugin>,
    state: &Arc<State>,
    phase: &str,
) -> Result<(), PluginFailure> {
    if phase == "start" {
        plugin.start(state).await
    } else {
        tokio::time::timeout(std::time::Duration::from_secs(10), plugin.stop(state))
            .await
            .unwrap_or_else(|_| {
                Err(PluginFailure::new(
                    "PLUGIN_STOP_TIMEOUT",
                    "Plugin shutdown deadline exceeded",
                ))
            })
    }
}
