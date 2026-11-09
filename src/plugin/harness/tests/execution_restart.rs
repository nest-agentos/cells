//! A killed Cells process must retain exact accepted turns without replaying them.
use serde_json::{json, Value};
use std::{os::unix::fs::PermissionsExt, path::Path, process::Stdio, time::Duration};
use tokio::process::{Child, Command};
const TOKEN: &str = "restart-fixture-token-at-least-24-bytes";

async fn call(http: &reqwest::Client, base: &str, method: &str, args: Value) -> Value {
    let response = http
        .post(format!("{base}/v1/api/harness/{method}"))
        .bearer_auth(TOKEN)
        .json(&args)
        .send()
        .await
        .unwrap();
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["ok"], true, "{value}");
    value["value"].clone()
}
async fn start(config: &Path, http: &reqwest::Client, base: &str) -> Child {
    let mut process = Command::new(env!("CARGO_BIN_EXE_cells"))
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    for _ in 0..100 {
        assert!(process.try_wait().unwrap().is_none());
        if let Ok(response) = http
            .post(format!("{base}/v1/api/harness/profiles"))
            .bearer_auth(TOKEN)
            .json(&json!({}))
            .send()
            .await
        {
            if response.status().is_success() {
                return process;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("fixture server did not become ready");
}
#[tokio::test]
async fn sigkill_restores_persisted_execution_identity_without_replaying_the_turn() {
    let root = tempfile::tempdir().unwrap();
    let executable = root.path().join("codex");
    std::fs::write(
        &executable,
        r#"#!/usr/bin/env python3
import json, os, sys
home=os.environ['CODEX_HOME']
thread={'id':'retained','cwd':home,'status':{'type':'notLoaded'},'turns':[]}
for line in sys.stdin:
    request=json.loads(line)
    if request.get('id') is None: continue
    method=request.get('method'); result={}
    if method in ('thread/start','thread/resume'):
        thread['status']={'type':'idle'}; result={'thread':thread}
    elif method=='thread/read': result={'thread':thread}
    elif method=='thread/turns/list': result={'data':[],'nextCursor':None}
    elif method=='turn/start':
        with open(home+'/turn-count','a') as output: output.write('turn\n')
        thread['status']={'type':'active'}; result={'turn':{'id':'lost-turn'}}
    print(json.dumps({'id':request['id'],'result':result}),flush=True)
"#,
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let config_path = root.path().join("config.toml");
    std::fs::write(&config_path,format!("listen={:?}\nexecution=false\ntoken={TOKEN:?}\n[[harnesses]]\nid='codex'\nkind='codex'\ncommand={:?}\nhome={:?}\n[[harnesses.workspaces]]\nid='project'\npath={:?}\n",address.to_string(),executable,root.path(),root.path())).unwrap();
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let base = format!("http://{address}");
    let mut process = start(&config_path, &http, &base).await;
    let epoch = call(&http, &base, "profiles", json!({})).await["epoch"].clone();
    call(
        &http,
        &base,
        "create",
        json!({"profileId":"codex","epoch":epoch,"requestId":"create","workspaceId":"project"}),
    )
    .await;
    let admitted=call(&http,&base,"turn",json!({"profileId":"codex","epoch":epoch,"requestId":"accepted-input","sessionId":"retained","prompt":"fixture only"})).await;
    assert_eq!(admitted["result"]["turnId"], "lost-turn");
    process.kill().await.unwrap();
    process.wait().await.unwrap();
    let mut process = start(&config_path, &http, &base).await;
    let info = call(
        &http,
        &base,
        "session_info",
        json!({"profileId":"codex","sessionId":"retained"}),
    )
    .await;
    let proof = &info["session"]["executionRecovery"]["interruptedTurns"][0];
    assert_eq!(proof["turnId"], "lost-turn");
    assert_eq!(proof["requestId"], "accepted-input");
    assert_eq!(proof["previousEpoch"], epoch);
    assert_eq!(info["session"]["executionRecovery"]["state"], "detached");
    assert!(info["session"]["lastTurnResult"].is_null());
    assert_eq!(
        std::fs::read_to_string(root.path().join("turn-count")).unwrap(),
        "turn\n"
    );
    process.kill().await.unwrap();
    process.wait().await.unwrap();
}
