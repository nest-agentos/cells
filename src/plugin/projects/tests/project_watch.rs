use cells::{app::State, config::Config, projects::call};
use serde_json::{json, Value};
use std::{os::unix::fs::symlink, sync::Arc};
fn fixture() -> (tempfile::TempDir, Arc<State>) {
    let root = tempfile::tempdir().unwrap();
    for path in [
        "export/project/ref",
        "export/project/private-native",
        "reference",
        "catalog",
    ] {
        std::fs::create_dir_all(root.path().join(path)).unwrap();
    }
    let config:Config=toml::from_str(&format!("listen=\"127.0.0.1:0\"\nexecution=false\nserver_id=\"watch-test\"\ntoken=\"watch-fixture-token-0123456789\"\n[projects]\nroot={catalog:?}\n[[exports]]\nalias=\"root\"\npath={export:?}\naccess=\"rw\"\n[[exports]]\nalias=\"reference\"\npath={reference:?}\n[[harnesses]]\nid=\"codex\"\nkind=\"codex\"\nhome={native:?}\nprojects=true\n",catalog=root.path().join("catalog"),export=root.path().join("export"),reference=root.path().join("reference"),native=root.path().join("export/project/private-native"))).unwrap();
    let state = State::from_config(&config).unwrap();
    (root, state)
}
async fn project(state: &Arc<State>) -> Value {
    call(state,0,"project_register",json!({"name":"P","alias":"root","path":"project","access":"ro","mounts":[{"alias":"reference","path":"","at":"/workspace/ref","access":"ro"}]})).await.unwrap()["project"].clone()
}
#[tokio::test]
async fn scoped_subscription_observes_only_the_requested_directory() {
    use futures_util::StreamExt;
    let (root, state) = fixture();
    let base = root.path().join("export/project");
    std::fs::create_dir_all(base.join("_nestos/kanban/inbox")).unwrap();
    let project = project(&state).await;
    let args = json!({"projectId":project["id"],"revision":project["revision"],"directory":"_nestos/kanban/inbox"});
    let mut source = state.plugins.subscribe(&state, 0, "projects", args.clone()).await.unwrap();
    let first = next(&mut source).await;
    std::fs::write(base.join("outside.md"), "unrelated").unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(40), source.next()).await.is_err());
    std::fs::write(base.join("_nestos/kanban/inbox/result.json"), "{}").unwrap();
    assert_ne!(next(&mut source).await["version"], first["version"]);
    for directory in ["../reference", "private-native", "ref", ".nestos/private"] {
        let mut invalid = args.clone(); invalid["directory"] = json!(directory);
        assert!(state.plugins.subscribe(&state, 0, "projects", invalid).await.is_err());
    }
}
#[tokio::test]
async fn subscription_waits_for_kernel_changes_and_releases_its_watch() {
    use futures_util::StreamExt;
    let (root, state) = fixture();
    let project = project(&state).await;
    let args = json!({"projectId":project["id"],"revision":project["revision"]});
    let mut source = state
        .plugins
        .subscribe(&state, 0, "projects", args.clone())
        .await
        .unwrap();
    let first = source.next().await.unwrap().unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(40), source.next())
            .await
            .is_err()
    );
    std::fs::write(
        root.path().join("export/project/from-cli.md"),
        "external change",
    )
    .unwrap();
    let next = tokio::time::timeout(std::time::Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(next["version"], first["version"]);
    assert!(state
        .plugins
        .subscribe(&state, 1, "projects", args.clone())
        .await
        .is_err());
    let mut stale = args.clone();
    stale["revision"] = json!(999);
    assert!(state
        .plugins
        .subscribe(&state, 0, "projects", stale)
        .await
        .is_err());
    drop(source);
    for _ in 0..20 {
        drop(
            state
                .plugins
                .subscribe(&state, 0, "projects", args.clone())
                .await
                .unwrap(),
        );
    }
}
async fn next(source: &mut cells::plugins::EventStream) -> Value {
    use futures_util::StreamExt;
    tokio::time::timeout(std::time::Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn detects_external_writes_without_exposing_private_or_shadowed_trees() {
    use futures_util::StreamExt;
    let (root, state) = fixture();
    let project = project(&state).await;
    let args = json!({"projectId":project["id"],"revision":project["revision"]});
    let mut source = state
        .plugins
        .subscribe(&state, 0, "projects", args)
        .await
        .unwrap();
    let first = next(&mut source).await;
    assert_eq!(first["truncated"], false);
    let base = root.path().join("export/project");
    std::fs::write(base.join("external.md"), "external").unwrap();
    let changed = next(&mut source).await;
    assert_ne!(changed["version"], first["version"]);
    std::fs::write(base.join("ref/shadowed.md"), "shadow").unwrap();
    std::fs::write(base.join("private-native/auth.json"), "private").unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(40), source.next())
            .await
            .is_err()
    );
    std::fs::write(root.path().join("reference/visible.md"), "mounted").unwrap();
    let mounted = next(&mut source).await;
    assert_ne!(mounted["version"], changed["version"]);
    std::fs::create_dir(base.join("new")).unwrap();
    next(&mut source).await;
    std::fs::write(base.join("new/nested.md"), "nested").unwrap();
    next(&mut source).await;
    symlink(root.path().join("reference"), base.join("link")).unwrap();
    next(&mut source).await;
    drop(source);
    for _ in 0..20 {
        drop(
            state
                .plugins
                .subscribe(
                    &state,
                    0,
                    "projects",
                    json!({"projectId":project["id"],"revision":project["revision"]}),
                )
                .await
                .unwrap(),
        );
    }
}
#[tokio::test]
async fn fences_subscriptions_by_owner_revision_and_directory_identity() {
    use futures_util::StreamExt;
    let (root, state) = fixture();
    let project = project(&state).await;
    let args = json!({"projectId":project["id"],"revision":project["revision"]});
    assert!(state
        .plugins
        .subscribe(&state, 1, "projects", args.clone())
        .await
        .is_err());
    let mut stale = args.clone();
    stale["revision"] = json!(999);
    assert!(state
        .plugins
        .subscribe(&state, 0, "projects", stale)
        .await
        .is_err());
    let mut source = state
        .plugins
        .subscribe(&state, 0, "projects", args)
        .await
        .unwrap();
    next(&mut source).await;
    std::fs::rename(
        root.path().join("export/project"),
        root.path().join("export/old"),
    )
    .unwrap();
    std::fs::create_dir(root.path().join("export/project")).unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), source.next())
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
}

fn isolated_limit_test(name: &str, hard: Option<libc::rlim_t>) -> bool {
    use std::{os::unix::process::CommandExt, process::Command};
    if std::env::var_os("CELLS_WATCH_LIMIT_TEST").is_some() { return false; }
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", name]).env("CELLS_WATCH_LIMIT_TEST", "1");
    unsafe { command.pre_exec(move || {
        let mut limits = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) != 0 {return Err(std::io::Error::last_os_error());}
        limits.rlim_cur = 64;
        if let Some(hard) = hard { limits.rlim_max = hard.min(limits.rlim_max); }
        if libc::setrlimit(libc::RLIMIT_NOFILE, &limits) != 0 {return Err(std::io::Error::last_os_error());}
        Ok(())
    }); }
    let output = command.output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    true
}

#[tokio::test]
async fn low_inherited_soft_limit_keeps_large_watches_rebuilds_and_file_reads_available() {
    if isolated_limit_test("low_inherited_soft_limit_keeps_large_watches_rebuilds_and_file_reads_available", None) {return;}
    let (root, state) = fixture();
    let base = root.path().join("export/project");
    for index in 0..1200 {std::fs::create_dir(base.join(format!("directory-{index}"))).unwrap();}
    let project = project(&state).await;
    let args = json!({"projectId":project["id"],"revision":project["revision"]});
    let baseline = std::fs::read_dir("/proc/self/fd").unwrap().count();
    let mut first = state.plugins.subscribe(&state, 0, "projects", args.clone()).await.unwrap();
    assert_eq!(next(&mut first).await["truncated"], false);
    let mut second = state.plugins.subscribe(&state, 0, "projects", args).await.unwrap();
    assert_eq!(next(&mut second).await["truncated"], false);
    std::fs::create_dir(base.join("added-after-subscription")).unwrap();
    assert_eq!(next(&mut first).await["truncated"], false);
    assert_eq!(next(&mut second).await["truncated"], false);
    assert!(state.exports.get("root").unwrap().stat("project/directory-1199").unwrap().is_some());
    std::fs::write(base.join("added-after-subscription/change.md"), "update").unwrap();
    next(&mut first).await;next(&mut second).await;
    drop(first);drop(second);
    assert!(std::fs::read_dir("/proc/self/fd").unwrap().count() <= baseline);
}

#[test]
fn watch_capacity_never_raises_the_administrator_hard_limit() {
    if isolated_limit_test("watch_capacity_never_raises_the_administrator_hard_limit", Some(128)) {return;}
    let (_root, _state) = fixture();
    let mut limits = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    assert_eq!(unsafe {libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits)}, 0);
    assert_eq!(limits.rlim_cur, 128);assert_eq!(limits.rlim_max, 128);
}
