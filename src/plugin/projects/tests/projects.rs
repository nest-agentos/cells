use cells::{
    app::State,
    config::Config,
    projects::{call, runtime::ProjectRuntime},
};
use serde_json::{json, Value};
use std::{path::Path, sync::Arc};

fn deployment(root: &Path) -> Config {
    for name in ["exports", "reference", "catalog", "codex"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    std::fs::create_dir_all(root.join("exports/group/existing/ref")).unwrap();
    toml::from_str(&format!(
        r#"
listen="127.0.0.1:0"
execution=true
server_id="test-node"
token="project-test-secret-0123456789"
[projects]
root={catalog:?}
[[exports]]
alias="home"
path={exports:?}
access="rw"
[[exports]]
alias="reference"
path={reference:?}
[[harnesses]]
id="codex"
kind="codex"
home={home:?}
projects=true
"#,
        catalog = root.join("catalog"),
        exports = root.join("exports"),
        reference = root.join("reference"),
        home = root.join("codex")
    ))
    .unwrap()
}
async fn register(state: &Arc<State>, path: &str, create: bool) -> Value {
    call(
        state,
        0,
        "project_register",
        json!({"name":"Notes","alias":"home","path":path,"access":"rw","createDirectory":create}),
    )
    .await
    .unwrap()["project"]
        .clone()
}

#[tokio::test]
async fn channel_relative_paths_follow_runtime_configuration_and_keep_legacy_clients_working() {
    for isolation in [cells::projects::model::Isolation::Bubblewrap, cells::projects::model::Isolation::None] {
        let root = tempfile::tempdir().unwrap();
        let relative = "_nestos/kanban/conversations/check";
        let physical = root.path().join("exports/group/existing").join(relative);
        std::fs::create_dir_all(&physical).unwrap();
        std::os::unix::fs::symlink(root.path().join("reference"), root.path().join("exports/group/existing/escape")).unwrap();
        let mut config = deployment(root.path());
        config.projects.as_mut().unwrap().isolation = isolation;
        config.projects.as_mut().unwrap().network = true;
        config.projects.as_mut().unwrap().channel_endpoint = Some("http://127.0.0.1:8787/v1/channel/mcp".into());
        config.plugins.insert("mcp".into(), true);
        let state = State::from_config(&config).unwrap();
        let project = register(&state,"group/existing",false).await;
        let stored = state.services.require::<cells::projects::ProjectService>().unwrap().get(0,project["id"].as_str().unwrap()).unwrap();
        let runtime = ProjectRuntime::new(&state,0,stored).unwrap();
        let expected = format!("{}/{relative}",runtime.cwd());
        assert_eq!(runtime.channel_directory(relative).unwrap(),expected);
        let mut args = json!({"projectId":project["id"],"revision":project["revision"],"scopeId":"channel-test",
            "cwd":relative,"endpoint":"http://localhost/v1/channel/mcp","snapshot":{"context":{}}});
        assert_eq!(call(&state,0,"project_channel_open",args.clone()).await.unwrap()["accepted"],true);
        for cwd in [relative.to_string(),format!("/projects/{}/{relative}",project["id"].as_str().unwrap()),expected] {
            args["cwd"] = json!(cwd);
            assert_eq!(call(&state,0,"project_channel_current",args.clone()).await.unwrap()["scopeId"],"channel-test");
            assert_eq!(call(&state,0,"project_channel_open",args.clone()).await.unwrap()["accepted"],true);
        }
        let channel = state.services.require::<cells::projects::ProjectService>().unwrap().channels.config(
            project["id"].as_str().unwrap(),&runtime.working_directory(relative).unwrap(),&runtime.project.owner).unwrap().unwrap();
        assert_eq!(channel["mcp_servers.cells_session"]["url"],"http://127.0.0.1:8787/v1/channel/mcp");
        for cwd in ["../reference","/etc/passwd","/projects/another/work","escape/child",""] {
            args["cwd"] = json!(cwd);
            assert!(call(&state,0,"project_channel_open",args.clone()).await.is_err());
        }
    }
}

#[tokio::test]
async fn harness_home_executable_can_read_sibling_resources_without_extra_mounts() {
    if std::env::var("CELLS_PROJECT_TEST").as_deref() != Ok("1") {return;}
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let config = deployment(root.path());
    let state = State::from_config(&config).unwrap();
    let project = register(&state, "group/existing", false).await;
    state.services.require::<cells::process::Execution>().unwrap().accept();
    let stored = state.services.require::<cells::projects::ProjectService>().unwrap()
        .get(0, project["id"].as_str().unwrap()).unwrap();
    let runtime = ProjectRuntime::new(&state, 0, stored).unwrap();
    let mut profile = cells::harness::config::validate(&config).unwrap().remove(0);
    let binary = profile.home.join("native");
    std::fs::write(&binary, "#!/bin/sh\nexec /bin/cat \"$(dirname \"$0\")/resource\"\n").unwrap();
    std::fs::write(profile.home.join("resource"), "ready").unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    profile.command = binary.to_string_lossy().into_owned();
    let mut prepared = runtime.prepare(&profile, vec![]).unwrap();
    let result = prepared.command.output().await.unwrap();
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    assert_eq!(result.stdout, b"ready");
}

#[tokio::test]
async fn source_aliases_refuse_runtime_paths_without_changing_default_namespaces() {
    let root = tempfile::tempdir().unwrap();
    let config = deployment(root.path());
    let state = State::from_config(&config).unwrap();
    let project = register(&state, "group/existing", false).await;
    let stored = state
        .services
        .get::<cells::projects::ProjectService>()
        .unwrap()
        .get(0, project["id"].as_str().unwrap())
        .unwrap();
    let runtime = ProjectRuntime::new(&state, 0, stored).unwrap();
    state
        .services
        .require::<cells::process::Execution>()
        .unwrap()
        .accept();
    let mut profile = cells::harness::config::validate(&config)
        .unwrap()
        .remove(0);
    profile.command = "/bin/true".into();
    match runtime.prepare_with_source_paths(&profile, vec![]) {
        Ok(_) => panic!("Source aliases must not shadow /tmp"),
        Err(error) => assert_eq!(error.code, "HARNESS_SOURCE_PATH_UNAVAILABLE"),
    }
    assert!(runtime.prepare(&profile, vec![]).is_ok());
}
async fn remount(
    state: &Arc<State>,
    project: &Value,
    access: &str,
) -> Result<Value, cells::core::error::Error> {
    call(
        state,
        0,
        "project_remount",
        json!({"projectId":project["id"], "revision":project["revision"], "access":access}),
    )
    .await
}

#[tokio::test]
async fn remount_preserves_identity_and_files_and_fences_stale_revisions() {
    let root = tempfile::tempdir().unwrap();
    let state = State::from_config(&deployment(root.path())).unwrap();
    let original = register(&state, "group/existing", false).await;
    let file = root.path().join("exports/group/existing/note.txt");
    std::fs::write(&file, "preserved").unwrap();
    let readonly = remount(&state, &original, "ro").await.unwrap()["project"].clone();
    assert_eq!(readonly["id"], original["id"]);
    assert_eq!(readonly["access"], "ro");
    assert_eq!(readonly["revision"], 2);
    assert_eq!(
        remount(&state, &original, "rw").await.unwrap_err().code,
        "PROJECT_REVISION_CHANGED"
    );
    let writable = remount(&state, &readonly, "rw").await.unwrap()["project"].clone();
    assert_eq!(writable["id"], original["id"]);
    assert_eq!(writable["access"], "rw");
    assert_eq!(writable["revision"], 3);
    assert_eq!(std::fs::read_to_string(file).unwrap(), "preserved");
    let _guard = state.files.exclusive().unwrap();
    assert_eq!(
        remount(&state, &writable, "ro").await.unwrap_err().code,
        "EBUSY"
    );
}

#[tokio::test]
async fn remount_cannot_escalate_a_readonly_export() {
    let root = tempfile::tempdir().unwrap();
    let state = State::from_config(&deployment(root.path())).unwrap();
    let project = call(
        &state,
        0,
        "project_register",
        json!({"name":"Reference", "alias":"reference", "path":"", "access":"ro"}),
    )
    .await
    .unwrap()["project"]
        .clone();
    assert!(remount(&state, &project, "rw").await.is_err());
    let stored = call(
        &state,
        0,
        "project_read",
        json!({"projectId":project["id"]}),
    )
    .await
    .unwrap();
    assert_eq!(stored["project"]["access"], "ro");
    assert_eq!(stored["project"]["revision"], 1);
}
#[tokio::test]
async fn registers_subdirectories_creates_one_child_deduplicates_and_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let config = deployment(root.path());
    let state = State::from_config(&config).unwrap();
    let first = register(&state, "group//existing/", false).await;
    assert_eq!(first["path"], "group/existing");
    assert_eq!(first, register(&state, "group/existing", false).await);
    let created = register(&state, "group/new", true).await;
    assert!(root.path().join("exports/group/new").is_dir());
    assert_ne!(first["id"], created["id"]);
    for path in ["../escape", "/etc", "group/no-parent/child"] {
        assert!(call(
            &state,
            0,
            "project_register",
            json!({"name":"X","alias":"home","path":path,"access":"rw","createDirectory":true})
        )
        .await
        .is_err());
    }
    drop(state);
    let state = State::from_config(&config).unwrap();
    assert_eq!(first, register(&state, "group/existing", false).await);
    assert_eq!(
        call(&state, 0, "project_list", json!({})).await.unwrap()["projects"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}
#[tokio::test]
async fn mounts_are_server_owned_attenuated_and_revision_fenced() {
    let root = tempfile::tempdir().unwrap();
    let state = State::from_config(&deployment(root.path())).unwrap();
    let project = register(&state, "group/existing", false).await;
    let id = project["id"].as_str().unwrap();
    let configured=call(&state,0,"project_configure",json!({"projectId":id,"revision":1,"name":"Notes","mounts":[{"alias":"reference","path":"","at":"/workspace/ref","access":"ro"}]})).await.unwrap()["project"].clone();
    assert_eq!(configured["revision"], 2);
    assert!(call(
        &state,
        0,
        "project_configure",
        json!({"projectId":id,"revision":1,"name":"Notes","mounts":[]})
    )
    .await
    .is_err());
    for (alias, at, access) in [
        ("unknown", "/workspace/ref", "ro"),
        ("reference", "/etc", "ro"),
        ("reference", "/workspace/ref", "rw"),
        ("home", "/workspace/ref", "ro"),
    ] {
        assert!(call(&state,0,"project_configure",json!({"projectId":id,"revision":2,"name":"X","mounts":[{"alias":alias,"path":"","at":at,"access":access}]})).await.is_err());
    }
    let guard = state.files.exclusive().unwrap();
    assert_eq!(
        call(
            &state,
            0,
            "project_configure",
            json!({"projectId":id,"revision":2,"name":"X","mounts":[]})
        )
        .await
        .unwrap_err()
        .code,
        "EBUSY"
    );
    drop(guard);
    assert_eq!(call(&state,0,"project_exec",json!({"projectId":id,"revision":1,"epoch":state.services.require::<cells::process::Execution>().unwrap().epoch(),"requestId":"cmd","command":"/bin/true","args":[]})).await.unwrap_err().code,"PROJECT_REVISION_CHANGED");
}
#[tokio::test]
async fn replacement_does_not_silently_retarget_projects_and_launcher_contains_only_grants() {
    let root = tempfile::tempdir().unwrap();
    let config = deployment(root.path());
    let state = State::from_config(&config).unwrap();
    let project = register(&state, "group/existing", false).await;
    state
        .services
        .require::<cells::process::Execution>()
        .unwrap()
        .accept();
    let stored = state
        .services
        .get::<cells::projects::ProjectService>()
        .unwrap()
        .get(0, project["id"].as_str().unwrap())
        .unwrap();
    let runtime = ProjectRuntime::new(&state, 0, stored).unwrap();
    let fixture = root.path().join("codex-native");
    std::fs::create_dir_all(root.path().join("exports/group/existing/_nestos/worktrees/test")).unwrap();
    assert_eq!(runtime.working_directory("_nestos/worktrees/test").unwrap(), format!("{}/_nestos/worktrees/test", runtime.cwd()));
    for directory in ["../reference", "/tmp", "missing", ".nestos/private"] {
        assert!(runtime.working_directory(directory).is_err());
    }
    std::fs::write(&fixture, "executable").unwrap();
    let mut profile = cells::harness::config::validate(&config)
        .unwrap()
        .remove(0);
    profile.command = fixture.to_str().unwrap().into();
    assert_eq!(
        runtime
            .resolve_cwd(root.path().join("exports/group/existing").to_str().unwrap())
            .unwrap(),
        runtime.cwd()
    );
    assert!(runtime
        .resolve_cwd(root.path().join("reference").to_str().unwrap())
        .is_err());
    let prepared = runtime
        .prepare(&profile, vec!["app-server".into(), "--stdio".into()])
        .unwrap();
    let args = prepared
        .command
        .as_std()
        .get_args()
        .map(|s| s.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(args.contains(&runtime.cwd()));
    assert!(args.contains(&"/harness".into()));
    assert!(args.contains(&fixture.to_string_lossy().into_owned()));
    assert!(!args.contains(&root.path().to_string_lossy().into_owned()));
    assert!(!args.contains(&"--share-net".into()));
    runtime.begin("thread", "operation").await.unwrap();
    runtime.bind_turn("thread", "operation", "turn");
    assert!(state.files.shared().is_ok());
    assert!(state.files.exclusive().is_err());
    runtime.event(
        &json!({"method":"turn/completed","params":{"threadId":"other","turn":{"id":"turn"}}}),
    );
    assert!(state.files.shared().is_ok());
    assert!(state.files.exclusive().is_err());
    runtime.event(
        &json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn"}}}),
    );
    assert!(state.files.shared().is_ok());
    drop(prepared);
    std::fs::rename(
        root.path().join("exports/group/existing"),
        root.path().join("exports/group/old"),
    )
    .unwrap();
    std::fs::create_dir(root.path().join("exports/group/existing")).unwrap();
    assert_eq!(register_error(&state).await, "PROJECT_DIRECTORY_CHANGED");
    let forgotten = call(
        &state,
        0,
        "project_forget",
        json!({"projectId":project["id"],"revision":1}),
    )
    .await
    .unwrap();
    assert_eq!(forgotten["forgotten"], project["id"]);
    let replacement = register(&state, "group/existing", false).await;
    assert_ne!(replacement["id"], project["id"]);
    assert!(root.path().join("exports/group/old").exists());
}
async fn register_error(state: &Arc<State>) -> &'static str {
    call(
        state,
        0,
        "project_register",
        json!({"name":"Notes","alias":"home","path":"group/existing","access":"rw"}),
    )
    .await
    .unwrap_err()
    .code
}

#[tokio::test]
async fn native_project_bash_and_codex_share_directory_grants() {
    if std::env::var("CELLS_PROJECT_TEST").as_deref() != Ok("1") {
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let mut config = deployment(root.path());
    config.execution = true;
    let fixture = root.path().join("codex-fixture.py");
    std::fs::write(
        &fixture,
        include_str!("../../harness/tests/fixtures/codex-app-server.py"),
    )
    .unwrap();
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o700)).unwrap();
    config.harnesses[0].command = if std::env::var("CELLS_PROJECT_CODEX").as_deref() == Ok("1") {
        "codex".into()
    } else {
        fixture.to_str().unwrap().into()
    };
    let state = State::from_config(&config).unwrap();
    cells::process::enable(&state).await.unwrap();
    let project = register(&state, "group/existing", false).await;
    let id = project["id"].as_str().unwrap();
    let configured=call(&state,0,"project_configure",json!({"projectId":id,"revision":1,"name":"Notes","mounts":[{"alias":"reference","path":"","at":"/workspace/ref","access":"ro"}]})).await.unwrap()["project"].clone();
    std::fs::write(root.path().join("reference/guide"), "reference").unwrap();
    let reader = state.files.read_only().unwrap();
    let mut admission=Box::pin(call(&state,0,"project_exec",json!({"projectId":id,"revision":2,"epoch":state.services.require::<cells::process::Execution>().unwrap().epoch(),"requestId":"bash","command":"/bin/bash","args":["-c","set -eu; test $(pwd) = /workspace; cat ref/guide; test ! -e /etc/passwd; if echo x > ref/guide 2>/dev/null; then exit 7; fi; echo project > note"],"timeoutMs":5000})));
    tokio::select! {biased; _ = &mut admission => panic!("reader still held"), _ = tokio::task::yield_now() => {}}
    drop(reader);
    let started = admission.await.unwrap();
    assert!(started["process"].is_object());
    let result = loop {
        let result = cells::process::status(
            &state,
            0,
            state
                .services
                .require::<cells::process::Execution>()
                .unwrap()
                .epoch(),
            "bash",
        )
        .unwrap();
        if result.state != "running" {
            break result;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert_eq!(result.code, Some(0));
    assert_eq!(
        std::fs::read_to_string(root.path().join("exports/group/existing/note")).unwrap(),
        "project\n"
    );
    let epoch = state
        .services
        .require::<cells::harness::Harnesses>()
        .unwrap()
        .profiles()["epoch"]
        .clone();
    let base =
        json!({"profileId":"codex","projectId":id,"revision":configured["revision"],"epoch":epoch});
    let mut args = base.clone();
    args["workspaceId"] = json!(id);
    args["requestId"] = json!("create");
    let created = state
        .services
        .require::<cells::harness::Harnesses>()
        .unwrap()
        .project_call(&state, 0, "harness_create", args)
        .await
        .unwrap();
    assert_eq!(created["outcome"], "committed", "{created}");
    assert_eq!(
        created["result"]["session"]["cwd"],
        format!("/projects/{id}")
    );
    if std::env::var("CELLS_PROJECT_CODEX").as_deref() != Ok("1") {
        let session = created["result"]["session"]["id"].clone();
        let mut args = base.clone();
        args["sessionId"] = session.clone();
        args["requestId"] = json!("turn");
        args["prompt"] = json!("test");
        let turn = state
            .services
            .require::<cells::harness::Harnesses>()
            .unwrap()
            .project_call(&state, 0, "harness_turn", args)
            .await
            .unwrap();
        assert_eq!(turn["outcome"], "committed");
        assert!(state.files.shared().is_ok());
    assert!(state.files.exclusive().is_err());
        assert_eq!(
            call(
                &state,
                0,
                "project_configure",
                json!({"projectId":id,"revision":2,"name":"Notes","mounts":[]})
            )
            .await
            .unwrap_err()
            .code,
            "EBUSY"
        );
        let mut args = base.clone();
        args["sessionId"] = session;
        args["turnId"] = turn["result"]["turnId"].clone();
        args["requestId"] = json!("interrupt");
        assert_eq!(
            state
                .services
                .require::<cells::harness::Harnesses>()
                .unwrap()
                .project_call(&state, 0, "harness_interrupt", args)
                .await
                .unwrap()["outcome"],
            "committed"
        );
        assert!(state.files.shared().is_ok());
    }
    let other = register(&state, "group/new", true).await;
    let mut args = base.clone();
    args["projectId"] = other["id"].clone();
    args["revision"] = json!(1);
    args["sessionId"] = created["result"]["session"]["id"].clone();
    assert!(state
        .services
        .require::<cells::harness::Harnesses>()
        .unwrap()
        .project_call(&state, 0, "harness_session_read", args)
        .await
        .is_err());
    state
        .services
        .require::<cells::harness::Harnesses>()
        .unwrap()
        .close()
        .await;
}

fn patch_note(export: &cells::fs::Export, path: &str) {
    export
        .apply(
            &cells::fs::mutation::Change::Metadata {
                path: path.into(),
                patch: json!({"_document_chat":{"session":"stable-session"}})
                    .as_object()
                    .unwrap()
                    .clone(),
                remove: vec![],
            },
            || Ok(()),
        )
        .unwrap();
}

#[tokio::test]
async fn project_scope_moves_ancestor_metadata_and_preserves_identity_across_remount() {
    use cells::fs::mutation::Change;
    let root = tempfile::tempdir().unwrap();
    let config = deployment(root.path());
    let state = State::from_config(&config).unwrap();
    let export = state.exports.get("home").unwrap();
    let path = "group/existing/note.md";
    std::fs::write(root.path().join("exports").join(path), "note").unwrap();
    patch_note(export, path);
    let before = export.stat(path).unwrap().unwrap();
    let project = register(&state, "group/existing", false).await;
    let scoped = root
        .path()
        .join("exports/group/existing/.nestos/meta.sqlite");
    let db = rusqlite::Connection::open(&scoped).unwrap();
    let (relative, identity): (String, String) = db
        .query_row("SELECT path,identity FROM file_metadata", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(relative, "note.md");
    assert_eq!(Some(identity), before.source_identity);
    let parent =
        rusqlite::Connection::open(root.path().join("exports/.nestos/meta.sqlite")).unwrap();
    assert_eq!(
        parent
            .query_row::<i64, _, _>("SELECT count(*) FROM file_metadata", [], |r| r.get(0))
            .unwrap(),
        0
    );
    assert_eq!(
        export.stat(path).unwrap().unwrap().metadata,
        before.metadata
    );
    assert_eq!(
        export
            .apply(
                &Change::Rename {
                    path: path.into(),
                    to: "outside.md".into()
                },
                || Ok(())
            )
            .unwrap_err()
            .code,
        "METADATA_SCOPE_CROSS_MOVE"
    );
    assert!(root.path().join("exports").join(path).exists());
    export
        .apply(
            &Change::Rename {
                path: path.into(),
                to: "group/existing/renamed.md".into(),
            },
            || Ok(()),
        )
        .unwrap();
    let readonly = remount(&state, &project, "ro").await.unwrap()["project"].clone();
    remount(&state, &readonly, "rw").await.unwrap();
    assert_eq!(
        export
            .stat("group/existing/renamed.md")
            .unwrap()
            .unwrap()
            .source_identity,
        before.source_identity
    );
}

#[tokio::test]
async fn project_scope_migrates_legacy_directory_and_keeps_owner_records_separate() {
    let root = tempfile::tempdir().unwrap();
    let config = deployment(root.path());
    let project_root = root.path().join("exports/group/existing");
    std::fs::write(project_root.join("note.md"), "note").unwrap();
    {
        let export = cells::fs::Export::exclusive(&project_root).unwrap();
        patch_note(&export, "note.md");
    }
    std::fs::write(project_root.join(".nestos/info.seq"), "preserved-sequence").unwrap();
    std::fs::rename(project_root.join(".nestos"), project_root.join(".mindos")).unwrap();
    let state = State::from_config(&config).unwrap();
    let project = register(&state, "group/existing", false).await;
    assert!(!project_root.join(".mindos").exists());
    assert_eq!(
        std::fs::read_to_string(project_root.join(".nestos/info.seq")).unwrap(),
        "preserved-sequence"
    );
    let export = state.exports.get("home").unwrap();
    let id = project["id"].as_str().unwrap();
    export
        .initialize_metadata_scope(
            "group/existing",
            "other-server:other-owner",
            id,
            &json!({"name":"Other"}),
        )
        .unwrap();
    let db = rusqlite::Connection::open(project_root.join(".nestos/meta.sqlite")).unwrap();
    let names: Vec<String> = db
        .prepare("SELECT json_extract(value,'$.name') FROM scope_info ORDER BY owner")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, vec!["Other", "Notes"]);
    assert_eq!(
        export
            .stat("group/existing/note.md")
            .unwrap()
            .unwrap()
            .metadata["_document_chat"]["session"],
        "stable-session"
    );
}

#[tokio::test]
async fn readonly_project_does_not_create_metadata_directory() {
    let root = tempfile::tempdir().unwrap();
    let state = State::from_config(&deployment(root.path())).unwrap();
    call(
        &state,
        0,
        "project_register",
        json!({"name":"Reference","alias":"home","path":"group/existing","access":"ro"}),
    )
    .await
    .unwrap();
    assert!(!root.path().join("exports/group/existing/.nestos").exists());
    assert!(!root.path().join("exports/group/existing/.mindos").exists());
}

#[tokio::test]
async fn execution_configuration_is_authorized_revision_checked_and_persistent() {
    let root = tempfile::tempdir().unwrap();
    let mut config = deployment(root.path());
    config.projects.as_mut().unwrap().execution_profiles = vec![serde_json::from_value(json!({
        "id":"dev","name":"Development","principals":[],"variables":{"GIT_AUTHOR_NAME":"Rain Li"}
    })).unwrap()];
    let state = State::from_config(&config).unwrap();
    let project = register(&state, "group/existing", false).await;
    let id = project["id"].as_str().unwrap();
    let settings = call(&state, 0, "project_execution", json!({"projectId":id})).await.unwrap();
    assert_eq!(settings["profiles"], json!([]));
    let denied = call(&state, 0, "project_set_execution", json!({"projectId":id,"revision":1,
        "execution":{"profile":"dev","variables":{}}})).await.unwrap_err();
    assert_eq!(denied.code, "PROJECT_EXECUTION_PROFILE_DENIED");
    let invalid = call(&state, 0, "project_set_execution", json!({"projectId":id,"revision":1,
        "execution":{"profile":null,"variables":{"CELLS_PROJECT":"forged"}}})).await.unwrap_err();
    assert_eq!(invalid.code, "PROJECT_ENV_RESERVED");
    let saved = call(&state, 0, "project_set_execution", json!({"projectId":id,"revision":1,
        "execution":{"profile":null,"variables":{"GIT_AUTHOR_NAME":"Rain Li"}}})).await.unwrap();
    assert_eq!(saved["project"]["revision"], 2);
    let stale = call(&state, 0, "project_set_execution", json!({"projectId":id,"revision":1,
        "execution":{"profile":null,"variables":{}}})).await.unwrap_err();
    assert_eq!(stale.code, "PROJECT_REVISION_CHANGED");
    drop(state);
    let reopened = State::from_config(&config).unwrap();
    let settings = call(&reopened, 0, "project_execution", json!({"projectId":id})).await.unwrap();
    assert_eq!(settings["execution"]["variables"]["GIT_AUTHOR_NAME"], "Rain Li");
}

#[tokio::test]
async fn native_isolation_uses_real_cwd_and_keeps_package_siblings_visible() {
    use std::os::unix::fs::PermissionsExt;
    let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());
    config.projects.as_mut().unwrap().isolation=cells::projects::model::Isolation::None;
    let state=State::from_config(&config).unwrap();cells::process::enable(&state).await.unwrap();
    let project=register(&state,"group/existing",false).await;
    call(&state,0,"project_set_execution",json!({"projectId":project["id"],"revision":project["revision"],"execution":{"profile":null,"variables":{"CUSTOM_ENV":"kept"}}})).await.unwrap();
    let stored=state.services.get::<cells::projects::ProjectService>().unwrap().get(0,project["id"].as_str().unwrap()).unwrap();
    let runtime=ProjectRuntime::new(&state,0,stored).unwrap();assert!(!runtime.isolated());
    assert_eq!(runtime.cwd(),root.path().join("exports/group/existing").to_str().unwrap());
    let package=root.path().join("package");std::fs::create_dir(&package).unwrap();
    std::fs::write(package.join("sibling"),"package-dependency").unwrap();
    std::fs::write(package.join("entry"),"#!/bin/sh\nprintf '%s\\n' \"$PWD\"; cat \"$(dirname \"$0\")/sibling\"; printf '%s' \"$CUSTOM_ENV\"\n").unwrap();
    std::fs::set_permissions(package.join("entry"),std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut profile=cells::harness::config::validate(&config).unwrap().remove(0);profile.command=package.join("entry").to_str().unwrap().into();
    let mut prepared=runtime.prepare(&profile,vec![]).unwrap();runtime.finish_environment(&mut prepared.command).unwrap();
    let output=prepared.command.output().await.unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    let text=String::from_utf8(output.stdout).unwrap();assert!(text.contains(&runtime.cwd()));assert!(text.contains("package-dependency"));assert!(text.ends_with("kept"));
    assert_eq!(runtime.resolve_cwd(&runtime.cwd()).unwrap(),runtime.cwd());assert!(runtime.resolve_cwd("/etc").is_err());
}

#[test]
fn project_isolation_defaults_to_bubblewrap_and_rejects_unknown_modes() {
    use cells::projects::{Config,model::Isolation};
    let default:Config=toml::from_str("root='/tmp/projects'").unwrap();assert!(default.isolation==Isolation::Bubblewrap);
    let native:Config=toml::from_str("root='/tmp/projects'\nisolation='none'").unwrap();assert!(native.isolation==Isolation::None);
    assert!(toml::from_str::<Config>("root='/tmp/projects'\nisolation='invalid'").is_err());
}

#[tokio::test]
async fn native_mode_rejects_virtual_mount_replacement_and_retains_directory_pins() {
    let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());config.projects.as_mut().unwrap().isolation=cells::projects::model::Isolation::None;
    let state=State::from_config(&config).unwrap();cells::process::enable(&state).await.unwrap();
    let project=call(&state,0,"project_register",json!({"name":"Native","alias":"home","path":"group/existing","access":"rw",
        "mounts":[{"alias":"reference","path":"","at":"/workspace/ref","access":"ro"}]})).await.unwrap()["project"].clone();
    let stored=state.services.get::<cells::projects::ProjectService>().unwrap().get(0,project["id"].as_str().unwrap()).unwrap();
    let runtime=ProjectRuntime::new(&state,0,stored).unwrap();let profile=cells::harness::config::validate(&config).unwrap().remove(0);
    assert_eq!(runtime.prepare(&profile,vec![]).err().unwrap().code,"NATIVE_MOUNT_REMAP_UNSUPPORTED");
    let source=root.path().join("exports/group/existing");std::fs::rename(&source,root.path().join("original")).unwrap();std::fs::create_dir_all(source.join("ref")).unwrap();
    assert_eq!(runtime.prepare(&profile,vec![]).err().unwrap().code,"PROJECT_DIRECTORY_CHANGED");
}

#[tokio::test]
async fn native_process_api_advertises_cooperative_mode_and_cancels_owned_children() {
    use tower::ServiceExt;
    use http_body_util::BodyExt;
    let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());config.projects.as_mut().unwrap().isolation=cells::projects::model::Isolation::None;
    let state=State::from_config(&config).unwrap();cells::process::enable(&state).await.unwrap();
    let response=cells::router(state.clone(),&[]).unwrap().oneshot(axum::http::Request::builder().uri("/v1/capabilities")
        .header("authorization","Bearer project-test-secret-0123456789").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    let caps:Value=serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(caps["executionModel"],"trusted-cooperative-host");assert_eq!(caps["readOnlyEnforcement"],"none");assert_eq!(caps["pathModel"],"host-mapped");
    let project=register(&state,"group/existing",false).await;let epoch=state.services.require::<cells::process::Execution>().unwrap().epoch();
    call(&state,0,"project_exec",json!({"projectId":project["id"],"revision":project["revision"],"epoch":epoch,"requestId":"native-cancel",
        "command":"/bin/sh","args":["-c","sleep 120 & echo $! > child; wait"],"timeoutMs":10000})).await.unwrap();
    let child_path=root.path().join("exports/group/existing/child");
    tokio::time::timeout(std::time::Duration::from_secs(3),async{while !child_path.exists(){tokio::time::sleep(std::time::Duration::from_millis(5)).await;}}).await.unwrap();
    let pid:usize=std::fs::read_to_string(&child_path).unwrap().trim().parse().unwrap();
    cells::process::cancel(&state,0,epoch,"native-cancel").unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3),async{loop{
        if cells::process::status(&state,0,epoch,"native-cancel").unwrap().state=="cancelled"{break;}tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }}).await.unwrap();
    if let Ok(stat)=std::fs::read_to_string(format!("/proc/{pid}/stat")){assert!(stat.split_whitespace().nth(2).is_some_and(|state|state=="Z"||state=="X"));}
}

#[tokio::test]
async fn channel_project_context_maps_selected_projects_and_rejects_changed_scopes() {
    for isolation in [cells::projects::model::Isolation::Bubblewrap,cells::projects::model::Isolation::None] {
        let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());
        config.projects.as_mut().unwrap().isolation=isolation;
        config.projects.as_mut().unwrap().network=true;
        config.projects.as_mut().unwrap().channel_endpoint=Some("http://localhost/v1/channel/mcp".into());config.plugins.insert("mcp".into(),true);
        std::fs::create_dir_all(root.path().join("exports/group/existing/discussion")).unwrap();
        let state=State::from_config(&config).unwrap();
        let primary=register(&state,"group/existing",false).await;let secondary=register(&state,"group/secondary",true).await;
        let args=json!({"projectId":primary["id"],"revision":primary["revision"],"scopeId":"decision","cwd":"discussion","snapshot":{"context":{"scope":"selected"}},
            "contextProjects":[{"projectId":primary["id"],"revision":primary["revision"]},{"projectId":secondary["id"],"revision":secondary["revision"]}]});
        let result=call(&state,0,"project_channel_open",args.clone()).await.unwrap();
        let context=&result["projectContext"];assert_eq!(context["primaryProjectId"],primary["id"]);
        let expected=if isolation==cells::projects::model::Isolation::None {root.path().join("exports/group/secondary").to_str().unwrap().to_string()}else{format!("/projects/{}",secondary["id"].as_str().unwrap())};
        assert_eq!(context["projects"][1]["directory"],expected);
        let service=state.services.require::<cells::projects::ProjectService>().unwrap();
        let stored=service.get(0,primary["id"].as_str().unwrap()).unwrap();let runtime=ProjectRuntime::new(&state,0,stored).unwrap();
        service.channels.bind(primary["id"].as_str().unwrap(),&runtime.working_directory("discussion").unwrap(),"native").unwrap();
        assert_eq!(service.channels.project_context(primary["id"].as_str().unwrap(),None,Some("native")).unwrap(),Some(context.clone()));
        assert_eq!(call(&state,0,"project_channel_open",args.clone()).await.unwrap(),result);
        let mut changed=args.clone();changed["contextProjects"][1]["revision"]=json!(999);
        assert_eq!(call(&state,0,"project_channel_open",changed).await.unwrap_err().code,"PROJECT_CONTEXT_CHANGED");
        let mut injected=args;injected["snapshot"]["project-context"]=json!({"projects":[]});
        assert!(call(&state,0,"project_channel_open",injected).await.is_err());
    }
}

#[tokio::test]
async fn selected_context_mounts_are_readable_and_rechecked_before_harness_execution() {
    for isolation in [cells::projects::model::Isolation::Bubblewrap,cells::projects::model::Isolation::None] {
        let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());config.projects.as_mut().unwrap().isolation=isolation;
        let state=State::from_config(&config).unwrap();let primary=register(&state,"group/existing",false).await;let secondary=register(&state,"group/secondary",true).await;
        std::fs::write(root.path().join("exports/group/secondary/selected.txt"),"selected-project").unwrap();
        state.services.require::<cells::process::Execution>().unwrap().accept();
        let service=state.services.require::<cells::projects::ProjectService>().unwrap();
        let peer=service.get(0,secondary["id"].as_str().unwrap()).unwrap();
        let directory=ProjectRuntime::new(&state,0,peer.clone()).unwrap().cwd();
        let runtime=ProjectRuntime::new(&state,0,service.get(0,primary["id"].as_str().unwrap()).unwrap()).unwrap().with_context(vec![peer]);
        let mut profile=cells::harness::config::validate(&config).unwrap().remove(0);profile.command="/bin/sh".into();
        let mut prepared=runtime.prepare(&profile,vec!["-c".into(),format!("cat '{directory}/selected.txt'")]).unwrap();
        let output=prepared.command.output().await.unwrap();assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));assert_eq!(output.stdout,b"selected-project");
        if isolation==cells::projects::model::Isolation::Bubblewrap {
            let mut write=runtime.prepare(&profile,vec!["-c".into(),format!("echo forbidden > '{directory}/selected.txt'")]).unwrap();
            assert!(!write.command.output().await.unwrap().status.success());
        }
        call(&state,0,"project_configure",json!({"projectId":secondary["id"],"revision":secondary["revision"],"name":"Updated","mounts":[]})).await.unwrap();
        assert_eq!(runtime.prepare(&profile,vec![]).err().unwrap().code,"PROJECT_CONTEXT_CHANGED");
    }
}

#[tokio::test]
async fn remount_recovers_after_export_widening_and_alias_change_without_replacing_project() {
    let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());
    config.exports[0].path=root.path().join("exports/group/existing").to_string_lossy().into_owned();
    let state=State::from_config(&config).unwrap();let original=register(&state,"",false).await;
    let file=root.path().join("exports/group/existing/note.txt");std::fs::write(&file,"preserved").unwrap();
    let service=state.services.require::<cells::projects::ProjectService>().unwrap();
    service.channels.create(original["id"].as_str().unwrap(),&json!({"scopeId":"keep","cwd":"/work","endpoint":"http://localhost/channel","snapshot":{"task":{"title":"Preserved"}}})).unwrap();
    drop(state);
    config.exports[0].alias=Some("broader".into());config.exports[0].path=root.path().join("exports").to_string_lossy().into_owned();
    let state=State::from_config(&config).unwrap();
    let query=json!({"projectId":original["id"]});
    assert!(call(&state,0,"project_read",query.clone()).await.is_err());
    let binding=call(&state,0,"project_binding",query.clone()).await.unwrap();assert_eq!(binding["project"]["id"],original["id"]);
    let args=json!({"projectId":original["id"],"revision":original["revision"],"access":"rw","directory":{"alias":"broader","path":"group/existing"}});
    let updated=call(&state,0,"project_remount",args.clone()).await.unwrap()["project"].clone();
    assert_eq!(updated["id"],original["id"]);assert_eq!(updated["revision"],2);assert_eq!(updated["alias"],"broader");
    assert_eq!(std::fs::read_to_string(file).unwrap(),"preserved");
    assert!(state.services.require::<cells::projects::ProjectService>().unwrap().channels.list(original["id"].as_str().unwrap(),"keep").is_ok());
    assert_eq!(call(&state,0,"project_remount",args.clone()).await.unwrap_err().code,"PROJECT_REVISION_CHANGED");
    let mut retry=args;retry["revision"]=updated["revision"].clone();
    assert_eq!(call(&state,0,"project_remount",retry).await.unwrap()["project"]["revision"],updated["revision"]);
}

#[tokio::test]
async fn remount_rejects_another_directory_escape_and_active_execution_without_changing_binding() {
    let root=tempfile::tempdir().unwrap();let state=State::from_config(&deployment(root.path())).unwrap();
    let original=register(&state,"group/existing",false).await;
    std::fs::create_dir_all(root.path().join("exports/other")).unwrap();
    std::os::unix::fs::symlink(root.path().join("reference"),root.path().join("exports/escape")).unwrap();
    let request=|path:&str|json!({"projectId":original["id"],"revision":original["revision"],"access":"rw","directory":{"alias":"home","path":path}});
    assert_eq!(call(&state,0,"project_remount",request("other")).await.unwrap_err().code,"PROJECT_DIRECTORY_CHANGED");
    assert!(call(&state,0,"project_remount",request("../reference")).await.is_err());
    assert!(call(&state,0,"project_remount",request("escape")).await.is_err());
    let guard=state.files.exclusive().unwrap();
    assert_eq!(call(&state,0,"project_remount",request("group/existing")).await.unwrap_err().code,"EBUSY");drop(guard);
    let current=call(&state,0,"project_read",json!({"projectId":original["id"]})).await.unwrap();
    assert_eq!(current["project"]["revision"],original["revision"]);assert_eq!(current["project"]["path"],original["path"]);
}

#[tokio::test]
async fn recovery_binding_is_owner_only_and_does_not_restore_revoked_directory_access() {
    let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());config.token=None;
    config.principals=["owner","bob"].into_iter().map(|id|serde_json::from_value(json!({"id":id,"token":format!("{id}-local-fixture-0123456789abcdef")})).unwrap()).collect();
    config.harnesses[0].principal_id=Some("owner".into());
    config.exports[0].grants=vec![serde_json::from_value(json!({"principal":"owner","access":"rw","execute":true})).unwrap()];
    let state=State::from_config(&config).unwrap();let project=register(&state,"group/existing",false).await;drop(state);
    config.exports[0].grants.clear();let state=State::from_config(&config).unwrap();let args=json!({"projectId":project["id"]});
    assert!(call(&state,0,"project_binding",args.clone()).await.is_ok());
    assert!(call(&state,0,"project_read",args.clone()).await.is_err());
    assert!(call(&state,1,"project_binding",args.clone()).await.is_err());
    assert!(call(&state,1,"project_remount",json!({"projectId":project["id"],"revision":1,"access":"rw","directory":{"alias":"home","path":"group/existing"}})).await.is_err());
}

#[tokio::test]
async fn remount_does_not_create_two_registered_project_identities_for_one_new_binding() {
    let root=tempfile::tempdir().unwrap();let mut config=deployment(root.path());
    let state=State::from_config(&config).unwrap();let original=register(&state,"group/existing",false).await;drop(state);
    config.exports[0].alias=Some("new".into());let state=State::from_config(&config).unwrap();
    call(&state,0,"project_register",json!({"name":"Another","alias":"new","path":"group/existing","access":"rw"})).await.unwrap();
    let error=call(&state,0,"project_remount",json!({"projectId":original["id"],"revision":original["revision"],"access":"rw","directory":{"alias":"new","path":"group/existing"}})).await.unwrap_err();
    assert_eq!(error.code,"PROJECT_DIRECTORY_ALREADY_REGISTERED");
}

#[tokio::test]
async fn project_stat_reads_existing_files_without_waiting_for_an_unrelated_command() {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let mut config = deployment(root.path());
    config.projects.as_mut().unwrap().isolation = cells::projects::model::Isolation::None;
    let state = State::from_config(&config).unwrap();
    cells::process::enable(&state).await.unwrap();
    let project = register(&state, "group/existing", false).await;
    std::fs::write(root.path().join("exports/group/existing/retry-note"),"original").unwrap();
    let epoch = state.services.require::<cells::process::Execution>().unwrap().epoch().to_owned();
    call(&state, 0, "project_exec", json!({"projectId":project["id"],"revision":project["revision"],
        "epoch":epoch,"requestId":"short-stat-conflict","command":"/bin/sh",
        "args":["-c","sleep 0.2; printf ready > other-note"],"timeoutMs":2000})).await.unwrap();
    let started = std::time::Instant::now();
    let response = cells::router(state.clone(), &[]).unwrap().oneshot(axum::http::Request::builder()
        .method("POST").uri("/v1/fs/home/stat").header("authorization", "Bearer project-test-secret-0123456789")
        .header("x-fsagent-project", project["id"].as_str().unwrap())
        .header("x-fsagent-project-revision", project["revision"].to_string())
        .header("content-type", "application/json")
        .body(axum::body::Body::from(r#"{"paths":["group/existing/retry-note"]}"#)).unwrap()).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let data: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(data["results"][0]["stat"]["kind"], "file");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    let deadline=tokio::time::Instant::now()+std::time::Duration::from_secs(3);
    loop {
        if cells::process::status(&state,0,&epoch,"short-stat-conflict").unwrap().state=="exited"{break;}
        assert!(tokio::time::Instant::now()<deadline);tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(std::fs::read_to_string(root.path().join("exports/group/existing/retry-note")).unwrap(),"original");
    eprintln!("stat contention: elapsedMs={}, logicalApiCalls=1, httpRequests=1", started.elapsed().as_millis());
}

#[tokio::test]
async fn registration_uses_catalog_and_metadata_transactions_while_native_turns_are_running() {
    let root = tempfile::tempdir().unwrap();
    let state = State::from_config(&deployment(root.path())).unwrap();
    let original = register(&state, "group/existing", false).await;
    let project = state.services.require::<cells::projects::ProjectService>().unwrap().get(0, original["id"].as_str().unwrap()).unwrap();
    let runtime = ProjectRuntime::new(&state, 0, project).unwrap();
    runtime.begin("active-session", "native-operation").await.unwrap();
    runtime.bind_turn("active-session", "native-operation", "active-turn");
    assert!(state.files.shared().is_ok());
    assert!(state.files.exclusive().is_err());
    assert_eq!(register(&state, "group/existing", false).await, original);
    let new = register(&state, "group/independent", true).await;
    assert_ne!(new["id"], original["id"]);
    assert!(root.path().join("exports/group/independent/.nestos").is_dir());
    assert_eq!(remount(&state, &original, "ro").await.unwrap_err().code, "EBUSY");
    runtime.event(&json!({"method":"turn/completed","params":{"threadId":"active-session","turn":{"id":"active-turn"}}}));
    assert!(state.files.shared().is_ok());
    assert_eq!(register(&state, "group/independent", false).await, new);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registration_preserves_one_identity_and_failures_do_not_leak_catalog_admission() {
    let root = tempfile::tempdir().unwrap();
    let state = State::from_config(&deployment(root.path())).unwrap();
    let _native = state.files.activity().unwrap();
    let workers: Vec<_> = (0..8).map(|_| {
        let state = state.clone();
        tokio::spawn(async move {register(&state, "group/existing", false).await})
    }).collect();
    let mut projects = Vec::new();
    for worker in workers {projects.push(worker.await.unwrap());}
    assert!(projects.iter().all(|project|project == &projects[0]));
    let failed = call(&state, 0, "project_register", json!({"name":"Missing","alias":"home","path":"missing/child","access":"rw","createDirectory":true})).await;
    assert!(failed.is_err());
    assert_eq!(register(&state, "group/existing", false).await, projects[0]);
    state.files.poison();
    assert_eq!(call(&state, 0, "project_register", json!({"name":"Refused","alias":"home","path":"group/refused","access":"rw","createDirectory":true})).await.unwrap_err().code,"EIO");
    assert!(!root.path().join("exports/group/refused").exists());
}
