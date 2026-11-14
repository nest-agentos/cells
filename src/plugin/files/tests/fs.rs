//! Export-level behaviour that does not need HTTP: capability opening, restart
//! recovery and revision retirement.

use cells::fs::{Export, Exports};

#[test]
fn writable_exports_do_not_exclusively_lock_the_directory() {
    let root = tempfile::tempdir().unwrap();
    let export = Export::open_writable(root.path()).unwrap();
    assert!(export.writable());
    assert!(Export::open_writable(root.path()).is_ok());
    // Read and independent write capabilities can coexist.
    assert!(Export::open(root.path()).is_ok());
}

#[test]
fn restart_retires_validators_without_scanning_unregistered_uploads() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("note"), "content").unwrap();
    let old = {
        let export = Export::open_writable(root.path()).unwrap();
        export.stat("note").unwrap().unwrap().revision
    };
    std::fs::write(root.path().join(".itookit-upload-abandoned"), "incomplete").unwrap();
    // An outside entry with a reserved name must not block startup.
    std::fs::create_dir(root.path().join(".itookit-upload-directory")).unwrap();
    let export = Export::open_writable(root.path()).unwrap();
    assert_ne!(export.stat("note").unwrap().unwrap().revision, old);
    assert!(root.path().join(".itookit-upload-abandoned").exists());
    assert!(root.path().join(".itookit-upload-directory").exists());
}

/// Reserved staging names are never listed, but their siblings are.
#[test]
fn reserved_names_are_hidden_from_listings() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("visible"), "x").unwrap();
    std::fs::write(root.path().join(".itookit-upload-hidden"), "x").unwrap();
    let export = Export::open(root.path()).unwrap();
    let listing = export
        .list("", &tokio_util::sync::CancellationToken::new())
        .unwrap();
    let names: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, ["visible"]);
}

#[test]
fn the_registry_lists_aliases_and_can_invalidate_all_revisions() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("note"), "content").unwrap();
    let writable = Export::open_writable(root.path()).unwrap();
    let reference = root.path().join("reference");
    std::fs::create_dir(&reference).unwrap();
    let exports = Exports::new(
        [
            ("project".to_owned(), std::sync::Arc::new(writable)),
            (
                "reference".to_owned(),
                std::sync::Arc::new(Export::open(&reference).unwrap()),
            ),
        ]
        .into_iter()
        .collect(),
    );
    assert_eq!(
        exports.aliases().collect::<Vec<_>>(),
        ["project", "reference"]
    );
    assert!(exports.get("project").unwrap().writable());
    assert!(!exports.get("reference").unwrap().writable());
    let before = exports
        .get("project")
        .unwrap()
        .stat("note")
        .unwrap()
        .unwrap();
    exports.invalidate_revisions();
    let after = exports
        .get("project")
        .unwrap()
        .stat("note")
        .unwrap()
        .unwrap();
    assert_ne!(before.revision, after.revision);
}

#[test]
fn file_metadata_is_lazy_remote_durable_and_mount_independent() {
    use cells::fs::mutation::Change;
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("nested")).unwrap();
    std::fs::write(root.path().join("nested/note.md"), "Original").unwrap();
    let export = Export::open_writable(root.path()).unwrap();
    let before = export.stat("nested/note.md").unwrap().unwrap().revision;
    export
        .list("", &tokio_util::sync::CancellationToken::new())
        .unwrap();
    assert!(!root.path().join(".nestos").exists());
    export
        .apply(
            &Change::Metadata {
                path: "nested/note.md".into(),
                patch: json!({"a":1,"nested":{"id":"anchor"}})
                    .as_object()
                    .unwrap()
                    .clone(),
                remove: vec![],
            },
            || Ok(()),
        )
        .unwrap();
    assert!(root.path().join(".nestos/meta.sqlite").is_file());
    let stat = export.stat("nested/note.md").unwrap().unwrap();
    assert_eq!(stat.revision, before);
    let identity = stat.source_identity.clone();
    assert!(identity.is_some());
    assert_eq!(stat.metadata["a"], 1);
    let other = Export::open(root.path()).unwrap();
    assert_eq!(
        other.stat("nested/note.md").unwrap().unwrap().metadata["nested"]["id"],
        "anchor"
    );
    assert!(other
        .apply(
            &Change::Metadata {
                path: "nested/note.md".into(),
                patch: json!({"a":2}).as_object().unwrap().clone(),
                remove: vec![]
            },
            || Ok(())
        )
        .is_err());
    assert!(export.stat(".nestos/meta.sqlite").is_err());
    assert_eq!(
        export
            .list("", &tokio_util::sync::CancellationToken::new())
            .unwrap()
            .entries
            .len(),
        1
    );
    export
        .apply(
            &Change::Rename {
                path: "nested".into(),
                to: "moved".into(),
            },
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        other.stat("moved/note.md").unwrap().unwrap().metadata["a"],
        1
    );
    export
        .apply(
            &Change::Metadata {
                path: "moved/note.md".into(),
                patch: json!({"b":true}).as_object().unwrap().clone(),
                remove: vec!["a".into()],
            },
            || Ok(()),
        )
        .unwrap();
    assert!(!other
        .stat("moved/note.md")
        .unwrap()
        .unwrap()
        .metadata
        .contains_key("a"));
    drop(other);
    drop(export);
    let reopened = Export::open_writable(root.path()).unwrap();
    assert_eq!(
        reopened
            .stat("moved/note.md")
            .unwrap()
            .unwrap()
            .source_identity,
        identity
    );
    assert_eq!(
        reopened.stat("moved/note.md").unwrap().unwrap().metadata["b"],
        true
    );
    reopened
        .apply(
            &Change::Remove {
                path: "moved/note.md".into(),
            },
            || Ok(()),
        )
        .unwrap();
    std::fs::write(root.path().join("moved/note.md"), "New").unwrap();
    assert!(reopened
        .stat("moved/note.md")
        .unwrap()
        .unwrap()
        .metadata
        .is_empty());
}

#[test]
fn entry_projection_never_opens_unneeded_metadata() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("note.md"), "content").unwrap();
    let metadata = root.path().join(".nestos");
    std::fs::create_dir(&metadata).unwrap();
    let database = metadata.join("meta.sqlite");
    std::fs::write(&database, "invalid database").unwrap();
    std::fs::set_permissions(database, std::fs::Permissions::from_mode(0o600)).unwrap();
    let export = Export::open(root.path()).unwrap();
    let entry = export
        .stat_fields("note.md", cells::fs::model::ReadFields::Entry)
        .unwrap()
        .unwrap();
    assert!(entry.metadata.is_empty());
    assert!(entry.revision.is_none());
    assert_eq!(
        export
            .list_fields(
                "",
                &tokio_util::sync::CancellationToken::new(),
                cells::fs::model::ReadFields::Entry
            )
            .unwrap()
            .entries
            .len(),
        1
    );
    assert!(export.stat("note.md").is_err());
}

#[test]
fn metadata_payloads_are_per_file_and_cross_export_patches_do_not_lose_fields() {
    use cells::fs::mutation::Change;
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    for name in ["a", "b"] {
        std::fs::write(root.path().join(name), "content").unwrap();
    }
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for index in 0..8 {
            let path = root.path();
            let barrier = barrier.clone();
            workers.push(scope.spawn(move || {
                let export = Export::open_writable(path).unwrap();
                barrier.wait();
                export
                    .apply(
                        &Change::Metadata {
                            path: "a".into(),
                            patch: json!({format!("field{index}"):index})
                                .as_object()
                                .unwrap()
                                .clone(),
                            remove: vec![],
                        },
                        || Ok(()),
                    )
                    .unwrap();
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let export = Export::open_writable(root.path()).unwrap();
    assert_eq!(export.stat("a").unwrap().unwrap().metadata.len(), 8);
    export
        .apply(
            &Change::Metadata {
                path: "b".into(),
                patch: json!({"other":true}).as_object().unwrap().clone(),
                remove: vec![],
            },
            || Ok(()),
        )
        .unwrap();
    let db = rusqlite::Connection::open(root.path().join(".nestos/meta.sqlite")).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM file_metadata WHERE data='{}'",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        2
    );
    let mut ids = db.prepare("SELECT identity FROM file_metadata").unwrap();
    let ids = ids
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for id in ids {
        assert!(root
            .path()
            .join(format!(".nestos/metadata-{id}.sqlite"))
            .exists());
    }
}

#[test]
fn simultaneous_conditional_replacements_across_exports_have_one_winner() {
    use cells::fs::upload;
    use std::io::Write;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("note"), "before").unwrap();
    let first = Export::open_writable(root.path()).unwrap();
    let second = Export::open_writable(root.path()).unwrap();
    let a = first.stat("note").unwrap().unwrap().revision.unwrap();
    let b = second.stat("note").unwrap().unwrap().revision.unwrap();
    let x = first.stage("note").unwrap();
    x.writer().unwrap().write_all(b"first").unwrap();
    let y = second.stage("note").unwrap();
    y.writer().unwrap().write_all(b"second").unwrap();
    let barrier = std::sync::Barrier::new(2);
    let outcomes = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            barrier.wait();
            upload::commit(&first, "note", x, Some(&a), || Ok(())).is_ok()
        });
        barrier.wait();
        let other = upload::commit(&second, "note", y, Some(&b), || Ok(())).is_ok();
        (worker.join().unwrap(), other)
    });
    assert_ne!(outcomes.0, outcomes.1);
    let content = std::fs::read(root.path().join("note")).unwrap();
    assert!(content == b"first" || content == b"second");
}

#[test]
fn established_file_properties_do_not_write_the_shared_directory_index() {
    use cells::fs::mutation::Change;
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("note"), "content").unwrap();
    let export = Export::open_writable(root.path()).unwrap();
    let change = |value| Change::Metadata {
        path: "note".into(),
        patch: json!({"value":value}).as_object().unwrap().clone(),
        remove: vec![],
    };
    export.apply(&change(1), || Ok(())).unwrap();
    let db = rusqlite::Connection::open(root.path().join(".nestos/meta.sqlite")).unwrap();
    db.execute_batch("BEGIN IMMEDIATE;").unwrap();
    std::thread::scope(|scope| {
        let (done, finished) = std::sync::mpsc::channel();
        let export = &export;
        let worker = scope.spawn(move || done.send(export.apply(&change(2), || Ok(()))).unwrap());
        let result = finished.recv_timeout(std::time::Duration::from_secs(1));
        db.execute_batch("ROLLBACK;").unwrap();
        worker.join().unwrap();
        result.unwrap().unwrap();
    });
    assert_eq!(export.stat("note").unwrap().unwrap().metadata["value"], 2);
}

#[test]
fn a_published_validator_can_be_used_for_the_next_write_without_refresh() {
    use cells::fs::upload;
    use std::io::Write;
    let root = tempfile::tempdir().unwrap();
    let export = Export::open_writable(root.path()).unwrap();
    let upload = export.stage("note").unwrap();
    upload.writer().unwrap().write_all(b"first").unwrap();
    let first = upload::commit(&export, "note", upload, None, || Ok(())).unwrap();
    assert_eq!(
        first.revision,
        export.stat("note").unwrap().unwrap().revision
    );
    let upload = export.stage("note").unwrap();
    upload.writer().unwrap().write_all(b"second").unwrap();
    let second = upload::commit(
        &export,
        "note",
        upload,
        first.revision.as_deref(),
        || Ok(()),
    )
    .unwrap();
    assert_eq!(
        second.revision,
        export.stat("note").unwrap().unwrap().revision
    );
}
