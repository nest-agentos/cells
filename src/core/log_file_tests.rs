use super::*;
use std::os::unix::fs::PermissionsExt;

fn fixture(config: Config) -> (tempfile::TempDir, FileLog) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    fs::write(&path, "").unwrap();
    let log = FileLog::open(&config, &path).unwrap();
    (root, log)
}

#[test]
fn writes_complete_json_lines_immediately_with_private_permissions() {
    let (root, mut log) = fixture(Config::default());
    log.write_line(r#"{"event":"http.failed","status":409}"#)
        .unwrap();
    let text = fs::read_to_string(log.path()).unwrap();
    assert!(text.ends_with('\n'));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(text.trim()).unwrap()["status"],
        409
    );
    assert_eq!(log.path().parent().unwrap(), root.path().join("logs"));
    assert_eq!(
        fs::metadata(log.path()).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(log.path().parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
}

#[test]
fn resolves_directory_relative_to_config_and_separates_instances() {
    let config = Config {
        directory: Some("diagnostics".into()),
        ..Config::default()
    };
    let (root, log) = fixture(config);
    let other = FileLog::open(&Config::default(), &root.path().join("config.toml")).unwrap();
    assert_eq!(
        log.path().parent().unwrap(),
        root.path().join("diagnostics")
    );
    assert_ne!(log.path(), other.path());
}

#[test]
fn rotates_whole_records_and_bounds_retained_segments() {
    let (_root, mut log) = fixture(Config {
        max_file_bytes: 1024,
        retained_files: 3,
        ..Config::default()
    });
    for n in 0..6 {
        log.write_line(&format!(r#"{{"n":{n},"padding":"{}"}}"#, "a".repeat(700)))
            .unwrap();
    }
    for (index, expected) in [(0, 5), (1, 4), (2, 3)] {
        let text = fs::read_to_string(log.segment(index)).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(text.trim()).unwrap()["n"],
            expected
        );
    }
    assert!(!log.segment(3).exists());
}

#[test]
fn retaining_one_file_discards_old_records() {
    let (_root, mut log) = fixture(Config {
        max_file_bytes: 1024,
        retained_files: 1,
        ..Config::default()
    });
    log.write_line(&"a".repeat(800)).unwrap();
    log.write_line(&"b".repeat(800)).unwrap();
    assert_eq!(
        fs::read_to_string(log.path()).unwrap(),
        format!("{}\n", "b".repeat(800))
    );
    assert!(!log.segment(1).exists());
}

#[test]
fn rejects_invalid_limits_and_unwritable_directory() {
    for config in [
        Config {
            max_file_bytes: 0,
            ..Config::default()
        },
        Config {
            retained_files: 0,
            ..Config::default()
        },
    ] {
        assert!(FileLog::open(&config, Path::new("missing")).is_err());
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    fs::write(&path, "").unwrap();
    let config = Config {
        directory: Some("config.toml/subdirectory".into()),
        ..Config::default()
    };
    assert!(FileLog::open(&config, &path).is_err());
}
