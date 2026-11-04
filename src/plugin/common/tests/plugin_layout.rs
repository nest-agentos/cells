use std::path::{Path, PathBuf};
fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = vec![];
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.file_name().unwrap() == "tests" {
            continue;
        }
        if path.is_dir() {
            sources.extend(rust_sources(&path));
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            sources.push(path);
        }
    }
    sources
}
#[test]
fn common_framework_and_mcp_transport_do_not_import_business_plugins() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for folder in ["plugin/common", "plugin/mcp", "http"] {
        for path in rust_sources(&root.join(folder)) {
            let source = std::fs::read_to_string(&path).unwrap();
            for feature in ["harness", "projects", "process", "sync"] {
                for prefix in ["crate::", "crate::plugin::"] {
                    let forbidden = format!("{prefix}{feature}::");
                    assert!(
                        !source.contains(&forbidden),
                        "{} imports {forbidden}",
                        path.display()
                    );
                }
            }
            for concrete in ["ProjectService", "SyncService", "Harnesses"] {
                assert!(
                    !source.contains(concrete),
                    "{} depends on {concrete}",
                    path.display()
                );
            }
        }
    }
}
#[test]
fn implementations_and_test_targets_remain_owned_by_their_plugins() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for feature in ["files", "process", "sync", "projects", "harness", "mcp"] {
        let owner = root.join("src/plugin").join(feature);
        assert!(owner.join("plugin.rs").is_file());
        assert!(owner.join("tests").is_dir());
    }
    for legacy in [
        "fs",
        "operations",
        "process",
        "sync",
        "projects",
        "harness",
        "plugins",
    ] {
        assert!(!root.join("src").join(legacy).exists());
    }
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    for target in manifest.lines().filter_map(|line| {
        line.strip_prefix("path = \"")
            .and_then(|line| line.strip_suffix('"'))
    }) {
        assert!(
            target.starts_with("src/plugin/"),
            "test target outside its plugin: {target}"
        );
        assert!(root.join(target).is_file());
    }
}
