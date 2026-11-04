//! Bounded literal search over the same read-only, pinned project view as file access.
use super::{
    model::Project,
    runtime::{Bubblewrap, ProjectLauncher},
    service,
};
use crate::{
    app::State,
    core::{error::Error, search::KeywordQuery},
    process::{policy, sandbox::Prepared},
};
use serde_json::{json, Value};
use std::{process::Stdio, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_MATCHES: usize = 100;
static SEARCHES: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

pub async fn search(state: &Arc<State>, identity: usize, args: Value) -> Result<Value, Error> {
    let _gate = state.files.read_only()?;
    let _permit = SEARCHES
        .try_acquire()
        .map_err(|_| Error::too_many("SEARCH_BUSY"))?;
    let query = args["query"]
        .as_str()
        .filter(|q| !q.trim().is_empty() && q.len() <= 1024 && !q.contains('\0'))
        .ok_or_else(Error::invalid)?;
    let mode = args["mode"]
        .as_str()
        .filter(|m| matches!(*m, "name" | "path" | "content"))
        .ok_or_else(Error::invalid)?;
    let directory = directory(&args)?;
    let query = KeywordQuery::new(query);
    let scope = scope_rules(directory);
    let project = authorized(state, identity, &args)?;
    let mut request = service::request(
        state,
        &project,
        "/usr/bin/rg".into(),
        arguments(&query, mode, scope.is_some()),
        10_000,
    );
    request.read_only = true;
    for mount in &mut request.mounts {
        mount.access = "ro".into();
    }
    let mut plan = policy::plan(state, identity, &request)?;
    service::pin(&mut plan, &project)?;
    exclude_harness_homes(state, &mut plan)?;
    if !state
        .services
        .get::<crate::process::Execution>()
        .is_some_and(|service| service.ready())
        || !std::path::Path::new("/usr/bin/rg").is_file()
    {
        return Err(Error::unsupported());
    }
    let prepared = Bubblewrap { network: false }.prepare_process(
        &plan,
        state
            .services
            .require::<crate::process::Execution>()?
            .lock_handle()?,
        None,
    )?;
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        collect(prepared, &query, mode, directory, scope),
    )
    .await
    .map_err(|_| Error::timed_out())??;
    let current = authorized(state, identity, &args)?;
    if current.root_identity != project.root_identity
        || current.mount_identities != project.mount_identities
    {
        return Err(Error::conflict("PROJECT_DIRECTORY_CHANGED"));
    }
    Ok(result)
}
fn directory(args: &Value) -> Result<&str, Error> {
    let path = match args.get("directory") {
        None => "",
        Some(value) => value.as_str().ok_or_else(Error::invalid)?,
    };
    if path.len() > 4096
        || path.contains(['\0', '\\'])
        || !path.is_empty()
            && path
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(Error::invalid());
    }
    Ok(path)
}
fn in_directory(row: &Value, directory: &str) -> bool {
    directory.is_empty()
        || row["path"]
            .as_str()
            .is_some_and(|path| path.starts_with(&format!("{directory}/")))
}
/// Prune sibling trees without turning the selected directory into an explicit rg root.
/// Explicit roots bypass ignore/hidden filtering and can follow root symlinks.
fn scope_rules(directory: &str) -> Option<String> {
    if directory.is_empty()
        || directory.contains(['\n', '\r'])
        || directory.split('/').any(|part| part.starts_with('.'))
    {
        return None;
    }
    let mut rules = String::new();
    let mut parent = String::new();
    for part in directory.split('/') {
        let escaped = escape_scope_component(part);
        rules.push_str(&format!("/{parent}*\n!/{parent}{escaped}/\n"));
        parent.push_str(&escaped);
        parent.push('/');
        if rules.len() > MAX_BYTES {
            return None;
        }
    }
    Some(rules)
}
fn escape_scope_component(part: &str) -> String {
    part.chars()
        .flat_map(|c| {
            if "\\*?[]{}!# ".contains(c) {
                vec!['\\', c]
            } else {
                vec![c]
            }
        })
        .collect()
}
fn exclude_harness_homes(state: &State, plan: &mut policy::Plan) -> Result<(), Error> {
    use std::os::fd::AsRawFd;
    let mut globs = Vec::new();
    for mount in &plan.mounts {
        let directory = mount.export.open_dir(&mount.path)?;
        let source = std::fs::read_link(format!("/proc/self/fd/{}", directory.as_raw_fd()))?;
        for home in state
            .services
            .get::<crate::harness::Harnesses>()
            .into_iter()
            .flat_map(|service| service.private_homes())
        {
            let home = home.canonicalize()?;
            if source.starts_with(&home) {
                return Err(Error::forbidden("SEARCH_PRIVATE_ROOT"));
            }
            if let Ok(relative) = home.strip_prefix(&source) {
                let at = mount
                    .at
                    .strip_prefix("/workspace")
                    .ok_or_else(Error::invalid)?;
                let path = format!(
                    "{}{}{}",
                    at.trim_start_matches('/'),
                    if at.is_empty() { "" } else { "/" },
                    relative.to_string_lossy()
                );
                // Glob metacharacters in private paths must never weaken the exclusion.
                let escaped: String = path
                    .chars()
                    .flat_map(|c| {
                        if "\\*?[]{}!".contains(c) {
                            vec!['\\', c]
                        } else {
                            vec![c]
                        }
                    })
                    .collect();
                globs.extend([
                    "--glob".into(),
                    format!("!/{escaped}/**"),
                    "--glob".into(),
                    format!("!/{escaped}"),
                ]);
            }
        }
    }
    plan.args.splice(0..0, globs);
    Ok(())
}
fn authorized(state: &State, identity: usize, args: &Value) -> Result<Project, Error> {
    let id = args["projectId"].as_str().ok_or_else(Error::invalid)?;
    let project = state
        .services
        .get::<crate::projects::ProjectService>()
        .ok_or_else(Error::unsupported)?
        .get(identity, id)?;
    service::authorize(state, identity, &project)?;
    if args["revision"].as_u64() != Some(project.revision) {
        return Err(Error::conflict("PROJECT_REVISION_CHANGED"));
    }
    Ok(project)
}
fn arguments(query: &KeywordQuery, mode: &str, scoped: bool) -> Vec<String> {
    let mut args: Vec<String> = [
        "--no-config",
        "--no-require-git",
        "--no-ignore-global",
        "--color",
        "never",
        "--max-filesize",
        "2M",
    ]
    .map(String::from)
    .into();
    if scoped {
        args.extend(["--ignore-file".into(), "/dev/stdin".into()]);
    }
    for private in [
        ".git",
        ".codex",
        ".claude",
        ".ssh",
        ".aws",
        ".agents",
        ".env",
        ".env.*",
        "node_modules",
    ] {
        args.extend([
            "--glob".into(),
            format!("!{private}/**"),
            "--glob".into(),
            format!("!{private}"),
        ]);
    }
    if mode != "content" {
        args.extend([
            "--files".into(),
            "--null".into(),
            "--".into(),
            "/workspace".into(),
        ]);
    } else {
        // Count accepted AND matches in the collector, not single-keyword candidates.
        if !query.multiple() {
            args.extend(["--max-count".into(), "100".into()]);
        }
        args.extend([
            "--json".into(),
            "--multiline".into(),
            "--fixed-strings".into(),
            "--ignore-case".into(),
            "--".into(),
            query.anchor().into(),
            "/workspace".into(),
        ]);
    }
    args
}
async fn collect(
    mut prepared: Prepared,
    query: &KeywordQuery,
    mode: &str,
    directory: &str,
    scope: Option<String>,
) -> Result<Value, Error> {
    prepared.command.stderr(Stdio::piped());
    if scope.is_some() {
        prepared.command.stdin(Stdio::piped());
    }
    let mut child = prepared.command.spawn()?;
    if let Some(rules) = scope {
        let mut stdin = child.stdin.take().ok_or_else(Error::internal)?;
        stdin.write_all(rules.as_bytes()).await?;
    }
    let stderr = child.stderr.take().ok_or_else(Error::internal)?;
    let diagnostic = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stderr
            .take(65537)
            .read_to_end(&mut bytes)
            .await
            .map(|_| bytes.is_empty())
    });
    let mut stdout = child.stdout.take().ok_or_else(Error::internal)?;
    let mut bytes = Vec::new();
    let mut matches = Vec::new();
    let mut scanned = 0usize;
    let delimiter = if mode == "content" { b'\n' } else { 0 };
    let mut chunk = [0u8; 8192];
    let mut truncated = false;
    loop {
        let count = stdout.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        scanned += count;
        bytes.extend_from_slice(&chunk[..count]);
        while let Some(end) = bytes.iter().position(|byte| *byte == delimiter) {
            let record: Vec<_> = bytes.drain(..=end).collect();
            matches.extend(
                parse(&record, query, mode)?
                    .into_iter()
                    .filter(|row| in_directory(row, directory)),
            );
            if matches.len() > MAX_MATCHES {
                break;
            }
        }
        if scanned > 16 * 1024 * 1024 || bytes.len() > MAX_BYTES || matches.len() > MAX_MATCHES {
            truncated = true;
            child.kill().await?;
            break;
        }
    }
    let status = child.wait().await?;
    let clean = diagnostic.await.map_err(|_| Error::internal())??;
    if !truncated && !clean {
        return Err(Error::unavailable());
    }
    if !truncated && !matches!(status.code(), Some(0 | 1)) {
        return Err(Error::unavailable());
    }
    truncated |= matches.len() > MAX_MATCHES;
    matches.truncate(MAX_MATCHES);
    Ok(json!({"matches":matches,"truncated":truncated,"nextCursor":null}))
}
fn parse(bytes: &[u8], query: &KeywordQuery, mode: &str) -> Result<Vec<Value>, Error> {
    if mode != "content" {
        return Ok(bytes
            .split_inclusive(|b| *b == 0)
            .filter(|raw| raw.last() == Some(&0))
            .map(|raw| &raw[..raw.len() - 1])
            .filter_map(|raw| std::str::from_utf8(raw).ok())
            .filter_map(|path| path.strip_prefix("/workspace/"))
            .filter(|path| {
                let candidate = if mode == "name" {
                    path.rsplit('/').next().unwrap_or("")
                } else {
                    path
                };
                query.matches(candidate)
            })
            .map(|path| json!({"path":path,"summary":path}))
            .collect());
    }
    let mut matches = Vec::new();
    for line in bytes.split(|b| *b == b'\n') {
        let Ok(record) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if record["type"] != "match" {
            continue;
        }
        let data = &record["data"];
        let path = data["path"]["text"]
            .as_str()
            .and_then(|p| p.strip_prefix("/workspace/"))
            .ok_or_else(Error::invalid)?;
        let text = data["lines"]["text"].as_str().unwrap_or("");
        if text.contains('\0') || !query.matches(text) {
            continue;
        }
        matches.push(json!({"path":path,"line":data["line_number"],"summary":text.chars().take(500).collect::<String>()}));
    }
    Ok(matches)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoped_walk_rules_keep_ancestor_ignore_precedence_and_literal_paths() {
        assert_eq!(
            scope_rules("selected/deep").unwrap(),
            "/*\n!/selected/\n/selected/*\n!/selected/deep/\n"
        );
        assert_eq!(scope_rules("[ab] ").unwrap(), "/*\n!/\\[ab\\]\\ /\n");
        for directory in [
            "",
            ".hidden",
            "parent/.hidden",
            "line\nbreak",
            "line\rbreak",
        ] {
            assert!(scope_rules(directory).is_none());
        }
        let args = arguments(&KeywordQuery::new("needle"), "content", true);
        let ignore = args.iter().position(|arg| arg == "--ignore-file").unwrap();
        assert_eq!(args[ignore + 1], "/dev/stdin");
    }
    #[test]
    fn literal_arguments_and_newline_paths_are_structured() {
        let args = arguments(&KeywordQuery::new("-e $(secret)"), "content", false);
        let index = args.iter().position(|arg| arg == "--").unwrap();
        assert_eq!(args[index + 1], "$(secret)");
        assert!(!args.iter().any(|arg| arg == "--max-count"));
        assert!(!args.iter().any(|arg| arg == "--follow"));
        let rows = parse(
            b"/workspace/a\nb\0/workspace/other\0",
            &KeywordQuery::new("a\nb"),
            "path",
        )
        .unwrap();
        assert_eq!(rows[0]["path"], "a\nb");
        assert!(
            parse(b"/workspace/partial", &KeywordQuery::new("partial"), "path")
                .unwrap()
                .is_empty()
        );
    }
}
