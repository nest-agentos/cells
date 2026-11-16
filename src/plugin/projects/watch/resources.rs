//! Admit the existing bounded watch capacity without exhausting ordinary file requests.
use crate::core::events::{emit, Level};
use serde_json::json;

pub(in crate::plugin::projects) fn prepare() {
    let started = std::time::Instant::now();
    let required = (super::MAX_SUBSCRIPTIONS * super::tree::MAX_DIRECTORIES * 2 + 4096) as libc::rlim_t;
    let result = configure(required);
    let (before, current, hard, errno) = match result {
        Ok((before, current, hard)) => (Some(before), Some(current), Some(hard), None),
        Err(error) => (None, None, None, error.raw_os_error()),
    };
    let enough = current.is_some_and(|limit| limit >= required);
    emit(if enough { Level::Info } else { Level::Warn }, "project.watch.capacity",
        json!({"operation":"project.watch.capacity","stage":"process.nofile",
            "result":if enough {"success"}else{"limited"},
            "code":if enough {None}else{Some("PROJECT_WATCH_FD_LIMIT_INSUFFICIENT")},
            "previousLimit":before,"softLimit":current,"hardLimit":hard,"requiredLimit":required,"errno":errno,
            "maxSubscriptions":super::MAX_SUBSCRIPTIONS,"maxDirectories":super::tree::MAX_DIRECTORIES,
            "durationMs":started.elapsed().as_millis()}));
}

fn configure(required: libc::rlim_t) -> std::io::Result<(libc::rlim_t, libc::rlim_t, libc::rlim_t)> {
    let mut limits = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // Raising only the soft limit stays inside the administrator's hard ceiling.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let previous = limits.rlim_cur;
    limits.rlim_cur = previous.max(required.min(limits.rlim_max));
    if limits.rlim_cur != previous && unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limits) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((previous, limits.rlim_cur, limits.rlim_max))
}
