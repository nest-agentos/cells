//! Bounded system sampling; no shell commands, process arguments or file contents.
use serde_json::{json, Value};
use std::io::{BufRead, Read};

fn text(path: &str) -> Option<String> {
    let mut value = String::new();
    std::fs::File::open(path)
        .ok()?
        .take(65536)
        .read_to_string(&mut value)
        .ok()?;
    Some(value)
}
fn cpu_ticks() -> Option<(u64, u64)> {
    let mut line = String::new();
    std::io::BufReader::new(std::fs::File::open("/proc/stat").ok()?)
        .take(4096)
        .read_line(&mut line)
        .ok()?;
    let values = line
        .split_whitespace()
        .skip(1)
        .take(8)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    (values.len() >= 5).then(|| (values.iter().sum(), values[3] + values[4]))
}
fn memory(text: &str) -> Option<(u64, u64)> {
    let get = |key: &str| {
        text.lines()
            .find_map(|line| {
                line.strip_prefix(key)?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })?
            .checked_mul(1024)
    };
    Some((get("MemTotal:")?, get("MemAvailable:")?))
}
pub fn sample() -> Value {
    let before = cpu_ticks();
    std::thread::sleep(std::time::Duration::from_millis(100));
    let usage = before.zip(cpu_ticks()).and_then(|((a, ai), (b, bi))| {
        let total = b.checked_sub(a)?;
        let idle = bi.checked_sub(ai)?;
        (total > 0 && idle <= total).then_some(100.0 * (total - idle) as f64 / total as f64)
    });
    let load = text("/proc/loadavg").and_then(|s| {
        s.split_whitespace()
            .take(3)
            .map(str::parse::<f64>)
            .collect::<Result<Vec<_>, _>>()
            .ok()
    });
    let mem = text("/proc/meminfo").and_then(|s| memory(&s));
    json!({"scope":"system","sampledAt":chrono::Utc::now().timestamp_millis(),
        "cpu":load.map(|load|json!({"cores":std::thread::available_parallelism().map(|n|n.get()).unwrap_or(1),"usagePercent":usage,"loadAverage":load})),
        "memory":mem.map(|(total,available)|json!({"totalBytes":total,"availableBytes":available}))})
}

#[cfg(test)]
mod tests {
    #[test]
    fn memory_uses_available_memory_including_reclaimable_cache() {
        assert_eq!(
            super::memory("MemTotal: 1000 kB\nMemFree: 10 kB\nMemAvailable: 200 kB\n"),
            Some((1024000, 204800))
        );
        assert_eq!(super::memory("MemTotal: invalid kB\n"), None);
    }
}
