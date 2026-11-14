//! Bounded retries for explicitly read-only HTTP operations, including authorization.
use super::error::Error;
use std::future::Future;
use tokio::time::{Duration, Instant};

pub async fn retry<T, F: Future<Output = Result<T, Error>>>(
    deadline: Instant,
    mut work: impl FnMut() -> F,
) -> Result<T, Error> {
    let started = Instant::now();
    for retry in 0..=3 {
        let result = if Instant::now() >= deadline {
            Err(Error::timed_out())
        } else {
            tokio::time::timeout_at(deadline, work())
                .await
                .unwrap_or_else(|_| Err(Error::timed_out()))
        };
        if !matches!(&result, Err(error) if error.code == "EBUSY") || retry == 3 {
            if retry > 0 {
                report(started, retry, &result);
            }
            return result;
        }
        if tokio::time::timeout_at(
            deadline,
            tokio::time::sleep(Duration::from_millis(50 << retry)),
        )
        .await
        .is_err()
        {
            let result = Err(Error::timed_out());
            report(started, retry, &result);
            return result;
        }
    }
    unreachable!("bounded read attempts")
}

fn report<T>(started: Instant, retries: u32, result: &Result<T, Error>) {
    use super::events::{emit, Level};
    emit(
        if result.is_ok() {
            Level::Info
        } else {
            Level::Warn
        },
        "file.gate.read_retry",
        serde_json::json!({"operation":"file.gate.acquire","stage":"read-retry",
            "result":if result.is_ok(){"success"}else{"failure"},
            "code":result.as_ref().err().map(|error|error.code),"retryCount":retries,
            "durationMs":started.elapsed().as_millis()}),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::gate::FileGate;

    async fn read(
        gate: &FileGate,
        deadline: Instant,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, Error> {
        retry(deadline, || std::future::ready(gate.read_only())).await
    }

    #[tokio::test]
    async fn reads_do_not_wait_for_commands_but_topology_retries_are_bounded() {
        let gate = FileGate::new();
        let command = gate.command("independent", 1).await.unwrap();
        let reader = read(&gate, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(gate.shared().is_ok());
        drop((reader, command));
        let topology = gate.exclusive().unwrap();
        assert_eq!(
            read(&gate, Instant::now() + Duration::from_millis(10))
                .await
                .unwrap_err()
                .code,
            "ETIMEDOUT"
        );
        drop(topology);
        assert!(gate.shared().is_ok());
        gate.poison();
        assert_eq!(
            read(&gate, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap_err()
                .code,
            "EIO"
        );
    }
}
