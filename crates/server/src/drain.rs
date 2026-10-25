//! Shutdown signals and the worker drain (worker-graceful-drain).
//!
//! On SIGTERM or Ctrl-C a worker reports `draining`, keeps running its
//! tasks, and exits once the coordinator has released or cancelled every
//! query it holds, or once the drain timeout passes.

use std::time::Duration;

/// Resolves on SIGTERM or Ctrl-C (Ctrl-C only off Unix).
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGTERM; only Ctrl-C stops the server");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// How the wait in [`wait_until_idle`] ended.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// No query left on the node.
    Idle,
    /// The timeout passed with this many queries still on the node.
    TimedOut(usize),
}

/// Wait `settle` (so tasks of a query planned just before the coordinator
/// saw the drain are tracked), then poll `active_queries` every `poll`
/// until it is 0 or `timeout` (counted from the start) passes.
pub async fn wait_until_idle(
    active_queries: impl Fn() -> usize,
    settle: Duration,
    timeout: Duration,
    poll: Duration,
) -> DrainOutcome {
    let drained = tokio::time::timeout(timeout, async {
        tokio::time::sleep(settle).await;
        while active_queries() > 0 {
            tokio::time::sleep(poll).await;
        }
    })
    .await;
    match (drained, active_queries()) {
        (Ok(()), _) | (Err(_), 0) => DrainOutcome::Idle,
        (Err(_), left) => DrainOutcome::TimedOut(left),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn returns_once_the_last_query_is_released() {
        let active = Arc::new(AtomicUsize::new(2));
        let a = active.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            a.store(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(3)).await;
            a.store(0, Ordering::SeqCst);
        });
        let start = tokio::time::Instant::now();
        let outcome = wait_until_idle(
            || active.load(Ordering::SeqCst),
            Duration::from_secs(2),
            Duration::from_secs(60),
            Duration::from_millis(500),
        )
        .await;
        assert_eq!(outcome, DrainOutcome::Idle);
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_secs(6) && waited < Duration::from_secs(7),
            "{waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_node_still_waits_the_settle_time() {
        let start = tokio::time::Instant::now();
        let outcome = wait_until_idle(
            || 0,
            Duration::from_secs(2),
            Duration::from_secs(60),
            Duration::from_millis(500),
        )
        .await;
        assert_eq!(outcome, DrainOutcome::Idle);
        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_at_the_timeout() {
        let start = tokio::time::Instant::now();
        let outcome = wait_until_idle(
            || 3,
            Duration::from_secs(2),
            Duration::from_secs(10),
            Duration::from_millis(500),
        )
        .await;
        assert_eq!(outcome, DrainOutcome::TimedOut(3));
        assert_eq!(start.elapsed(), Duration::from_secs(10));
    }
}
