//! #245: coordinator-side liveness probe for silent workers.
//!
//! A worker past its heartbeat deadline is only *suspect* (see
//! [`NodeRegistry::enable_liveness_probe`]). The prober pings each suspect
//! worker's Flight server: an answer keeps the worker, no answer makes it
//! lost and fails its queries. It runs on its own thread and runtime, so a
//! coordinator whose execution runtime is saturated, and so reads
//! heartbeats late, still gets a timely verdict instead of failing queries
//! on healthy workers.

use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;

use arneb_scheduler::NodeRegistry;

/// How often the prober looks for suspect workers.
const PROBE_TICK: Duration = Duration::from_millis(500);

/// How long a probe waits for an answer: half the heartbeat timeout, at
/// least 1 s. A hung worker is lost about 1.5 heartbeat timeouts after its
/// last heartbeat.
pub(crate) fn probe_timeout(heartbeat_timeout: Duration) -> Duration {
    (heartbeat_timeout / 2).max(Duration::from_secs(1))
}

/// Turn on probing in `registry` and start the `worker-probe` thread.
pub(crate) fn start(registry: NodeRegistry) {
    let timeout = probe_timeout(registry.heartbeat_timeout());
    registry.enable_liveness_probe();
    let spawned = std::thread::Builder::new()
        .name("worker-probe".into())
        .spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(probe_loop(
                    registry,
                    PROBE_TICK,
                    move |address| async move { arneb_rpc::ping(&address, timeout).await },
                )),
                Err(e) => tracing::error!(error = %e, "failed to start the worker probe runtime"),
            }
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "failed to start the worker probe thread");
    }
}

/// Probe every suspect worker in `registry`, at most one probe per worker
/// at a time, and record each verdict. Never returns.
pub(crate) async fn probe_loop<P, F>(registry: NodeRegistry, tick: Duration, probe: P)
where
    P: Fn(String) -> F,
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let mut probing: HashSet<String> = HashSet::new();
    let mut verdicts = tokio::task::JoinSet::new();
    loop {
        for (worker_id, address) in registry.suspects() {
            if probing.insert(worker_id.clone()) {
                tracing::debug!(target: "arneb::cluster", worker_id = %worker_id, "probing suspect worker");
                let call = probe(address);
                verdicts.spawn(async move { (worker_id, call.await) });
            }
        }
        tokio::select! {
            Some(done) = verdicts.join_next() => {
                if let Ok((worker_id, verdict)) = done {
                    probing.remove(&worker_id);
                    match verdict {
                        Ok(()) => registry.probe_answered(&worker_id),
                        Err(e) => registry.probe_failed(&worker_id, &e),
                    }
                }
            }
            _ = tokio::time::sleep(tick) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn silence(registry: &NodeRegistry, id: &str) {
        registry.heartbeat(id.into(), format!("{id}:9090"), 256);
        // Let the heartbeat age past the 1 s timeout.
        std::thread::sleep(Duration::from_millis(1100));
    }

    async fn run_until<F: Fn() -> bool>(
        registry: NodeRegistry,
        probe_ok: bool,
        done: F,
    ) -> Vec<String> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        async move {
            let prober = tokio::spawn(probe_loop(
                registry,
                Duration::from_millis(10),
                move |address: String| {
                    seen.lock().unwrap().push(address);
                    async move {
                        if probe_ok {
                            Ok(())
                        } else {
                            Err("no answer".to_string())
                        }
                    }
                },
            ));
            for _ in 0..200 {
                if done() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            prober.abort();
        }
        .await;
        let calls = calls.lock().unwrap().clone();
        calls
    }

    #[tokio::test]
    async fn an_answering_suspect_is_kept() {
        let registry = NodeRegistry::new(1);
        registry.enable_liveness_probe();
        silence(&registry, "w1");
        assert_eq!(registry.suspects().len(), 1);
        let r = registry.clone();
        let calls = run_until(registry.clone(), true, move || r.suspects().is_empty()).await;
        assert_eq!(calls, vec!["w1:9090"]);
        assert_eq!(registry.lost_worker(&["w1".into()]), None);
        assert_eq!(registry.active_workers().len(), 1);
    }

    #[tokio::test]
    async fn a_silent_suspect_is_lost() {
        let registry = NodeRegistry::new(1);
        registry.enable_liveness_probe();
        silence(&registry, "w1");
        let r = registry.clone();
        let calls = run_until(registry.clone(), false, move || {
            r.lost_worker(&["w1".into()]).is_some()
        })
        .await;
        assert_eq!(calls, vec!["w1:9090"]);
        assert!(registry.lost_worker(&["w1".into()]).is_some());
    }

    #[test]
    fn probe_timeout_is_half_the_heartbeat_timeout_at_least_a_second() {
        assert_eq!(
            probe_timeout(Duration::from_secs(10)),
            Duration::from_secs(5)
        );
        assert_eq!(
            probe_timeout(Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }
}
