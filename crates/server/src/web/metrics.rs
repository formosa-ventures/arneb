//! `GET /metrics`: Prometheus text exposition (Phase 3 P5).

use arneb_common::metrics::{render, Gauge};
use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;

use super::WebState;

/// Query counters and latency (recorded by the protocol layer) plus gauges
/// read now: admission queue, memory pool and workers.
pub(super) async fn metrics(State(state): State<WebState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render(&gauges(&state)),
    )
}

fn gauges(state: &WebState) -> Vec<Gauge> {
    let mut gauges = Vec::new();
    if let Some(admission) = &state.admission {
        let a = admission.snapshot();
        gauges.push(Gauge::single(
            "arneb_admission_running",
            "Queries holding an admission slot.",
            a.running as f64,
        ));
        gauges.push(Gauge::single(
            "arneb_admission_queued",
            "Queries waiting for an admission slot.",
            a.queued as f64,
        ));
    }
    gauges.push(Gauge::single(
        "arneb_memory_pool_reserved_bytes",
        "Bytes reserved in this node's memory pool.",
        state.memory_pool.reserved() as f64,
    ));
    gauges.push(Gauge::single(
        "arneb_memory_pool_peak_bytes",
        "High-water mark of this node's memory pool.",
        state.memory_pool.reserved_peak() as f64,
    ));
    if let Some(limit) = state.memory_limit_bytes {
        gauges.push(Gauge::single(
            "arneb_memory_pool_limit_bytes",
            "This node's memory pool budget.",
            limit as f64,
        ));
    }
    let workers = state.node_registry.all_workers();
    let alive = workers.iter().filter(|w| w.alive).count();
    gauges.push(Gauge {
        name: "arneb_workers",
        help: "Registered workers, by state.",
        samples: vec![
            (vec![("state", "alive".to_string())], alive as f64),
            (
                vec![("state", "dead".to_string())],
                (workers.len() - alive) as f64,
            ),
        ],
    });
    if !workers.is_empty() {
        gauges.push(Gauge {
            name: "arneb_worker_up",
            help: "1 while a worker heartbeats within the timeout.",
            samples: workers
                .iter()
                .map(|w| {
                    (
                        vec![("worker_id", w.worker_id.clone())],
                        if w.alive { 1.0 } else { 0.0 },
                    )
                })
                .collect(),
        });
        gauges.push(Gauge {
            name: "arneb_worker_heartbeat_age_seconds",
            help: "Seconds since a worker's last heartbeat.",
            samples: workers
                .iter()
                .map(|w| {
                    (
                        vec![("worker_id", w.worker_id.clone())],
                        w.last_heartbeat.elapsed().as_secs_f64(),
                    )
                })
                .collect(),
        });
    }
    gauges.push(Gauge::single(
        "arneb_uptime_seconds",
        "Seconds since this process started.",
        state.start_time.elapsed().as_secs_f64(),
    ));
    gauges
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn metrics_report_workers_admission_and_memory() {
        let registry = arneb_scheduler::NodeRegistry::new(30);
        registry.heartbeat("worker-1".into(), "http://w1:9090".into(), 256);
        let pool: Arc<dyn arneb_common::memory_pool::MemoryPool> =
            Arc::new(arneb_common::memory_pool::GreedyMemoryPool::new(1 << 20));
        let state = WebState {
            query_tracker: Arc::new(arneb_scheduler::QueryTracker::new()),
            node_registry: registry,
            start_time: std::time::Instant::now(),
            role: "coordinator".into(),
            admission: Some(arneb_scheduler::AdmissionController::new(4, 8)),
            max_queued_queries: 8,
            memory_pool: pool,
            memory_limit_bytes: Some(1 << 20),
            credentials: None,
            state: None,
            access_rules: None,
            metrics_auth: false,
        };
        let text = render(&gauges(&state));
        for line in [
            r#"arneb_workers{state="alive"} 1"#,
            r#"arneb_workers{state="dead"} 0"#,
            r#"arneb_worker_up{worker_id="worker-1"} 1"#,
            "arneb_admission_queued 0",
            "arneb_memory_pool_limit_bytes 1048576",
            "arneb_memory_pool_reserved_bytes 0",
            "# TYPE arneb_queries_total counter",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing `{line}` in\n{text}"
            );
        }
    }
}
