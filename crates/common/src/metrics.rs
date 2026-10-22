//! Process-wide query metrics in the Prometheus text exposition format.
//!
//! Query lifecycle counters live here as atomics so that every protocol
//! (pgwire, Trino) records through one [`QueryObservation`] without passing a
//! registry around. Values that are cheap to read at scrape time (admission
//! queue, memory pool, workers) are not stored: the `/metrics` handler reads
//! them and hands them to [`render`] as [`Gauge`]s.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// How a query ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOutcome {
    /// Returned its result.
    Finished,
    /// Returned an error.
    Failed,
    /// Dropped before it ended: cancelled, timed out, or the client left.
    Cancelled,
}

impl QueryOutcome {
    const ALL: [QueryOutcome; 3] = [Self::Finished, Self::Failed, Self::Cancelled];

    fn label(self) -> &'static str {
        match self {
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Upper bounds (seconds) of the query-duration histogram buckets.
pub const DURATION_BUCKETS_SECS: [f64; 12] = [
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

struct Histogram {
    buckets: [AtomicU64; DURATION_BUCKETS_SECS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; DURATION_BUCKETS_SECS.len()],
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }

    fn observe(&self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        if let Some(i) = DURATION_BUCKETS_SECS.iter().position(|&b| secs <= b) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }
}

struct QueryMetrics {
    running: AtomicI64,
    outcomes: [AtomicU64; 3],
    durations: [Histogram; 3],
}

static QUERY_METRICS: QueryMetrics = QueryMetrics {
    running: AtomicI64::new(0),
    outcomes: [const { AtomicU64::new(0) }; 3],
    durations: [const { Histogram::new() }; 3],
};

/// Default slow-query threshold, in milliseconds.
pub const DEFAULT_SLOW_QUERY_THRESHOLD_MS: u64 = 10_000;

/// The slow-query threshold in milliseconds; `0` = log off.
static SLOW_QUERY_THRESHOLD_MS: AtomicU64 = AtomicU64::new(DEFAULT_SLOW_QUERY_THRESHOLD_MS);

/// Set the slow-query threshold; `None` turns the slow-query log off. The
/// server sets it once at startup from `[observability]
/// slow_query_threshold_ms`.
pub fn set_slow_query_threshold(threshold: Option<Duration>) {
    let ms = threshold.map_or(0, |d| d.as_millis().max(1) as u64);
    SLOW_QUERY_THRESHOLD_MS.store(ms, Ordering::Relaxed);
}

/// The slow-query threshold; `None` when the log is off.
pub fn slow_query_threshold() -> Option<Duration> {
    let ms = SLOW_QUERY_THRESHOLD_MS.load(Ordering::Relaxed);
    (ms > 0).then(|| Duration::from_millis(ms))
}

/// Longest SQL text the slow-query log records.
const SLOW_QUERY_SQL_CHARS: usize = 2000;

/// One query's lifetime: counted as running from [`start`](Self::start),
/// recorded with its outcome and duration when it [`finish`](Self::finish)es,
/// or as cancelled when dropped first.
pub struct QueryObservation {
    started: Instant,
    sql: Option<String>,
    done: bool,
}

impl QueryObservation {
    /// A query begins. `sql` is kept only if the slow-query log is on.
    pub fn start(sql: &str) -> Self {
        QUERY_METRICS.running.fetch_add(1, Ordering::Relaxed);
        Self {
            started: Instant::now(),
            sql: slow_query_threshold().map(|_| sql.chars().take(SLOW_QUERY_SQL_CHARS).collect()),
            done: false,
        }
    }

    /// The query ended with `outcome`; `rows` and `error` are logged when it
    /// was slow.
    pub fn finish(mut self, outcome: QueryOutcome, rows: Option<usize>, error: Option<&str>) {
        self.record(outcome, rows, error);
    }

    fn record(&mut self, outcome: QueryOutcome, rows: Option<usize>, error: Option<&str>) {
        if self.done {
            return;
        }
        self.done = true;
        let elapsed = self.started.elapsed();
        QUERY_METRICS.running.fetch_sub(1, Ordering::Relaxed);
        QUERY_METRICS.outcomes[outcome.index()].fetch_add(1, Ordering::Relaxed);
        QUERY_METRICS.durations[outcome.index()].observe(elapsed);
        if let Some(threshold) = slow_query_threshold() {
            if elapsed >= threshold {
                tracing::warn!(
                    target: "arneb::slow_query",
                    duration_ms = elapsed.as_millis() as u64,
                    outcome = outcome.label(),
                    rows,
                    error,
                    sql = self.sql.as_deref().unwrap_or(""),
                    "slow query"
                );
            }
        }
    }
}

impl Drop for QueryObservation {
    fn drop(&mut self) {
        self.record(QueryOutcome::Cancelled, None, None);
    }
}

/// A value read at scrape time, rendered as a Prometheus gauge.
#[derive(Debug, Clone)]
pub struct Gauge {
    /// Metric name (`arneb_…`).
    pub name: &'static str,
    /// `# HELP` text.
    pub help: &'static str,
    /// `(labels, value)` samples; labels as `key="value"` pairs.
    pub samples: Vec<(Vec<(&'static str, String)>, f64)>,
}

impl Gauge {
    /// A gauge with one unlabelled sample.
    pub fn single(name: &'static str, help: &'static str, value: f64) -> Self {
        Self {
            name,
            help,
            samples: vec![(Vec::new(), value)],
        }
    }
}

fn label_set(labels: &[(&str, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let inner: Vec<String> = labels
        .iter()
        .map(|(k, v)| {
            let v = v
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            format!("{k}=\"{v}\"")
        })
        .collect();
    format!("{{{}}}", inner.join(","))
}

fn header(out: &mut String, name: &str, help: &str, kind: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

/// The query metrics plus `gauges`, in the Prometheus text format (0.0.4).
pub fn render(gauges: &[Gauge]) -> String {
    let m = &QUERY_METRICS;
    let mut out = String::new();
    header(&mut out, "arneb_build_info", "Arneb build.", "gauge");
    let _ = writeln!(
        out,
        "arneb_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );
    header(
        &mut out,
        "arneb_queries_running",
        "Queries executing now.",
        "gauge",
    );
    let _ = writeln!(
        out,
        "arneb_queries_running {}",
        m.running.load(Ordering::Relaxed).max(0)
    );
    header(
        &mut out,
        "arneb_queries_total",
        "Queries that ended, by outcome.",
        "counter",
    );
    for o in QueryOutcome::ALL {
        let _ = writeln!(
            out,
            "arneb_queries_total{{outcome=\"{}\"}} {}",
            o.label(),
            m.outcomes[o.index()].load(Ordering::Relaxed)
        );
    }
    header(
        &mut out,
        "arneb_query_duration_seconds",
        "Query wall time, by outcome.",
        "histogram",
    );
    for o in QueryOutcome::ALL {
        let outcome = o.label();
        let h = &m.durations[o.index()];
        let mut cumulative = 0;
        for (bound, bucket) in DURATION_BUCKETS_SECS.iter().zip(&h.buckets) {
            cumulative += bucket.load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "arneb_query_duration_seconds_bucket{{outcome=\"{outcome}\",le=\"{bound}\"}} {cumulative}"
            );
        }
        let count = h.count.load(Ordering::Relaxed);
        let sum = h.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(
            out,
            "arneb_query_duration_seconds_bucket{{outcome=\"{outcome}\",le=\"+Inf\"}} {count}\n\
             arneb_query_duration_seconds_sum{{outcome=\"{outcome}\"}} {sum}\n\
             arneb_query_duration_seconds_count{{outcome=\"{outcome}\"}} {count}"
        );
    }
    for g in gauges {
        header(&mut out, g.name, g.help, "gauge");
        for (labels, value) in &g.samples {
            let _ = writeln!(out, "{}{} {value}", g.name, label_set(labels));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(text: &str, series: &str) -> f64 {
        text.lines()
            .find_map(|l| l.strip_prefix(series).and_then(|r| r.strip_prefix(' ')))
            .unwrap_or_else(|| panic!("{series} missing in\n{text}"))
            .parse()
            .unwrap()
    }

    // The registry is process-wide, so one test drives every path and
    // asserts deltas.
    #[test]
    fn observations_are_counted_by_outcome_and_rendered() {
        let before = render(&[]);
        let finished = r#"arneb_queries_total{outcome="finished"}"#;
        let failed = r#"arneb_queries_total{outcome="failed"}"#;
        let cancelled = r#"arneb_queries_total{outcome="cancelled"}"#;
        let fin_count = r#"arneb_query_duration_seconds_count{outcome="finished"}"#;
        let fin_inf = r#"arneb_query_duration_seconds_bucket{outcome="finished",le="+Inf"}"#;

        let running = QueryObservation::start("SELECT 1");
        let mid = render(&[]);
        assert!(sample(&mid, "arneb_queries_running") >= 1.0);
        running.finish(QueryOutcome::Finished, Some(1), None);
        QueryObservation::start("SELECT x").finish(QueryOutcome::Failed, None, Some("no x"));
        drop(QueryObservation::start("SELECT slow")); // dropped = cancelled

        let after = render(&[Gauge {
            name: "arneb_workers",
            help: "Workers, by state.",
            samples: vec![
                (vec![("state", "alive".to_string())], 2.0),
                (vec![("state", "dead".to_string())], 0.0),
            ],
        }]);
        for (series, delta) in [
            (finished, 1.0),
            (failed, 1.0),
            (cancelled, 1.0),
            (fin_count, 1.0),
            (fin_inf, 1.0),
        ] {
            assert_eq!(
                sample(&after, series) - sample(&before, series),
                delta,
                "{series}"
            );
        }
        assert!(after.contains("# TYPE arneb_query_duration_seconds histogram"));
        assert!(
            after.contains(r#"arneb_workers{state="alive"} 2"#),
            "{after}"
        );
        assert!(after.contains("arneb_build_info{version="));
    }

    #[test]
    fn label_values_are_escaped() {
        assert_eq!(
            label_set(&[("worker_id", "a\"b\\c".to_string())]),
            r#"{worker_id="a\"b\\c"}"#
        );
    }
}
