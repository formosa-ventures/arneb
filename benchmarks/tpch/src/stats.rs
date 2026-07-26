//! Per-query statistics computed from measurement-run durations.

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    /// Sample stddev (N − 1). `None` when fewer than two measurements exist.
    pub stddev_ms: Option<f64>,
    pub measurement_count: usize,
}

impl QueryStats {
    pub fn from_durations(durations: &[Duration]) -> Option<Self> {
        if durations.is_empty() {
            return None;
        }
        let mut ms: Vec<f64> = durations.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
        ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = ms.len();
        let min = ms[0];
        let p50 = percentile_nearest_rank(&ms, 50);
        let p95 = percentile_nearest_rank(&ms, 95);
        let p99 = percentile_nearest_rank(&ms, 99);
        let stddev = if n >= 2 {
            let mean = ms.iter().sum::<f64>() / n as f64;
            let var = ms.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);
            Some(var.sqrt())
        } else {
            None
        };
        Some(Self {
            min_ms: min,
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
            stddev_ms: stddev,
            measurement_count: n,
        })
    }
}

/// Nearest-rank percentile. `pct` is in 0..=100.
fn percentile_nearest_rank(sorted: &[f64], pct: u32) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let rank = ((pct as f64 / 100.0) * n as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(n - 1);
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(ms: f64) -> Duration {
        Duration::from_secs_f64(ms / 1000.0)
    }

    #[test]
    fn single_sample_collapses_percentiles() {
        let s = QueryStats::from_durations(&[d(10.0)]).unwrap();
        assert_eq!(s.min_ms, 10.0);
        assert_eq!(s.p50_ms, 10.0);
        assert_eq!(s.p95_ms, 10.0);
        assert_eq!(s.p99_ms, 10.0);
        assert!(s.stddev_ms.is_none());
        assert_eq!(s.measurement_count, 1);
    }

    #[test]
    fn five_samples_percentiles() {
        // sorted: 10, 20, 30, 40, 50
        // p50 rank ceil(0.5*5)=3 → idx 2 → 30
        // p95 rank ceil(0.95*5)=5 → idx 4 → 50
        let s = QueryStats::from_durations(&[d(40.0), d(10.0), d(30.0), d(50.0), d(20.0)]).unwrap();
        assert_eq!(s.p50_ms, 30.0);
        assert_eq!(s.p95_ms, 50.0);
        assert_eq!(s.p99_ms, 50.0);
        assert!(s.stddev_ms.unwrap() > 0.0);
    }

    #[test]
    fn empty_returns_none() {
        assert!(QueryStats::from_durations(&[]).is_none());
    }
}
