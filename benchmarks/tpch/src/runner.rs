//! Multi-engine TPC-H runner. Iterates engine-major then query-major so that
//! one engine's connection state doesn't bleed into another engine's timings.

use std::path::PathBuf;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::correctness;
use crate::engines::{BenchmarkEngine, EngineError};
use crate::skip::SkipList;
use crate::stats::QueryStats;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub engine: String,
    pub host: String,
    pub port: Option<u16>,
    pub timestamp: String,
    pub warm_up: usize,
    pub measurement_runs: usize,
    pub queries: Vec<QueryResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub query_id: String,
    pub query_file: String,
    pub status: String,
    pub runs: Vec<RunResult>,
    pub median_ms: Option<f64>,
    pub error: Option<String>,
    /// New in this change. Old result files have this field absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<QueryStats>,
    /// New in this change. Old result files have this field absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_hash: Option<String>,
    /// Coarser-precision hash, used only to adjudicate a `result_hash` mismatch
    /// that is a rounding-boundary artifact rather than a real difference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_hash_relaxed: Option<String>,
    /// New in this change. Populated when an engine declared this query as skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub run_number: usize,
    pub wall_clock_ms: f64,
    pub rows_returned: usize,
    pub is_warmup: bool,
}

#[derive(Debug, Clone)]
pub struct LoadedQuery {
    pub query_id: String,
    pub path: PathBuf,
    pub sql: String,
}

pub struct RunPlan<'a> {
    pub queries: &'a [LoadedQuery],
    pub warm_up: usize,
    pub num_runs: usize,
    pub skip_list: &'a SkipList,
}

/// Run the plan against one engine. Caller decides ordering across engines.
pub async fn run_engine(engine: &mut dyn BenchmarkEngine, plan: &RunPlan<'_>) -> BenchmarkResult {
    let engine_name = engine.name().to_string();
    let host = engine.host();
    let port = engine.port();
    let timestamp = chrono::Utc::now().to_rfc3339();
    let measurement_runs = plan.num_runs.saturating_sub(plan.warm_up);

    println!("\n=== Engine: {engine_name} ===");
    let connect_result = engine.connect().await;
    let mut connect_error: Option<String> = None;
    if let Err(e) = connect_result {
        eprintln!("connect failed: {e}");
        connect_error = Some(e.to_string());
    }

    let mut query_results = Vec::with_capacity(plan.queries.len());

    for q in plan.queries {
        // Honor skip list.
        if let Some(entry) = plan.skip_list.lookup(&engine_name, &q.query_id) {
            println!("{}: SKIP ({})", q.query_id, entry.reason);
            query_results.push(QueryResult {
                query_id: q.query_id.clone(),
                query_file: q.path.display().to_string(),
                status: "skipped".into(),
                runs: vec![],
                median_ms: None,
                error: None,
                stats: None,
                result_hash: None,
                result_hash_relaxed: None,
                skip_reason: Some(entry.reason.to_string()),
            });
            continue;
        }

        // If connect failed earlier, every query is a failure.
        if let Some(ref err) = connect_error {
            query_results.push(QueryResult {
                query_id: q.query_id.clone(),
                query_file: q.path.display().to_string(),
                status: "failed".into(),
                runs: vec![],
                median_ms: None,
                error: Some(err.clone()),
                stats: None,
                result_hash: None,
                result_hash_relaxed: None,
                skip_reason: None,
            });
            continue;
        }

        print!("{}: ", q.query_id);
        let mut runs = Vec::new();
        let mut measured_durations = Vec::new();
        let mut hash: Option<String> = None;
        let mut hash_relaxed: Option<String> = None;
        let mut error_msg: Option<String> = None;

        for run_idx in 0..plan.num_runs {
            let is_warmup = run_idx < plan.warm_up;
            let started = Instant::now();
            match engine.execute(&q.sql).await {
                Ok(result) => {
                    let elapsed = result.elapsed;
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    runs.push(RunResult {
                        run_number: run_idx + 1,
                        wall_clock_ms: ms,
                        rows_returned: result.rows.len(),
                        is_warmup,
                    });
                    if is_warmup {
                        print!("w");
                    } else {
                        print!(".");
                        measured_durations.push(elapsed);
                        if hash.is_none() {
                            hash = Some(correctness::hash(&result.rows));
                            hash_relaxed = Some(correctness::hash_relaxed(&result.rows));
                        }
                    }
                }
                Err(e) => {
                    let msg = match e {
                        EngineError::Connect(m) => m,
                        EngineError::Query(m) => m,
                    };
                    println!(" FAIL ({msg})");
                    error_msg = Some(msg);
                    break;
                }
            }
            // Touch `started` so a future hook (e.g. wall-clock fallback) can use it.
            let _ = started;
        }

        if let Some(err) = error_msg {
            query_results.push(QueryResult {
                query_id: q.query_id.clone(),
                query_file: q.path.display().to_string(),
                status: "failed".into(),
                runs,
                median_ms: None,
                error: Some(err),
                stats: None,
                result_hash: hash,
                result_hash_relaxed: hash_relaxed,
                skip_reason: None,
            });
            continue;
        }

        let stats = QueryStats::from_durations(&measured_durations);
        let median = stats.as_ref().map(|s| s.p50_ms);
        match &stats {
            Some(s) => println!(" p50={:.1}ms p95={:.1}ms", s.p50_ms, s.p95_ms),
            None => println!(" (no measurements)"),
        }

        query_results.push(QueryResult {
            query_id: q.query_id.clone(),
            query_file: q.path.display().to_string(),
            status: "ok".into(),
            runs,
            median_ms: median,
            error: None,
            stats,
            result_hash: hash,
            result_hash_relaxed: hash_relaxed,
            skip_reason: None,
        });
    }

    BenchmarkResult {
        engine: engine_name,
        host,
        port,
        timestamp,
        warm_up: plan.warm_up,
        measurement_runs,
        queries: query_results,
    }
}
