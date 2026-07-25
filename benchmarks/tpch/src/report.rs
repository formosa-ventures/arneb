//! Markdown comparison report.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::{Path, PathBuf};

use crate::runner::{BenchmarkResult, QueryResult};

/// Engines we render in the per-query table, in display order.
const RENDER_ORDER: &[&str] = &["arneb", "trino", "datafusion"];

pub fn render(results: &[BenchmarkResult]) -> String {
    let mut out = String::new();
    write_header(&mut out, results);
    write_per_query_table(&mut out, results);
    write_summary(&mut out, results);
    write_divergence(&mut out, results);
    out
}

fn write_header(out: &mut String, results: &[BenchmarkResult]) {
    let _ = writeln!(out, "# TPC-H Comparison Report");
    let _ = writeln!(out);
    if results.is_empty() {
        let _ = writeln!(out, "_No result files found._");
        return;
    }
    let _ = writeln!(out, "**Engines:** {}", engines_present(results).join(", "));
    let _ = writeln!(out, "**Generated:** {}", chrono::Utc::now().to_rfc3339());

    // Pull warmup/measurement info from first result; warn if not uniform.
    let warm_up = results[0].warm_up;
    let measurement_runs = results[0].measurement_runs;
    let uniform = results
        .iter()
        .all(|r| r.warm_up == warm_up && r.measurement_runs == measurement_runs);
    if uniform {
        let _ = writeln!(
            out,
            "**Run plan:** {} warmup + {} measurement runs per query",
            warm_up, measurement_runs
        );
    } else {
        let _ = writeln!(
            out,
            "**Run plan:** mixed across input files — see each result file for details"
        );
    }
    if measurement_runs < 20 {
        let _ = writeln!(
            out,
            "\n> **Note.** p95/p99 are heuristic at this sample count ({} measurement runs). \
             Increase `--num-runs` for a tighter estimate.",
            measurement_runs
        );
    }
    let _ = writeln!(out);
}

fn engines_present(results: &[BenchmarkResult]) -> Vec<String> {
    let mut seen = Vec::new();
    for r in results {
        if !seen.iter().any(|n: &String| n == &r.engine) {
            seen.push(r.engine.clone());
        }
    }
    seen
}

fn write_per_query_table(out: &mut String, results: &[BenchmarkResult]) {
    let engines = ordered_engines(results);
    if engines.is_empty() {
        return;
    }

    // Collect every query id mentioned by any result.
    let mut query_ids: BTreeMap<String, ()> = BTreeMap::new();
    for r in results {
        for q in &r.queries {
            query_ids.insert(q.query_id.clone(), ());
        }
    }

    let _ = writeln!(out, "## Per-query latency (p50)");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster."
    );
    let _ = writeln!(out);

    let mut header = String::from("| Query | Status |");
    for e in &engines {
        let _ = write!(header, " {} (ms) |", e);
    }
    if engines.len() >= 2 {
        // Pairwise speedup columns, one per engine pair. `A→B` is A's speedup
        // over B — B's time divided by A's — so a value above 1 always means the
        // left-hand engine is that many times faster.
        for i in 0..engines.len() {
            for j in (i + 1)..engines.len() {
                let _ = write!(header, " {}→{} |", engines[i], engines[j]);
            }
        }
    }
    let _ = writeln!(out, "{header}");

    let mut sep = String::from("|---|---|");
    for _ in &engines {
        sep.push_str("---:|");
    }
    if engines.len() >= 2 {
        for i in 0..engines.len() {
            for _j in (i + 1)..engines.len() {
                sep.push_str("---:|");
            }
        }
    }
    let _ = writeln!(out, "{sep}");

    for q in query_ids.keys() {
        let mut line = format!("| {} |", q);
        // Status: combined per engine.
        let statuses: Vec<String> = engines.iter().map(|e| status_for(results, e, q)).collect();
        let summary_status = if statuses.iter().all(|s| s == "ok") {
            "ok".to_string()
        } else {
            statuses.join("/")
        };
        let _ = write!(line, " {} |", summary_status);
        // p50 columns.
        let p50s: Vec<Option<f64>> = engines.iter().map(|e| p50_for(results, e, q)).collect();
        for p in &p50s {
            match p {
                Some(v) => {
                    let _ = write!(line, " {:.1} |", v);
                }
                None => {
                    let _ = write!(line, " - |");
                }
            }
        }
        // Speedup columns.
        if engines.len() >= 2 {
            for i in 0..engines.len() {
                for j in (i + 1)..engines.len() {
                    // Speedup of `left` over `right`: how many times faster the
                    // left engine is, so >1 always means left wins. Dividing the
                    // other way round produces the reciprocal, which reads as
                    // the exact opposite of what it means.
                    let speed = match (p50s[i], p50s[j]) {
                        (Some(left), Some(right)) if left > 0.0 => Some(right / left),
                        _ => None,
                    };
                    match speed {
                        Some(s) => {
                            let _ = write!(line, " {:.2}x |", s);
                        }
                        None => {
                            let _ = write!(line, " - |");
                        }
                    }
                }
            }
        }
        let _ = writeln!(out, "{line}");
    }
    let _ = writeln!(out);
}

fn ordered_engines(results: &[BenchmarkResult]) -> Vec<String> {
    let present = engines_present(results);
    let mut ordered: Vec<String> = RENDER_ORDER
        .iter()
        .filter(|e| present.iter().any(|p| p == *e))
        .map(|s| s.to_string())
        .collect();
    // Append any engines not in RENDER_ORDER (e.g., a future engine).
    for e in &present {
        if !ordered.iter().any(|o| o == e) {
            ordered.push(e.clone());
        }
    }
    ordered
}

fn p50_for(results: &[BenchmarkResult], engine: &str, query_id: &str) -> Option<f64> {
    find_query(results, engine, query_id).and_then(|q| {
        if q.status != "ok" {
            return None;
        }
        q.stats.as_ref().map(|s| s.p50_ms).or(q.median_ms)
    })
}

fn status_for(results: &[BenchmarkResult], engine: &str, query_id: &str) -> String {
    match find_query(results, engine, query_id) {
        Some(q) => q.status.clone(),
        None => "missing".into(),
    }
}

fn find_query<'a>(
    results: &'a [BenchmarkResult],
    engine: &str,
    query_id: &str,
) -> Option<&'a QueryResult> {
    for r in results {
        if r.engine == engine {
            if let Some(q) = r.queries.iter().find(|q| q.query_id == query_id) {
                return Some(q);
            }
        }
    }
    None
}

fn write_summary(out: &mut String, results: &[BenchmarkResult]) {
    let engines = ordered_engines(results);
    if engines.is_empty() {
        return;
    }
    let _ = writeln!(out, "## Suite summary");
    let _ = writeln!(out);
    let _ = writeln!(out, "| Engine | OK | Failed | Skipped | Geomean p50 (ms) |");
    let _ = writeln!(out, "|---|---:|---:|---:|---:|");
    for e in &engines {
        let mut ok = 0u32;
        let mut failed = 0u32;
        let mut skipped = 0u32;
        let mut p50s: Vec<f64> = Vec::new();
        for r in results.iter().filter(|r| r.engine == *e) {
            for q in &r.queries {
                match q.status.as_str() {
                    "ok" => {
                        ok += 1;
                        if let Some(s) = q.stats.as_ref().map(|s| s.p50_ms).or(q.median_ms) {
                            p50s.push(s);
                        }
                    }
                    "failed" => failed += 1,
                    "skipped" => skipped += 1,
                    _ => {}
                }
            }
        }
        let geomean = geomean(&p50s);
        let geomean_s = geomean
            .map(|g| format!("{g:.1}"))
            .unwrap_or_else(|| "-".into());
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} |",
            e, ok, failed, skipped, geomean_s
        );
    }
    let _ = writeln!(out);

    if engines.len() >= 2 {
        let _ = writeln!(out, "### Pairwise geomean speedup");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Pair | Geomean |");
        let _ = writeln!(out, "|---|---:|");
        for i in 0..engines.len() {
            for j in (i + 1)..engines.len() {
                let mut ratios = Vec::new();
                let query_ids: Vec<String> = results
                    .iter()
                    .flat_map(|r| r.queries.iter().map(|q| q.query_id.clone()))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect();
                for qid in &query_ids {
                    let left = p50_for(results, &engines[i], qid);
                    let right = p50_for(results, &engines[j], qid);
                    if let (Some(l), Some(r)) = (left, right) {
                        if l > 0.0 {
                            // Same orientation as the per-query table: >1 means
                            // the left engine is that many times faster.
                            ratios.push(r / l);
                        }
                    }
                }
                let g = geomean(&ratios);
                let g_s = g.map(|v| format!("{v:.2}x")).unwrap_or_else(|| "-".into());
                let _ = writeln!(out, "| {} → {} | {} |", engines[i], engines[j], g_s);
            }
        }
        let _ = writeln!(out);
    }
}

fn geomean(vs: &[f64]) -> Option<f64> {
    if vs.is_empty() {
        return None;
    }
    let logsum: f64 = vs.iter().filter(|v| **v > 0.0).map(|v| v.ln()).sum();
    let n = vs.iter().filter(|v| **v > 0.0).count();
    if n == 0 {
        return None;
    }
    Some((logsum / n as f64).exp())
}

fn write_divergence(out: &mut String, results: &[BenchmarkResult]) {
    let engines = ordered_engines(results);
    if engines.len() < 2 {
        return;
    }
    type EngineEntry = (String, String, usize);
    type DivergentRow = (String, Vec<EngineEntry>);
    let mut divergent: Vec<DivergentRow> = Vec::new();
    let query_ids: Vec<String> = results
        .iter()
        .flat_map(|r| r.queries.iter().map(|q| q.query_id.clone()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    for qid in &query_ids {
        let mut entries: Vec<EngineEntry> = Vec::new();
        for e in &engines {
            if let Some(q) = find_query(results, e, qid) {
                if let Some(h) = &q.result_hash {
                    let row_count = q.runs.first().map(|r| r.rows_returned).unwrap_or(0);
                    entries.push((e.clone(), h.clone(), row_count));
                }
            }
        }
        // Need at least two engines with hashes to compare.
        if entries.len() >= 2 {
            let first_hash = &entries[0].1;
            if entries.iter().any(|(_, h, _)| h != first_hash) {
                divergent.push((qid.clone(), entries));
            }
        }
    }
    if divergent.is_empty() {
        return;
    }
    let _ = writeln!(out, "## Correctness divergences");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Queries below produced canonically-different result sets across engines."
    );
    let _ = writeln!(out);
    for (qid, entries) in &divergent {
        let _ = writeln!(out, "### {qid}");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Engine | Hash (first 8) | Rows |");
        let _ = writeln!(out, "|---|---|---:|");
        for (e, h, rows) in entries {
            let short = h.chars().take(8).collect::<String>();
            let _ = writeln!(out, "| {} | {} | {} |", e, short, rows);
        }
        let _ = writeln!(out);
    }
}

/// Load result JSONs. Either explicit paths or, in directory mode, the most
/// recently-modified file per engine label parsed from the filename prefix.
pub fn load_inputs(
    explicit_files: &[PathBuf],
    dir: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let mut paths = Vec::new();
    if !explicit_files.is_empty() {
        paths.extend(explicit_files.iter().cloned());
    }
    if let Some(d) = dir {
        if !d.is_dir() {
            return Err(format!("{} is not a directory", d.display()));
        }
        let mut by_engine: std::collections::HashMap<String, (PathBuf, std::time::SystemTime)> =
            std::collections::HashMap::new();
        for entry in std::fs::read_dir(d).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            // Filename pattern: `<engine>_<YYYYMMDD>_<HHMMSS>`.
            let engine = stem.split('_').next().unwrap_or("").to_string();
            if engine.is_empty() {
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            match by_engine.get(&engine) {
                Some((_, prev_mod)) if *prev_mod >= modified => {}
                _ => {
                    by_engine.insert(engine, (path, modified));
                }
            }
        }
        for (_, (path, _)) in by_engine {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        return Err("no input files found — pass result JSONs as args or use --dir".to_string());
    }
    let mut results = Vec::with_capacity(paths.len());
    for p in &paths {
        let bytes = std::fs::read(p).map_err(|e| format!("read {}: {e}", p.display()))?;
        let r: BenchmarkResult =
            serde_json::from_slice(&bytes).map_err(|e| format!("parse {}: {e}", p.display()))?;
        results.push(r);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::{QueryResult, RunResult};
    use crate::stats::QueryStats;

    fn result(engine: &str, query: &str, p50_ms: f64) -> BenchmarkResult {
        BenchmarkResult {
            engine: engine.to_string(),
            host: "test".into(),
            port: None,
            timestamp: "2026-07-26T00:00:00Z".into(),
            warm_up: 1,
            measurement_runs: 2,
            queries: vec![QueryResult {
                query_id: query.to_string(),
                query_file: format!("/queries/{query}.sql"),
                status: "ok".into(),
                runs: vec![RunResult {
                    run_number: 1,
                    wall_clock_ms: p50_ms,
                    rows_returned: 1,
                    is_warmup: false,
                }],
                median_ms: Some(p50_ms),
                error: None,
                stats: Some(QueryStats {
                    min_ms: p50_ms,
                    p50_ms,
                    p95_ms: p50_ms,
                    p99_ms: p50_ms,
                    stddev_ms: None,
                    measurement_count: 2,
                }),
                result_hash: Some("deadbeef".into()),
                skip_reason: None,
            }],
        }
    }

    /// `A→B` is A's speedup over B, so a faster left-hand engine must produce a
    /// value above 1. Dividing the other way round yields the reciprocal, which
    /// reads as the exact opposite of what it means — a 2x win rendered as
    /// "0.50x" understates the result fourfold.
    #[test]
    fn speedup_is_above_one_when_the_left_engine_is_faster() {
        // arneb takes 100ms, trino 400ms — arneb is 4x faster.
        let results = vec![result("arneb", "q01", 100.0), result("trino", "q01", 400.0)];
        let rendered = render(&results);

        assert!(
            rendered.contains("4.00x"),
            "expected a 4.00x speedup for the faster left-hand engine, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("0.25x"),
            "speedup is inverted — the reciprocal appeared:\n{rendered}"
        );
    }

    /// And below 1 when the left-hand engine is the slower one.
    #[test]
    fn speedup_is_below_one_when_the_left_engine_is_slower() {
        let results = vec![result("arneb", "q01", 400.0), result("trino", "q01", 100.0)];
        let rendered = render(&results);
        assert!(
            rendered.contains("0.25x"),
            "expected 0.25x when the left engine is 4x slower, got:\n{rendered}"
        );
    }

    /// The geomean row must use the same orientation as the per-query columns,
    /// or the summary contradicts the table above it.
    #[test]
    fn geomean_uses_the_same_orientation_as_the_table() {
        let results = vec![result("arneb", "q01", 100.0), result("trino", "q01", 400.0)];
        let rendered = render(&results);
        let geomean_section = rendered
            .split("### Pairwise geomean speedup")
            .nth(1)
            .expect("no geomean section");
        assert!(
            geomean_section.contains("4.00x"),
            "geomean disagrees with the per-query table:\n{geomean_section}"
        );
    }
}
