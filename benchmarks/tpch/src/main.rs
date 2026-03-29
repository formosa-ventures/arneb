use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use serde::{Deserialize, Serialize};

/// TPC-H Benchmark Runner for trino-alt and Trino
///
/// Supports two engines:
/// - trino-alt: connects via PostgreSQL wire protocol
/// - trino: connects via Trino REST API (/v1/statement)
#[derive(Parser)]
#[command(name = "tpch-bench", version, about)]
struct Args {
    /// Engine to benchmark: "trino-alt" or "trino"
    #[arg(long, default_value = "trino-alt")]
    engine: String,

    /// Host address of the database server
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port number (5432 for trino-alt, 8080 for trino)
    #[arg(long)]
    port: Option<u16>,

    /// Trino catalog (only for engine=trino)
    #[arg(long, default_value = "tpch")]
    catalog: String,

    /// Trino schema (only for engine=trino)
    #[arg(long, default_value = "sf1")]
    schema: String,

    /// Directory containing query SQL files
    #[arg(long, default_value = "benchmarks/tpch/queries")]
    queries_dir: PathBuf,

    /// Number of runs per query (including warm-up)
    #[arg(long, default_value = "5")]
    num_runs: usize,

    /// Number of warm-up runs to discard
    #[arg(long, default_value = "2")]
    warm_up: usize,

    /// Output directory for JSON results
    #[arg(long, default_value = "benchmarks/tpch/results")]
    output_dir: PathBuf,

    /// Only run specific queries (e.g., "1,3,6")
    #[arg(long)]
    queries: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkResult {
    engine: String,
    host: String,
    port: u16,
    timestamp: String,
    queries: Vec<QueryResult>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryResult {
    query_id: String,
    query_file: String,
    status: String,
    runs: Vec<RunResult>,
    median_ms: Option<f64>,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunResult {
    run_number: usize,
    wall_clock_ms: f64,
    rows_returned: usize,
    is_warmup: bool,
}

/// Trait for database clients — both pgwire and Trino REST use the same interface.
#[async_trait::async_trait]
trait BenchClient: Send + Sync {
    async fn execute_query(&self, sql: &str) -> Result<usize, String>;
}

// ---------------------------------------------------------------------------
// PostgreSQL wire protocol client (for trino-alt)
// ---------------------------------------------------------------------------

struct PgClient {
    client: tokio_postgres::Client,
}

#[async_trait::async_trait]
impl BenchClient for PgClient {
    async fn execute_query(&self, sql: &str) -> Result<usize, String> {
        let rows = self.client.query(sql, &[]).await.map_err(|e| e.to_string())?;
        Ok(rows.len())
    }
}

// ---------------------------------------------------------------------------
// Trino REST API client
// ---------------------------------------------------------------------------

struct TrinoClient {
    base_url: String,
    catalog: String,
    schema: String,
    http: reqwest::Client,
}

#[async_trait::async_trait]
impl BenchClient for TrinoClient {
    async fn execute_query(&self, sql: &str) -> Result<usize, String> {
        let url = format!("{}/v1/statement", self.base_url);
        let resp = self
            .http
            .post(&url)
            .header("X-Trino-User", "benchmark")
            .header("X-Trino-Catalog", &self.catalog)
            .header("X-Trino-Schema", &self.schema)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let mut result: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
        let mut total_rows = 0usize;

        loop {
            if let Some(data) = result.get("data") {
                if let Some(arr) = data.as_array() {
                    total_rows += arr.len();
                }
            }
            if let Some(err) = result.get("error") {
                let msg = err
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error");
                return Err(msg.to_string());
            }
            let next_uri = match result.get("nextUri").and_then(|u| u.as_str()) {
                Some(uri) => uri.to_string(),
                None => break,
            };
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let resp = self
                .http
                .get(&next_uri)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            result = resp.json().await.map_err(|e| e.to_string())?;
        }

        Ok(total_rows)
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let port = args.port.unwrap_or(match args.engine.as_str() {
        "trino" => 8080,
        _ => 5432,
    });

    println!("TPC-H Benchmark Runner");
    println!("======================");
    println!("Engine: {}", args.engine);
    println!("Target: {}:{}", args.host, port);
    println!("Runs: {} (warm-up: {})", args.num_runs, args.warm_up);
    println!();

    // Create client based on engine type.
    let client: Box<dyn BenchClient> = match args.engine.as_str() {
        "trino" => {
            let base_url = format!("http://{}:{}", args.host, port);
            println!("Connecting to Trino at {base_url} (catalog={}, schema={})...", args.catalog, args.schema);

            // Verify Trino is reachable
            let http = reqwest::Client::new();
            let info_url = format!("{base_url}/v1/info");
            http.get(&info_url).send().await.map_err(|e| {
                format!("Cannot connect to Trino at {base_url}: {e}")
            })?;
            println!("Connected!");

            Box::new(TrinoClient {
                base_url,
                catalog: args.catalog.clone(),
                schema: args.schema.clone(),
                http,
            })
        }
        _ => {
            let conn_str = format!(
                "host={} port={} user=test dbname=test",
                args.host, port
            );
            let (pg_client, connection) =
                tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {e}");
                }
            });
            println!("Connected to trino-alt at {}:{}", args.host, port);
            Box::new(PgClient { client: pg_client })
        }
    };

    println!();

    // Discover query files.
    let mut query_files = discover_queries(&args.queries_dir, args.queries.as_deref())?;
    query_files.sort();

    if query_files.is_empty() {
        eprintln!("No query files found in {}", args.queries_dir.display());
        return Ok(());
    }

    println!("Found {} queries", query_files.len());
    println!();

    // Run benchmarks.
    let mut results = Vec::new();

    for (query_file, query_id) in &query_files {
        print!("Q{query_id}: ");

        let sql = std::fs::read_to_string(query_file)?;
        let sql = sql.trim();

        if sql.is_empty() {
            println!("SKIP (empty)");
            results.push(QueryResult {
                query_id: query_id.clone(),
                query_file: query_file.display().to_string(),
                status: "skipped".into(),
                runs: vec![],
                median_ms: None,
                error: Some("empty query file".into()),
            });
            continue;
        }

        let mut runs = Vec::new();
        let mut error_msg: Option<String> = None;

        for run in 0..args.num_runs {
            let is_warmup = run < args.warm_up;
            let start = Instant::now();

            match client.execute_query(sql).await {
                Ok(row_count) => {
                    let elapsed = start.elapsed();
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    runs.push(RunResult {
                        run_number: run + 1,
                        wall_clock_ms: ms,
                        rows_returned: row_count,
                        is_warmup,
                    });
                    if is_warmup {
                        print!("w");
                    } else {
                        print!(".");
                    }
                }
                Err(e) => {
                    println!("FAIL ({e})");
                    error_msg = Some(e);
                    break;
                }
            }
        }

        if let Some(err) = error_msg {
            results.push(QueryResult {
                query_id: query_id.clone(),
                query_file: query_file.display().to_string(),
                status: "failed".into(),
                runs,
                median_ms: None,
                error: Some(err),
            });
            continue;
        }

        {
            let mut timings: Vec<f64> = runs
                .iter()
                .filter(|r| !r.is_warmup)
                .map(|r| r.wall_clock_ms)
                .collect();
            timings.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let median = if timings.is_empty() {
                None
            } else {
                Some(timings[timings.len() / 2])
            };

            println!(" {:.1}ms", median.unwrap_or(0.0));

            results.push(QueryResult {
                query_id: query_id.clone(),
                query_file: query_file.display().to_string(),
                status: "ok".into(),
                runs,
                median_ms: median,
                error: None,
            });
        }
    }

    // Write results.
    std::fs::create_dir_all(&args.output_dir)?;
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let engine_label = args.engine.replace('-', "_");
    let output_path = args
        .output_dir
        .join(format!("{engine_label}_{timestamp}.json"));

    let benchmark = BenchmarkResult {
        engine: args.engine.clone(),
        host: args.host,
        port,
        timestamp: chrono::Utc::now().to_rfc3339(),
        queries: results,
    };

    let json = serde_json::to_string_pretty(&benchmark)?;
    std::fs::write(&output_path, &json)?;

    println!();
    println!("Results written to {}", output_path.display());

    // Print summary table.
    println!();
    println!(
        "{:<8} {:<10} {:<12} {:<8}",
        "Query", "Status", "Median (ms)", "Rows"
    );
    println!("{}", "-".repeat(40));
    for q in &benchmark.queries {
        let median = q
            .median_ms
            .map(|m| format!("{m:.1}"))
            .unwrap_or_else(|| "-".into());
        let rows = q
            .runs
            .last()
            .map(|r| r.rows_returned.to_string())
            .unwrap_or_else(|| "-".into());
        println!("{:<8} {:<10} {:<12} {:<8}", q.query_id, q.status, median, rows);
    }

    Ok(())
}

/// Find all .sql files in the queries directory, returning (path, query_id).
fn discover_queries(
    dir: &Path,
    filter: Option<&str>,
) -> Result<Vec<(PathBuf, String)>, Box<dyn std::error::Error>> {
    let filter_set: Option<Vec<String>> = filter.map(|f| {
        f.split(',')
            .map(|s| format!("q{:02}", s.trim().parse::<u32>().unwrap_or(0)))
            .collect()
    });

    let mut queries = Vec::new();

    if !dir.exists() {
        return Ok(queries);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |e| e == "sql") {
            let stem = path.file_stem().unwrap().to_string_lossy().to_string();
            if let Some(ref filter) = filter_set {
                if !filter.contains(&stem) {
                    continue;
                }
            }
            queries.push((path, stem));
        }
    }

    Ok(queries)
}
