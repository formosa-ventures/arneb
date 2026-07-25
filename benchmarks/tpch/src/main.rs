//! TPC-H benchmark harness.
//!
//! Runs the TPC-H suite against any subset of {arneb, trino, datafusion} under
//! one shared run plan, and renders a comparison report from the resulting JSON
//! documents. All engine-specific logic lives behind the `engines` module's
//! adapter trait; this file is CLI parsing and dispatch only.

mod canonical;
mod correctness;
mod engines;
mod report;
mod runner;
mod skip;
mod stats;

use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};

use engines::arneb::ArnebEngine;
use engines::datafusion::{DataFusionConfig, DataFusionEngine};
use engines::trino::TrinoEngine;
use engines::BenchmarkEngine;
use runner::{LoadedQuery, RunPlan};

const ALL_ENGINES: &[&str] = &["arneb", "trino", "datafusion"];
const DEFAULT_ARNEB_PORT: u16 = 5432;
const DEFAULT_TRINO_PORT: u16 = 8080;

#[derive(Parser)]
#[command(name = "tpch-bench", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Flags for the default `run` behaviour when no subcommand is given.
    #[command(flatten)]
    run: RunArgs,
}

// Constructed exactly once, at startup, from argv — the variant size gap costs
// nothing, and boxing the payload fights clap's derive.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
enum Command {
    /// Execute the query suite against one or more engines.
    Run(RunArgs),
    /// Render a comparison report from previously written result JSONs.
    Report(ReportArgs),
}

#[derive(Args, Clone)]
struct RunArgs {
    /// Engines to benchmark, comma-separated (arneb,trino,datafusion).
    /// Defaults to all three when neither this nor --engine is given.
    #[arg(long)]
    engines: Option<String>,

    /// Single-engine alias for --engines, kept for backwards compatibility.
    #[arg(long)]
    engine: Option<String>,

    /// Fallback host for the networked engines. Used when the per-engine host
    /// flags are not given — on the compose network the engines have distinct
    /// service names, so they must be addressed separately.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Host for arneb's pgwire listener. Defaults to --host.
    #[arg(long)]
    arneb_host: Option<String>,

    /// Host for Trino's HTTP endpoint. Defaults to --host.
    #[arg(long)]
    trino_host: Option<String>,

    /// Port override. Only applies when exactly one engine is selected.
    #[arg(long)]
    port: Option<u16>,

    /// Port for arneb's pgwire listener.
    #[arg(long, default_value_t = DEFAULT_ARNEB_PORT)]
    arneb_port: u16,

    /// Port for Trino's HTTP endpoint.
    #[arg(long, default_value_t = DEFAULT_TRINO_PORT)]
    trino_port: u16,

    /// Trino catalog.
    #[arg(long, default_value = "tpch")]
    catalog: String,

    /// Trino schema.
    #[arg(long, default_value = "sf1")]
    schema: String,

    /// S3/MinIO endpoint for the in-process DataFusion engine.
    /// Precedence: this flag > AWS_ENDPOINT_URL > built-in default.
    #[arg(long)]
    minio_endpoint: Option<String>,

    /// Bucket holding the TPC-H Parquet files.
    #[arg(long, default_value = "warehouse")]
    s3_bucket: String,

    /// Key prefix under the bucket.
    #[arg(long, default_value = "tpch")]
    s3_prefix: String,

    /// Directory containing query SQL files.
    #[arg(long, default_value = "benchmarks/tpch/queries")]
    queries_dir: PathBuf,

    /// Total runs per query, warm-up included.
    #[arg(long, default_value_t = 8)]
    num_runs: usize,

    /// Leading runs discarded before measurement.
    #[arg(long, default_value_t = 3)]
    warm_up: usize,

    /// Output directory for result JSONs.
    #[arg(long, default_value = "benchmarks/tpch/results")]
    output_dir: PathBuf,

    /// Restrict to specific queries, e.g. "1,3,6".
    #[arg(long)]
    queries: Option<String>,
}

#[derive(Args, Clone)]
struct ReportArgs {
    /// Result JSON files to compare.
    files: Vec<PathBuf>,

    /// Directory to scan; the most recent file per engine is used.
    #[arg(long)]
    dir: Option<PathBuf>,

    /// Also write the report to this path.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Report(args)) => run_report(args),
        Some(Command::Run(args)) => run_benchmark(args).await,
        None => run_benchmark(cli.run).await,
    }
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

async fn run_benchmark(args: RunArgs) -> Result<(), Box<dyn std::error::Error>> {
    let selected = resolve_engines(&args)?;

    let mut queries = load_queries(&args.queries_dir, args.queries.as_deref())?;
    queries.sort_by(|a, b| a.query_id.cmp(&b.query_id));
    if queries.is_empty() {
        return Err(format!("no .sql files found in {}", args.queries_dir.display()).into());
    }

    let measurement_runs = args.num_runs.saturating_sub(args.warm_up);
    if measurement_runs == 0 {
        return Err(format!(
            "--num-runs ({}) must exceed --warm-up ({}) so at least one run is measured",
            args.num_runs, args.warm_up
        )
        .into());
    }

    println!("TPC-H Benchmark Runner");
    println!("======================");
    println!("Engines: {}", selected.join(", "));
    println!("Queries: {}", queries.len());
    println!(
        "Runs: {} ({} warm-up + {} measured)",
        args.num_runs, args.warm_up, measurement_runs
    );

    let skip_list = skip::default_skip_list();
    let plan = RunPlan {
        queries: &queries,
        warm_up: args.warm_up,
        num_runs: args.num_runs,
        skip_list: &skip_list,
    };

    std::fs::create_dir_all(&args.output_dir)?;
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();

    // Engine-major: one engine connects, runs every query, and is dropped
    // before the next starts, so no engine's warmup state bleeds into another's
    // timings.
    for name in &selected {
        let mut engine = build_engine(name, &args)?;
        let result = runner::run_engine(engine.as_mut(), &plan).await;

        let path = args
            .output_dir
            .join(format!("{}_{timestamp}.json", name.replace('-', "_")));
        std::fs::write(&path, serde_json::to_string_pretty(&result)?)?;
        println!("-> {}", path.display());
    }

    println!(
        "\nRun `tpch-bench report --dir {}` to compare.",
        args.output_dir.display()
    );
    Ok(())
}

/// Resolve the engine selection: `--engines` wins, then `--engine`, else all three.
fn resolve_engines(args: &RunArgs) -> Result<Vec<String>, String> {
    let raw = match (&args.engines, &args.engine) {
        (Some(list), _) => list.clone(),
        (None, Some(one)) => one.clone(),
        (None, None) => ALL_ENGINES.join(","),
    };

    let mut selected = Vec::new();
    for part in raw.split(',') {
        let name = part.trim().to_ascii_lowercase();
        if name.is_empty() {
            continue;
        }
        if !ALL_ENGINES.contains(&name.as_str()) {
            return Err(format!(
                "unknown engine `{name}` (expected one of: {})",
                ALL_ENGINES.join(", ")
            ));
        }
        if !selected.contains(&name) {
            selected.push(name);
        }
    }

    if selected.is_empty() {
        return Err("no engines selected".into());
    }
    Ok(selected)
}

fn build_engine(
    name: &str,
    args: &RunArgs,
) -> Result<Box<dyn BenchmarkEngine>, Box<dyn std::error::Error>> {
    // A bare --port is only unambiguous when one engine was selected.
    let single = args.engines.is_none() && args.engine.is_some();
    let override_port = if single { args.port } else { None };

    Ok(match name {
        "arneb" => Box::new(ArnebEngine::new(
            args.arneb_host.clone().unwrap_or_else(|| args.host.clone()),
            override_port.unwrap_or(args.arneb_port),
        )),
        "trino" => Box::new(TrinoEngine::new(
            args.trino_host.clone().unwrap_or_else(|| args.host.clone()),
            override_port.unwrap_or(args.trino_port),
            args.catalog.clone(),
            args.schema.clone(),
        )),
        "datafusion" => Box::new(DataFusionEngine::new(datafusion_config(args))),
        other => return Err(format!("unknown engine `{other}`").into()),
    })
}

/// CLI flag > environment variable > built-in default, per engine spec.
fn datafusion_config(args: &RunArgs) -> DataFusionConfig {
    let defaults = DataFusionConfig::default();
    let from_env = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());

    DataFusionConfig {
        endpoint: args
            .minio_endpoint
            .clone()
            .or_else(|| from_env("AWS_ENDPOINT_URL"))
            .unwrap_or(defaults.endpoint),
        region: from_env("AWS_REGION").unwrap_or(defaults.region),
        access_key_id: from_env("AWS_ACCESS_KEY_ID").unwrap_or(defaults.access_key_id),
        secret_access_key: from_env("AWS_SECRET_ACCESS_KEY").unwrap_or(defaults.secret_access_key),
        bucket: args.s3_bucket.clone(),
        prefix: args.s3_prefix.clone(),
        allow_http: defaults.allow_http,
    }
}

/// Load `.sql` files, optionally filtered to a comma-separated query number list.
fn load_queries(
    dir: &Path,
    filter: Option<&str>,
) -> Result<Vec<LoadedQuery>, Box<dyn std::error::Error>> {
    let wanted: Option<Vec<String>> = filter.map(|f| {
        f.split(',')
            .filter_map(|s| s.trim().parse::<u32>().ok())
            .map(|n| format!("q{n:02}"))
            .collect()
    });

    let mut queries = Vec::new();
    if !dir.exists() {
        return Ok(queries);
    }

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let query_id = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        if let Some(ref wanted) = wanted {
            if !wanted.contains(&query_id) {
                continue;
            }
        }
        let sql = std::fs::read_to_string(&path)?.trim().to_string();
        if sql.is_empty() {
            eprintln!("warning: skipping empty query file {}", path.display());
            continue;
        }
        queries.push(LoadedQuery {
            query_id,
            path,
            sql,
        });
    }

    Ok(queries)
}

// ---------------------------------------------------------------------------
// report
// ---------------------------------------------------------------------------

fn run_report(args: ReportArgs) -> Result<(), Box<dyn std::error::Error>> {
    let results = report::load_inputs(&args.files, args.dir.as_deref())?;
    let rendered = report::render(&results);

    print!("{rendered}");
    if let Some(path) = &args.output {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(path, &rendered)?;
        eprintln!("report written to {}", path.display());
    }
    Ok(())
}
