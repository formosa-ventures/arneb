// Use jemalloc as the global allocator. glibc malloc's dynamic mmap
// threshold ratchets up after the first few large frees, so freed
// Arrow buffers under the high-water mark stay resident in the arena
// — cgroup memory.peak then reflects allocator history rather than
// the engine's true working set. jemalloc's dirty_decay/muzzy_decay
// returns pages to the OS via madvise. The decay interval is set at
// startup via `mallctl` in `configure_jemalloc_decay()` (knob
// `ARNEB_DIRTY_DECAY_MS`, default 500) — NOT via `MALLOC_CONF`, which
// tikv-jemalloc ignores (it reads the `_rjem_`-prefixed name). See the
// build-time vs runtime convention in docs/guide/configuration.md.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Apply jemalloc page-decay at startup. Runtime-tunable via
/// `ARNEB_DIRTY_DECAY_MS` (default 500 ms — the measured sweet spot:
/// low enough to keep RSS under the worker admission-gate threshold,
/// high enough to avoid per-allocation page re-faults). Applied through
/// `mallctl` and the effective value is logged, so a misapply is visible
/// rather than a silent no-op (the failure mode of the `MALLOC_CONF`
/// env var). See docs/guide/configuration.md.
///
/// MUST be called BEFORE the tokio runtime is built: the `arena.4096`
/// (`MALLCTL_ARENAS_ALL`) decay write triggers a synchronous purge across
/// all arenas, which intermittently SIGSEGVs if other threads are
/// allocating concurrently. Called as the first thing in `main()` it runs
/// single-threaded, so the purge has no concurrent allocator. Logging uses
/// `eprintln!` because the tracing subscriber is not initialised yet; the
/// line still lands in container logs (stderr).
#[cfg(not(target_env = "msvc"))]
fn configure_jemalloc_decay() {
    use tikv_jemalloc_ctl::raw;
    let decay_ms: isize = std::env::var("ARNEB_DIRTY_DECAY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let background_thread_enabled = std::env::var("ARNEB_JEMALLOC_BG_THREAD")
        .ok()
        .map(|s| matches!(s.trim(), "1" | "true"))
        .unwrap_or(false);
    // `background_thread` is optionally enabled via
    // `ARNEB_JEMALLOC_BG_THREAD` — set once here while single-threaded,
    // before the tokio runtime starts, which is safe. The race the
    // original comment warned about is a runtime toggle during concurrent
    // thread startup.
    //
    // SAFETY: each mallctl key is paired with the value type jemalloc
    // expects — the `*decay_ms` keys are `ssize_t` (== `isize` on the LP64
    // targets we build for), and `background_thread` is a bool encoded as
    // `u8` by jemalloc's mallctl ABI. Arena index 4096 is
    // `MALLCTL_ARENAS_ALL`, so the decay write retroactively covers every
    // existing arena; `arenas.*` writes set the default for arenas created
    // later (e.g. new tokio worker threads).
    unsafe {
        let _ = raw::write(b"arena.4096.dirty_decay_ms\0", decay_ms);
        let _ = raw::write(b"arena.4096.muzzy_decay_ms\0", decay_ms);
        let _ = raw::write(b"arenas.dirty_decay_ms\0", decay_ms);
        let _ = raw::write(b"arenas.muzzy_decay_ms\0", decay_ms);
        let effective: isize = raw::read(b"arenas.dirty_decay_ms\0").unwrap_or(-1);
        eprintln!("jemalloc decay configured requested={decay_ms} effective={effective}");

        if background_thread_enabled {
            if let Err(e) = raw::write(b"background_thread\0", true as u8) {
                tracing::warn!(
                    error = ?e,
                    "failed to enable jemalloc background_thread via mallctl"
                );
            }
        }
        match raw::read::<u8>(b"background_thread\0") {
            Ok(effective) => tracing::info!(
                "jemalloc background_thread: enabled={} (effective={})",
                background_thread_enabled,
                effective != 0
            ),
            Err(e) => tracing::warn!(
                error = ?e,
                "failed to read jemalloc background_thread effective value"
            ),
        }
    }
}

#[cfg(target_env = "msvc")]
fn configure_jemalloc_decay() {}

mod config;
pub mod coordinator;
mod drain;
pub mod dynamic_filter;
mod fragment_pruning;
pub mod memory_probe;
pub mod memory_profile;
pub mod task_manager;
mod web;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arneb_catalog::CatalogManager;
use arneb_connectors::file::{FileCatalog, FileConnectorFactory, FileFormat, FileSchema};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema};
use arneb_connectors::{ConnectorRegistry, StorageRegistry};
use arneb_execution::memory_pool::{
    GreedyMemoryPool, MemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use arneb_protocol::{
    PgwireTls, ProtocolConfig, ProtocolServer, ServerTls, TrinoConfig, TrinoServer,
};
use clap::Parser;

use crate::config::{parse_data_type, AppConfig, ServerRole};

fn dfrpc_domain_variant(domain: &arneb_common::Domain) -> String {
    match domain {
        arneb_common::Domain::DistinctValues(values) => {
            format!("DistinctValues(len={})", values.len())
        }
        arneb_common::Domain::Range { .. } => "Range".to_string(),
        arneb_common::Domain::Bloom(_) => "Bloom".to_string(),
        arneb_common::Domain::All => "All".to_string(),
    }
}

/// Arneb — Distributed SQL query engine
#[derive(Parser)]
#[command(name = "arneb", version, about)]
struct CliArgs {
    /// Path to TOML config file
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// Override bind address
    #[arg(long)]
    bind: Option<String>,

    /// Override listen port
    #[arg(long)]
    port: Option<u16>,

    /// Server role: standalone (default), coordinator, or worker
    #[arg(long, default_value = "standalone")]
    role: String,

    /// Trino client protocol HTTP port (overrides `[trino] port` and
    /// `ARNEB_TRINO_PORT`; default 8080).
    #[arg(long)]
    trino_port: Option<u16>,

    /// Disable the Trino client protocol listener.
    #[arg(long)]
    no_trino: bool,

    /// Emit per-operator wall-time tracing on the `arneb::profile`
    /// target. Equivalent to setting `arneb::profile=info` in
    /// `RUST_LOG`. The events show op name, partition, elapsed ms,
    /// row count, bytes, and batch count when an operator's output
    /// stream terminates — useful for narrowing down per-stage
    /// costs after `EXPLAIN ANALYZE` highlights a suspect operator.
    #[arg(long)]
    profile: bool,

    /// Overwrite state users that differ from the `[[auth.users]]` in the
    /// config file (the file only seeds users on first start). Only
    /// meaningful with `[auth] type = "password"`.
    #[arg(long)]
    reseed_state: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Read a password from stdin and print a SCRAM-SHA-256 verifier for
    /// the `password_hash` field of an `[[auth.users]]` entry.
    HashPassword {
        /// PBKDF2 iteration count. The default (4096) matches PostgreSQL;
        /// production deployments should use a much higher count.
        #[arg(long, default_value_t = arneb_protocol::auth::DEFAULT_SCRAM_ITERATIONS,
              value_parser = clap::value_parser!(u32).range(1..))]
        iterations: u32,
    },
}

/// `arneb hash-password`: read one line from stdin, print its verifier.
fn hash_password(iterations: u32) -> Result<()> {
    use std::io::{BufRead, IsTerminal, Write};
    if std::io::stdin().is_terminal() {
        eprint!("Password (input is echoed): ");
        std::io::stderr().flush()?;
    }
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    let password = line.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        bail!("password must not be empty");
    }
    println!(
        "{}",
        arneb_protocol::ScramVerifier::generate_with_iterations(password, iterations)?
    );
    Ok(())
}

fn main() -> Result<()> {
    // Set jemalloc page-decay while still single-threaded — BEFORE building
    // the tokio runtime. A runtime decay-set purges all arenas and races
    // with concurrent allocation on the worker threads, which SIGSEGVs
    // intermittently. See configure_jemalloc_decay's doc comment.
    #[cfg(not(target_env = "msvc"))]
    configure_jemalloc_decay();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    // 1. Parse CLI args
    let args = CliArgs::parse();
    if let Some(Command::HashPassword { iterations }) = args.command {
        return hash_password(iterations);
    }

    // 2. Load config (file + env overrides)
    let mut config =
        AppConfig::load(args.config.as_deref()).context("failed to load configuration")?;
    let reseed_state = args.reseed_state;

    // 3. Apply CLI overrides
    if let Some(bind) = args.bind {
        config.server.bind_address = bind;
    }
    if let Some(port) = args.port {
        config.server.port = port;
    }
    config.cluster.role = args.role;
    if let Some(port) = args.trino_port {
        config.trino.port = port;
    }
    if args.no_trino {
        config.trino.enabled = false;
    }
    config
        .server
        .validate()
        .context("configuration validation failed")?;

    let role = ServerRole::parse(&config.cluster.role).context("invalid server role")?;

    // 4. Initialize tracing. `--profile` appends an `arneb::profile=info`
    // directive after the base filter (defaults to `RUST_LOG`, else
    // global `info`) so the flag works regardless of how the user
    // configured the rest of the filter.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let env_filter = if args.profile {
        env_filter.add_directive(
            "arneb::profile=info"
                .parse()
                .expect("static directive parses"),
        )
    } else {
        env_filter
    };
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
    // 5. Create catalog manager + connector registry
    // Default catalog priority: first hive catalog > file (if tables configured) > memory
    let default_catalog = if let Some(first_catalog) = config.catalogs.first() {
        first_catalog.name.as_str()
    } else if !config.tables.is_empty() {
        "file"
    } else {
        "memory"
    };
    let default_schema = config
        .catalogs
        .first()
        .map(|c| c.effective_default_schema())
        .unwrap_or("default");
    let catalog_manager = CatalogManager::new(default_catalog, default_schema);

    // Register memory catalog (empty)
    let mem_schema = Arc::new(MemorySchema::new());
    let mem_catalog = Arc::new(MemoryCatalog::new());
    mem_catalog.register_schema("default", mem_schema);
    let mem_factory = MemoryConnectorFactory::new(mem_catalog.clone(), "default");
    catalog_manager.register_catalog("memory", mem_catalog);

    let mut connector_registry = ConnectorRegistry::new();
    connector_registry.register("memory", Arc::new(mem_factory));

    // 6. Register file tables from config
    let global_cloud_config = config.storage.to_cloud_config();
    let file_storage_registry = Arc::new(StorageRegistry::with_config(global_cloud_config.clone()));
    let table_count = config.tables.len();
    if !config.tables.is_empty() {
        let file_factory = Arc::new(FileConnectorFactory::new(file_storage_registry.clone()));

        for table in &config.tables {
            let format = match table.format.as_str() {
                "csv" => FileFormat::Csv,
                "parquet" => FileFormat::Parquet,
                other => bail!(
                    "unsupported table format '{}' for table '{}'",
                    other,
                    table.name
                ),
            };

            let schema = match format {
                FileFormat::Csv => {
                    let col_schemas = table.schema.as_ref().with_context(|| {
                        format!("CSV table '{}' requires an explicit schema", table.name)
                    })?;
                    let columns: Result<Vec<_>> = col_schemas
                        .iter()
                        .map(|cs| {
                            let dt = parse_data_type(&cs.r#type).with_context(|| {
                                format!(
                                    "invalid type '{}' for column '{}' in table '{}'",
                                    cs.r#type, cs.name, table.name
                                )
                            })?;
                            Ok(arneb_common::types::ColumnInfo {
                                name: cs.name.clone(),
                                data_type: dt,
                                nullable: true,
                            })
                        })
                        .collect();
                    Some(columns?)
                }
                FileFormat::Parquet => None,
            };

            if let Err(e) = file_factory
                .register_table(&table.name, &table.path, format, schema)
                .await
            {
                tracing::warn!(
                    table = %table.name,
                    path = %table.path,
                    error = %e,
                    "failed to register table, skipping"
                );
            }
        }

        let file_schema = Arc::new(FileSchema::new(file_factory.clone()));
        let file_catalog = Arc::new(FileCatalog::new("default", file_schema));
        catalog_manager.register_catalog("file", file_catalog);
        connector_registry.register("file", file_factory);
    }

    // 6.5. Register external catalogs from config: Hive (Iceberg tables in
    // the same metastore are redirected to the Iceberg reader by the Hive
    // catalog) and PostgreSQL. Runs on every role, so workers can create
    // data sources for the scans they are sent.
    for catalog_cfg in &config.catalogs {
        if catalog_cfg.catalog_type == "postgresql" {
            register_postgresql_catalog(catalog_cfg, &catalog_manager, &mut connector_registry)
                .await;
            continue;
        }
        if catalog_cfg.catalog_type != "hive" {
            tracing::warn!(
                catalog = %catalog_cfg.name,
                catalog_type = %catalog_cfg.catalog_type,
                "unsupported catalog type, skipping"
            );
            continue;
        }

        let metastore_uri = match &catalog_cfg.metastore_uri {
            Some(uri) => uri.clone(),
            None => {
                tracing::warn!(
                    catalog = %catalog_cfg.name,
                    "hive catalog missing metastore_uri, skipping"
                );
                continue;
            }
        };

        // Per-catalog storage: merge catalog override with global config
        let merged_storage =
            config::StorageConfig::merge(&config.storage, catalog_cfg.storage.as_ref());
        let catalog_storage_registry = Arc::new(StorageRegistry::with_config(
            merged_storage.to_cloud_config(),
        ));

        match arneb_hive::catalog::HmsClient::new(&metastore_uri).await {
            Ok(hms_client) => {
                let hms_client = Arc::new(hms_client);
                let hive_catalog = Arc::new(arneb_hive::catalog::HiveCatalogProvider::new(
                    hms_client.clone(),
                    catalog_storage_registry.clone(),
                ));
                catalog_manager.register_catalog(&catalog_cfg.name, hive_catalog);

                let hive_connector =
                    arneb_hive::datasource::HiveConnectorFactory::new(catalog_storage_registry);
                connector_registry.register(&catalog_cfg.name, Arc::new(hive_connector));

                tracing::info!(
                    catalog = %catalog_cfg.name,
                    metastore = %metastore_uri,
                    "registered hive catalog"
                );
            }
            Err(e) => {
                tracing::warn!(
                    catalog = %catalog_cfg.name,
                    metastore = %metastore_uri,
                    error = %e,
                    "failed to connect to hive metastore, skipping catalog"
                );
            }
        }
    }

    // 7. Create protocol server
    let listen_addr = format!("{}:{}", config.server.bind_address, config.server.port);
    let protocol_config = ProtocolConfig {
        bind_address: listen_addr.clone(),
    };

    let catalog_manager = Arc::new(catalog_manager);
    let connector_registry = Arc::new(connector_registry);

    // 8. Set up Flight RPC server + heartbeat handling
    let heartbeat = config
        .cluster
        .heartbeat(|k| std::env::var(k).ok())
        .context("invalid [cluster] heartbeat configuration")?;
    let drain_timeout_secs = config
        .cluster
        .drain_timeout_secs(|k| std::env::var(k).ok())
        .context("invalid [cluster] drain_timeout_secs")?;
    match role {
        ServerRole::Coordinator => tracing::info!(
            target: "arneb::config",
            heartbeat_timeout_secs = heartbeat.timeout_secs,
            "worker heartbeat timeout effective value ([cluster] heartbeat_timeout_secs / ARNEB_HEARTBEAT_TIMEOUT_SECS)"
        ),
        ServerRole::Worker => {
            tracing::info!(
                target: "arneb::config",
                heartbeat_interval_secs = heartbeat.interval_secs,
                "heartbeat interval effective value ([cluster] heartbeat_interval_secs / ARNEB_HEARTBEAT_INTERVAL_SECS)"
            );
            tracing::info!(
                target: "arneb::config",
                drain_timeout_secs,
                "drain timeout effective value ([cluster] drain_timeout_secs / ARNEB_DRAIN_TIMEOUT_SECS)"
            );
        }
        ServerRole::Standalone => {}
    }
    let node_registry = arneb_scheduler::NodeRegistry::new(heartbeat.timeout_secs);
    let rpc_addr = format!("{}:{}", config.server.bind_address, config.cluster.rpc_port);

    // Cluster RPC security ([cluster] secret / [cluster.tls] / ARNEB_CLUSTER_*):
    // installed once, before any Flight connection, for every outbound call
    // and for this node's Flight server.
    let cluster_security = config
        .cluster
        .security(|k| std::env::var(k).ok())
        .context("invalid [cluster] security configuration")?;
    tracing::info!(
        target: "arneb::config",
        cluster_auth = if cluster_security.secret.is_some() { "secret" } else { "off" },
        cluster_tls = if cluster_security.tls.is_some() { "on" } else { "off" },
        "cluster RPC security effective mode ([cluster] secret / [cluster.tls] / ARNEB_CLUSTER_*)"
    );
    config::check_cluster_transport(
        cluster_security.secret.is_some(),
        cluster_security.tls.is_some(),
        &config.server.bind_address,
    )?;
    if cluster_security.secret.is_some()
        && cluster_security.tls.is_none()
        && !config::is_loopback_host(&config.server.bind_address)
    {
        tracing::warn!(
            target: "arneb::config",
            bind_address = %config.server.bind_address,
            "[cluster] secret is set without [cluster.tls]: the secret and all exchange data \
             cross the network unencrypted"
        );
    }
    arneb_rpc::install_cluster_security(cluster_security);
    let query_tracker = Arc::new(arneb_scheduler::QueryTracker::with_history_limit(
        config.observability.query_history_size,
    ));
    // A1.3 (2026-05-27): per-query dynamic-filter service registry,
    // shared between QueryCoordinator (registers on execute) and the
    // FlightState `df_report_callback` (routes worker reports in).
    let df_registry = arneb_scheduler::DynamicFilterServiceRegistry::new();

    let mut flight_state = match role {
        ServerRole::Coordinator | ServerRole::Standalone => {
            // Coordinator receives heartbeats from workers.
            let registry = node_registry.clone();
            let mut state = arneb_rpc::FlightState::with_heartbeat_callback(std::sync::Arc::new(
                move |msg: arneb_rpc::HeartbeatMessage| {
                    if msg.state == arneb_rpc::WorkerState::Stopped {
                        registry.deregister(&msg.worker_id);
                        return Ok(());
                    }
                    registry.admit(
                        msg.worker_id,
                        msg.flight_address,
                        msg.max_splits,
                        &msg.version,
                        msg.state == arneb_rpc::WorkerState::Draining,
                    )
                },
            ));
            // A1.3: route worker DF reports into the per-query
            // `DynamicFilterService`. The callback is sync (`Fn`),
            // so spawn the async `report_partition` call. Errors are
            // logged + dropped — a stale report after the query
            // already finished is benign (caller bug at producer side).
            let df_registry_for_cb = df_registry.clone();
            state.set_df_report_callback(std::sync::Arc::new(
                move |req: arneb_rpc::ReportDynamicFilterRequest| {
                    tracing::debug!(
                        target: "arneb::dynamic_filter",
                        "coord flight received_report query_id={} task_id={} partition_idx={} df_id={} domain={}",
                        req.query_id,
                        req.task_id,
                        req.partition_idx,
                        req.df_id,
                        dfrpc_domain_variant(&req.domain)
                    );
                    let registry = df_registry_for_cb.clone();
                    tokio::spawn(async move {
                        if let Err(e) = registry
                            .report_partition(
                                req.query_id,
                                req.df_id,
                                req.partition_idx,
                                req.domain,
                            )
                            .await
                        {
                            tracing::warn!(
                                query_id = %req.query_id,
                                df_id = %req.df_id,
                                error = %e,
                                "dropped dynamic filter report"
                            );
                        }
                    });
                },
            ));
            state
        }
        ServerRole::Worker => {
            // Workers don't receive heartbeats, just serve data.
            // A1.3: the `df_notify_callback` is set later after the
            // TaskManager is built (TaskManager owns the per-task
            // collector map).
            arneb_rpc::FlightState::new()
        }
    };

    // 8.4. Resolve the spillable-operator memory budget. Order of
    // precedence inside `MemoryConfig::resolve_budget`:
    //   1. explicit `[memory] spill_budget_bytes = N`
    //   2. cgroup v2 / v1 limit × `cgroup_ratio`
    //   3. unbounded (no enforcement)
    // Wired into ProtocolServer (handles standalone + coordinator
    // root-plan execution) and TaskManager (worker tasks).
    let (budget, budget_source) = config.memory.resolve_budget();
    let global_pool: Arc<dyn MemoryPool> = match budget {
        // D4 (exec-memory-accounting): wrap the bounded pool so an OOM error
        // names the top consumers — the fastest way to see the SF30 memory
        // hog. No effect on the success path. (Unbounded never OOMs, so it
        // is left unwrapped.)
        Some(bytes) => Arc::new(TrackConsumersPool::new(
            Arc::new(GreedyMemoryPool::new(bytes)),
            5,
        )),
        None => Arc::new(UnboundedMemoryPool::new()),
    };
    // admission-control D1: `query_max_memory_per_node` is enforced per
    // query. The node pool stays shared; each query gets its own
    // `QueryMemoryPool` over it — per statement on the coordinator /
    // standalone (`ProtocolServer`, `TrinoServer`) and per query id on a
    // worker (`TaskManager`). A query over its limit fails with
    // `ResourceExhausted` without starving the other queries on the node.
    let query_cap_resolved = config.memory.resolve_query_cap();
    let query_memory_limit = query_cap_resolved.map(|b| b as usize);
    let memory_pool: Arc<dyn MemoryPool> = if memory_profile::mem_profile_enabled() {
        let pool = Arc::new(memory_profile::MemoryProfilePool::new(global_pool));
        pool.start_resident_sampler(std::time::Duration::from_millis(50));
        pool
    } else {
        global_pool
    };
    match (budget, query_cap_resolved) {
        (Some(bytes), Some(qcap)) => tracing::info!(
            spill_budget_bytes = bytes,
            spill_budget_source = budget_source,
            query_max_memory_per_node = qcap,
            "memory pool installed (per-query limit on top of the node spill budget)"
        ),
        (Some(bytes), None) => tracing::info!(
            spill_budget_bytes = bytes,
            spill_budget_source = budget_source,
            "memory pool installed (global spill budget only; no per-query cap)"
        ),
        (None, Some(qcap)) => tracing::info!(
            query_max_memory_per_node = qcap,
            "memory pool installed (per-query limit only; no node spill budget)"
        ),
        (None, None) => tracing::info!(
            spill_budget_source = budget_source,
            "memory pool unbounded — spillable operators will not enforce a budget"
        ),
    }

    // 8.5. Set up distributed execution components
    // Coordinator: create QueryCoordinator and wire into protocol server
    // Worker: create TaskManager and register task callback
    let distributed_executor: Option<Arc<dyn arneb_protocol::DistributedExecutor>> = match role {
        ServerRole::Coordinator => {
            let retry = config
                .cluster
                .query_retry(|k| std::env::var(k).ok())
                .context("invalid [cluster] retry_policy")?;
            tracing::info!(
                target: "arneb::config",
                retry_policy = if retry.is_some() { "query" } else { "none" },
                query_retry_attempts = retry.map(|r| r.max_retries),
                "query retry effective value ([cluster] retry_policy / ARNEB_RETRY_POLICY, query_retry_attempts / ARNEB_QUERY_RETRY_ATTEMPTS)"
            );
            let coord =
                coordinator::QueryCoordinator::new(node_registry.clone(), df_registry.clone())
                    .with_retry(retry);
            Some(Arc::new(coord))
        }
        _ => None,
    };

    let mut worker_tasks: Option<task_manager::TaskManager> = None;
    if matches!(role, ServerRole::Worker) {
        // Phase A (2026-05-23): the count-based `[memory]
        // task_concurrency` field is deprecated. The semaphore it
        // configured held a permit across the entire task body, which
        // deadlocked downstream stream back-pressure. Field is still
        // deserialised (so existing configs don't break), but it has no
        // effect — warn loudly when set to a non-default value so
        // operators notice the schema change.
        if config.memory.task_concurrency != config::default_task_concurrency() {
            tracing::warn!(
                configured_task_concurrency = config.memory.task_concurrency,
                "[memory] task_concurrency is DEPRECATED (Phase A, 2026-05-23) \
                 and has no effect — the per-worker admission semaphore was \
                 removed. Worker concurrency is now bounded by tokio worker \
                 threads + MemoryPool + the optional RSS probe. Remove this \
                 field from your config to silence this warning.",
            );
        }
        // Phase 3b.6b: optional RSS probe + admission threshold.
        let memory_probe = config
            .memory
            .task_admission_threshold_bytes
            .map(|threshold| {
                let probe = Arc::new(memory_probe::MemoryProbe::new(threshold));
                memory_probe::spawn_probe_task(
                    Arc::clone(&probe),
                    std::time::Duration::from_millis(100),
                );
                tracing::info!(
                    rss_admission_threshold_bytes = threshold,
                    "RSS-based admission probe installed (polls jemalloc stats.resident every 100ms)"
                );
                probe
            });
        tracing::info!(
            rss_probe_enabled = memory_probe.is_some(),
            "task admission gate installed (RSS probe only; concurrency semaphore removed in Phase A)"
        );
        // A1.5 (2026-05-27): coord address for the worker's
        // `FlightDynamicFilterPublisher`. We mirror the heartbeat
        // address normalisation (add `http://` when the config omits
        // it) so the publisher and heartbeats use the same wire form.
        let coord_address_for_publisher = config
            .cluster
            .coordinator_address
            .as_ref()
            .map(|addr| coordinator::flight_address(addr));
        let tm = task_manager::TaskManager::with_admission_gate(
            flight_state.clone(),
            catalog_manager.clone(),
            connector_registry.clone(),
            Arc::clone(&memory_pool),
            memory_probe,
            coord_address_for_publisher,
        )
        .with_query_memory_limit(query_memory_limit);
        let tm_for_task = tm.clone();
        flight_state.set_task_callback(Arc::new(move |descriptor| {
            tm_for_task.handle_task(descriptor);
        }));
        let tm_for_status = tm.clone();
        flight_state.set_task_status_callback(Arc::new(move |task_id| {
            tm_for_status.task_status_as_response(task_id)
        }));
        // admission-control D4: the coordinator's cancel guard stops a
        // query's tasks here when the query ends early anywhere.
        let tm_for_cancel = tm.clone();
        flight_state.set_cancel_query_callback(Arc::new(move |query_id| {
            tm_for_cancel.cancel_query(query_id)
        }));
        // worker-graceful-drain: a finished query is released at once
        // instead of waiting for the per-task cleanup polls.
        let tm_for_release = tm.clone();
        flight_state.set_release_query_callback(Arc::new(move |query_id| {
            tm_for_release.release_query(query_id)
        }));
        worker_tasks = Some(tm.clone());
        // A1.3 (2026-05-27): route coord-pushed Domains into the
        // per-task `DynamicFilterCollector` (populated at handle_task
        // time). Sync callback wraps an async spawn — the notify
        // path is fire-and-forget; the collector update happens off
        // the Flight handler thread. A `false` route_notify return
        // means no matching task (cleaned up, never started, or
        // stale push); log + drop.
        let tm_for_notify = tm.clone();
        flight_state.set_df_notify_callback(Arc::new(
            move |req: arneb_rpc::NotifyDynamicFilterRequest| {
                tracing::debug!(
                    target: "arneb::dynamic_filter",
                    "worker flight received_notify query_id={} task_id={} df_id={} domain={}",
                    req.query_id,
                    req.task_id,
                    req.df_id,
                    dfrpc_domain_variant(&req.domain)
                );
                let tm = tm_for_notify.clone();
                tokio::spawn(async move {
                    let routed = tm
                        .route_notify(req.query_id, req.task_id, req.df_id, req.domain)
                        .await;
                    if !routed {
                        tracing::debug!(
                            query_id = %req.query_id,
                            task_id = %req.task_id,
                            df_id = %req.df_id,
                            "dropped dynamic filter notify (no live task)"
                        );
                    }
                });
            },
        ));
    }

    // Validate [auth] on every role (workers never open the state store).
    let config_auth = config
        .auth
        .to_auth_method()
        .context("invalid [auth] configuration")?;
    // Password auth on a coordinator/standalone: users live in the state
    // store, seeded from the config file; the same store serves pgwire,
    // Trino and the Web API.
    let state_store = if matches!(role, ServerRole::Coordinator | ServerRole::Standalone)
        && matches!(config_auth, arneb_protocol::AuthMethod::ScramSha256(_))
    {
        let verifiers = config
            .auth
            .user_verifiers()
            .context("invalid [auth] configuration")?;
        let store = arneb_state::StateStore::open(config.state.path.as_deref())
            .context("cannot open the state store ([state] path / ARNEB_STATE_PATH)")?;
        let report = store
            .seed_users(&verifiers, reseed_state)
            .context("cannot seed the state store from [auth] users")?;
        tracing::info!(
            target: "arneb::config",
            state_path = %store
                .path()
                .map_or_else(|| "in-memory".to_owned(), |p| p.display().to_string()),
            persistent = store.is_persistent(),
            users = arneb_protocol::CredentialSource::user_count(&store),
            first_start = report.first_start,
            seeded = ?report.seeded,
            diverged = ?report.diverged,
            reseed = reseed_state,
            "state store effective settings ([state] path / ARNEB_STATE_PATH / --reseed-state)"
        );
        if !store.is_persistent() {
            tracing::warn!(
                target: "arneb::config",
                "no [state] path: API tokens are lost on restart"
            );
        }
        Some(Arc::new(store))
    } else {
        if reseed_state {
            tracing::info!(
                target: "arneb::config",
                "--reseed-state ignored: it only applies with [auth] type = \"password\" \
                 on a coordinator or standalone server"
            );
        }
        None
    };
    let auth_method = match &state_store {
        Some(store) => arneb_protocol::AuthMethod::ScramSha256(store.clone()),
        None => config_auth,
    };
    let access_rules = if matches!(role, ServerRole::Coordinator | ServerRole::Standalone) {
        config
            .access_control
            .load_rules()
            .context("invalid [access_control] configuration")?
            .map(Arc::new)
    } else {
        None
    };
    if let (Some(rules), Some(path)) = (&access_rules, &config.access_control.rules_file) {
        let [catalogs, schemas, tables, queries] = rules
            .section_sizes()
            .map(|n| n.map_or_else(|| "absent (allow all)".to_owned(), |n| n.to_string()));
        tracing::info!(
            target: "arneb::config",
            rules_file = %path.display(),
            catalogs = %catalogs,
            schemas = %schemas,
            tables = %tables,
            queries = %queries,
            "access control rules loaded"
        );
        if matches!(auth_method, arneb_protocol::AuthMethod::None) {
            tracing::warn!(
                target: "arneb::config",
                "access control rules are set but [auth] type = \"none\": user names are \
                 not verified, so any client can claim any user"
            );
        }
    }
    if !matches!(role, ServerRole::Worker) {
        let users = match &auth_method {
            arneb_protocol::AuthMethod::ScramSha256(creds) => creds.user_count(),
            arneb_protocol::AuthMethod::None => 0,
        };
        tracing::info!(
            target: "arneb::config",
            auth = auth_method.name(),
            users,
            "pgwire authentication effective mode"
        );
    }

    // Client-facing TLS ([tls] / ARNEB_TLS_*): one certificate for pgwire and
    // the Trino listener. Workers serve neither, so they skip loading it.
    let server_tls = match (&config.tls_settings, role) {
        (Some(t), ServerRole::Coordinator | ServerRole::Standalone) => Some(
            ServerTls::load(&t.cert_path, &t.key_path).context("invalid [tls] configuration")?,
        ),
        _ => None,
    };
    if !matches!(role, ServerRole::Worker) {
        match &config.tls_settings {
            Some(t) => tracing::info!(
                target: "arneb::config",
                tls_pgwire = t.pgwire.name(),
                tls_cert_path = %t.cert_path.display(),
                "client TLS effective mode ([tls] / ARNEB_TLS_*)"
            ),
            None => tracing::info!(
                target: "arneb::config",
                tls = "off",
                "client TLS effective mode ([tls] / ARNEB_TLS_*)"
            ),
        }
    }
    let pgwire_tls = server_tls
        .as_ref()
        .zip(config.tls_settings.as_ref())
        .map(|(tls, t)| PgwireTls {
            acceptor: tls.pgwire_acceptor(),
            mode: t.pgwire,
        });

    let password_users = match &auth_method {
        arneb_protocol::AuthMethod::ScramSha256(creds) => Some(Arc::clone(creds)),
        arneb_protocol::AuthMethod::None => None,
    };
    let state_for_web = state_store.clone();
    let credentials_for_web: Option<Arc<dyn arneb_protocol::CredentialSource>> =
        state_store.clone().map(|s| s as _);

    // admission-control D5/D8: one controller for the process, shared by
    // pgwire, the Trino listener and the Web UI. Workers have none; their
    // admission stays the RSS probe, and this cap bounds what reaches them.
    // `max_running_queries = 0` admits every query at once, so no queue can
    // form whatever `max_queued_queries` says: build no controller, and
    // statements skip admission (and its lock) entirely.
    let serves_sql = matches!(role, ServerRole::Coordinator | ServerRole::Standalone);
    let admission_config = &config.admission;
    let admission = (serves_sql && admission_config.max_running_queries > 0).then(|| {
        arneb_scheduler::AdmissionController::new(
            admission_config.max_running_queries,
            admission_config.max_queued_queries,
        )
    });
    if let Some(controller) = &admission {
        controller.spawn_snapshot_logger(std::time::Duration::from_secs(10));
    }
    if serves_sql {
        tracing::info!(
            target: "arneb::config",
            max_running_queries = admission_config.max_running_queries,
            max_queued_queries = admission_config.max_queued_queries,
            statement_timeout = %arneb_common::duration::format_pg_time_limit(
                admission_config.statement_timeout
            ),
            query_max_memory_per_node = %query_memory_limit
                .map_or_else(|| "off".to_owned(), |b| b.to_string()),
            "admission effective settings ([admission] / ARNEB_MAX_RUNNING_QUERIES / \
             ARNEB_MAX_QUEUED_QUERIES / ARNEB_STATEMENT_TIMEOUT / ARNEB_QUERY_MAX_MEMORY_BYTES)"
        );
        let agent_limits = config.agent_api.limits();
        tracing::info!(
            target: "arneb::config",
            default_max_rows = agent_limits.default_max_rows,
            max_rows = agent_limits.max_rows,
            default_page_bytes = agent_limits.default_page_bytes,
            max_page_bytes = agent_limits.max_page_bytes,
            max_cell_bytes = agent_limits.max_cell_bytes,
            max_retained_bytes = agent_limits.max_retained_bytes,
            max_total_retained_bytes = agent_limits.max_total_retained_bytes,
            max_wait_ms = config.agent_api.max_wait_ms,
            idle_timeout_secs = config.agent_api.idle_timeout_secs,
            "agent_api effective settings ([agent_api] / ARNEB_AGENT_*)"
        );
        arneb_common::metrics::set_slow_query_threshold(
            config.observability.slow_query_threshold(),
        );
        tracing::info!(
            target: "arneb::config",
            slow_query_threshold_ms = config.observability.slow_query_threshold_ms,
            query_history_size = config.observability.query_history_size,
            "observability effective settings ([observability] / \
             ARNEB_SLOW_QUERY_THRESHOLD_MS (0 = slow-query log off) / ARNEB_QUERY_HISTORY_SIZE)"
        );
    } else {
        tracing::info!(
            target: "arneb::config",
            query_max_memory_per_node = %query_memory_limit
                .map_or_else(|| "off".to_owned(), |b| b.to_string()),
            "worker per-query memory limit (ARNEB_QUERY_MAX_MEMORY_BYTES)"
        );
    }

    let mut server = ProtocolServer::new(
        protocol_config,
        catalog_manager.clone(),
        connector_registry.clone(),
    )
    .with_memory_pool(Arc::clone(&memory_pool))
    .with_query_memory_limit(query_memory_limit)
    .with_admission(admission.clone())
    .with_statement_timeout(config.admission.statement_timeout)
    .with_auth(auth_method)
    .with_tls(pgwire_tls)
    .with_query_tracker(query_tracker.clone())
    .with_access_rules(access_rules.clone());
    if let Some(ref executor) = distributed_executor {
        server = server.with_distributed_executor(Arc::clone(executor));
    }

    // Trino client REST protocol: coordinator/standalone only, same engine
    // state (catalogs, connectors, memory pool, distributed executor) as
    // pgwire. Queries are registered with the tracker so they show in the
    // Web UI.
    let serves_clients = matches!(role, ServerRole::Coordinator | ServerRole::Standalone);
    let trino_enabled = config.trino.enabled && serves_clients;
    let trino_addr = format!("{}:{}", config.server.bind_address, config.trino.port);
    // Fail closed: Trino clients only send passwords over HTTPS, so password
    // auth on a plain-HTTP listener would be unusable or a bypass.
    config::check_trino_transport(
        trino_enabled,
        password_users.is_some(),
        server_tls.is_some(),
    )?;
    tracing::info!(
        target: "arneb::config",
        trino_enabled,
        trino_address = %trino_addr,
        tls_trino = if server_tls.is_some() { "https" } else { "http" },
        trino_auth = if password_users.is_some() { "password (basic)" } else { "none" },
        "Trino client protocol effective settings ([trino] / ARNEB_TRINO_ENABLED / ARNEB_TRINO_PORT / --trino-port / --no-trino)"
    );
    let trino_server = if trino_enabled {
        let mut trino = TrinoServer::new(
            TrinoConfig {
                bind_address: trino_addr.clone(),
                ..TrinoConfig::default()
            },
            catalog_manager.clone(),
            connector_registry.clone(),
        )
        .with_memory_pool(Arc::clone(&memory_pool))
        .with_query_memory_limit(query_memory_limit)
        .with_admission(admission.clone())
        .with_statement_timeout(config.admission.statement_timeout)
        .with_query_tracker(query_tracker.clone())
        .with_access_rules(access_rules.clone())
        .with_tls(server_tls.as_ref().map(ServerTls::trino_acceptor))
        .with_password_auth(password_users.clone())
        .context("Trino listener authentication")?;
        if let Some(ref executor) = distributed_executor {
            trino = trino.with_distributed_executor(Arc::clone(executor));
        }
        Some(trino)
    } else {
        None
    };

    // Agent query API (`/api/v2` statements): whenever SQL is served,
    // independent of the Trino listener. Same engine state as the other
    // protocols; statements show in the tracker with source "api".
    let agent_service = if serves_clients {
        let limits = config.agent_api.limits();
        let mut service = arneb_protocol::agent::AgentQueryService::new(
            catalog_manager.clone(),
            connector_registry.clone(),
            query_tracker.clone(),
            limits,
        )
        .with_memory_pool(Arc::clone(&memory_pool))
        .with_query_memory_limit(query_memory_limit)
        .with_admission(admission.clone())
        .with_statement_timeout(config.admission.statement_timeout)
        .with_access_rules(access_rules.clone());
        if let Some(ref executor) = distributed_executor {
            service = service.with_distributed_executor(Arc::clone(executor));
        }
        service.spawn_reaper();
        Some(service)
    } else {
        None
    };

    // 9. Startup banner
    match role {
        ServerRole::Worker => {
            tracing::info!(
                rpc_address = %rpc_addr,
                role = %config.cluster.role,
                "arneb worker starting"
            );
        }
        _ => {
            tracing::info!(
                pgwire_address = %listen_addr,
                rpc_address = %rpc_addr,
                role = %config.cluster.role,
                tables = table_count,
                "arneb listening"
            );
        }
    }

    // 10. Set up Web UI (coordinator + standalone only)
    let web_state = web::WebState {
        query_tracker: query_tracker.clone(),
        node_registry: node_registry.clone(),
        start_time: std::time::Instant::now(),
        role: config.cluster.role.clone(),
        admission: admission.clone(),
        max_queued_queries: config.admission.max_queued_queries,
        memory_pool: memory_pool.clone(),
        memory_limit_bytes: budget.map(|b| b as u64),
        basic: credentials_for_web
            .clone()
            .map(arneb_protocol::BasicAuthenticator::new)
            .transpose()
            .context("Web API Basic authentication")?,
        credentials: credentials_for_web,
        state: state_for_web,
        access_rules: access_rules.clone(),
        metrics_auth: config.web.metrics_auth,
        sessions: web::SessionStore::new(),
        agent: agent_service,
    };
    let web_router = web::build_router(web_state);
    let web_port = config.server.port + 1000; // default: pgwire port + 1000 (e.g., 5432 → 6432)
    let (web_host, web_reason) = config::resolve_web_bind(
        config.auth.auth_type,
        &config.server.bind_address,
        config.web.bind_address.as_deref(),
    )?;
    let web_addr = format!("{web_host}:{web_port}");
    if serves_clients {
        tracing::info!(
            target: "arneb::config",
            web_bind_address = %web_addr,
            reason = web_reason,
            // Credentials exist only under password auth, so only then can
            // `/metrics` require them.
            metrics_auth = config.web.metrics_auth && password_users.is_some(),
            "Web UI effective bind ([web] bind_address / ARNEB_WEB_BIND_ADDRESS) and \
             /metrics auth ([web] metrics_auth / ARNEB_WEB_METRICS_AUTH)"
        );
        if password_users.is_some() && !config::is_loopback_host(&web_host) {
            tracing::warn!(
                target: "arneb::config",
                web_bind_address = %web_addr,
                "the Web UI and API require sign-in under [auth] type = \"password\", but \
                 serves plain HTTP (credentials and query text are unencrypted) and is \
                 reachable beyond loopback"
            );
        }
    }

    // 11. Run services based on role
    let heartbeat_target = match role {
        ServerRole::Worker => HeartbeatTarget::from_config(&config, &rpc_addr),
        _ => None,
    };
    let (worker_state, worker_state_rx) =
        tokio::sync::watch::channel(arneb_rpc::WorkerState::Active);
    let flight_state_clone = flight_state.clone();
    let rpc_addr_clone = rpc_addr.clone();

    tokio::select! {
        // pgwire server (coordinator + standalone only)
        result = server.start(), if matches!(role, ServerRole::Coordinator | ServerRole::Standalone) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "pgwire server error");
                bail!("server error: {e}");
            }
        }
        // Flight RPC server (all roles)
        result = arneb_rpc::start_flight_server(&rpc_addr_clone, flight_state_clone) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "flight server error");
                bail!("flight server error: {e}");
            }
        }
        // Web UI HTTP server (coordinator + standalone only). Like the Trino
        // listener, a bind failure is logged but not fatal.
        _ = non_fatal_listener(
            async {
                let listener = tokio::net::TcpListener::bind(&web_addr).await?;
                tracing::info!(web_address = %web_addr, "web UI listening");
                axum::serve(listener, web_router).await
            },
            |e| {
                tracing::error!(
                    error = %e,
                    address = %web_addr,
                    "web UI listener failed; continuing without it"
                )
            },
        ), if serves_clients => {}
        // Trino client protocol. A bind failure (e.g. a local Trino already
        // on 8080) is logged but not fatal: pgwire keeps serving.
        _ = non_fatal_listener(
            async {
                match &trino_server {
                    Some(trino) => trino.start().await,
                    None => futures::future::pending().await,
                }
            },
            |e| {
                tracing::error!(
                    error = %e,
                    address = %trino_addr,
                    "Trino client protocol listener failed; continuing without it \
                     (set [trino] port / ARNEB_TRINO_PORT / --trino-port, or --no-trino)"
                )
            },
        ) => {}

        // Worker heartbeat loop
        _ = worker_heartbeat_loop(
            heartbeat_target.as_ref(),
            role,
            worker_state_rx,
            heartbeat.interval_secs,
        ) => {}
        // SIGTERM / Ctrl-C; a worker drains first (worker-graceful-drain).
        _ = run_until_shutdown(
            heartbeat_target.as_ref().zip(worker_tasks.as_ref()),
            &worker_state,
            heartbeat.interval_secs,
            drain_timeout_secs,
        ) => {}
    }

    Ok(())
}

/// Run an auxiliary listener (Web UI, Trino) whose failure must not stop
/// the server: an error is passed to `on_error` (which logs it) and the
/// future then never completes, so the `select!` in `main` keeps serving.
async fn non_fatal_listener<F>(listener: F, on_error: impl FnOnce(std::io::Error))
where
    F: std::future::Future<Output = std::io::Result<()>>,
{
    if let Err(e) = listener.await {
        on_error(e);
    }
    futures::future::pending::<()>().await
}

/// Where and as whom a worker heartbeats.
struct HeartbeatTarget {
    coordinator_address: String,
    message: arneb_rpc::HeartbeatMessage,
}

impl HeartbeatTarget {
    /// The worker's heartbeat target, or `None` (logged) without
    /// `[cluster] coordinator_address`.
    fn from_config(config: &AppConfig, my_rpc_addr: &str) -> Option<Self> {
        let Some(addr) = &config.cluster.coordinator_address else {
            tracing::error!("worker mode requires --coordinator-address");
            return None;
        };
        let worker_id = config
            .cluster
            .worker_id
            .clone()
            .unwrap_or_else(|| format!("worker-{}", uuid::Uuid::new_v4()));
        // Workers advertise a routable hostname:port to the coordinator
        // for inbound task RPCs. `bind_address` of "0.0.0.0" means listen
        // on all interfaces — useless to send back as a destination. Use
        // `advertised_address` if set (docker compose / k8s case), else
        // fall back to `bind_address:rpc_port` for same-host setups.
        let advertised = config
            .cluster
            .advertised_address
            .clone()
            .unwrap_or_else(|| my_rpc_addr.to_string());
        Some(Self {
            coordinator_address: format!("http://{addr}"),
            message: arneb_rpc::HeartbeatMessage::new(
                worker_id,
                format!("http://{advertised}"),
                256,
            ),
        })
    }

    /// Send one heartbeat reporting `state`, logging a failure.
    async fn send(&self, state: arneb_rpc::WorkerState) {
        let mut message = self.message.clone();
        message.state = state;
        let worker_id = &message.worker_id;
        match arneb_rpc::send_heartbeat_with(
            &self.coordinator_address,
            &message,
            arneb_rpc::cluster_security(),
        )
        .await
        {
            Ok(()) => {
                tracing::debug!(worker_id = %worker_id, ?state, "heartbeat sent");
            }
            Err(
                e @ (arneb_rpc::HeartbeatError::Unauthenticated(_)
                | arneb_rpc::HeartbeatError::Incompatible(..)),
            ) => {
                // Keep retrying: fixing the secret, or replacing the
                // coordinator with this worker's release, recovers without
                // restarting workers.
                tracing::error!(
                    target: "arneb::cluster",
                    worker_id = %worker_id,
                    error = %e,
                    "heartbeat rejected"
                );
            }
            Err(e) => {
                tracing::warn!(
                    worker_id = %worker_id,
                    error = %e,
                    "heartbeat failed"
                );
            }
        }
    }
}

/// Send a heartbeat every `interval_secs`, and at once whenever `state`
/// changes (a drain starting). Pending forever off workers; returns at once
/// on a worker with no heartbeat target, which stops the server.
async fn worker_heartbeat_loop(
    target: Option<&HeartbeatTarget>,
    role: ServerRole,
    mut state: tokio::sync::watch::Receiver<arneb_rpc::WorkerState>,
    interval_secs: u64,
) {
    if !matches!(role, ServerRole::Worker) {
        futures::future::pending::<()>().await;
    }
    let Some(target) = target else {
        return;
    };
    tracing::info!(
        worker_id = %target.message.worker_id,
        coordinator = %target.coordinator_address,
        "starting heartbeat loop"
    );
    loop {
        let current = *state.borrow_and_update();
        target.send(current).await;
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(interval_secs)) => {}
            changed = state.changed() => {
                if changed.is_err() {
                    futures::future::pending::<()>().await;
                }
            }
        }
    }
}

/// Wait for SIGTERM or Ctrl-C. On a worker, then drain: report `draining`,
/// wait until the coordinator has released every query here (or the drain
/// timeout passes), and report `stopped`. A second signal ends the drain.
async fn run_until_shutdown(
    worker: Option<(&HeartbeatTarget, &task_manager::TaskManager)>,
    state: &tokio::sync::watch::Sender<arneb_rpc::WorkerState>,
    interval_secs: u64,
    drain_timeout_secs: u64,
) {
    drain::shutdown_signal().await;
    let Some((target, tasks)) = worker else {
        tracing::info!("shutting down");
        return;
    };
    let worker_id = &target.message.worker_id;
    tracing::info!(
        target: "arneb::cluster",
        worker_id = %worker_id,
        active_queries = tasks.active_queries(),
        drain_timeout_secs,
        "worker draining"
    );
    state.send_replace(arneb_rpc::WorkerState::Draining);
    let started = std::time::Instant::now();
    let wait = drain::wait_until_idle(
        || tasks.active_queries(),
        std::time::Duration::from_secs(interval_secs),
        std::time::Duration::from_secs(drain_timeout_secs),
        std::time::Duration::from_millis(500),
    );
    tokio::select! {
        outcome = wait => match outcome {
            drain::DrainOutcome::Idle => tracing::info!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                drained_secs = started.elapsed().as_secs(),
                "worker drained"
            ),
            drain::DrainOutcome::TimedOut(left) => tracing::warn!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                active_queries = left,
                drain_timeout_secs,
                "drain timed out"
            ),
        },
        _ = drain::shutdown_signal() => {
            tracing::warn!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                active_queries = tasks.active_queries(),
                "second shutdown signal, exiting without finishing the drain"
            );
        }
    }
    // A `draining` heartbeat still in flight may land after this; the
    // coordinator ignores those once the worker has deregistered. Bounded:
    // an unreachable coordinator must not hold up the exit; it then drops
    // this worker at its heartbeat timeout.
    if tokio::time::timeout(
        std::time::Duration::from_secs(5),
        target.send(arneb_rpc::WorkerState::Stopped),
    )
    .await
    .is_err()
    {
        tracing::warn!(worker_id = %worker_id, "stopped heartbeat timed out");
    }
}

/// Mount one `type = "postgresql"` catalog. Like an unreachable Hive
/// Metastore, a bad config or unreachable database is logged and the
/// catalog skipped; the server keeps serving.
async fn register_postgresql_catalog(
    catalog_cfg: &config::CatalogConfig,
    catalog_manager: &CatalogManager,
    connector_registry: &mut ConnectorRegistry,
) {
    let pg_config = match catalog_cfg.postgres_config() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(catalog = %catalog_cfg.name, error = %e, "invalid postgresql catalog, skipping");
            return;
        }
    };
    let target = pg_config.display_target();
    match arneb_postgresql::PostgresCatalog::connect(&catalog_cfg.name, pg_config).await {
        Ok(catalog) => {
            let c = catalog.config();
            tracing::info!(
                catalog = %catalog_cfg.name,
                target = %target,
                max_connections = c.max_connections,
                split_min_rows = c.split_min_rows,
                max_splits = c.max_splits,
                unconstrained_numeric_scale = c.unconstrained_numeric_scale,
                connect_timeout_secs = c.connect_timeout_secs,
                pool_wait_timeout_secs = c.pool_wait_timeout_secs,
                domain_compaction_threshold = c.domain_compaction_threshold,
                "registered postgresql catalog"
            );
            connector_registry.register(&catalog_cfg.name, Arc::new(catalog.connector_factory()));
            catalog_manager.register_catalog(&catalog_cfg.name, Arc::new(catalog));
        }
        Err(e) => {
            tracing::warn!(
                catalog = %catalog_cfg.name,
                target = %target,
                error = %e,
                "failed to connect to postgresql, skipping catalog"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_auxiliary_listener_keeps_the_server_up() {
        let mut logged = None;
        let finished = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            non_fatal_listener(
                async { Err(std::io::Error::from(std::io::ErrorKind::AddrInUse)) },
                |e| logged = Some(e.kind()),
            ),
        )
        .await;
        assert!(finished.is_err(), "a failed listener must not end select!");
        assert_eq!(logged, Some(std::io::ErrorKind::AddrInUse));
    }
}
