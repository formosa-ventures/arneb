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
    GreedyMemoryPool, MemoryPool, QueryMemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use arneb_protocol::{ProtocolConfig, ProtocolServer};
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

    /// Emit per-operator wall-time tracing on the `arneb::profile`
    /// target. Equivalent to setting `arneb::profile=info` in
    /// `RUST_LOG`. The events show op name, partition, elapsed ms,
    /// row count, bytes, and batch count when an operator's output
    /// stream terminates — useful for narrowing down per-stage
    /// costs after `EXPLAIN ANALYZE` highlights a suspect operator.
    #[arg(long)]
    profile: bool,
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

    // 2. Load config (file + env overrides)
    let mut config =
        AppConfig::load(args.config.as_deref()).context("failed to load configuration")?;

    // 3. Apply CLI overrides
    if let Some(bind) = args.bind {
        config.server.bind_address = bind;
    }
    if let Some(port) = args.port {
        config.server.port = port;
    }
    config.cluster.role = args.role;
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
        .map(|c| c.default_schema.as_str())
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

    // 6.5. Register Hive catalogs from config
    for catalog_cfg in &config.catalogs {
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
    let node_registry = arneb_scheduler::NodeRegistry::default();
    let rpc_addr = format!("{}:{}", config.server.bind_address, config.cluster.rpc_port);
    let query_tracker = Arc::new(arneb_scheduler::QueryTracker::new());
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
                    registry.heartbeat(msg.worker_id, msg.flight_address, msg.max_splits);
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
                    eprintln!(
                        "[DFRPC] coord flight received_report query_id={} task_id={} partition_idx={} df_id={} domain={}",
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
    // Phase 4 (2026-05-21): wrap the global pool with a per-task
    // QueryMemoryPool when `query_max_memory_per_node` is set. This
    // closes the swiss-cheese gap where untracked operators
    // (Filter / Project / scan buffers / Repartition queue) accumulate
    // uncounted allocations and OOM the worker before any single
    // spillable operator hits its own `spill_budget_bytes`. With this
    // wrap, the QUERY's cumulative `MemoryReservation` total fails
    // the query cleanly instead.
    let query_cap_resolved = config.memory.resolve_query_cap();
    let memory_pool: Arc<dyn MemoryPool> = match query_cap_resolved {
        Some(query_cap) => Arc::new(QueryMemoryPool::new(global_pool, query_cap as usize)),
        None => global_pool,
    };
    let memory_pool: Arc<dyn MemoryPool> = if memory_profile::mem_profile_enabled() {
        Arc::new(memory_profile::MemoryProfilePool::new(memory_pool))
    } else {
        memory_pool
    };
    match (budget, query_cap_resolved) {
        (Some(bytes), Some(qcap)) => tracing::info!(
            spill_budget_bytes = bytes,
            spill_budget_source = budget_source,
            query_max_memory_per_node = qcap,
            "memory pool installed (per-task query cap on top of global spill budget)"
        ),
        (Some(bytes), None) => tracing::info!(
            spill_budget_bytes = bytes,
            spill_budget_source = budget_source,
            "memory pool installed (global spill budget only; no per-query cap)"
        ),
        (None, Some(qcap)) => tracing::info!(
            query_max_memory_per_node = qcap,
            "memory pool installed (per-task query cap only; no global spill budget)"
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
            let coord = coordinator::QueryCoordinator::new(
                node_registry.clone(),
                query_tracker.clone(),
                df_registry.clone(),
            );
            Some(Arc::new(coord))
        }
        _ => None,
    };

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
        let coord_address_for_publisher = config.cluster.coordinator_address.as_ref().map(|addr| {
            if addr.starts_with("http") {
                addr.clone()
            } else {
                format!("http://{addr}")
            }
        });
        let tm = task_manager::TaskManager::with_admission_gate(
            flight_state.clone(),
            catalog_manager.clone(),
            connector_registry.clone(),
            Arc::clone(&memory_pool),
            memory_probe,
            coord_address_for_publisher,
        );
        let tm_for_task = tm.clone();
        flight_state.set_task_callback(Arc::new(move |descriptor| {
            tm_for_task.handle_task(descriptor);
        }));
        let tm_for_status = tm.clone();
        flight_state.set_task_status_callback(Arc::new(move |task_id| {
            tm_for_status.task_status_as_response(task_id)
        }));
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
                eprintln!(
                    "[DFRPC] worker flight received_notify query_id={} task_id={} df_id={} domain={}",
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

    let mut server = ProtocolServer::new(
        protocol_config,
        catalog_manager.clone(),
        connector_registry.clone(),
    )
    .with_memory_pool(Arc::clone(&memory_pool));
    if let Some(ref executor) = distributed_executor {
        server = server.with_distributed_executor(Arc::clone(executor));
    }

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
    };
    let web_router = web::build_router(web_state);
    let web_port = config.server.port + 1000; // default: pgwire port + 1000 (e.g., 5432 → 6432)
    let web_addr = format!("{}:{}", config.server.bind_address, web_port);

    // 11. Run services based on role
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
        // Web UI HTTP server (coordinator + standalone only)
        result = async {
            let listener = tokio::net::TcpListener::bind(&web_addr).await?;
            tracing::info!(web_address = %web_addr, "web UI listening");
            axum::serve(listener, web_router).await
        }, if matches!(role, ServerRole::Coordinator | ServerRole::Standalone) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "web server error");
            }
        }
        // Worker heartbeat loop
        _ = worker_heartbeat_loop(role, &config, &rpc_addr) => {}
        // Graceful shutdown
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
        }
    }

    Ok(())
}

/// Periodically send heartbeat to coordinator (worker mode only).
async fn worker_heartbeat_loop(role: ServerRole, config: &AppConfig, my_rpc_addr: &str) {
    if !matches!(role, ServerRole::Worker) {
        // Non-workers just wait forever (this future is never selected).
        futures::future::pending::<()>().await;
        return;
    }

    let coordinator_address = match &config.cluster.coordinator_address {
        Some(addr) => format!("http://{addr}"),
        None => {
            tracing::error!("worker mode requires --coordinator-address");
            return;
        }
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
    let message = arneb_rpc::HeartbeatMessage {
        worker_id: worker_id.clone(),
        flight_address: format!("http://{advertised}"),
        max_splits: 256,
    };

    tracing::info!(
        worker_id = %worker_id,
        coordinator = %coordinator_address,
        "starting heartbeat loop"
    );

    loop {
        match arneb_rpc::send_heartbeat(&coordinator_address, &message).await {
            Ok(()) => {
                tracing::debug!(worker_id = %worker_id, "heartbeat sent");
            }
            Err(e) => {
                tracing::warn!(
                    worker_id = %worker_id,
                    error = %e,
                    "heartbeat failed"
                );
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
