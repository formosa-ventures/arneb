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

mod catalogs;
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

    /// Let `[[auth.users]]` overwrite users created through the state store
    /// whose name collides with a config user (revoking their tokens).
    /// Config-seeded users follow the config on every start without it.
    /// Also re-seeds the rest of the state from the config file, like
    /// `POST /api/v2/state/reseed`: the stored access rules become the
    /// `[access_control] rules_file` (none when unset), the stored catalogs
    /// become `[[catalogs]]` (runtime-created ones are removed), the
    /// runtime settings are cleared so the config values apply, and user
    /// tombstones are cleared. Only meaningful with `[auth] type = "password"`.
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
    let args_config_path = args.config.clone();

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
    // configured the rest of the filter; `rmcp=warn` is appended the same
    // way (see `log_filter_directives`).
    let rust_log = std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).ok();
    let env_filter = tracing_subscriber::EnvFilter::new(log_filter_directives(
        rust_log.as_deref(),
        args.profile,
    ));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
    // Validate [auth] on every role (workers never open the state store).
    let (config_auth, verifiers) = config
        .auth
        .to_auth_method()
        .context("invalid [auth] configuration")?;
    if matches!(config_auth, arneb_protocol::AuthMethod::ScramSha256(_)) {
        // Fail startup, not the first unknown-user login, if the RNG is broken.
        arneb_protocol::init_decoy_secret().context("cannot create the decoy secret for [auth]")?;
    }
    // Cluster RPC security, read here so the startup checks below know
    // whether catalog secrets may be served; installed further down.
    let cluster_security = config
        .cluster
        .security(|k| std::env::var(k).ok())
        .context("invalid [cluster] security configuration")?;
    let has_cluster_secret = cluster_security.secret.is_some();
    // Config checks that can refuse the start run before the state store
    // below is written, so a refused start leaves the state untouched.
    let heartbeat = config
        .cluster
        .heartbeat(|k| std::env::var(k).ok())
        .context("invalid [cluster] heartbeat configuration")?;
    let drain_timeout_secs = config
        .cluster
        .drain_timeout_secs(|k| std::env::var(k).ok())
        .context("invalid [cluster] drain_timeout_secs")?;
    config::check_cluster_transport(
        cluster_security.secret.is_some(),
        cluster_security.tls.is_some(),
        &config.server.bind_address,
    )?;
    // Parsed once here, before the state write, and used further down.
    let query_retry = match role {
        ServerRole::Coordinator => config
            .cluster
            .query_retry(|k| std::env::var(k).ok())
            .context("invalid [cluster] retry_policy")?,
        _ => None,
    };
    // Client-facing TLS ([tls] / ARNEB_TLS_*): one certificate for pgwire and
    // the Trino listener. Workers serve neither, so they skip loading it.
    let server_tls = match (&config.tls_settings, role) {
        (Some(t), ServerRole::Coordinator | ServerRole::Standalone) => Some(
            ServerTls::load(&t.cert_path, &t.key_path).context("invalid [tls] configuration")?,
        ),
        _ => None,
    };
    let serves_clients = matches!(role, ServerRole::Coordinator | ServerRole::Standalone);
    // Fail closed: Trino clients only send passwords over HTTPS, so password
    // auth on a plain-HTTP listener would be unusable or a bypass.
    let trino_enabled = config.trino.enabled && serves_clients;
    config::check_trino_transport(
        trino_enabled,
        matches!(config_auth, arneb_protocol::AuthMethod::ScramSha256(_)),
        server_tls.is_some(),
    )?;
    let (web_host, web_reason) = config::resolve_web_bind(
        config.auth.auth_type,
        &config.server.bind_address,
        config.web.bind_address.as_deref(),
    )?;
    let file_tables = parse_file_tables(&config.tables)?;
    // Password auth on a coordinator/standalone: users live in the state
    // store; config-seeded users follow the config file on every start
    // (a changed hash or a removed user revokes tokens). The same store
    // serves pgwire, Trino and the Web API.
    let (state_store, prepared) =
        if matches!(role, ServerRole::Coordinator | ServerRole::Standalone)
            && matches!(config_auth, arneb_protocol::AuthMethod::ScramSha256(_))
        {
            let store = arneb_state::StateStore::open(config.state.path.as_deref())
                .context("cannot open the state store ([state] path / ARNEB_STATE_PATH)")?;
            let (report, prepared) = prepare_state_store(
                &store,
                &config,
                &verifiers,
                &role,
                has_cluster_secret,
                reseed_state,
            )?;
            tracing::info!(
                target: "arneb::config",
                state_path = %store
                    .path()
                    .map_or_else(|| "in-memory".to_owned(), |p| p.display().to_string()),
                persistent = store.is_persistent(),
                users = arneb_protocol::CredentialSource::user_count(&store),
                first_start = report.first_start,
                added = ?report.added,
                updated = ?report.updated,
                removed = ?report.removed,
                adopted = ?report.adopted,
                conflicts = ?report.conflicts,
                revoked_tokens = report.revoked_tokens,
                reseed = reseed_state,
                "state store effective settings ([state] path / ARNEB_STATE_PATH / --reseed-state)"
            );
            if !store.is_persistent() {
                tracing::warn!(
                    target: "arneb::config",
                    "no [state] path: API tokens, worker cordons, admission limits and the audit \
                     log are lost on restart"
                );
            }
            (Some(Arc::new(store)), Some(prepared))
        } else {
            if reseed_state {
                tracing::info!(
                    target: "arneb::config",
                    "--reseed-state ignored: it only applies with [auth] type = \"password\" \
                     on a coordinator or standalone server"
                );
            }
            // No state store: the config's catalogs are what is served.
            check_catalog_exposure(
                &role,
                has_cluster_secret,
                &config.storage,
                &config.catalogs,
                false,
                config.cluster.rpc_port,
            )?;
            (None, None)
        };
    // 5. Create catalog manager + connector registry. The default catalog
    // and schema come from the catalogs this node will serve (the state
    // store's under password auth), so they never name a catalog that is
    // not defined (see `default_catalog_and_schema`).
    let served_specs = match role {
        // Workers ignore [[catalogs]] and resolve with the coordinator's
        // pushed defaults (Ruling R12).
        ServerRole::Worker => Vec::new(),
        _ => match &prepared {
            Some(p) => p.served.clone(),
            None => config.catalogs.clone(),
        },
    };
    let (default_catalog, default_schema) =
        default_catalog_and_schema(&config.catalogs, &served_specs, !config.tables.is_empty());
    tracing::info!(
        target: "arneb::config",
        default_catalog = %default_catalog,
        default_schema = %default_schema,
        "default catalog and schema (the first [[catalogs]] entry if served, else the \
         first served catalog by name, else file or memory)"
    );
    let catalog_manager = CatalogManager::new(default_catalog, default_schema);

    // Register memory catalog (empty)
    let mem_schema = Arc::new(MemorySchema::new());
    let mem_catalog = Arc::new(MemoryCatalog::new());
    mem_catalog.register_schema("default", mem_schema);
    let mem_factory = MemoryConnectorFactory::new(mem_catalog.clone(), "default");
    catalog_manager.register_catalog("memory", mem_catalog);

    let connector_registry = ConnectorRegistry::new();
    connector_registry.register("memory", Arc::new(mem_factory));

    // 6. Register file tables from config
    let global_cloud_config = config.storage.to_cloud_config();
    let file_storage_registry = Arc::new(StorageRegistry::with_config(global_cloud_config.clone()));
    let table_count = config.tables.len();
    if !config.tables.is_empty() {
        let file_factory = Arc::new(FileConnectorFactory::new(file_storage_registry.clone()));

        for (table, format, schema) in file_tables {
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

    // 7. Create protocol server
    let listen_addr = format!("{}:{}", config.server.bind_address, config.server.port);
    let protocol_config = ProtocolConfig {
        bind_address: listen_addr.clone(),
    };

    let catalog_manager = Arc::new(catalog_manager);
    let connector_registry = Arc::new(connector_registry);

    // 6.5. External catalogs (Hive, with Iceberg tables in the same
    // metastore redirected to the Iceberg reader, and PostgreSQL), kept live
    // so they can change at runtime. The coordinator and standalone build
    // them once the state store is open (runtime-config Rulings R2, R4).
    // Workers ignore their own [[catalogs]]: they start empty at generation
    // 0 and take the coordinator's set over Flight (D8). An unreachable
    // catalog is skipped with a WARN; the server keeps serving.
    // Ruling R12: a worker builds the coordinator's effective specs (its
    // global [storage] already merged in), so it merges over no storage of
    // its own.
    let catalog_storage = match role {
        ServerRole::Worker => config::StorageConfig::default(),
        _ => config.storage.clone(),
    };
    let live_catalogs = catalogs::LiveCatalogs::new(
        catalog_manager.clone(),
        connector_registry.clone(),
        catalog_storage,
    );
    if matches!(role, ServerRole::Worker) {
        start_worker_catalogs(&live_catalogs, &config.catalogs);
    } else {
        // Ruling R6: a fresh epoch per start, so workers replace their set
        // after a coordinator restart even when its generation went back.
        live_catalogs.set_epoch(coordinator_epoch());
    }

    // 8. Set up Flight RPC server + heartbeat handling
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
    tracing::info!(
        target: "arneb::config",
        cluster_auth = if cluster_security.secret.is_some() { "secret" } else { "off" },
        cluster_tls = if cluster_security.tls.is_some() { "on" } else { "off" },
        "cluster RPC security effective mode ([cluster] secret / [cluster.tls] / ARNEB_CLUSTER_*)"
    );
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
            // Coordinator receives heartbeats from workers. The reply names
            // the live catalog generation; a worker holding another one pulls
            // the set with `get_catalogs` (runtime-config D8).
            let registry = node_registry.clone();
            let live = live_catalogs.clone();
            // Standalone serves no `get_catalogs`, so it names no generation.
            let serves_catalogs = matches!(role, ServerRole::Coordinator);
            let mut state = arneb_rpc::FlightState::with_heartbeat_callback(std::sync::Arc::new(
                move |msg: arneb_rpc::HeartbeatMessage| {
                    if msg.state == arneb_rpc::WorkerState::Stopped {
                        registry.deregister(&msg.worker_id);
                        return Ok(arneb_rpc::HeartbeatReply::default());
                    }
                    registry.admit(
                        msg.worker_id,
                        msg.flight_address,
                        msg.max_splits,
                        &msg.version,
                        msg.state == arneb_rpc::WorkerState::Draining,
                    )?;
                    Ok(if serves_catalogs {
                        arneb_rpc::HeartbeatReply {
                            catalog_generation: Some(live.generation()),
                            catalog_epoch: Some(live.epoch()),
                        }
                    } else {
                        arneb_rpc::HeartbeatReply::default()
                    })
                },
            ));
            // Only a coordinator has workers to serve its catalogs to; the
            // set carries raw secrets, so standalone does not expose it.
            if matches!(role, ServerRole::Coordinator) {
                let live = live_catalogs.clone();
                state.set_catalog_source(Arc::new(move || live.snapshot()));
            }
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
            let mut state = arneb_rpc::FlightState::new();
            // runtime-config D8: apply a catalog set the coordinator pushes.
            let live = live_catalogs.clone();
            state.set_catalog_sync_callback(Arc::new(move |set: arneb_rpc::CatalogSet| {
                let live = live.clone();
                Box::pin(async move { apply_pushed_catalogs(&live, set).await })
            }));
            state
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
            let retry = query_retry;
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

    // Where and as whom a worker heartbeats; built here so the task
    // manager's catalog puller shares the worker id and coordinator.
    let heartbeat_target = match role {
        ServerRole::Worker => HeartbeatTarget::from_config(&config, &rpc_addr),
        _ => None,
    };
    // runtime-config D8: the heartbeat loop and the task manager pull the
    // coordinator's catalogs through one single-flight puller. None off
    // workers (standalone has no workers).
    let catalog_puller = heartbeat_target.as_ref().map(|target| {
        catalogs::CatalogPuller::from_coordinator(
            target.coordinator_address.clone(),
            live_catalogs.clone(),
        )
    });
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
        let tm = match (&catalog_puller, &heartbeat_target) {
            (Some(puller), Some(target)) => {
                tm.with_catalog_puller(puller.clone(), target.message.worker_id.clone())
            }
            _ => tm,
        };
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

    // manage-api D8: stored cordons apply before the Flight server below
    // accepts heartbeats, so a cordoned worker never gets a query on rejoin.
    if let Some(store) = &state_store {
        let count = web::restore_worker_cordons(store, &node_registry);
        tracing::info!(target: "arneb::config", count, "restored worker cordons");
    }
    // runtime-config D1, R4: under password auth the coordinator or
    // standalone server seeds the state store from [[catalogs]] once and
    // serves the stored definitions from then on; without a state store it
    // serves the config's.
    if matches!(role, ServerRole::Coordinator | ServerRole::Standalone) {
        load_catalogs(
            state_store.as_deref(),
            &live_catalogs,
            &config.catalogs,
            // `prepare_state_store` already applied --reseed-state.
            false,
        )
        .await?;
    }
    // Before the Flight server starts: without a [cluster] secret,
    // get_catalogs and sync_catalogs are open to any peer.
    if matches!(role, ServerRole::Coordinator) && !has_cluster_secret {
        // The startup check already refused served secrets; from now on the
        // catalog API and the re-seed refuse a spec that would add one.
        live_catalogs.set_unauthenticated_flight(true);
    }
    let auth_method = match &state_store {
        Some(store) => arneb_protocol::AuthMethod::ScramSha256(store.clone()),
        None => config_auth,
    };
    // runtime-config D1: under password auth the rules file seeds the state
    // store once and the stored copy wins; the handle is shared by every
    // protocol and the Web API, so a runtime change applies everywhere.
    let (access_rules, rules_source, rules_seed) =
        if matches!(role, ServerRole::Coordinator | ServerRole::Standalone) {
            // `prepare_state_store` already applied --reseed-state.
            let resolved = config::resolve_access_rules(
                &config.access_control,
                state_store.as_deref(),
                false,
                prepared.map(|p| p.rules_file),
            )
            .context("invalid [access_control] configuration")?;
            (resolved.handle, resolved.source, resolved.seed)
        } else {
            (arneb_catalog::RulesHandle::default(), "config", None)
        };
    if let Some(rules) = access_rules.current() {
        let [catalogs, schemas, tables, queries, system_information] = rules
            .section_sizes()
            .map(|n| n.map_or_else(|| "absent (allow all)".to_owned(), |n| n.to_string()));
        let rules_file = config
            .access_control
            .rules_file
            .as_ref()
            .map_or_else(|| "unset".to_owned(), |p| p.display().to_string());
        tracing::info!(
            target: "arneb::config",
            rules_file = %rules_file,
            source = rules_source,
            seed = ?rules_seed,
            catalogs = %catalogs,
            schemas = %schemas,
            tables = %tables,
            queries = %queries,
            system_information = %system_information,
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
    // Only the coordinator's Trino and Web listeners check Basic credentials
    // (workers serve neither). They share one PBKDF2 limiter and success
    // cache, but each gets its own wait queue (`sibling`), so a flood on one
    // port cannot 503 the other.
    let basic_auth = password_users
        .clone()
        .filter(|_| serves_clients)
        .map(arneb_protocol::BasicAuthenticator::new)
        .transpose()
        .context("HTTP Basic authentication")?;
    // The audit log (manage-api D6): `<state file name>.audit.jsonl` next to
    // the state file, in memory without one, and an empty in-memory log under `none`
    // (nothing is audited there).
    let audit_path = state_store
        .as_ref()
        .and_then(|s| s.path())
        .map(arneb_state::AuditLog::sibling_of);
    let audit_log = Arc::new(
        arneb_state::AuditLog::open(audit_path.as_deref(), config.audit.settings()).context(
            "cannot open the audit log (<state file name>.audit.jsonl next to [state] path)",
        )?,
    );
    // Kept for the shutdown flush; the web state takes `audit_log`.
    let audit_for_shutdown = Arc::clone(&audit_log);
    if matches!(role, ServerRole::Coordinator | ServerRole::Standalone) {
        // Retries an unwritable log in the background, so a completion whose
        // write failed reaches the file once the disk recovers.
        web::spawn_audit_retry(Arc::clone(&audit_log), web::AUDIT_RETRY_INTERVAL);
        tracing::info!(
            target: "arneb::config",
            path = %audit_path
                .as_ref()
                .map_or_else(|| "in-memory".to_owned(), |p| p.display().to_string()),
            max_entries = config.audit.max_entries,
            retention_days = config.audit.retention_days,
            loaded = audit_log.len(),
            "audit effective settings ([audit] / ARNEB_AUDIT_*)"
        );
        if let Some(legacy) = state_store
            .as_ref()
            .and_then(|s| s.path())
            .and_then(arneb_state::AuditLog::legacy_sibling_of)
        {
            tracing::warn!(
                target: "arneb::audit",
                legacy = %legacy.display(),
                path = %audit_path.as_ref().map_or_else(String::new, |p| p.display().to_string()),
                "an audit log under the earlier name exists and is not read: its entries do \
                 not appear in GET /api/v2/audit; keep or archive it (it is not migrated)"
            );
        }
        if state_store.as_ref().is_some_and(|s| !s.is_persistent()) {
            tracing::warn!(
                target: "arneb::config",
                "the audit log is kept in memory (no [state] path): its entries are lost on \
                 restart"
            );
        }
    }
    let state_for_web = state_store.clone();
    let credentials_for_web: Option<Arc<dyn arneb_protocol::CredentialSource>> =
        state_store.clone().map(|s| s as _);

    // Privilege layers (manage-api D1). Admin users are only meaningful
    // under password auth; say so rather than silently ignoring them.
    if !matches!(role, ServerRole::Worker) {
        let admins = &config.access_control.admin_users;
        tracing::info!(
            target: "arneb::config",
            admin_users = %if admins.is_empty() { "none".to_owned() } else { admins.join(",") },
            auth = if password_users.is_some() { "password" } else { "none" },
            "manage privileges effective settings ([access_control] admin_users / ARNEB_ADMIN_USERS)"
        );
        match &credentials_for_web {
            None if !admins.is_empty() => tracing::warn!(
                target: "arneb::config",
                "[access_control] admin_users is set but [auth] type = \"none\": manage \
                 actions need an authenticated user, so it has no effect"
            ),
            Some(creds) => {
                let unknown: Vec<&str> = admins
                    .iter()
                    .filter(|u| creds.scram_verifiers(u).is_empty())
                    .map(String::as_str)
                    .collect();
                if !unknown.is_empty() {
                    tracing::warn!(
                        target: "arneb::config",
                        "[access_control] admin_users names users that are not in the state \
                         store: {}",
                        unknown.join(", ")
                    );
                }
            }
            None => {}
        }
    }
    let authorizer = web::privilege::Authorizer::new(
        access_rules.clone(),
        config.access_control.admin_users.clone(),
        password_users.is_some(),
    );

    // admission-control D5/D8: one controller for the process, shared by
    // pgwire, the Trino listener, the agent API and the Web UI. Workers have
    // none; their admission stays the RSS probe, and this cap bounds what
    // reaches them. manage-api D9: the controller is built whenever SQL is
    // served, also for `max_running_queries = 0` (unlimited), so the limits
    // can change at runtime; a value stored in the state store wins over
    // the config seed.
    let serves_sql = matches!(role, ServerRole::Coordinator | ServerRole::Standalone);
    let admission_config = &config.admission;
    let resolved_admission = config::resolve_admission_limits(
        admission_config,
        state_store.as_ref().and_then(|s| s.admission_limits()),
    );
    let admission = serves_sql.then(|| {
        arneb_scheduler::AdmissionController::new(
            resolved_admission.limits.max_running_queries,
            resolved_admission.limits.max_queued_queries,
        )
    });
    if let Some(controller) = &admission {
        controller.spawn_snapshot_logger(std::time::Duration::from_secs(10));
    }
    if serves_sql {
        for (knob, from_config, from_state) in &resolved_admission.diverging {
            tracing::warn!(
                target: "arneb::config",
                "[admission] {knob} / ARNEB_{} = {from_config} is overridden by the runtime \
                 value {from_state} in the state store (DELETE /api/v2/admission returns to \
                 the config, as does a re-seed: POST /api/v2/state/reseed?dry_run=true \
                 previews it)",
                knob.to_ascii_uppercase()
            );
        }
        tracing::info!(
            target: "arneb::config",
            max_running_queries = resolved_admission.limits.max_running_queries,
            max_queued_queries = resolved_admission.limits.max_queued_queries,
            admission_source = resolved_admission.source,
            statement_timeout = %arneb_common::duration::format_pg_time_limit(
                admission_config.statement_timeout
            ),
            query_max_memory_per_node = %query_memory_limit
                .map_or_else(|| "off".to_owned(), |b| b.to_string()),
            "admission effective settings ([admission] / ARNEB_MAX_RUNNING_QUERIES / \
             ARNEB_MAX_QUEUED_QUERIES / ARNEB_STATEMENT_TIMEOUT / ARNEB_QUERY_MAX_MEMORY_BYTES)"
        );
        let agent_limits = config.agent_api.limits();
        if let Some(configured) = config.agent_api.user_share_raised_from {
            tracing::warn!(
                target: "arneb::config",
                configured,
                effective = agent_limits.max_user_retained_bytes,
                "[agent_api] max_user_retained_bytes (ARNEB_AGENT_MAX_USER_RETAINED_BYTES) \
                 is below max_retained_bytes; raised to it so a user can retain one full \
                 statement"
            );
        }
        tracing::info!(
            target: "arneb::config",
            default_max_rows = agent_limits.default_max_rows,
            max_rows = agent_limits.max_rows,
            default_page_bytes = agent_limits.default_page_bytes,
            max_page_bytes = agent_limits.max_page_bytes,
            max_cell_bytes = agent_limits.max_cell_bytes,
            max_retained_bytes = agent_limits.max_retained_bytes,
            max_user_retained_bytes = agent_limits.max_user_retained_bytes,
            max_total_retained_bytes = agent_limits.max_total_retained_bytes,
            max_wait_ms = config.agent_api.max_wait_ms,
            idle_timeout_secs = config.agent_api.idle_timeout_secs,
            "agent_api effective settings ([agent_api] / ARNEB_AGENT_*)"
        );
        tracing::info!(
            target: "arneb::config",
            enabled = config.mcp.enabled,
            allowed_hosts = %{
                let hosts = web::mcp::resolve_allowed_hosts(
                    &web::mcp::McpSettings::from(&config.mcp),
                    password_users.is_some(),
                );
                if hosts.is_empty() { "any".to_owned() } else { hosts.join(",") }
            },
            allowed_origins = %{
                let origins = web::mcp::resolve_allowed_origins(
                    &web::mcp::McpSettings::from(&config.mcp),
                );
                if origins.is_empty() { "none".to_owned() } else { origins.join(",") }
            },
            default_page_bytes = config.mcp.default_page_bytes,
            "mcp effective settings ([mcp] / ARNEB_MCP_*)"
        );
        // runtime-config D1/D9: a threshold stored at runtime wins.
        let slow_query = config::resolve_slow_query_threshold(
            config.observability.slow_query_threshold_ms,
            state_store
                .as_ref()
                .and_then(|s| s.slow_query_threshold_ms()),
        );
        if let Some((from_config, from_state)) = slow_query.diverging {
            tracing::warn!(
                target: "arneb::config",
                "[observability] slow_query_threshold_ms / ARNEB_SLOW_QUERY_THRESHOLD_MS = \
                 {from_config} is overridden by the runtime value {from_state} in the state \
                 store. Preview a re-seed with POST /api/v2/state/reseed?dry_run=true"
            );
        }
        arneb_common::metrics::set_slow_query_threshold(config::slow_query_threshold_from_ms(
            slow_query.ms,
        ));
        tracing::info!(
            target: "arneb::config",
            slow_query_threshold_ms = slow_query.ms,
            slow_query_source = slow_query.source,
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
    let trino_addr = format!("{}:{}", config.server.bind_address, config.trino.port);
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
        .with_authenticator(basic_auth.as_ref().map(|a| a.sibling()));
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
        admission_changes: Default::default(),
        cordon_changes: Default::default(),
        max_running_queries: config.admission.max_running_queries,
        max_queued_queries: config.admission.max_queued_queries,
        memory_pool: memory_pool.clone(),
        memory_limit_bytes: budget.map(|b| b as u64),
        // Same source as `credentials_for_web` (the state store) whenever
        // that is set: password auth makes the state store the auth method.
        basic: credentials_for_web
            .as_ref()
            .and(basic_auth.as_ref().map(|a| a.sibling())),
        credentials: credentials_for_web,
        state: state_for_web,
        audit: audit_log,
        rules: access_rules.clone(),
        config_changes: Default::default(),
        // Re-seed reloads the same file the process started with (Ruling R3).
        config_path: args_config_path,
        // Workers serve no Web API; the catalog routes run on the node
        // that owns the catalog definitions (runtime-config D7).
        catalogs: (!matches!(role, ServerRole::Worker)).then(|| live_catalogs.clone()),
        authorizer,
        metrics_auth: config.web.metrics_auth,
        sessions: web::SessionStore::new(),
        mcp: (serves_clients && agent_service.is_some() && config.mcp.enabled)
            .then(|| web::mcp::McpSettings::from(&config.mcp)),
        agent: agent_service,
    };
    let web_router = web::build_router(web_state);
    let web_port = config.server.port + 1000; // default: pgwire port + 1000 (e.g., 5432 → 6432)
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
            catalog_puller.as_ref(),
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
    // Completions whose write failed get one last try before exiting.
    web::flush_audit(audit_for_shutdown).await;

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

    /// Send one heartbeat reporting `state`, logging a failure. Returns
    /// the coordinator's reply, or `None` on a failure.
    async fn send(&self, state: arneb_rpc::WorkerState) -> Option<arneb_rpc::HeartbeatReply> {
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
            Ok(reply) => {
                tracing::debug!(worker_id = %worker_id, ?state, "heartbeat sent");
                return Some(reply);
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
        None
    }
}

/// Send a heartbeat every `interval_secs`, and at once whenever `state`
/// changes (a drain starting). A reply naming another catalog generation
/// than the applied one starts a pull in the background, so a slow pull
/// never delays a heartbeat. Pending forever off workers; returns at once
/// on a worker with no heartbeat target, which stops the server.
async fn worker_heartbeat_loop(
    target: Option<&HeartbeatTarget>,
    catalogs: Option<&catalogs::CatalogPuller>,
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
        if let (Some(reply), Some(puller)) = (target.send(current).await, catalogs) {
            let puller = puller.clone();
            tokio::spawn(async move { puller.after_heartbeat(&reply).await });
        }
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

/// A worker's catalogs at startup (runtime-config D8): none. Its own
/// `[[catalogs]]` are ignored, with one INFO naming them; the set comes from
/// the coordinator. Returns the ignored names.
fn start_worker_catalogs(
    live: &catalogs::LiveCatalogs,
    specs: &[config::CatalogConfig],
) -> Vec<String> {
    let ignored: Vec<String> = specs.iter().map(|c| c.name.clone()).collect();
    if !ignored.is_empty() {
        tracing::info!(
            target: "arneb::config",
            catalogs = ?ignored,
            "[[catalogs]] is ignored on a worker: catalogs come from the coordinator"
        );
    }
    debug_assert_eq!(live.generation(), 0, "a worker starts at generation 0");
    ignored
}

/// The coordinator epoch (Ruling R6): the start time in Unix milliseconds
/// in the high bits and 16 random bits (the system RNG the Web sessions
/// use) below, so a later start has a larger epoch and a worker can refuse
/// sets from an older one. Never 0.
fn coordinator_epoch() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    epoch_from(millis, {
        let mut bytes = [0u8; 2];
        aws_lc_rs::rand::fill(&mut bytes).expect("system random number generator failed");
        u16::from_le_bytes(bytes)
    })
}

/// `(millis << 16) | random`, never 0.
fn epoch_from(millis: u64, random: u16) -> u64 {
    ((millis << 16) | u64::from(random)).max(1)
}

/// Cluster RPC carries the catalog definitions (runtime-config D8): a
/// coordinator serves them, raw secrets included, with `get_catalogs`, and
/// a worker replaces its set on `sync_catalogs`. Without a `[cluster]
/// secret` both are open to any peer that reaches the RPC port, so a
/// coordinator refuses to start while any catalog it would serve (`served`,
/// built or not, with `global_storage` merged into hive specs) carries a
/// secret, and otherwise WARNs; a worker WARNs. Runs before anything is
/// written to the state store. `stored`: the specs come from the state
/// store (already seeded), so removing a secret from the config alone does
/// not help, and the message says so.
fn check_catalog_exposure(
    role: &ServerRole,
    has_cluster_secret: bool,
    global_storage: &config::StorageConfig,
    served: &[config::CatalogConfig],
    stored: bool,
    rpc_port: u16,
) -> Result<()> {
    if has_cluster_secret {
        return Ok(());
    }
    match role {
        ServerRole::Coordinator => {
            let exposed = catalogs::effective_secrets(global_storage, served);
            if !exposed.is_empty() {
                let remedy = if stored {
                    "or, since these catalogs are stored in the state store, remove the \
                     secrets through PUT /api/v2/catalog-configs on a server with a secret, or \
                     remove them from [[catalogs]] and restart once with --reseed-state (the \
                     users, catalogs, rules and settings become the config's)"
                } else {
                    "or remove these secrets from the catalogs"
                };
                bail!(
                    "refusing to start: the coordinator serves its catalog definitions to \
                     workers over cluster RPC (port {rpc_port}), and without a [cluster] \
                     secret any peer that reaches it can read them, secrets included ({}). \
                     Set [cluster] secret (or ARNEB_CLUSTER_SECRET, at least 32 characters) \
                     on every node, {remedy}",
                    exposed.join(", ")
                );
            }
            tracing::warn!(
                target: "arneb::config",
                rpc_port,
                "no [cluster] secret: cluster RPC is unauthenticated, so any peer that reaches \
                 the RPC port can read the catalog definitions (get_catalogs) and replace or \
                 wipe a worker's catalogs (sync_catalogs). Catalogs that carry a secret are \
                 refused until [cluster] secret (ARNEB_CLUSTER_SECRET) is set on every node"
            );
        }
        ServerRole::Worker => tracing::warn!(
            target: "arneb::config",
            rpc_port,
            "no [cluster] secret: cluster RPC is unauthenticated, so any peer that reaches \
             the RPC port can replace or wipe this worker's catalogs (sync_catalogs). Set \
             [cluster] secret (ARNEB_CLUSTER_SECRET) on every node"
        ),
        // Standalone serves no catalogs over RPC.
        ServerRole::Standalone => {}
    }
    Ok(())
}

/// Startup with a state store (runtime-config D1, D2). Every fatal check
/// runs first, against the state that would be served, so a refused start
/// writes nothing: the rules file must load; rules changed at runtime must
/// not meet a rules file edited since its seed (Ruling R14), and must still
/// parse when they stay in force; and a
/// coordinator without a `[cluster] secret` must not serve a catalog
/// secret ([`check_catalog_exposure`]). Then `reseed` replaces the users,
/// rules, settings and catalogs with the config's in one write
/// ([`arneb_state::StateStore::reseed`]); otherwise the config users are
/// applied (catalogs and rules are seeded or compared later, by
/// `load_catalogs` and `resolve_access_rules`). Returns what user seeding
/// did.
fn prepare_state_store(
    store: &arneb_state::StateStore,
    config: &config::AppConfig,
    verifiers: &[(String, arneb_protocol::ScramVerifier)],
    role: &ServerRole,
    has_cluster_secret: bool,
    reseed: bool,
) -> Result<(arneb_state::SeedReport, PreparedState)> {
    let rules = config
        .access_control
        .load_rules_text()
        .context("invalid [access_control] configuration")?;
    let rules_text = rules.as_ref().map(|(text, _)| text.as_str());
    match store.preview_seed_rules(rules_text, reseed) {
        arneb_state::RulesSeed::FileChanged => {
            return Err(config::rules_file_changed(&config.access_control));
        }
        // The stored (runtime) rules stay in force: they must still parse.
        arneb_state::RulesSeed::Diverged => {
            if let Some(stored) = store.access_rules() {
                arneb_catalog::AccessRules::from_json(&stored.json)
                    .map_err(|e| anyhow::anyhow!("the access rules in the state store: {e}"))?;
            }
        }
        _ => {}
    }
    let stored = store.seeded().catalogs && !reseed;
    let served = served_catalog_specs(Some(store), &config.catalogs, reseed);
    check_catalog_exposure(
        role,
        has_cluster_secret,
        &config.storage,
        &served,
        stored,
        config.cluster.rpc_port,
    )?;
    if !reseed {
        let report = store
            .seed_users(verifiers, false)
            .context("cannot seed the state store from [auth] users")?;
        return Ok((
            report,
            PreparedState {
                served,
                rules_file: rules,
            },
        ));
    }
    let config_specs = config
        .catalogs
        .iter()
        .map(|c| Ok((c.name.clone(), serde_json::to_value(c)?)))
        .collect::<Result<Vec<_>, serde_json::Error>>()
        .context("cannot serialize [[catalogs]]")?;
    let outcome = store
        .reseed(verifiers, rules_text, &config_specs)
        .context("cannot re-seed the state store from the config file")?;
    tracing::info!(
        target: "arneb::config",
        catalog_generation = outcome.catalog_generation,
        "--reseed-state: users, catalogs, access rules and settings replaced with the \
         config file's in one write"
    );
    Ok((
        outcome.users,
        PreparedState {
            served,
            rules_file: rules,
        },
    ))
}

/// What [`prepare_state_store`] read, so startup reads it only once.
struct PreparedState {
    /// The catalog specs this node will serve ([`served_catalog_specs`]).
    served: Vec<config::CatalogConfig>,
    /// The `[access_control] rules_file` text and its parsed rules.
    rules_file: Option<(String, arneb_catalog::AccessRules)>,
}

impl std::fmt::Debug for PreparedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let served: Vec<&str> = self.served.iter().map(|s| s.name.as_str()).collect();
        f.debug_struct("PreparedState")
            .field("served", &served)
            .field("rules_file", &self.rules_file.is_some())
            .finish()
    }
}

/// A `[[tables]]` entry, its format and, for CSV, its columns.
type FileTable<'a> = (
    &'a config::TableConfig,
    FileFormat,
    Option<Vec<arneb_common::types::ColumnInfo>>,
);

/// Each `[[tables]]` entry with its format and, for CSV, its schema. An
/// unsupported format, a CSV table without a schema or a bad column type
/// refuses the start; checked before the state store is written.
fn parse_file_tables(tables: &[config::TableConfig]) -> Result<Vec<FileTable<'_>>> {
    tables
        .iter()
        .map(|table| {
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
            Ok((table, format, schema))
        })
        .collect()
}

/// The external catalog specs this coordinator or standalone server will
/// serve: with a state store, the config's on a seeding start (first start
/// or `reseed`) and the stored ones otherwise (an undecodable record is
/// skipped, as `load_stored_catalogs` does); without one, the config's.
fn served_catalog_specs(
    store: Option<&arneb_state::StateStore>,
    config: &[config::CatalogConfig],
    reseed: bool,
) -> Vec<config::CatalogConfig> {
    match store {
        Some(store) if store.seeded().catalogs && !reseed => store
            .catalogs()
            .1
            .into_iter()
            .filter_map(|r| serde_json::from_value(r.spec).ok())
            .collect(),
        _ => config.to_vec(),
    }
}

/// The default catalog and schema (unqualified table names resolve in
/// them, and workers receive them with the catalog set): the first
/// `[[catalogs]]` entry when it is among the `served` specs (as before
/// runtime config), else the first served catalog by name, else `file`
/// when `[[tables]]` are configured, else `memory`. The schema is the
/// chosen catalog's `default_schema` (`public` for postgresql, `default`
/// otherwise; `default` for `file` and `memory`).
fn default_catalog_and_schema(
    config: &[config::CatalogConfig],
    served: &[config::CatalogConfig],
    has_tables: bool,
) -> (String, String) {
    let chosen = config
        .first()
        .and_then(|first| served.iter().find(|s| s.name == first.name))
        .or_else(|| served.iter().min_by(|a, b| a.name.cmp(&b.name)));
    match chosen {
        Some(spec) => (
            spec.name.clone(),
            spec.effective_default_schema().to_owned(),
        ),
        None if has_tables => ("file".to_owned(), "default".to_owned()),
        None => ("memory".to_owned(), "default".to_owned()),
    }
}

/// The `sync_catalogs` handler: applies a set the coordinator pushed. A
/// stale set (same epoch, not newer) is acknowledged unchanged. An applied
/// set is Ok even when some catalogs failed to build; their names are
/// returned (Ruling R7).
async fn apply_pushed_catalogs(
    live: &catalogs::LiveCatalogs,
    set: arneb_rpc::CatalogSet,
) -> Result<Vec<String>, String> {
    let (epoch, generation) = (set.epoch, set.generation);
    let report = live.apply_remote(set).await;
    if report.stale_epoch {
        // Not synced: the sender is not the coordinator this worker's
        // heartbeat follows (an old one's delayed push).
        return Err(catalogs::stale_epoch_error(epoch, live.current_epoch()));
    }
    if !report.applied {
        tracing::debug!(
            target: "arneb::cluster",
            generation,
            applied_generation = live.generation(),
            "ignored a catalog push that is not newer"
        );
        return Ok(Vec::new());
    }
    tracing::info!(
        target: "arneb::cluster",
        generation,
        catalogs = live.specs().len(),
        failed = report.failed.len(),
        "catalogs pushed by the coordinator applied"
    );
    Ok(report.failed.into_iter().map(|(name, _)| name).collect())
}

/// Loads the external catalogs: from the state store when one is open
/// (coordinator or standalone under password auth; Ruling R4), else from
/// the config's `[[catalogs]]` at generation 1. Workers never open a store.
async fn load_catalogs(
    store: Option<&arneb_state::StateStore>,
    live: &catalogs::LiveCatalogs,
    specs: &[config::CatalogConfig],
    reseed: bool,
) -> Result<()> {
    match store {
        Some(store) => load_stored_catalogs(store, live, specs, reseed).await,
        None => {
            load_config_catalogs(live, specs).await;
            Ok(())
        }
    }
}

/// Builds the config's `[[catalogs]]` as the live set at generation 1, so a
/// coordinator without a state store never advertises generation 0 (which
/// a fresh worker already holds; runtime-config Ruling R1).
async fn load_config_catalogs(live: &catalogs::LiveCatalogs, specs: &[config::CatalogConfig]) {
    let report = live
        .apply_set(CONFIG_CATALOG_GENERATION, specs.to_vec())
        .await;
    tracing::info!(
        target: "arneb::config",
        source = "config",
        catalogs = live.specs().len(),
        failed = report.failed.len(),
        generation = live.generation(),
        "external catalogs loaded"
    );
}

/// The generation of a catalog set built from the config file.
const CONFIG_CATALOG_GENERATION: u64 = 1;

/// Seeds the state store from `[[catalogs]]` (first start or `reseed`),
/// logs one WARN per config catalog that diverges from the stored set, and
/// builds the stored set as the live one.
async fn load_stored_catalogs(
    store: &arneb_state::StateStore,
    live: &catalogs::LiveCatalogs,
    specs: &[config::CatalogConfig],
    reseed: bool,
) -> Result<()> {
    let config_specs = specs
        .iter()
        .map(|c| Ok((c.name.clone(), serde_json::to_value(c)?)))
        .collect::<Result<Vec<_>, serde_json::Error>>()
        .context("cannot serialize [[catalogs]]")?;
    let report = store
        .seed_catalogs(&config_specs, reseed)
        .context("cannot seed the state store from [[catalogs]]")?;
    for (names, how) in [
        (&report.added, "is in the config but not in the state store"),
        (&report.changed, "differs from its copy in the state store"),
        (
            &report.removed,
            "was removed from the config but is still in the state store",
        ),
    ] {
        for name in names {
            tracing::warn!(
                target: "arneb::config",
                catalog = %name,
                "[[catalogs]] '{name}' {how}; the state store wins. Preview a re-seed with \
                 POST /api/v2/state/reseed?dry_run=true"
            );
        }
    }
    let (generation, records) = store.catalogs();
    let stored = records
        .into_iter()
        .filter_map(
            |r| match serde_json::from_value::<config::CatalogConfig>(r.spec) {
                Ok(spec) => Some(spec),
                Err(e) => {
                    // The serde error names a field, never the spec's values.
                    tracing::warn!(
                        target: "arneb::config",
                        catalog = %r.name,
                        error = %e,
                        "stored catalog definition is invalid, skipping"
                    );
                    None
                }
            },
        )
        .collect();
    let applied = live.apply_set(generation, stored).await;
    tracing::info!(
        target: "arneb::config",
        source = "state",
        seeded = report.seeded,
        catalogs = live.specs().len(),
        failed = applied.failed.len(),
        generation = live.generation(),
        "external catalogs loaded"
    );
    Ok(())
}

/// The tracing filter: `rust_log` (when set and valid, else `info`), then
/// `arneb::profile=info` under `--profile`, then `rmcp=warn` unless
/// `rust_log` has a directive whose target is exactly `rmcp` (such as
/// `rmcp=debug`). rmcp logs every tool result at debug, new API tokens
/// included, so a global `debug` must not reach it, and it logs three info
/// lines per stateless MCP request. A directive on the `rmcp` target itself
/// is the operator opting in; one on a module (`rmcp::transport=warn`)
/// keeps the cap, and tracing still applies it to that module because the
/// most specific directive wins.
fn log_filter_directives(rust_log: Option<&str>, profile: bool) -> String {
    let base = rust_log
        .map(str::trim)
        .filter(|s| !s.is_empty() && tracing_subscriber::EnvFilter::try_new(s).is_ok())
        .unwrap_or("info");
    let mut directives = base.to_owned();
    if profile {
        directives.push_str(",arneb::profile=info");
    }
    let opts_in = base
        .split(',')
        .any(|d| d.split(['=', '[']).next().unwrap_or("").trim() == "rmcp");
    if !opts_in {
        directives.push_str(",rmcp=warn");
    }
    directives
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `[[tables]]` refusals are pure and run before the state store is
    /// written, so a first start refused by one does not mark it seeded.
    #[test]
    fn file_tables_are_checked_before_anything_is_written() {
        let tables = |toml_text: &str| -> Vec<config::TableConfig> {
            #[derive(serde::Deserialize)]
            struct T {
                tables: Vec<config::TableConfig>,
            }
            toml::from_str::<T>(toml_text).unwrap().tables
        };
        let ok = tables(
            "[[tables]]\nname = \"a\"\npath = \"/a.parquet\"\nformat = \"parquet\"\n\
             [[tables]]\nname = \"b\"\npath = \"/b.csv\"\nformat = \"csv\"\n\
             schema = [{name = \"x\", type = \"int64\"}]",
        );
        let parsed = parse_file_tables(&ok).unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].2.is_none() && parsed[1].2.as_ref().unwrap().len() == 1);
        for (bad, says) in [
            (
                "[[tables]]\nname = \"a\"\npath = \"/a\"\nformat = \"orc\"",
                "unsupported table format 'orc'",
            ),
            (
                "[[tables]]\nname = \"b\"\npath = \"/b\"\nformat = \"csv\"",
                "requires an explicit schema",
            ),
            (
                "[[tables]]\nname = \"b\"\npath = \"/b\"\nformat = \"csv\"\n\
                 schema = [{name = \"x\", type = \"nope\"}]",
                "invalid type 'nope'",
            ),
        ] {
            let e = format!("{:#}", parse_file_tables(&tables(bad)).unwrap_err());
            assert!(e.contains(says), "{e}");
        }
    }

    fn empty_live() -> Arc<catalogs::LiveCatalogs> {
        catalogs::LiveCatalogs::new(
            Arc::new(CatalogManager::new("memory", "default")),
            Arc::new(ConnectorRegistry::new()),
            config::StorageConfig::default(),
        )
    }

    /// Ruling R1: a coordinator's catalog generation is never 0, which a
    /// fresh worker holds, so every worker pulls on its first heartbeat.
    #[tokio::test]
    async fn coordinator_catalog_generation_is_never_zero() {
        let live = empty_live();
        load_config_catalogs(&live, &[]).await;
        assert_eq!(live.generation(), 1, "config-built set, no state store");

        let store = arneb_state::StateStore::open(None).unwrap();
        let live = empty_live();
        load_stored_catalogs(&store, &live, &[], false)
            .await
            .unwrap();
        assert!(store.seeded().catalogs);
        assert_eq!(live.generation(), 1, "seeded state with zero catalogs");
        // A later start keeps the stored generation.
        let live = empty_live();
        load_stored_catalogs(&store, &live, &[], false)
            .await
            .unwrap();
        assert_eq!(live.generation(), store.catalogs().0);
        assert!(live.generation() >= 1);
    }

    /// Ruling R4: with a state store open (coordinator or standalone under
    /// password auth) the catalogs come from the store, so a runtime-created
    /// catalog wins over the config; without one, from the config at
    /// generation 1.
    #[tokio::test]
    async fn catalogs_load_from_the_state_store_when_one_is_open() {
        let config_spec: config::CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "from_cfg", "type": "mysql"
        }))
        .unwrap();
        let store = arneb_state::StateStore::open(None).unwrap();
        // First start seeds the (unbuildable) config catalog into the store.
        let live = empty_live();
        load_catalogs(
            Some(&store),
            &live,
            std::slice::from_ref(&config_spec),
            false,
        )
        .await
        .unwrap();
        assert!(store.seeded().catalogs);
        assert_eq!(store.catalogs().1.len(), 1);
        // A runtime change replaces it; the next start serves the store's
        // set (empty) and its generation, not the config's.
        let g = store.remove_catalog("from_cfg").unwrap();
        let live = empty_live();
        load_catalogs(
            Some(&store),
            &live,
            std::slice::from_ref(&config_spec),
            false,
        )
        .await
        .unwrap();
        assert_eq!(live.generation(), g);
        assert!(
            store.catalogs().1.is_empty(),
            "the store wins; nothing re-seeded"
        );
        // No store: the config path at generation 1.
        let live = empty_live();
        load_catalogs(None, &live, &[], false).await.unwrap();
        assert_eq!(live.generation(), 1);
    }

    /// runtime-config D2: `--reseed-state` re-seeds every object at
    /// startup: a runtime catalog is replaced by the config's, runtime
    /// rules by the rules file, and the stored settings are cleared.
    #[tokio::test]
    async fn cli_reseed_state_covers_catalogs_and_rules() {
        let config_spec: config::CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "from_cfg", "type": "mysql"
        }))
        .unwrap();
        let store = arneb_state::StateStore::open(None).unwrap();
        let live = empty_live();
        load_catalogs(
            Some(&store),
            &live,
            std::slice::from_ref(&config_spec),
            false,
        )
        .await
        .unwrap();
        // Runtime changes: a new catalog, the config one dropped, rules set,
        // settings stored.
        store.remove_catalog("from_cfg").unwrap();
        store
            .put_catalog(arneb_state::CatalogRecord {
                name: "runtime".into(),
                spec: serde_json::json!({"name": "runtime", "type": "mysql"}),
                from_config: false,
            })
            .unwrap();
        let runtime_rules = r#"{"catalogs":[{"allow":"all"}]}"#;
        store
            .set_access_rules(Some(arneb_state::StoredRules {
                json: runtime_rules.into(),
                from_config: false,
            }))
            .unwrap();
        store.set_slow_query_threshold_ms(5).unwrap();

        // A start without --reseed-state keeps them all.
        let config: config::AppConfig =
            toml::from_str("[[catalogs]]\nname = \"from_cfg\"\ntype = \"mysql\"\n").unwrap();
        let standalone = ServerRole::Standalone;
        prepare_state_store(&store, &config, &[], &standalone, false, false).unwrap();
        let live = empty_live();
        load_catalogs(
            Some(&store),
            &live,
            std::slice::from_ref(&config_spec),
            false,
        )
        .await
        .unwrap();
        let no_rules = config::AccessControlConfig::default();
        config::resolve_access_rules(&no_rules, Some(&store), false, None).unwrap();
        let names = |s: &arneb_state::StateStore| -> Vec<(String, bool)> {
            s.catalogs()
                .1
                .into_iter()
                .map(|c| (c.name, c.from_config))
                .collect()
        };
        assert_eq!(names(&store), vec![("runtime".to_owned(), false)]);
        assert!(store.access_rules().is_some());
        assert_eq!(store.slow_query_threshold_ms(), Some(5));

        // --reseed-state restores the config for all of them, in one write;
        // the loaders then run without re-seeding, as in `run`.
        let before = store.catalogs().0;
        prepare_state_store(&store, &config, &[], &standalone, false, true).unwrap();
        assert_eq!(store.catalogs().0, before + 1, "one catalog write");
        let live = empty_live();
        load_catalogs(
            Some(&store),
            &live,
            std::slice::from_ref(&config_spec),
            false,
        )
        .await
        .unwrap();
        let resolved = config::resolve_access_rules(&no_rules, Some(&store), false, None).unwrap();
        assert_eq!(names(&store), vec![("from_cfg".to_owned(), true)]);
        assert_eq!(live.generation(), store.catalogs().0);
        assert_eq!(store.access_rules(), None, "no rules file: no rules");
        assert!(resolved.handle.current().is_none());
        assert_eq!(store.slow_query_threshold_ms(), None);
    }

    /// Everything a startup could write, to compare before and after.
    fn observed(store: &arneb_state::StateStore) -> String {
        format!(
            "{:?} {:?} {:?} {:?} {:?}",
            store.list_users(),
            store.catalogs(),
            store.access_rules(),
            store.slow_query_threshold_ms(),
            store.seeded()
        )
    }

    /// I1: the cluster-secret refusal runs before anything is stored, so
    /// removing the secret from the config and restarting starts; a secret
    /// already stored names the ways out.
    #[tokio::test]
    async fn exposure_refusal_writes_nothing_so_removing_the_secret_works() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let with_secret: config::AppConfig = toml::from_str(
            "[[catalogs]]\nname = \"pg\"\ntype = \"postgresql\"\n\
             connection_url = \"postgresql://u@127.0.0.1:1/db\"\npassword = \"pw-Zx9\"\n",
        )
        .unwrap();
        let coordinator = ServerRole::Coordinator;
        let alice = vec![(
            "alice".to_owned(),
            arneb_protocol::ScramVerifier::from_password("pw", b"0123456789abcdef", 4096),
        )];
        let store = arneb_state::StateStore::open(Some(&path)).unwrap();
        let e = prepare_state_store(&store, &with_secret, &alice, &coordinator, false, false)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("catalog 'pg': password") && !e.contains("pw-Zx9"),
            "{e}"
        );
        assert!(!path.exists(), "nothing was written");
        assert!(store.list_users().is_empty());
        drop(store);

        // The operator removes the password and restarts.
        let without: config::AppConfig = toml::from_str(
            "[[catalogs]]\nname = \"pg\"\ntype = \"postgresql\"\n\
             connection_url = \"postgresql://u@127.0.0.1:1/db\"\n",
        )
        .unwrap();
        let store = arneb_state::StateStore::open(Some(&path)).unwrap();
        prepare_state_store(&store, &without, &alice, &coordinator, false, false).unwrap();
        let live = empty_live();
        load_catalogs(Some(&store), &live, &without.catalogs, false)
            .await
            .unwrap();
        assert!(store.catalogs().1[0].spec["password"].is_null());
        assert_eq!(store.list_users().len(), 1);

        // A secret that is already stored: the message names the way out
        // through the state store.
        let config_specs = vec![(
            "pg".to_owned(),
            serde_json::to_value(&with_secret.catalogs[0]).unwrap(),
        )];
        store.seed_catalogs(&config_specs, true).unwrap();
        let e = prepare_state_store(&store, &without, &alice, &coordinator, false, false)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("--reseed-state") && e.contains("state store"),
            "{e}"
        );
        // With a cluster secret both start.
        prepare_state_store(&store, &with_secret, &alice, &coordinator, true, false).unwrap();
    }

    /// I2: `--reseed-state` with a typo in the rules file refuses before any
    /// write, so users, tokens, catalogs, rules and settings are untouched.
    #[tokio::test]
    async fn reseed_with_a_bad_rules_file_leaves_the_state_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let rules = tmp.path().join("rules.json");
        std::fs::write(&rules, "{not json").unwrap();
        let config: config::AppConfig = toml::from_str(&format!(
            "[access_control]\nrules_file = {:?}\n[[catalogs]]\nname = \"lake\"\n\
             type = \"hive\"\nmetastore_uri = \"h:9083\"\n",
            rules.display().to_string()
        ))
        .unwrap();
        let store = arneb_state::StateStore::open(Some(&tmp.path().join("state.json"))).unwrap();
        let v =
            |pw: &str| arneb_protocol::ScramVerifier::from_password(pw, b"0123456789abcdef", 4096);
        store
            .seed_users(&[("alice".to_owned(), v("a"))], false)
            .unwrap();
        store.seed_catalogs(&[], false).unwrap();
        store.seed_rules(None, false).unwrap();
        store
            .set_access_rules(Some(arneb_state::StoredRules {
                json: r#"{"catalogs":[{"allow":"all"}]}"#.into(),
                from_config: false,
            }))
            .unwrap();
        store.set_slow_query_threshold_ms(5).unwrap();
        let before = observed(&store);
        let role = ServerRole::Coordinator;
        let users = vec![("bob".to_owned(), v("b"))];
        assert!(prepare_state_store(&store, &config, &users, &role, true, true).is_err());
        assert_eq!(observed(&store), before);

        // Stored runtime rules that no longer parse refuse before any write.
        let good = r#"{"catalogs":[{"allow":"all"}]}"#;
        std::fs::write(&rules, good).unwrap();
        let runtime_bad = arneb_state::StateStore::open(None).unwrap();
        runtime_bad.seed_rules(Some(good), false).unwrap();
        runtime_bad
            .set_access_rules(Some(arneb_state::StoredRules {
                json: r#"{"bogus_section":[]}"#.into(),
                from_config: false,
            }))
            .unwrap();
        let untouched = observed(&runtime_bad);
        let e = prepare_state_store(&runtime_bad, &config, &users, &role, true, false)
            .unwrap_err()
            .to_string();
        assert!(e.contains("state store"), "{e}");
        assert_eq!(observed(&runtime_bad), untouched);

        // A rules file edited since its seed while the rules were changed at
        // runtime refuses a plain start before any write, too.
        std::fs::write(&rules, r#"{"catalogs":[{"allow":"none"}]}"#).unwrap();
        let e = prepare_state_store(&store, &config, &users, &role, true, false)
            .unwrap_err()
            .to_string();
        assert!(e.contains("--reseed-state") && e.contains("users"), "{e}");
        assert_eq!(observed(&store), before);

        // Fixed: the re-seed replaces everything.
        prepare_state_store(&store, &config, &users, &role, true, true).unwrap();
        assert_eq!(store.list_users()[0].name, "bob");
        assert_eq!(store.catalogs().1[0].name, "lake");
        assert!(!store.rules_changed_at_runtime());
        assert_eq!(store.slow_query_threshold_ms(), None);
    }

    /// A coordinator without a `[cluster] secret` refuses to serve a catalog
    /// secret, built or not, including global `[storage]` keys merged into
    /// hive specs; a secret or another role changes nothing.
    #[test]
    fn coordinator_without_cluster_secret_refuses_served_secrets() {
        let with_password: config::CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "pg", "type": "postgresql",
            "connection_url": "postgresql://u@127.0.0.1:1/db", "password": "pw-Zx9"
        }))
        .unwrap();
        let none = config::StorageConfig::default();
        let served = std::slice::from_ref(&with_password);
        let e = check_catalog_exposure(&ServerRole::Coordinator, false, &none, served, false, 9090)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("[cluster] secret") && e.contains("catalog 'pg': password"),
            "{e}"
        );
        assert!(!e.contains("pw-Zx9"), "the error names fields, not values");
        for (role, secret) in [
            (ServerRole::Coordinator, true),
            (ServerRole::Worker, false),
            (ServerRole::Standalone, false),
        ] {
            check_catalog_exposure(&role, secret, &none, served, false, 9090).unwrap();
        }
        let hive: config::CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "lake", "type": "hive", "metastore_uri": "h:9083"
        }))
        .unwrap();
        let keys: config::StorageConfig = serde_json::from_value(serde_json::json!({
            "s3": {"secret_access_key": "k"}
        }))
        .unwrap();
        let e = check_catalog_exposure(
            &ServerRole::Coordinator,
            false,
            &keys,
            std::slice::from_ref(&hive),
            false,
            9090,
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("catalog 'lake': storage.s3.secret_access_key"),
            "{e}"
        );
        check_catalog_exposure(&ServerRole::Coordinator, false, &none, &[hive], false, 9090)
            .unwrap();
    }

    /// The default catalog follows the served catalogs: the first config
    /// entry while it is served, else the first served name, else file or
    /// memory; the schema is the chosen catalog's.
    #[test]
    fn default_catalog_comes_from_the_served_catalogs() {
        let spec = |name: &str, kind: &str| -> config::CatalogConfig {
            serde_json::from_value(serde_json::json!({
                "name": name, "type": kind,
                "metastore_uri": "h:9083", "connection_url": "postgresql://u@h/db"
            }))
            .unwrap()
        };
        let pair = |c: &str, s: &str| (c.to_owned(), s.to_owned());
        let config = vec![spec("lake", "hive"), spec("pg", "postgresql")];
        assert_eq!(
            default_catalog_and_schema(&config, &config, false),
            pair("lake", "default")
        );
        // The config's first catalog was dropped at runtime: the first
        // served catalog by name, with its schema.
        let served = vec![spec("zeta", "hive"), spec("pg", "postgresql")];
        assert_eq!(
            default_catalog_and_schema(&config, &served, true),
            pair("pg", "public")
        );
        // Created at runtime with no config catalogs at all.
        assert_eq!(
            default_catalog_and_schema(&[], &served, false),
            pair("pg", "public")
        );
        assert_eq!(
            default_catalog_and_schema(&config, &[], true),
            pair("file", "default")
        );
        assert_eq!(
            default_catalog_and_schema(&[], &[], false),
            pair("memory", "default")
        );

        // The served specs: the state store's once seeded, the config's on
        // a seeding start or without a store.
        let store = arneb_state::StateStore::open(None).unwrap();
        let names = |specs: Vec<config::CatalogConfig>| -> Vec<String> {
            specs.into_iter().map(|s| s.name).collect()
        };
        assert_eq!(
            names(served_catalog_specs(Some(&store), &config, false)),
            ["lake", "pg"]
        );
        store
            .seed_catalogs(
                &[(
                    "zeta".into(),
                    serde_json::to_value(spec("zeta", "hive")).unwrap(),
                )],
                false,
            )
            .unwrap();
        assert_eq!(
            names(served_catalog_specs(Some(&store), &config, false)),
            ["zeta"]
        );
        assert_eq!(
            names(served_catalog_specs(Some(&store), &config, true)),
            ["lake", "pg"]
        );
        assert_eq!(
            names(served_catalog_specs(None, &config, false)),
            ["lake", "pg"]
        );
    }

    /// runtime-config D8: workers ignore their own `[[catalogs]]`; they
    /// start empty at generation 0 and take the coordinator's set.
    #[tokio::test]
    async fn worker_ignores_config_catalogs() {
        let spec: config::CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "lake", "type": "hive", "metastore_uri": "127.0.0.1:1"
        }))
        .unwrap();
        let live = empty_live();
        let ignored = start_worker_catalogs(&live, std::slice::from_ref(&spec));
        assert_eq!(ignored, vec!["lake".to_string()]);
        assert!(live.specs().is_empty());
        assert_eq!(live.generation(), 0);
        assert!(start_worker_catalogs(&live, &[]).is_empty());
    }

    /// runtime-config D8, Review Focus 4: a worker whose applied generation
    /// (0) differs from the coordinator's (3) pulls once and records 3; a
    /// second heartbeat naming 3 does not pull.
    #[tokio::test]
    async fn heartbeat_generation_mismatch_pulls() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let fetches = Arc::new(AtomicUsize::new(0));
        let f = fetches.clone();
        let mut state = arneb_rpc::FlightState::with_heartbeat_callback(Arc::new(|_| {
            Ok(arneb_rpc::HeartbeatReply {
                catalog_generation: Some(3),
                catalog_epoch: None,
            })
        }));
        state.set_catalog_source(Arc::new(move || {
            f.fetch_add(1, Ordering::SeqCst);
            arneb_rpc::CatalogSet {
                epoch: 0,
                generation: 3,
                catalogs: vec![],
                ..Default::default()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(arneb_rpc::__serve_flight_for_test(listener, state));

        let target = HeartbeatTarget {
            coordinator_address: addr.clone(),
            message: arneb_rpc::HeartbeatMessage::new("w1".into(), "http://localhost:1".into(), 8),
        };
        let live = empty_live();
        let puller = catalogs::CatalogPuller::from_coordinator(addr, live.clone());
        for _ in 0..2 {
            let reply = target
                .send(arneb_rpc::WorkerState::Active)
                .await
                .expect("heartbeat reply");
            puller.after_heartbeat(&reply).await;
        }
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(live.generation(), 3);
    }

    /// A coordinator Flight server serving `set` (its heartbeat reply
    /// follows it), counting `get_catalogs` calls. Returns its address.
    async fn fake_coordinator(
        set: Arc<std::sync::Mutex<arneb_rpc::CatalogSet>>,
        fetches: Arc<std::sync::atomic::AtomicUsize>,
    ) -> String {
        let s = set.clone();
        let mut state = arneb_rpc::FlightState::with_heartbeat_callback(Arc::new(move |_| {
            let s = s.lock().unwrap();
            Ok(arneb_rpc::HeartbeatReply {
                catalog_generation: Some(s.generation),
                catalog_epoch: Some(s.epoch),
            })
        }));
        state.set_catalog_source(Arc::new(move || {
            fetches.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            set.lock().unwrap().clone()
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(arneb_rpc::__serve_flight_for_test(listener, state));
        addr
    }

    fn memory_built() -> catalogs::BuiltCatalog {
        let catalog = Arc::new(MemoryCatalog::new());
        catalogs::BuiltCatalog {
            provider: catalog.clone(),
            connector: Arc::new(MemoryConnectorFactory::new(catalog, "default")),
        }
    }

    /// Ruling R6: a coordinator restarted with a lower generation (new
    /// epoch, e.g. an in-memory state) is followed within one heartbeat;
    /// so is a no-store coordinator restarted with a changed config (new
    /// epoch, generation 1 again).
    #[tokio::test]
    async fn coordinator_restart_with_a_new_epoch_converges() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let lake: config::CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "lake", "type": "hive", "metastore_uri": "127.0.0.1:1"
        }))
        .unwrap();
        let set = Arc::new(std::sync::Mutex::new(arneb_rpc::CatalogSet {
            epoch: 100,
            generation: 5,
            catalogs: vec![serde_json::to_value(&lake).unwrap()],
            ..Default::default()
        }));
        let fetches = Arc::new(AtomicUsize::new(0));
        let addr = fake_coordinator(set.clone(), fetches.clone()).await;
        let target = HeartbeatTarget {
            coordinator_address: addr.clone(),
            message: arneb_rpc::HeartbeatMessage::new("w1".into(), "http://localhost:1".into(), 8),
        };
        let live = empty_live();
        // The worker already holds (100, 5) with `lake` installed.
        live.install(lake.clone(), memory_built());
        let held = set.lock().unwrap().clone();
        live.apply_remote(held).await;
        assert_eq!((live.epoch(), live.generation()), (100, 5));
        let puller = catalogs::CatalogPuller::from_coordinator(addr, live.clone());
        let beat = || async {
            let reply = target.send(arneb_rpc::WorkerState::Active).await.unwrap();
            puller.after_heartbeat(&reply).await
        };
        assert!(!beat().await, "in sync: no pull");

        // Restart with an in-memory state: new epoch, lower generation.
        *set.lock().unwrap() = arneb_rpc::CatalogSet {
            epoch: 200,
            generation: 2,
            catalogs: vec![],
            ..Default::default()
        };
        assert!(beat().await);
        assert_eq!((live.epoch(), live.generation()), (200, 2));
        assert!(live.specs().is_empty(), "the whole set was replaced");
        assert!(!beat().await, "converged");

        // A no-store coordinator: generation 1 at every start, new epoch,
        // changed config (`lake` back).
        *set.lock().unwrap() = arneb_rpc::CatalogSet {
            epoch: 300,
            generation: 1,
            catalogs: vec![serde_json::to_value(&lake).unwrap()],
            ..Default::default()
        };
        live.heartbeat_epoch(250);
        live.apply_remote(arneb_rpc::CatalogSet {
            epoch: 250,
            generation: 1,
            catalogs: vec![],
            ..Default::default()
        })
        .await;
        assert_eq!((live.epoch(), live.generation()), (250, 1));
        assert!(beat().await, "same generation, new epoch: pulls");
        assert_eq!((live.epoch(), live.generation()), (300, 1));
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    /// Rulings R6, R7: a push from the same epoch that is not newer is
    /// acknowledged and changes nothing; an applied push whose catalogs
    /// fail to build is Ok and lists them by name.
    #[tokio::test]
    async fn same_epoch_stale_push_is_ignored_and_failures_are_listed() {
        let live = empty_live();
        let push = |generation, catalogs| arneb_rpc::CatalogSet {
            epoch: 7,
            generation,
            catalogs,
            ..Default::default()
        };
        assert!(apply_pushed_catalogs(&live, push(5, vec![]))
            .await
            .unwrap()
            .is_empty());
        let bad = serde_json::json!({"name": "bad", "type": "mysql", "password": "s3cret"});
        let failed = apply_pushed_catalogs(&live, push(3, vec![bad.clone()]))
            .await
            .unwrap();
        assert!(failed.is_empty(), "stale: not applied, nothing built");
        assert_eq!(live.generation(), 5);
        let failed = apply_pushed_catalogs(&live, push(6, vec![bad]))
            .await
            .unwrap();
        assert_eq!(failed, vec!["bad".to_string()]);
        assert_eq!(live.generation(), 6);
    }

    /// Round 4: a push from another epoch than the one the last heartbeat
    /// named is refused with an error, so the coordinator that sent it
    /// never counts this worker as synced.
    #[tokio::test]
    async fn a_push_from_another_epoch_than_the_heartbeat_is_an_error() {
        let live = empty_live();
        live.heartbeat_epoch(400);
        let push = arneb_rpc::CatalogSet {
            epoch: 500,
            generation: 9,
            ..Default::default()
        };
        let e = apply_pushed_catalogs(&live, push).await.unwrap_err();
        assert!(e.contains("epoch 500") && e.contains("epoch 400"), "{e}");
        assert_eq!(live.generation(), 0);
    }

    /// Which `tracing` target an [`enabled`] probe checks.
    #[derive(Clone, Copy)]
    enum Probe {
        /// `rmcp::service`, which logs tool results at debug.
        Service,
        /// `rmcp::transport`.
        Transport,
        /// A non-rmcp target.
        Other,
    }

    /// Whether `directives` enable `level` events of the `probe` target.
    fn enabled(directives: &str, probe: Probe, level: tracing::Level) -> bool {
        use tracing::Level;
        use tracing_subscriber::layer::SubscriberExt as _;
        let subscriber =
            tracing_subscriber::registry().with(tracing_subscriber::EnvFilter::new(directives));
        // `enabled!` needs constant targets and levels, hence the matches.
        tracing::subscriber::with_default(subscriber, || match (probe, level) {
            (Probe::Service, Level::DEBUG) => {
                tracing::enabled!(target: "rmcp::service", Level::DEBUG)
            }
            (Probe::Service, Level::INFO) => {
                tracing::enabled!(target: "rmcp::service", Level::INFO)
            }
            (Probe::Service, _) => tracing::enabled!(target: "rmcp::service", Level::WARN),
            (Probe::Transport, Level::DEBUG) => {
                tracing::enabled!(target: "rmcp::transport", Level::DEBUG)
            }
            (Probe::Transport, Level::INFO) => {
                tracing::enabled!(target: "rmcp::transport", Level::INFO)
            }
            (Probe::Transport, _) => tracing::enabled!(target: "rmcp::transport", Level::WARN),
            (Probe::Other, _) => tracing::enabled!(target: "arneb::other", Level::DEBUG),
        })
    }

    #[test]
    fn rmcp_is_capped_at_warn_unless_rust_log_targets_rmcp() {
        use tracing::Level;
        // Unset, invalid or empty RUST_LOG: global info, rmcp capped.
        for unset in [None, Some(""), Some("bogus=[[[")] {
            assert_eq!(log_filter_directives(unset, false), "info,rmcp=warn");
        }
        // A global debug level does not reach rmcp's result logging.
        let d = log_filter_directives(Some("debug"), false);
        assert_eq!(d, "debug,rmcp=warn");
        assert!(enabled(&d, Probe::Other, Level::DEBUG));
        assert!(!enabled(&d, Probe::Service, Level::DEBUG));
        // Nor its per-request info lines; its warnings (rejected Origin or
        // Host) still log.
        assert!(!enabled(&d, Probe::Service, Level::INFO));
        assert!(enabled(&d, Probe::Service, Level::WARN));
        // A directive on the `rmcp` target itself is the operator opting in.
        for opted in ["debug,rmcp=debug", "rmcp[x]=debug", "rmcp"] {
            assert_eq!(log_filter_directives(Some(opted), false), opted);
        }
        assert!(enabled("debug,rmcp=debug", Probe::Service, Level::DEBUG));
        // #217 review: a directive on an rmcp module keeps the cap for the
        // rest of rmcp, and still applies to its module.
        let d = log_filter_directives(Some("debug,rmcp::transport=warn"), false);
        assert_eq!(d, "debug,rmcp::transport=warn,rmcp=warn");
        assert!(!enabled(&d, Probe::Service, Level::DEBUG));
        assert!(!enabled(&d, Probe::Service, Level::INFO));
        assert!(enabled(&d, Probe::Transport, Level::WARN));
        assert!(!enabled(&d, Probe::Transport, Level::INFO));
        let d = log_filter_directives(Some("info,rmcp::service=debug"), false);
        assert!(d.ends_with(",rmcp=warn"), "{d}");
        // The module's own directive is the more specific one.
        assert!(enabled(&d, Probe::Service, Level::DEBUG));
        assert!(!enabled(&d, Probe::Transport, Level::INFO));
        // A target that only starts with the letters is not rmcp.
        assert_eq!(
            log_filter_directives(Some("rmcpx=debug"), false),
            "rmcpx=debug,rmcp=warn"
        );
        // `--profile` composes with both.
        assert_eq!(
            log_filter_directives(Some("warn"), true),
            "warn,arneb::profile=info,rmcp=warn"
        );
    }

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
