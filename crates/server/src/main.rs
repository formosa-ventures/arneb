mod config;
pub mod coordinator;
pub mod task_manager;
mod web;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use trino_catalog::CatalogManager;
use trino_connectors::file::{FileCatalog, FileConnectorFactory, FileFormat, FileSchema};
use trino_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema};
use trino_connectors::ConnectorRegistry;
use trino_protocol::{ProtocolConfig, ProtocolServer};

use crate::config::{parse_data_type, AppConfig, ServerRole};

/// trino-alt — Distributed SQL query engine
#[derive(Parser)]
#[command(name = "trino-alt", version, about)]
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
}

#[tokio::main]
async fn main() -> Result<()> {
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

    // 4. Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // 5. Create catalog manager + connector registry
    // If config has file tables, default to "file" catalog; otherwise "memory"
    let default_catalog = if config.tables.is_empty() {
        "memory"
    } else {
        "file"
    };
    let catalog_manager = CatalogManager::new(default_catalog, "default");

    // Register memory catalog (empty)
    let mem_schema = Arc::new(MemorySchema::new());
    let mem_catalog = Arc::new(MemoryCatalog::new());
    mem_catalog.register_schema("default", mem_schema);
    let mem_factory = MemoryConnectorFactory::new(mem_catalog.clone(), "default");
    catalog_manager.register_catalog("memory", mem_catalog);

    let mut connector_registry = ConnectorRegistry::new();
    connector_registry.register("memory", Arc::new(mem_factory));

    // 6. Register file tables from config
    let table_count = config.tables.len();
    if !config.tables.is_empty() {
        let file_factory = Arc::new(FileConnectorFactory::new());

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
                            Ok(trino_common::types::ColumnInfo {
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

            if let Err(e) = file_factory.register_table(&table.name, &table.path, format, schema) {
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

    // 8. Set up Flight RPC server + heartbeat handling
    let node_registry = trino_scheduler::NodeRegistry::default();
    let rpc_addr = format!("{}:{}", config.server.bind_address, config.cluster.rpc_port);
    let query_tracker = Arc::new(trino_scheduler::QueryTracker::new());

    let mut flight_state = match role {
        ServerRole::Coordinator | ServerRole::Standalone => {
            // Coordinator receives heartbeats from workers.
            let registry = node_registry.clone();
            trino_rpc::FlightState::with_heartbeat_callback(std::sync::Arc::new(
                move |msg: trino_rpc::HeartbeatMessage| {
                    registry.heartbeat(msg.worker_id, msg.flight_address, msg.max_splits);
                },
            ))
        }
        ServerRole::Worker => {
            // Workers don't receive heartbeats, just serve data.
            trino_rpc::FlightState::new()
        }
    };

    // 8.5. Set up distributed execution components
    // Coordinator: create QueryCoordinator and wire into protocol server
    // Worker: create TaskManager and register task callback
    let distributed_executor: Option<Arc<dyn trino_protocol::DistributedExecutor>> = match role {
        ServerRole::Coordinator => {
            let coord =
                coordinator::QueryCoordinator::new(node_registry.clone(), query_tracker.clone());
            Some(Arc::new(coord))
        }
        _ => None,
    };

    if matches!(role, ServerRole::Worker) {
        let tm = task_manager::TaskManager::new(
            flight_state.clone(),
            catalog_manager.clone(),
            connector_registry.clone(),
        );
        flight_state.set_task_callback(Arc::new(move |descriptor| {
            tm.handle_task(descriptor);
        }));
    }

    let mut server = ProtocolServer::new(
        protocol_config,
        catalog_manager.clone(),
        connector_registry.clone(),
    );
    if let Some(ref executor) = distributed_executor {
        server = server.with_distributed_executor(Arc::clone(executor));
    }

    // 9. Startup banner
    match role {
        ServerRole::Worker => {
            tracing::info!(
                rpc_address = %rpc_addr,
                role = %config.cluster.role,
                "trino-alt worker starting"
            );
        }
        _ => {
            tracing::info!(
                pgwire_address = %listen_addr,
                rpc_address = %rpc_addr,
                role = %config.cluster.role,
                tables = table_count,
                "trino-alt listening"
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
        result = trino_rpc::start_flight_server(&rpc_addr_clone, flight_state_clone) => {
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

    let message = trino_rpc::HeartbeatMessage {
        worker_id: worker_id.clone(),
        flight_address: format!("http://{my_rpc_addr}"),
        max_splits: 256,
    };

    tracing::info!(
        worker_id = %worker_id,
        coordinator = %coordinator_address,
        "starting heartbeat loop"
    );

    loop {
        match trino_rpc::send_heartbeat(&coordinator_address, &message).await {
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
