mod config;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use trino_catalog::CatalogManager;
use trino_connectors::file::{FileCatalog, FileConnectorFactory, FileFormat, FileSchema};
use trino_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema};
use trino_connectors::ConnectorRegistry;
use trino_protocol::{ProtocolConfig, ProtocolServer};

use crate::config::{parse_data_type, AppConfig};

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
    config
        .server
        .validate()
        .context("configuration validation failed")?;

    // 4. Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // 5. Create catalog manager + connector registry
    let catalog_manager = CatalogManager::new("memory", "default");

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
    let server = ProtocolServer::new(protocol_config, catalog_manager, connector_registry);

    // 8. Startup banner
    tracing::info!(
        address = %listen_addr,
        tables = table_count,
        "trino-alt listening"
    );

    // 9. Run server with graceful shutdown
    tokio::select! {
        result = server.start() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "server error");
                bail!("server error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
        }
    }

    Ok(())
}
