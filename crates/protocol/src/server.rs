use std::sync::Arc;

use tokio::net::TcpListener;

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;

use crate::handler::{DistributedExecutor, HandlerFactory};

/// Configuration for the PostgreSQL wire protocol server.
#[derive(Debug, Clone)]
pub struct ProtocolConfig {
    pub bind_address: String,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:5433".to_string(),
        }
    }
}

/// PostgreSQL wire protocol server.
///
/// Accepts TCP connections and processes queries through the full
/// arneb pipeline: parse → plan → execute → encode results.
pub struct ProtocolServer {
    config: ProtocolConfig,
    catalog_manager: Arc<CatalogManager>,
    connector_registry: Arc<ConnectorRegistry>,
    distributed_executor: Option<Arc<dyn DistributedExecutor>>,
}

impl ProtocolServer {
    pub fn new(
        config: ProtocolConfig,
        catalog_manager: Arc<CatalogManager>,
        connector_registry: Arc<ConnectorRegistry>,
    ) -> Self {
        Self {
            config,
            catalog_manager,
            connector_registry,
            distributed_executor: None,
        }
    }

    /// Set the distributed executor for coordinator mode.
    pub fn with_distributed_executor(mut self, executor: Arc<dyn DistributedExecutor>) -> Self {
        self.distributed_executor = Some(executor);
        self
    }

    /// Start the server and begin accepting connections.
    /// This method runs until the process is terminated.
    pub async fn start(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        tracing::info!(
            address = %self.config.bind_address,
            "protocol server listening"
        );

        let handler_factory = Arc::new(HandlerFactory {
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            distributed_executor: self.distributed_executor.clone(),
        });

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    tracing::debug!(peer = %addr, "accepted connection");
                    let handler = handler_factory.clone();
                    tokio::spawn(async move {
                        if let Err(e) = pgwire::tokio::process_socket(socket, None, handler).await {
                            tracing::debug!(error = %e, peer = %addr, "connection closed");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to accept connection");
                }
            }
        }
    }
}
