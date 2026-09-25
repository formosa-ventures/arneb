use std::sync::Arc;

use tokio::net::TcpListener;

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

use crate::auth::AuthMethod;
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
    memory_pool: Arc<dyn MemoryPool>,
    auth: AuthMethod,
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
            memory_pool: Arc::new(UnboundedMemoryPool::new()),
            auth: AuthMethod::None,
        }
    }

    /// Set the client authentication mode (default: [`AuthMethod::None`]).
    pub fn with_auth(mut self, auth: AuthMethod) -> Self {
        self.auth = auth;
        self
    }

    /// Set the distributed executor for coordinator mode.
    pub fn with_distributed_executor(mut self, executor: Arc<dyn DistributedExecutor>) -> Self {
        self.distributed_executor = Some(executor);
        self
    }

    /// Install a memory pool that spillable operators reserve from
    /// (currently the SemiJoinExec build phase). The server creates a
    /// `GreedyMemoryPool` sized from cgroup memory.max × ratio at
    /// startup and threads it here; if unset, defaults to
    /// [`UnboundedMemoryPool`] (no budget enforcement).
    pub fn with_memory_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = pool;
        self
    }

    /// Start the server and begin accepting connections.
    /// This method runs until the process is terminated.
    pub async fn start(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        self.serve(listener).await
    }

    /// Accept connections on an already-bound listener. Useful when the
    /// caller needs the actual address of an ephemeral (`:0`) port.
    /// This method runs until the process is terminated.
    pub async fn serve(&self, listener: TcpListener) -> Result<(), std::io::Error> {
        let address = listener.local_addr()?;
        tracing::info!(
            address = %address,
            auth = self.auth.name(),
            "protocol server listening"
        );

        let handler_factory = Arc::new(HandlerFactory {
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            distributed_executor: self.distributed_executor.clone(),
            memory_pool: Arc::clone(&self.memory_pool),
            auth: self.auth.clone(),
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
