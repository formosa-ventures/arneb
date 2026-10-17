use std::sync::Arc;

use tokio::net::TcpListener;

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

use crate::auth::AuthMethod;
use crate::handler::{DistributedExecutor, HandlerFactory};
use crate::tls::{PgwireTls, PgwireTlsMode};

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
    tls: Option<PgwireTls>,
    query_memory_limit: Option<usize>,
    admission: Option<arneb_scheduler::AdmissionController>,
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
            tls: None,
            query_memory_limit: None,
            admission: None,
        }
    }

    /// Set the client authentication mode (default: [`AuthMethod::None`]).
    pub fn with_auth(mut self, auth: AuthMethod) -> Self {
        self.auth = auth;
        self
    }

    /// Serve TLS with the given acceptor and mode (default: no TLS; every
    /// `SSLRequest` is answered with `N`).
    pub fn with_tls(mut self, tls: Option<PgwireTls>) -> Self {
        self.tls = tls;
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

    /// Limit each statement to `limit` bytes of the memory pool on this node
    /// (`query_max_memory_per_node`; default: no per-query limit).
    pub fn with_query_memory_limit(mut self, limit: Option<usize>) -> Self {
        self.query_memory_limit = limit;
        self
    }

    /// Queue statements through this admission controller (shared with the
    /// Trino listener). Default: none, every statement runs at once.
    pub fn with_admission(
        mut self,
        admission: Option<arneb_scheduler::AdmissionController>,
    ) -> Self {
        self.admission = admission;
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
            tls = match &self.tls {
                None => "off",
                Some(t) => t.mode.name(),
            },
            "protocol server listening"
        );

        let handler_factory = Arc::new(HandlerFactory {
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            distributed_executor: self.distributed_executor.clone(),
            memory_pool: Arc::clone(&self.memory_pool),
            auth: self.auth.clone(),
            tls_required: matches!(
                self.tls,
                Some(PgwireTls {
                    mode: PgwireTlsMode::Required,
                    ..
                })
            ),
            query_memory_limit: self.query_memory_limit,
            admission: self.admission.clone(),
        });
        let acceptor = self.tls.as_ref().map(|t| t.acceptor.clone());

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    tracing::debug!(peer = %addr, "accepted connection");
                    let handler = handler_factory.clone();
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        // A failed TLS handshake surfaces here and only ends
                        // this connection.
                        if let Err(e) =
                            pgwire::tokio::process_socket(socket, acceptor, handler).await
                        {
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
