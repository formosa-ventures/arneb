pub mod auth;
mod encoding;
mod error;
mod handler;
mod memory_table;
mod metadata;
mod pg_catalog;
mod runtime_queries;
mod server;
mod session;
mod timeout;
pub mod tls;
pub mod trino;

pub use auth::{
    decoy_verifier, is_token_shaped, AuthConfigError, AuthMethod, ScramVerifier, UserCredentials,
};
pub use server::{ProtocolConfig, ProtocolServer};
pub use tls::{PgwireTls, PgwireTlsMode, ServerTls, TlsConfigError};
pub use trino::{TrinoConfig, TrinoServer};

// Re-export the distributed executor trait for server crate to implement
pub use handler::DistributedExecutor;

// Re-export for integration testing
#[doc(hidden)]
pub mod __private {
    pub use crate::handler::{execute_query, HandlerFactory};
}
