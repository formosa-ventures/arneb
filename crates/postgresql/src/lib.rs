//! PostgreSQL connector: a remote PostgreSQL database mounted as a
//! read-only Arneb catalog (`[[catalogs]] type = "postgresql"`).
//!
//! - Discovery and type mapping from the system catalogs ([`PostgresCatalog`]).
//! - Filter / projection / limit pushdown through the shared
//!   [`arneb_rdbms`] framework with [`PostgresDialect`].
//! - Primary-key range splits planned on the coordinator.
//! - Reads via `COPY (SELECT …) TO STDOUT (FORMAT binary)`, decoded
//!   straight into Arrow.
//!
//! See `docs/connectors/postgresql.md`.

mod catalog;
mod config;
mod copy;
mod dialect;
mod source;
mod time;
mod types;

pub use catalog::{PostgresCatalog, PostgresSchema, PostgresTable};
pub use config::{
    PostgresConfig, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_SPLITS, DEFAULT_SPLIT_MIN_ROWS,
    DEFAULT_UNCONSTRAINED_NUMERIC_SCALE,
};
pub use dialect::PostgresDialect;
pub use source::{PostgresConnectorFactory, PostgresDataSource};

/// A driver error as text: the server's message for database errors
/// (tokio-postgres' `Display` only says "db error"), the error chain
/// otherwise. Never contains connection credentials.
pub(crate) fn pg_error(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        return format!("{}: {} ({})", db.severity(), db.message(), db.code().code());
    }
    let mut msg = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
    }
    msg
}
