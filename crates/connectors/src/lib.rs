#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! Connector implementations for the arneb query engine.
//!
//! Provides [`ConnectorFactory`] trait and [`ConnectorRegistry`] for
//! dynamic data source creation, plus concrete connectors for in-memory
//! tables and local CSV/Parquet files.

pub mod file;
pub mod memory;
mod traits;

pub use traits::{ConnectorFactory, ConnectorRegistry, DDLProvider};
