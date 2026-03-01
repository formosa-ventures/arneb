#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! Shared types, error handling, and configuration for trino-alt.

pub mod config;
pub mod error;
pub mod types;

pub use config::ServerConfig;
pub use error::TrinoError;
pub use types::{
    ColumnInfo, DataType, InvalidTableReference, ScalarValue, TableReference, TimeUnit,
    UnsupportedArrowType,
};
