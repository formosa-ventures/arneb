//! Hive Metastore connector for Arneb.
//!
//! Provides catalog integration with Apache Hive Metastore via Thrift,
//! reading Parquet data from cloud object stores based on HMS metadata.

pub mod catalog;
pub mod datasource;
mod types;

pub use types::hive_type_to_arrow;
