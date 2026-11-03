//! Hive Metastore connector for Arneb.
//!
//! Provides catalog integration with Apache Hive Metastore via Thrift,
//! reading Parquet and ORC data from cloud object stores based on HMS metadata.

pub mod catalog;
pub mod datasource;
mod orc;
mod types;

pub use types::{hive_type_to_arrow, hive_type_to_data_type};

#[cfg(test)]
mod scan_tests;
