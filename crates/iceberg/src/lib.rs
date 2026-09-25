//! Apache Iceberg connector for Arneb (read-only).
//!
//! Resolves Iceberg tables through a Hive Metastore (`table_type=ICEBERG`,
//! `metadata_location=...`), plans scans from the current snapshot's
//! manifest list and manifests, and reads the live Parquet data files
//! with field-ID-based column resolution, manifest-level file pruning,
//! and the shared Parquet row-group / predicate pushdown.
//!
//! Out of scope (fails clearly rather than returning wrong results):
//! row-level delete files (merge-on-read), non-Parquet data files, writes.

pub mod catalog;
pub mod datasource;
pub mod manifest;
pub mod metadata;
pub mod pruning;

pub use catalog::{is_iceberg_table, IcebergCatalogProvider, IcebergTableProvider};
pub use datasource::{IcebergConnectorFactory, IcebergDataSource};
pub use metadata::TableMetadata;

#[cfg(test)]
mod scan_tests;
