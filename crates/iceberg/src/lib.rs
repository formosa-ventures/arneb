//! Apache Iceberg reader for Arneb (read-only).
//!
//! Iceberg tables live in a Hive Metastore (`table_type=ICEBERG`,
//! `metadata_location=...`); the Hive catalog redirects them here, the
//! way Trino's Hive connector redirects to its Iceberg connector. Scans
//! are planned from the pinned snapshot's manifest list and manifests,
//! and the live Parquet data files are read through the shared Parquet
//! scan with field-ID-based column resolution.
//!
//! Out of scope (fails clearly rather than returning wrong results):
//! row-level delete files (merge-on-read), non-Parquet data files, writes.

pub mod catalog;
pub mod datasource;
pub mod manifest;
pub mod metadata;

pub use catalog::{is_iceberg_table, IcebergTableProvider, TABLE_TYPE_PARAM};
pub use datasource::{create_data_source, IcebergDataSource};
pub use metadata::TableMetadata;

#[cfg(test)]
mod scan_tests;
