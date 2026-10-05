//! Iceberg table resolution from Hive Metastore table parameters.
//!
//! Iceberg tables registered in a Hive Metastore carry
//! `table_type=ICEBERG` and `metadata_location=<uri>` table parameters
//! (the convention shared by Trino, Spark, and Flink's `hive` Iceberg
//! catalogs). The Hive catalog redirects such tables here: the provider
//! reads the metadata file and exposes the *current* Iceberg schema; the
//! storage descriptor columns HMS keeps are ignored because they carry no
//! field IDs and may be stale.

use std::collections::HashMap;

use tracing::warn;

use arneb_catalog::{TableProvider, TableStatistics};
use arneb_common::error::ConnectorError;
use arneb_common::types::ColumnInfo;
use arneb_connectors::storage::StorageRegistry;

use crate::datasource::load_metadata;

/// HMS parameter (and table property) naming the table format.
pub const TABLE_TYPE_PARAM: &str = "table_type";
/// HMS parameter (and table property) pointing at the metadata JSON.
pub const METADATA_LOCATION_PARAM: &str = "metadata_location";
/// Table property carrying the snapshot pinned at planning time (absent
/// for an empty table). It travels inside the logical plan, so workers
/// read the same snapshot as the coordinator.
pub const SNAPSHOT_ID_PROP: &str = "snapshot_id";

/// `true` when HMS table parameters (or table properties) mark an
/// Iceberg table.
pub fn is_iceberg_table(parameters: &HashMap<String, String>) -> bool {
    parameters
        .get(TABLE_TYPE_PARAM)
        .is_some_and(|t| t.eq_ignore_ascii_case("ICEBERG"))
}

/// A resolved Iceberg table: current schema plus the pinned snapshot.
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    columns: Vec<ColumnInfo>,
    properties: HashMap<String, String>,
    statistics: Option<TableStatistics>,
}

impl IcebergTableProvider {
    /// Resolve an Iceberg table from its HMS table parameters.
    pub async fn resolve(
        storage: &StorageRegistry,
        qualified_name: &str,
        parameters: &HashMap<String, String>,
    ) -> Result<Self, ConnectorError> {
        let location = parameters.get(METADATA_LOCATION_PARAM).ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "Iceberg table {qualified_name} has no metadata_location parameter"
            ))
        })?;
        let md = load_metadata(storage, location).await?;
        let (cols, skipped) = md.current_schema.to_columns();
        if !skipped.is_empty() {
            warn!(
                table = qualified_name,
                "skipping Iceberg columns with unsupported types: {}",
                skipped.join(", ")
            );
        }
        let mut properties = HashMap::from([
            (TABLE_TYPE_PARAM.to_string(), "ICEBERG".to_string()),
            (METADATA_LOCATION_PARAM.to_string(), location.clone()),
        ]);
        let statistics = match md.current_snapshot_id {
            Some(id) => {
                properties.insert(SNAPSHOT_ID_PROP.to_string(), id.to_string());
                md.current_snapshot().map(|s| TableStatistics {
                    row_count: s.total_records(),
                    size_bytes: s.total_files_size(),
                    columns: HashMap::new(),
                })
            }
            // Empty table: zero rows is exact.
            None => Some(TableStatistics {
                row_count: Some(0),
                size_bytes: Some(0),
                columns: HashMap::new(),
            }),
        };
        Ok(Self {
            columns: cols.into_iter().map(|(_, c)| c).collect(),
            properties,
            statistics,
        })
    }
}

impl TableProvider for IcebergTableProvider {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }

    fn properties(&self) -> HashMap<String, String> {
        self.properties.clone()
    }

    fn statistics(&self) -> Option<TableStatistics> {
        self.statistics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(p: &[(&str, &str)]) -> HashMap<String, String> {
        p.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn detects_iceberg_tables_case_insensitively() {
        assert!(is_iceberg_table(&params(&[("table_type", "ICEBERG")])));
        assert!(is_iceberg_table(&params(&[("table_type", "iceberg")])));
        assert!(!is_iceberg_table(&params(&[])));
        assert!(!is_iceberg_table(&params(&[("table_type", "DELTA")])));
    }

    #[tokio::test]
    async fn iceberg_table_without_metadata_location_errors() {
        let storage = StorageRegistry::new();
        let err =
            IcebergTableProvider::resolve(&storage, "db.t", &params(&[("table_type", "ICEBERG")]))
                .await
                .unwrap_err();
        assert!(err.to_string().contains("metadata_location"));
    }
}
