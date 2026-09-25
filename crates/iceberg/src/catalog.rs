//! HMS-backed Iceberg catalog provider.
//!
//! Iceberg tables registered in a Hive Metastore carry
//! `table_type=ICEBERG` and `metadata_location=<uri>` table parameters
//! (the convention shared by Trino, Spark, and Flink's `hive` Iceberg
//! catalogs). The provider resolves a table by reading that metadata
//! file and exposing the *current* Iceberg schema; the storage descriptor
//! columns HMS keeps are ignored because they carry no field IDs and may
//! be stale.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use arneb_catalog::{CatalogProvider, SchemaProvider, TableProvider, TableStatistics};
use arneb_common::types::ColumnInfo;
use arneb_connectors::storage::StorageRegistry;
use arneb_hive::catalog::{HiveTableMeta, HmsClient};

use crate::datasource::{load_metadata, props};

/// HMS parameter naming the table format.
pub const HMS_TABLE_TYPE_PARAM: &str = "table_type";
/// HMS parameter pointing at the current metadata JSON.
pub const HMS_METADATA_LOCATION_PARAM: &str = "metadata_location";

/// `true` when an HMS table is an Iceberg table.
pub fn is_iceberg_table(parameters: &HashMap<String, String>) -> bool {
    parameters
        .get(HMS_TABLE_TYPE_PARAM)
        .is_some_and(|t| t.eq_ignore_ascii_case("ICEBERG"))
}

/// Catalog provider for Iceberg tables tracked by a Hive Metastore.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    client: Arc<HmsClient>,
    storage: Arc<StorageRegistry>,
}

impl IcebergCatalogProvider {
    /// Create a provider from an HMS client and the catalog's storage.
    pub fn new(client: Arc<HmsClient>, storage: Arc<StorageRegistry>) -> Self {
        Self { client, storage }
    }
}

#[async_trait]
impl CatalogProvider for IcebergCatalogProvider {
    async fn schema_names(&self) -> Vec<String> {
        match self.client.get_all_databases().await {
            Ok(names) => names,
            Err(e) => {
                warn!("failed to list HMS databases: {e}");
                vec![]
            }
        }
    }

    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(IcebergSchemaProvider {
            client: Arc::clone(&self.client),
            storage: Arc::clone(&self.storage),
            database: name.to_string(),
        }))
    }
}

/// Schema provider for one HMS database, exposing its Iceberg tables.
#[derive(Debug)]
pub struct IcebergSchemaProvider {
    client: Arc<HmsClient>,
    storage: Arc<StorageRegistry>,
    database: String,
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    async fn table_names(&self) -> Vec<String> {
        // Like Trino's Iceberg connector, list every HMS table; non-Iceberg
        // tables fail with a clear error when queried.
        match self.client.get_all_tables(&self.database).await {
            Ok(names) => names,
            Err(e) => {
                warn!("failed to list HMS tables in '{}': {e}", self.database);
                vec![]
            }
        }
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let meta = match self.client.get_table(&self.database, name).await {
            Ok(m) => m,
            Err(e) => {
                warn!("failed to get HMS table '{}.{}': {e}", self.database, name);
                return None;
            }
        };
        let qualified = format!("{}.{}", self.database, name);
        match IcebergTableProvider::resolve(&self.storage, &qualified, meta).await {
            Ok(p) => Some(Arc::new(p)),
            Err(e) => {
                warn!("failed to load Iceberg table '{qualified}': {e}");
                None
            }
        }
    }
}

/// A resolved Iceberg table: current schema plus the pinned snapshot.
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    columns: Vec<ColumnInfo>,
    properties: HashMap<String, String>,
    statistics: Option<TableStatistics>,
}

impl IcebergTableProvider {
    /// Resolve an HMS table entry. Non-Iceberg entries resolve to a
    /// provider that fails at scan time with an explanatory error (the
    /// catalog API has no way to surface one at resolution time).
    pub async fn resolve(
        storage: &StorageRegistry,
        qualified_name: &str,
        meta: HiveTableMeta,
    ) -> Result<Self, arneb_common::error::ConnectorError> {
        if !is_iceberg_table(&meta.parameters) {
            let mut properties = HashMap::new();
            properties.insert(
                props::ERROR.to_string(),
                format!(
                    "table {qualified_name} is not an Iceberg table (HMS table_type={}); \
                     query it through a catalog with type = \"hive\"",
                    meta.parameters
                        .get(HMS_TABLE_TYPE_PARAM)
                        .map(String::as_str)
                        .unwrap_or("<unset>")
                ),
            );
            return Ok(Self {
                columns: meta.columns,
                properties,
                statistics: None,
            });
        }
        let location = meta
            .parameters
            .get(HMS_METADATA_LOCATION_PARAM)
            .ok_or_else(|| {
                arneb_common::error::ConnectorError::ReadError(format!(
                    "Iceberg table {qualified_name} has no metadata_location parameter"
                ))
            })?
            .clone();
        let md = load_metadata(storage, &location).await?;
        let (cols, skipped) = md.current_schema.to_columns();
        if !skipped.is_empty() {
            warn!(
                table = qualified_name,
                "skipping Iceberg columns with unsupported types: {}",
                skipped.join(", ")
            );
        }
        let mut properties = HashMap::new();
        properties.insert(props::TABLE_TYPE.to_string(), "ICEBERG".to_string());
        properties.insert(props::METADATA_LOCATION.to_string(), location);
        properties.insert("location".to_string(), md.location.clone());
        let statistics = md.current_snapshot().map(|s| TableStatistics {
            row_count: s.total_records(),
            size_bytes: s.total_files_size(),
            columns: HashMap::new(),
        });
        if let Some(id) = md.current_snapshot_id {
            properties.insert(props::SNAPSHOT_ID.to_string(), id.to_string());
        } else {
            // Empty table: zero rows is exact.
            return Ok(Self {
                columns: cols.into_iter().map(|(_, c)| c).collect(),
                properties,
                statistics: Some(TableStatistics {
                    row_count: Some(0),
                    size_bytes: Some(0),
                    columns: HashMap::new(),
                }),
            });
        }
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
    use arneb_common::types::DataType;

    fn meta(params: &[(&str, &str)]) -> HiveTableMeta {
        HiveTableMeta {
            columns: vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: true,
            }],
            location: "s3://b/t".into(),
            input_format: String::new(),
            row_count: None,
            size_bytes: None,
            column_stats: HashMap::new(),
            parameters: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn detects_iceberg_tables_case_insensitively() {
        assert!(is_iceberg_table(
            &meta(&[("table_type", "ICEBERG")]).parameters
        ));
        assert!(is_iceberg_table(
            &meta(&[("table_type", "iceberg")]).parameters
        ));
        assert!(!is_iceberg_table(&meta(&[]).parameters));
        assert!(!is_iceberg_table(
            &meta(&[("table_type", "DELTA")]).parameters
        ));
    }

    #[tokio::test]
    async fn non_iceberg_table_resolves_to_error_marker() {
        let storage = StorageRegistry::new();
        let p = IcebergTableProvider::resolve(&storage, "db.t", meta(&[]))
            .await
            .unwrap();
        let err = p.properties().get(props::ERROR).cloned().unwrap();
        assert!(err.contains("not an Iceberg table"), "{err}");
        assert_eq!(p.schema().len(), 1);
    }

    #[tokio::test]
    async fn iceberg_table_without_metadata_location_errors() {
        let storage = StorageRegistry::new();
        let err =
            IcebergTableProvider::resolve(&storage, "db.t", meta(&[("table_type", "ICEBERG")]))
                .await
                .unwrap_err();
        assert!(err.to_string().contains("metadata_location"));
    }
}
