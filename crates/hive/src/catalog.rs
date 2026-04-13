//! Hive Metastore catalog provider.
//!
//! Implements the `CatalogProvider` / `SchemaProvider` / `TableProvider` traits
//! by talking to a Hive Metastore via Thrift (using the `hive_metastore` crate).

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use hive_metastore::{
    GetTableRequest, ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder,
};
use pilota::FastStr;
use tracing::{debug, warn};
use volo_thrift::MaybeException;

use arneb_catalog::{CatalogProvider, SchemaProvider, TableProvider};
use arneb_common::error::ConnectorError;
use arneb_common::types::{ColumnInfo, DataType};

use crate::hive_type_to_arrow;

// ---------------------------------------------------------------------------
// HiveTableMeta
// ---------------------------------------------------------------------------

/// Extracted metadata for a single Hive table.
#[derive(Debug, Clone)]
pub struct HiveTableMeta {
    /// Columns with their Arneb data types.
    pub columns: Vec<ColumnInfo>,
    /// HDFS / S3 / object-store location from the storage descriptor.
    pub location: String,
    /// Input format class (e.g. `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`).
    pub input_format: String,
}

// ---------------------------------------------------------------------------
// HmsClient
// ---------------------------------------------------------------------------

/// Thin wrapper around the Volo-based `hive_metastore` Thrift client.
///
/// All methods translate Thrift errors and HMS exceptions into
/// [`ConnectorError`] for uniform error handling.
pub struct HmsClient {
    client: ThriftHiveMetastoreClient,
}

impl fmt::Debug for HmsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HmsClient").finish_non_exhaustive()
    }
}

impl HmsClient {
    /// Connect to a Hive Metastore at the given `host:port` address.
    pub async fn new(uri: &str) -> Result<Self, ConnectorError> {
        let addr: SocketAddr = uri.parse().map_err(|e| {
            ConnectorError::ConnectionFailed(format!("invalid HMS address '{uri}': {e}"))
        })?;

        let client = ThriftHiveMetastoreClientBuilder::new("hive_metastore")
            .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
            .address(addr)
            .build();

        debug!("connected to Hive Metastore at {addr}");
        Ok(Self { client })
    }

    /// Return all database names from HMS.
    pub async fn get_all_databases(&self) -> Result<Vec<String>, ConnectorError> {
        let result = self.client.get_all_databases().await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!("HMS get_all_databases failed: {e}"))
        })?;

        match result {
            MaybeException::Ok(dbs) => Ok(dbs.into_iter().map(|s| s.to_string()).collect()),
            MaybeException::Exception(ex) => Err(ConnectorError::ConnectionFailed(format!(
                "HMS get_all_databases exception: {ex:?}"
            ))),
        }
    }

    /// Return all table names in the given database.
    pub async fn get_all_tables(&self, db: &str) -> Result<Vec<String>, ConnectorError> {
        let result = self
            .client
            .get_all_tables(FastStr::from(db.to_string()))
            .await
            .map_err(|e| {
                ConnectorError::ConnectionFailed(format!("HMS get_all_tables({db}) failed: {e}"))
            })?;

        match result {
            MaybeException::Ok(tables) => Ok(tables.into_iter().map(|s| s.to_string()).collect()),
            MaybeException::Exception(ex) => Err(ConnectorError::ConnectionFailed(format!(
                "HMS get_all_tables({db}) exception: {ex:?}"
            ))),
        }
    }

    /// Fetch full table metadata and convert HMS column types to Arneb types.
    ///
    /// Uses `get_table_req` (the `_req` variant introduced in Hive 3.x). The
    /// legacy `get_table(dbname, tbl_name)` method was removed in HMS 4.0
    /// (HIVE-26537), so Arneb must use this form to support HMS 4.x and 5.x.
    pub async fn get_table(&self, db: &str, table: &str) -> Result<HiveTableMeta, ConnectorError> {
        let req = GetTableRequest {
            db_name: FastStr::from(db.to_string()),
            tbl_name: FastStr::from(table.to_string()),
            capabilities: None,
            cat_name: None,
            valid_write_id_list: None,
            get_column_stats: None,
            processor_capabilities: None,
            processor_identifier: None,
            engine: None,
            id: None,
        };

        let result = self.client.get_table_req(req).await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!("HMS get_table_req({db}.{table}) failed: {e}"))
        })?;

        let hms_table = match result {
            MaybeException::Ok(resp) => resp.table,
            MaybeException::Exception(ex) => {
                return Err(ConnectorError::TableNotFound(format!(
                    "HMS get_table_req({db}.{table}) exception: {ex:?}"
                )));
            }
        };

        let sd = hms_table.sd.ok_or_else(|| {
            ConnectorError::ReadError(format!("table {db}.{table} has no storage descriptor"))
        })?;

        let location = sd.location.map(|s| s.to_string()).unwrap_or_default();
        let input_format = sd.input_format.map(|s| s.to_string()).unwrap_or_default();

        let hms_cols = sd.cols.unwrap_or_default();
        let columns = convert_field_schemas(&hms_cols, db, table)?;

        Ok(HiveTableMeta {
            columns,
            location,
            input_format,
        })
    }
}

/// Convert a list of HMS `FieldSchema` to Arneb `ColumnInfo`.
///
/// Columns whose Hive type cannot be mapped are skipped with a warning
/// rather than failing the entire table.
fn convert_field_schemas(
    fields: &[hive_metastore::FieldSchema],
    db: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>, ConnectorError> {
    let mut columns = Vec::with_capacity(fields.len());

    for field in fields {
        let name = field
            .name
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let hive_type = field
            .r#type
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default();

        match hive_type_to_arrow(&hive_type) {
            Ok(arrow_dt) => {
                let data_type = DataType::try_from(arrow_dt).map_err(|e| {
                    ConnectorError::UnsupportedOperation(format!("column {db}.{table}.{name}: {e}"))
                })?;
                columns.push(ColumnInfo {
                    name,
                    data_type,
                    nullable: true, // Hive columns are nullable by default
                });
            }
            Err(e) => {
                warn!(
                    "skipping column {db}.{table}.{name} with unsupported type '{hive_type}': {e}"
                );
            }
        }
    }

    Ok(columns)
}

// ---------------------------------------------------------------------------
// HiveCatalogProvider
// ---------------------------------------------------------------------------

/// Catalog provider backed by a Hive Metastore.
///
/// Each HMS database maps to a schema. Tables within a database are
/// discovered lazily through [`HiveSchemaProvider`].
#[derive(Debug)]
pub struct HiveCatalogProvider {
    client: Arc<HmsClient>,
}

impl HiveCatalogProvider {
    /// Create a new provider from an existing [`HmsClient`].
    pub fn new(client: Arc<HmsClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl CatalogProvider for HiveCatalogProvider {
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
        Some(Arc::new(HiveSchemaProvider {
            client: Arc::clone(&self.client),
            database: name.to_string(),
        }))
    }
}

// ---------------------------------------------------------------------------
// HiveSchemaProvider
// ---------------------------------------------------------------------------

/// Schema provider for a single Hive database.
#[derive(Debug)]
pub struct HiveSchemaProvider {
    client: Arc<HmsClient>,
    database: String,
}

impl HiveSchemaProvider {
    /// Create a new schema provider for the given database.
    pub fn new(client: Arc<HmsClient>, database: String) -> Self {
        Self { client, database }
    }
}

#[async_trait]
impl SchemaProvider for HiveSchemaProvider {
    async fn table_names(&self) -> Vec<String> {
        match self.client.get_all_tables(&self.database).await {
            Ok(names) => names,
            Err(e) => {
                warn!("failed to list HMS tables in '{}': {e}", self.database);
                vec![]
            }
        }
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match self.client.get_table(&self.database, name).await {
            Ok(meta) => Some(Arc::new(HiveTableProvider {
                columns: meta.columns,
                location: meta.location,
                input_format: meta.input_format,
            })),
            Err(e) => {
                warn!("failed to get HMS table '{}.{}': {e}", self.database, name);
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HiveTableProvider
// ---------------------------------------------------------------------------

/// Table provider holding metadata for a single Hive table.
///
/// Exposes the column schema derived from the HMS storage descriptor.
/// The `location` and `input_format` are available for downstream
/// connectors (e.g. Parquet reader) to use when scanning data.
#[derive(Debug, Clone)]
pub struct HiveTableProvider {
    columns: Vec<ColumnInfo>,
    /// Object-store path for the table data.
    pub location: String,
    /// HMS input format class name.
    pub input_format: String,
}

impl HiveTableProvider {
    /// Create a new table provider from pre-computed metadata.
    pub fn new(columns: Vec<ColumnInfo>, location: String, input_format: String) -> Self {
        Self {
            columns,
            location,
            input_format,
        }
    }

    /// Return the object-store location of this table.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Return the input format class string.
    pub fn input_format(&self) -> &str {
        &self.input_format
    }

    /// Check if this table uses Parquet storage.
    pub fn is_parquet(&self) -> bool {
        self.input_format.contains("Parquet") || self.input_format.contains("parquet")
    }
}

impl TableProvider for HiveTableProvider {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }

    fn properties(&self) -> std::collections::HashMap<String, String> {
        let mut props = std::collections::HashMap::new();
        props.insert("location".to_string(), self.location.clone());
        props.insert("input_format".to_string(), self.input_format.clone());
        props
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;

    #[test]
    fn hive_table_meta_construction() {
        let meta = HiveTableMeta {
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                },
            ],
            location: "s3://bucket/warehouse/db/table".to_string(),
            input_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
                .to_string(),
        };

        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.columns[0].name, "id");
        assert_eq!(meta.columns[1].data_type, DataType::Utf8);
        assert!(meta.location.starts_with("s3://"));
        assert!(meta.input_format.contains("Parquet"));
    }

    #[test]
    fn hive_table_provider_schema() {
        let provider = HiveTableProvider::new(
            vec![
                ColumnInfo {
                    name: "col_a".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                },
                ColumnInfo {
                    name: "col_b".to_string(),
                    data_type: DataType::Float64,
                    nullable: false,
                },
            ],
            "/data/warehouse/test_table".to_string(),
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_string(),
        );

        let schema = provider.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "col_a");
        assert_eq!(schema[0].data_type, DataType::Int32);
        assert!(schema[0].nullable);
        assert_eq!(schema[1].name, "col_b");
        assert_eq!(schema[1].data_type, DataType::Float64);
        assert!(!schema[1].nullable);
    }

    #[test]
    fn hive_table_provider_is_parquet() {
        let parquet_provider = HiveTableProvider::new(
            vec![],
            "/data/table".to_string(),
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_string(),
        );
        assert!(parquet_provider.is_parquet());

        let text_provider = HiveTableProvider::new(
            vec![],
            "/data/table".to_string(),
            "org.apache.hadoop.mapred.TextInputFormat".to_string(),
        );
        assert!(!text_provider.is_parquet());
    }

    #[test]
    fn hive_table_provider_location_and_format() {
        let provider = HiveTableProvider::new(
            vec![],
            "s3://my-bucket/warehouse/db/tbl".to_string(),
            "MapredParquetInputFormat".to_string(),
        );
        assert_eq!(provider.location(), "s3://my-bucket/warehouse/db/tbl");
        assert_eq!(provider.input_format(), "MapredParquetInputFormat");
    }

    #[test]
    fn convert_field_schemas_basic() {
        let fields = vec![
            hive_metastore::FieldSchema {
                name: Some("id".into()),
                r#type: Some("bigint".into()),
                comment: None,
            },
            hive_metastore::FieldSchema {
                name: Some("name".into()),
                r#type: Some("string".into()),
                comment: Some("user name".into()),
            },
            hive_metastore::FieldSchema {
                name: Some("amount".into()),
                r#type: Some("decimal(10,2)".into()),
                comment: None,
            },
        ];

        let columns = convert_field_schemas(&fields, "test_db", "test_table").unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, DataType::Int64);
        assert!(columns[0].nullable); // Hive default
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].data_type, DataType::Utf8);
        assert_eq!(columns[2].name, "amount");
        assert_eq!(
            columns[2].data_type,
            DataType::Decimal128 {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn convert_field_schemas_skips_unsupported() {
        let fields = vec![
            hive_metastore::FieldSchema {
                name: Some("id".into()),
                r#type: Some("int".into()),
                comment: None,
            },
            hive_metastore::FieldSchema {
                name: Some("tags".into()),
                r#type: Some("array<string>".into()),
                comment: None,
            },
            hive_metastore::FieldSchema {
                name: Some("value".into()),
                r#type: Some("double".into()),
                comment: None,
            },
        ];

        let columns = convert_field_schemas(&fields, "db", "tbl").unwrap();
        // array<string> should be skipped
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, DataType::Int32);
        assert_eq!(columns[1].name, "value");
        assert_eq!(columns[1].data_type, DataType::Float64);
    }

    #[test]
    fn convert_field_schemas_empty() {
        let columns = convert_field_schemas(&[], "db", "tbl").unwrap();
        assert!(columns.is_empty());
    }

    #[tokio::test]
    async fn hive_catalog_provider_debug() {
        // Verify Debug is implemented (required by CatalogProvider trait)
        let provider = HiveCatalogProvider {
            client: Arc::new(HmsClient {
                client: ThriftHiveMetastoreClientBuilder::new("test")
                    .address("127.0.0.1:9083".parse::<SocketAddr>().unwrap())
                    .build(),
            }),
        };
        let debug_str = format!("{provider:?}");
        assert!(debug_str.contains("HiveCatalogProvider"));
    }

    #[tokio::test]
    async fn hive_schema_provider_debug() {
        let provider = HiveSchemaProvider {
            client: Arc::new(HmsClient {
                client: ThriftHiveMetastoreClientBuilder::new("test")
                    .address("127.0.0.1:9083".parse::<SocketAddr>().unwrap())
                    .build(),
            }),
            database: "default".to_string(),
        };
        let debug_str = format!("{provider:?}");
        assert!(debug_str.contains("HiveSchemaProvider"));
    }

    #[test]
    fn hive_table_provider_trait_object() {
        let provider: Arc<dyn TableProvider> = Arc::new(HiveTableProvider::new(
            vec![ColumnInfo {
                name: "x".to_string(),
                data_type: DataType::Boolean,
                nullable: true,
            }],
            "/tmp/data".to_string(),
            "TextInputFormat".to_string(),
        ));
        assert_eq!(provider.schema().len(), 1);
        assert_eq!(provider.schema()[0].name, "x");
    }

    #[test]
    fn hive_table_provider_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HiveTableProvider>();
        assert_send_sync::<HiveTableMeta>();
        assert_send_sync::<HiveCatalogProvider>();
        assert_send_sync::<HiveSchemaProvider>();
        assert_send_sync::<HmsClient>();
    }

    /// Smoke test: connect to real HMS and call get_all_databases.
    /// Requires: docker compose up -d (HMS on localhost:9083).
    #[tokio::test]
    #[ignore]
    async fn smoke_test_hms_connection() {
        let client = HmsClient::new("127.0.0.1:9083")
            .await
            .expect("failed to connect to HMS");
        let dbs = client
            .get_all_databases()
            .await
            .expect("get_all_databases failed");
        println!("HMS databases: {dbs:?}");
        assert!(
            !dbs.is_empty(),
            "HMS should have at least one database (default)"
        );
    }

    #[test]
    fn convert_all_primitive_hive_types() {
        let type_pairs = vec![
            ("tinyint", DataType::Int8),
            ("smallint", DataType::Int16),
            ("int", DataType::Int32),
            ("bigint", DataType::Int64),
            ("float", DataType::Float32),
            ("double", DataType::Float64),
            ("boolean", DataType::Boolean),
            ("string", DataType::Utf8),
            ("binary", DataType::Binary),
            ("date", DataType::Date32),
        ];

        for (hive_type, expected_dt) in type_pairs {
            let fields = vec![hive_metastore::FieldSchema {
                name: Some("col".into()),
                r#type: Some(hive_type.into()),
                comment: None,
            }];
            let cols = convert_field_schemas(&fields, "db", "tbl").unwrap();
            assert_eq!(
                cols[0].data_type, expected_dt,
                "Hive type '{hive_type}' should map to {expected_dt:?}"
            );
        }
    }
}
