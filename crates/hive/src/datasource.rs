//! Hive data source and connector factory.
//!
//! [`HiveDataSource`] reads Parquet files from an object store location
//! (as discovered from Hive Metastore metadata) and implements the
//! [`DataSource`] trait for the execution engine.
//!
//! [`HiveConnectorFactory`] creates [`HiveDataSource`] instances by
//! resolving the table location from the catalog and listing Parquet files
//! at that location.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::debug;

use arneb_common::error::{ConnectorError, ExecutionError};
use arneb_common::stream::{stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_connectors::storage::{StorageRegistry, StorageUri};
use arneb_connectors::ConnectorFactory;
use arneb_execution::{DataSource, ScanContext};

// ---------------------------------------------------------------------------
// HiveDataSource
// ---------------------------------------------------------------------------

/// Data source that reads Parquet files from a Hive table location.
///
/// The file listing happens lazily at scan time: the data source stores the
/// object store reference and the prefix (directory) under which Parquet
/// files reside. On each [`scan()`](DataSource::scan) call it lists files
/// matching `*.parquet` and reads them with projection pushdown.
pub struct HiveDataSource {
    /// Object store backend (local, S3, GCS, Azure, etc.).
    store: Arc<dyn ObjectStore>,
    /// Prefix (directory path) under which Parquet files are stored.
    prefix: ObjectPath,
    /// Column schema from HMS metadata.
    column_schema: Vec<ColumnInfo>,
}

impl HiveDataSource {
    /// Create a new Hive data source.
    ///
    /// - `store`: the object store for the table location.
    /// - `prefix`: directory path within the store.
    /// - `column_schema`: column metadata from HMS.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        column_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            store,
            prefix,
            column_schema,
        }
    }
}

impl fmt::Debug for HiveDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveDataSource")
            .field("prefix", &self.prefix.to_string())
            .field("columns", &self.column_schema.len())
            .finish()
    }
}

#[async_trait]
impl DataSource for HiveDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<SendableRecordBatchStream, ExecutionError> {
        // 1. List all .parquet files under the prefix.
        let file_paths = list_parquet_files(&self.store, &self.prefix).await?;

        if file_paths.is_empty() {
            debug!("no Parquet files found at prefix '{}'", self.prefix);
            let arrow_schema = column_info_to_arrow_schema(&self.column_schema);
            return Ok(stream_from_batches(arrow_schema, vec![]));
        }

        debug!(
            "found {} Parquet file(s) at prefix '{}'",
            file_paths.len(),
            self.prefix
        );

        // 2. Read each Parquet file and collect batches.
        let mut all_batches = Vec::new();

        for file_path in &file_paths {
            let meta = self.store.head(file_path).await.map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "failed to stat Parquet file '{}': {}",
                    file_path, e
                ))
            })?;

            let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
                self.store.clone(),
                meta.location,
            )
            .with_file_size(meta.size);

            let mut builder =
                parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|e| {
                        ExecutionError::InvalidOperation(format!(
                            "Parquet reader error for '{}': {}",
                            file_path, e
                        ))
                    })?;

            // Apply row group pruning based on filters.
            if !ctx.filters.is_empty() {
                let column_names: Vec<String> =
                    self.column_schema.iter().map(|c| c.name.clone()).collect();
                let file_meta = builder.metadata().clone();
                let selected = arneb_connectors::parquet_pushdown::prune_row_groups(
                    file_meta.row_groups(),
                    &ctx.filters,
                    &column_names,
                );
                if selected.len() < file_meta.row_groups().len() {
                    let selectors = build_row_selection(file_meta.row_groups(), &selected);
                    let selection =
                        parquet::arrow::arrow_reader::RowSelection::from(selectors);
                    builder = builder.with_row_selection(selection);
                }
            }

            // Apply predicate pushdown for within-row-group filtering.
            if !ctx.filters.is_empty() {
                if let Some(row_filter) = arneb_connectors::parquet_pushdown::build_row_filter(
                    &ctx.filters,
                    builder.parquet_schema(),
                ) {
                    builder = builder.with_row_filter(row_filter);
                }
            }

            // Apply projection pushdown.
            if let Some(ref projection) = ctx.projection {
                let mask = parquet::arrow::ProjectionMask::roots(
                    builder.parquet_schema(),
                    projection.iter().copied(),
                );
                builder = builder.with_projection(mask);
            }

            // Apply batch size if configured.
            if let Some(batch_size) = ctx.batch_size {
                builder = builder.with_batch_size(batch_size);
            }

            let stream = builder.build().map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "Parquet reader build error for '{}': {}",
                    file_path, e
                ))
            })?;

            let mut stream = stream;
            while let Some(result) = stream.next().await {
                let batch = result.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "Parquet read error for '{}': {}",
                        file_path, e
                    ))
                })?;
                all_batches.push(batch);
            }
        }

        // Determine output schema from projection or full schema.
        let output_schema = if let Some(ref projection) = ctx.projection {
            let full_schema = column_info_to_arrow_schema(&self.column_schema);
            let fields: Vec<arrow::datatypes::FieldRef> = projection
                .iter()
                .map(|&i| full_schema.field(i).clone().into())
                .collect();
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            column_info_to_arrow_schema(&self.column_schema)
        };

        Ok(stream_from_batches(output_schema, all_batches))
    }
}

/// List all data files under a given prefix in an object store.
///
/// Includes files with `.parquet` extension as well as files without any
/// extension (Trino's Hive connector writes Parquet files without the
/// `.parquet` suffix). Hidden files and common non-data files are skipped.
async fn list_parquet_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
) -> Result<Vec<ObjectPath>, ExecutionError> {
    let mut paths = Vec::new();
    let mut listing = store.list(Some(prefix));
    while let Some(result) = listing.next().await {
        let meta = result.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to list files at '{}': {}", prefix, e))
        })?;
        let filename = meta.location.filename().unwrap_or_default();
        // Skip hidden files, metadata files, and known non-data files.
        if filename.starts_with('.')
            || filename.starts_with('_')
            || filename.ends_with(".crc")
            || filename.ends_with(".metadata")
        {
            continue;
        }
        if meta.size > 0 {
            paths.push(meta.location);
        }
    }
    Ok(paths)
}

/// Build a RowSelector list from selected row group indices.
fn build_row_selection(
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    selected: &[usize],
) -> Vec<parquet::arrow::arrow_reader::RowSelector> {
    use parquet::arrow::arrow_reader::RowSelector;
    let selected_set: std::collections::HashSet<usize> = selected.iter().copied().collect();
    let mut selectors = Vec::new();
    for (idx, rg) in row_groups.iter().enumerate() {
        let num_rows = rg.num_rows() as usize;
        if selected_set.contains(&idx) {
            selectors.push(RowSelector::select(num_rows));
        } else {
            selectors.push(RowSelector::skip(num_rows));
        }
    }
    selectors
}

/// Convert `ColumnInfo` slice to an Arrow schema.
fn column_info_to_arrow_schema(columns: &[ColumnInfo]) -> Arc<arrow::datatypes::Schema> {
    let fields: Vec<arrow::datatypes::Field> = columns.iter().map(|c| c.clone().into()).collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

// ---------------------------------------------------------------------------
// HiveConnectorFactory
// ---------------------------------------------------------------------------

/// Connector factory for Hive tables.
///
/// Creates [`HiveDataSource`] instances by resolving the table location
/// from a pre-populated location map (filled during catalog resolution)
/// and listing Parquet files at that location.
///
/// Since [`ConnectorFactory::create_data_source`] is synchronous, the
/// factory stores a map of table name to location string. The actual
/// file listing happens lazily inside [`HiveDataSource::scan()`].
pub struct HiveConnectorFactory {
    /// Storage registry for resolving object stores.
    storage_registry: Arc<StorageRegistry>,
    /// Map of table name → location URI string, populated during catalog resolution.
    locations: std::sync::RwLock<std::collections::HashMap<String, (String, Vec<ColumnInfo>)>>,
}

impl HiveConnectorFactory {
    /// Create a new Hive connector factory.
    pub fn new(storage_registry: Arc<StorageRegistry>) -> Self {
        Self {
            storage_registry,
            locations: std::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Register a table location for later data source creation.
    ///
    /// Called during catalog resolution when `HiveTableProvider` metadata
    /// is available (it carries the HMS location and column schema).
    pub fn register_table_location(
        &self,
        table_name: &str,
        location: &str,
        schema: Vec<ColumnInfo>,
    ) {
        let mut locations = self.locations.write().unwrap();
        locations.insert(table_name.to_string(), (location.to_string(), schema));
    }
}

impl fmt::Debug for HiveConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locations = self.locations.read().unwrap();
        f.debug_struct("HiveConnectorFactory")
            .field("tables", &locations.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ConnectorFactory for HiveConnectorFactory {
    fn name(&self) -> &str {
        "hive"
    }

    fn create_data_source(
        &self,
        table: &TableReference,
        schema: &[ColumnInfo],
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        // Auto-register location from properties if present and not already registered.
        // Pre-registered entries (e.g., manual overrides in tests) take precedence.
        if let Some(location) = properties.get("location") {
            let already_registered = self.locations.read().unwrap().contains_key(&table.table);
            if !already_registered {
                self.register_table_location(&table.table, location, schema.to_vec());
            }
        }

        // Look up the registered location for this table.
        let locations = self.locations.read().unwrap();
        let (location, column_schema) = match locations.get(&table.table) {
            Some(entry) => entry.clone(),
            None => {
                return Err(ConnectorError::TableNotFound(format!(
                    "Hive table '{}' location not available in properties or pre-registered map",
                    table.table
                )));
            }
        };

        let uri = StorageUri::parse(&location)?;
        let store = self.storage_registry.get_store(&uri)?;
        let prefix = uri.object_path();

        // Use the HMS schema if available, otherwise fall back to planner schema.
        let effective_schema = if column_schema.is_empty() {
            schema.to_vec()
        } else {
            column_schema
        };

        Ok(Arc::new(HiveDataSource::new(
            store,
            prefix,
            effective_schema,
        )))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::DataType;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::PutPayload;
    use parquet::arrow::arrow_writer::ArrowWriter;

    /// Write a Parquet file to bytes with the given rows.
    fn write_parquet_bytes(ids: Vec<i32>, names: Vec<&str>) -> Vec<u8> {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn test_column_schema() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ]
    }

    #[tokio::test]
    async fn scan_single_parquet_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parquet_bytes = write_parquet_bytes(vec![1, 2, 3], vec!["a", "b", "c"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/part-0.parquet"),
                PutPayload::from_bytes(parquet_bytes.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        );

        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(2), 3);
    }

    #[tokio::test]
    async fn scan_multiple_parquet_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes1 = write_parquet_bytes(vec![1, 2], vec!["a", "b"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/part-0.parquet"),
                PutPayload::from_bytes(bytes1.into()),
            )
            .await
            .unwrap();

        let bytes2 = write_parquet_bytes(vec![3, 4], vec!["c", "d"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/part-1.parquet"),
                PutPayload::from_bytes(bytes2.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        );

        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn scan_skips_non_parquet_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes = write_parquet_bytes(vec![10], vec!["x"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/data.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        // Non-parquet file should be skipped.
        store
            .put(
                &ObjectPath::from("warehouse/db/table/_SUCCESS"),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from("warehouse/db/table/metadata.json"),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        );

        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn scan_empty_directory() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/empty_table"),
            test_column_schema(),
        );

        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn scan_with_projection_pushdown() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes = write_parquet_bytes(vec![1, 2], vec!["a", "b"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/data.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        );

        // Project only the "name" column (index 1).
        let ctx = ScanContext::default().with_projection(vec![1]);
        let stream = ds.scan(&ctx).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();

        assert_eq!(batches[0].num_columns(), 1);
        let name_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "a");
        assert_eq!(name_col.value(1), "b");
    }

    #[tokio::test]
    async fn hive_data_source_debug() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        );
        let debug_str = format!("{ds:?}");
        assert!(debug_str.contains("HiveDataSource"));
        assert!(debug_str.contains("warehouse/db/table"));
    }

    #[tokio::test]
    async fn hive_data_source_schema() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        );
        let schema = ds.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "name");
    }

    #[test]
    fn hive_data_source_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HiveDataSource>();
        assert_send_sync::<HiveConnectorFactory>();
    }

    // -- HiveConnectorFactory tests --

    #[tokio::test]
    async fn factory_creates_data_source() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes = write_parquet_bytes(vec![10, 20], vec!["x", "y"]);
        store
            .put(
                &ObjectPath::from("data/table/part.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("s3://test-bucket", store);

        let factory = HiveConnectorFactory::new(registry);
        factory.register_table_location(
            "my_table",
            "s3://test-bucket/data/table",
            test_column_schema(),
        );

        let table_ref = TableReference::table("my_table");
        let ds = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .unwrap();

        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn factory_unregistered_table() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);

        let table_ref = TableReference::table("nonexistent");
        let result = factory.create_data_source(&table_ref, &[], &Default::default());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent"));
    }

    #[test]
    fn factory_name() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);
        assert_eq!(factory.name(), "hive");
    }

    #[test]
    fn factory_debug() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);
        factory.register_table_location("tbl", "s3://bucket/path", vec![]);
        let debug_str = format!("{factory:?}");
        assert!(debug_str.contains("HiveConnectorFactory"));
        assert!(debug_str.contains("tbl"));
    }

    // -- Compression codec tests --

    fn write_parquet_bytes_compressed(
        ids: Vec<i32>,
        names: Vec<&str>,
        compression: parquet::basic::Compression,
    ) -> Vec<u8> {
        use parquet::file::properties::WriterProperties;

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    async fn assert_hive_scan_reads_compressed(compression: parquet::basic::Compression) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let bytes = write_parquet_bytes_compressed(vec![1, 2, 3], vec!["a", "b", "c"], compression);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/data.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::new(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        );

        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn scan_gzip_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::GZIP(
            parquet::basic::GzipLevel::default(),
        ))
        .await;
    }

    #[tokio::test]
    async fn scan_zstd_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .await;
    }

    #[tokio::test]
    async fn scan_lz4_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::LZ4_RAW).await;
    }

    #[tokio::test]
    async fn scan_brotli_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::BROTLI(
            parquet::basic::BrotliLevel::default(),
        ))
        .await;
    }

    #[tokio::test]
    async fn factory_with_local_filesystem() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);
        factory.register_table_location(
            "local_table",
            "/data/warehouse/db/tbl",
            test_column_schema(),
        );

        let table_ref = TableReference::table("local_table");
        let ds = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .unwrap();

        // Scan will find no files (directory doesn't exist), returning empty stream.
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert!(batches.is_empty());
    }
}
