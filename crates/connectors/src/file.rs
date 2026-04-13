//! File-based connector: reads CSV and Parquet files from local or remote storage.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use arneb_catalog::{CatalogProvider, SchemaProvider, TableProvider};
use arneb_common::error::{ConnectorError, ExecutionError};
use arneb_common::stream::{stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_execution::{DataSource, ScanContext};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::storage::{StorageRegistry, StorageUri};
use crate::ConnectorFactory;

// ---------------------------------------------------------------------------
// FileFormat
// ---------------------------------------------------------------------------

/// Supported file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// Comma-separated values.
    Csv,
    /// Apache Parquet columnar format.
    Parquet,
}

// ---------------------------------------------------------------------------
// CsvDataSource
// ---------------------------------------------------------------------------

/// Reads a CSV file and produces Arrow RecordBatches.
#[derive(Debug)]
pub struct CsvDataSource {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    column_schema: Vec<ColumnInfo>,
    arrow_schema: Arc<Schema>,
}

impl CsvDataSource {
    /// Creates a new CSV data source with an explicit schema.
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjectPath, schema: Vec<ColumnInfo>) -> Self {
        let arrow_schema = column_info_to_arrow_schema(&schema);
        Self {
            store,
            path,
            column_schema: schema,
            arrow_schema,
        }
    }
}

#[async_trait]
impl DataSource for CsvDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Fetch the entire CSV content via ObjectStore.
        let result = self.store.get(&self.path).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to read CSV '{}': {}", self.path, e))
        })?;
        let bytes = result.bytes().await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to buffer CSV '{}': {}", self.path, e))
        })?;

        let cursor = std::io::Cursor::new(bytes);
        let reader = arrow_csv::ReaderBuilder::new(self.arrow_schema.clone())
            .with_header(true)
            .build(cursor)
            .map_err(|e| ExecutionError::InvalidOperation(format!("CSV reader error: {e}")))?;

        let mut batches = Vec::new();
        for result in reader {
            let batch = result?;
            if let Some(ref projection) = ctx.projection {
                let columns: Vec<arrow::array::ArrayRef> = projection
                    .iter()
                    .map(|&i| batch.column(i).clone())
                    .collect();
                let fields: Vec<arrow::datatypes::FieldRef> = projection
                    .iter()
                    .map(|&i| batch.schema().field(i).clone().into())
                    .collect();
                let projected_schema = Arc::new(arrow::datatypes::Schema::new(fields));
                batches.push(
                    arrow::array::RecordBatch::try_new(projected_schema, columns)
                        .map_err(ExecutionError::ArrowError)?,
                );
            } else {
                batches.push(batch);
            }
        }

        let output_schema = if let Some(ref projection) = ctx.projection {
            let fields: Vec<arrow::datatypes::FieldRef> = projection
                .iter()
                .map(|&i| self.arrow_schema.field(i).clone().into())
                .collect();
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            self.arrow_schema.clone()
        };
        Ok(stream_from_batches(output_schema, batches))
    }
}

// ---------------------------------------------------------------------------
// ParquetDataSource
// ---------------------------------------------------------------------------

/// Reads a Parquet file via ObjectStore and produces Arrow RecordBatches.
pub struct ParquetDataSource {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    column_schema: Vec<ColumnInfo>,
}

impl ParquetDataSource {
    /// Creates a new Parquet data source, reading schema from file metadata
    /// via the provided ObjectStore.
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
    ) -> Result<Self, ConnectorError> {
        let meta = store.head(&path).await.map_err(|e| {
            ConnectorError::ReadError(format!("failed to stat Parquet file '{}': {}", path, e))
        })?;

        let reader =
            parquet::arrow::async_reader::ParquetObjectReader::new(store.clone(), meta.location)
                .with_file_size(meta.size);

        let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("Parquet metadata error: {e}")))?;

        let arrow_schema = builder.schema().clone();
        let column_schema = arrow_schema_to_column_info(&arrow_schema)?;

        Ok(Self {
            store,
            path,
            column_schema,
        })
    }
}

impl fmt::Debug for ParquetDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetDataSource")
            .field("path", &self.path.to_string())
            .finish()
    }
}

#[async_trait]
impl DataSource for ParquetDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<SendableRecordBatchStream, ExecutionError> {
        let meta = self.store.head(&self.path).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "failed to stat Parquet file '{}': {}",
                self.path, e
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
                    ExecutionError::InvalidOperation(format!("Parquet reader error: {e}"))
                })?;

        // Apply projection pushdown: only read requested columns.
        if let Some(ref projection) = ctx.projection {
            let mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                projection.iter().copied(),
            );
            builder = builder.with_projection(mask);
        }

        let arrow_schema = builder.schema().clone();

        let stream = builder.build().map_err(|e| {
            ExecutionError::InvalidOperation(format!("Parquet reader build error: {e}"))
        })?;

        // Collect batches from the async stream.
        use futures::StreamExt;
        let mut batches = Vec::new();
        let mut stream = stream;
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| {
                ExecutionError::InvalidOperation(format!("Parquet read error: {e}"))
            })?;
            batches.push(batch);
        }
        Ok(stream_from_batches(arrow_schema, batches))
    }
}

// ---------------------------------------------------------------------------
// FileTable
// ---------------------------------------------------------------------------

/// Table metadata for a registered file.
#[derive(Debug)]
struct FileTableEntry {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    format: FileFormat,
    schema: Vec<ColumnInfo>,
}

/// A file-backed table exposing schema metadata via [`TableProvider`].
#[derive(Debug)]
pub struct FileTable {
    schema: Vec<ColumnInfo>,
}

impl FileTable {
    /// Creates a new file table with the given schema.
    pub fn new(schema: Vec<ColumnInfo>) -> Self {
        Self { schema }
    }
}

impl TableProvider for FileTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// FileConnectorFactory
// ---------------------------------------------------------------------------

/// Factory that creates data sources from registered file tables.
pub struct FileConnectorFactory {
    tables: RwLock<HashMap<String, FileTableEntry>>,
    storage_registry: Arc<StorageRegistry>,
}

impl FileConnectorFactory {
    /// Creates a new file connector factory with a storage registry.
    pub fn new(storage_registry: Arc<StorageRegistry>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            storage_registry,
        }
    }

    /// Registers a file as a named table.
    ///
    /// The `path` can be a local path or a remote URI (s3://, gs://, etc.).
    /// For CSV files, `schema` must be provided. For Parquet files, `schema`
    /// can be `None` and will be read from the file metadata.
    pub async fn register_table(
        &self,
        name: impl Into<String>,
        path: &str,
        format: FileFormat,
        schema: Option<Vec<ColumnInfo>>,
    ) -> Result<(), ConnectorError> {
        let uri = StorageUri::parse(path)?;
        let store = self.storage_registry.get_store(&uri)?;
        let obj_path = uri.object_path();

        let schema = match (format, schema) {
            (_, Some(s)) => s,
            (FileFormat::Parquet, None) => {
                let ds = ParquetDataSource::new(store.clone(), obj_path.clone()).await?;
                ds.column_schema.clone()
            }
            (FileFormat::Csv, None) => {
                return Err(ConnectorError::UnsupportedOperation(
                    "CSV tables require an explicit schema".to_string(),
                ));
            }
        };
        self.tables.write().unwrap().insert(
            name.into(),
            FileTableEntry {
                store,
                path: obj_path,
                format,
                schema,
            },
        );
        Ok(())
    }
}

impl fmt::Debug for FileConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tables = self.tables.read().unwrap();
        f.debug_struct("FileConnectorFactory")
            .field("tables", &tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ConnectorFactory for FileConnectorFactory {
    fn name(&self) -> &str {
        "file"
    }

    fn create_data_source(
        &self,
        table: &TableReference,
        _schema: &[ColumnInfo],
        _properties: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        let tables = self.tables.read().unwrap();
        let entry = tables.get(&table.table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("file table '{}' not registered", table.table))
        })?;

        match entry.format {
            FileFormat::Csv => {
                let ds = CsvDataSource::new(
                    entry.store.clone(),
                    entry.path.clone(),
                    entry.schema.clone(),
                );
                Ok(Arc::new(ds))
            }
            FileFormat::Parquet => {
                // For Parquet, we create a lightweight data source that will read on scan().
                // Schema was already resolved at registration time.
                Ok(Arc::new(PreResolvedParquetDataSource {
                    store: entry.store.clone(),
                    path: entry.path.clone(),
                    column_schema: entry.schema.clone(),
                }))
            }
        }
    }
}

/// A Parquet data source with pre-resolved schema (avoids async in create_data_source).
struct PreResolvedParquetDataSource {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    column_schema: Vec<ColumnInfo>,
}

impl fmt::Debug for PreResolvedParquetDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreResolvedParquetDataSource")
            .field("path", &self.path.to_string())
            .finish()
    }
}

#[async_trait]
impl DataSource for PreResolvedParquetDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<SendableRecordBatchStream, ExecutionError> {
        let meta = self.store.head(&self.path).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "failed to stat Parquet file '{}': {}",
                self.path, e
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
                    ExecutionError::InvalidOperation(format!("Parquet reader error: {e}"))
                })?;

        if let Some(ref projection) = ctx.projection {
            let mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                projection.iter().copied(),
            );
            builder = builder.with_projection(mask);
        }

        let arrow_schema = builder.schema().clone();

        let stream = builder.build().map_err(|e| {
            ExecutionError::InvalidOperation(format!("Parquet reader build error: {e}"))
        })?;

        use futures::StreamExt;
        let mut batches = Vec::new();
        let mut stream = stream;
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| {
                ExecutionError::InvalidOperation(format!("Parquet read error: {e}"))
            })?;
            batches.push(batch);
        }
        Ok(stream_from_batches(arrow_schema, batches))
    }
}

// ---------------------------------------------------------------------------
// FileSchema / FileCatalog
// ---------------------------------------------------------------------------

/// A schema backed by the file connector's registered tables.
pub struct FileSchema {
    factory: Arc<FileConnectorFactory>,
}

impl FileSchema {
    /// Creates a schema view over a file connector factory.
    pub fn new(factory: Arc<FileConnectorFactory>) -> Self {
        Self { factory }
    }
}

impl fmt::Debug for FileSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSchema").finish()
    }
}

#[async_trait]
impl SchemaProvider for FileSchema {
    async fn table_names(&self) -> Vec<String> {
        self.factory
            .tables
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.factory.tables.read().unwrap();
        tables
            .get(name)
            .map(|e| Arc::new(FileTable::new(e.schema.clone())) as Arc<dyn TableProvider>)
    }
}

/// A catalog backed by a single file schema.
pub struct FileCatalog {
    schemas: HashMap<String, Arc<FileSchema>>,
}

impl FileCatalog {
    /// Creates a catalog with a single schema.
    pub fn new(schema_name: impl Into<String>, schema: Arc<FileSchema>) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(schema_name.into(), schema);
        Self { schemas }
    }
}

impl fmt::Debug for FileCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCatalog")
            .field("schemas", &self.schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[async_trait]
impl CatalogProvider for FileCatalog {
    async fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

// ---------------------------------------------------------------------------
// Legacy convenience constructors
// ---------------------------------------------------------------------------

/// Convenience re-export for file-based schema info.
pub type FileSchemaInfo = Vec<ColumnInfo>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn column_info_to_arrow_schema(columns: &[ColumnInfo]) -> Arc<Schema> {
    let fields: Vec<arrow::datatypes::Field> = columns.iter().map(|c| c.clone().into()).collect();
    Arc::new(Schema::new(fields))
}

fn arrow_schema_to_column_info(schema: &Schema) -> Result<Vec<ColumnInfo>, ConnectorError> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let data_type = arneb_common::types::DataType::try_from(f.data_type().clone())
                .map_err(|e| {
                    ConnectorError::UnsupportedOperation(format!(
                        "unsupported Arrow type in Parquet file: {e}"
                    ))
                })?;
            Ok(ColumnInfo {
                name: f.name().clone(),
                data_type,
                nullable: f.is_nullable(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;
    use arneb_execution::ScanContext;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field};
    use std::io::Write;
    use std::path::Path;

    fn csv_schema() -> Vec<ColumnInfo> {
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

    fn write_test_csv(dir: &Path) -> std::path::PathBuf {
        let path = dir.join("test.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "id,name").unwrap();
        writeln!(f, "1,alice").unwrap();
        writeln!(f, "2,bob").unwrap();
        writeln!(f, "3,carol").unwrap();
        path
    }

    fn write_test_parquet(dir: &Path) -> std::path::PathBuf {
        use parquet::arrow::arrow_writer::ArrowWriter;

        let path = dir.join("test.parquet");
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn local_store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::local::LocalFileSystem::new())
    }

    fn to_object_path(path: &std::path::Path) -> ObjectPath {
        ObjectPath::from_absolute_path(path).unwrap()
    }

    // -- CSV tests --

    #[tokio::test]
    async fn csv_data_source_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let ds = CsvDataSource::new(local_store(), to_object_path(&path), csv_schema());
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn csv_data_source_file_not_found() {
        let ds = CsvDataSource::new(
            local_store(),
            ObjectPath::from("nonexistent/path.csv"),
            csv_schema(),
        );
        assert!(ds.scan(&ScanContext::default()).await.is_err());
    }

    // -- Parquet tests --

    #[tokio::test]
    async fn parquet_data_source_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(local_store(), to_object_path(&path))
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn parquet_data_source_schema_from_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(local_store(), to_object_path(&path))
            .await
            .unwrap();
        let schema = ds.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "name");
    }

    #[tokio::test]
    async fn parquet_data_source_file_not_found() {
        let result =
            ParquetDataSource::new(local_store(), ObjectPath::from("nonexistent/path.parquet"))
                .await;
        assert!(result.is_err());
    }

    // -- FileConnectorFactory tests --

    #[tokio::test]
    async fn file_factory_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let registry = Arc::new(StorageRegistry::new());
        let factory = FileConnectorFactory::new(registry);
        factory
            .register_table(
                "sales",
                path.to_str().unwrap(),
                FileFormat::Csv,
                Some(csv_schema()),
            )
            .await
            .unwrap();

        let table_ref = TableReference::table("sales");
        let ds = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .unwrap();
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn file_factory_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let registry = Arc::new(StorageRegistry::new());
        let factory = FileConnectorFactory::new(registry);
        factory
            .register_table("events", path.to_str().unwrap(), FileFormat::Parquet, None)
            .await
            .unwrap();

        let table_ref = TableReference::table("events");
        let ds = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .unwrap();
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn file_factory_table_not_found() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = FileConnectorFactory::new(registry);
        let table_ref = TableReference::table("nope");
        let result = factory.create_data_source(&table_ref, &[], &Default::default());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not registered"));
    }

    // -- FileSchema / FileCatalog tests --

    #[tokio::test]
    async fn file_schema_and_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let registry = Arc::new(StorageRegistry::new());
        let factory = Arc::new(FileConnectorFactory::new(registry));
        factory
            .register_table(
                "sales",
                path.to_str().unwrap(),
                FileFormat::Csv,
                Some(csv_schema()),
            )
            .await
            .unwrap();

        let schema = Arc::new(FileSchema::new(factory));
        assert_eq!(schema.table_names().await.len(), 1);
        let tp = schema.table("sales").await.unwrap();
        assert_eq!(tp.schema().len(), 2);

        let catalog = FileCatalog::new("default", schema);
        assert_eq!(catalog.schema_names().await.len(), 1);
        assert!(catalog.schema("default").await.is_some());
    }

    // -- Integration tests --

    #[tokio::test]
    async fn integration_memory_connector() {
        let catalog = Arc::new(MemoryCatalog::new());
        let mem_schema = Arc::new(MemorySchema::new());

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowDataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![10, 20]))])
                .unwrap();
        let table = Arc::new(MemoryTable::new(
            vec![ColumnInfo {
                name: "val".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            vec![batch],
        ));
        mem_schema.register_table("t", table);
        catalog.register_schema("default", mem_schema);

        let factory = MemoryConnectorFactory::new(catalog, "default");
        let ds = factory
            .create_data_source(&TableReference::table("t"), &[], &Default::default())
            .unwrap();
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn integration_csv_via_object_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let ds = CsvDataSource::new(local_store(), to_object_path(&path), csv_schema());
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
    }

    #[tokio::test]
    async fn integration_parquet_via_object_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(local_store(), to_object_path(&path))
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "x");
    }

    // -- InMemory ObjectStore tests (simulating cloud storage) --

    fn write_parquet_bytes() -> (Vec<u8>, Vec<ColumnInfo>) {
        use parquet::arrow::arrow_writer::ArrowWriter;

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("value", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100, 200, 300])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let schema = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "value".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ];
        (buf, schema)
    }

    #[tokio::test]
    async fn parquet_via_inmemory_store() {
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store = Arc::new(InMemory::new());
        let obj_path = ObjectPath::from("data/test.parquet");
        let (parquet_bytes, _) = write_parquet_bytes();
        store
            .put(&obj_path, PutPayload::from_bytes(parquet_bytes.into()))
            .await
            .unwrap();

        // Register InMemory store as "s3://test-bucket" in StorageRegistry
        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("s3://test-bucket", store);

        let factory = FileConnectorFactory::new(registry);
        factory
            .register_table(
                "remote_events",
                "s3://test-bucket/data/test.parquet",
                FileFormat::Parquet,
                None,
            )
            .await
            .unwrap();

        let table_ref = TableReference::table("remote_events");
        let ds = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .unwrap();
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 100);
    }

    #[tokio::test]
    async fn csv_via_inmemory_store() {
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store = Arc::new(InMemory::new());
        let obj_path = ObjectPath::from("data/test.csv");
        let csv_content = b"id,name\n1,alice\n2,bob\n";
        store
            .put(&obj_path, PutPayload::from_static(csv_content))
            .await
            .unwrap();

        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("gs://analytics", store);

        let factory = FileConnectorFactory::new(registry);
        factory
            .register_table(
                "users",
                "gs://analytics/data/test.csv",
                FileFormat::Csv,
                Some(csv_schema()),
            )
            .await
            .unwrap();

        let table_ref = TableReference::table("users");
        let ds = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .unwrap();

        // Test with projection pushdown
        let ctx = ScanContext::default().with_projection(vec![1]); // only "name" column
        let stream = ds.scan(&ctx).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        let name_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "alice");
    }

    #[tokio::test]
    async fn unregistered_cloud_scheme_returns_error() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = FileConnectorFactory::new(registry);
        let result = factory
            .register_table(
                "missing",
                "s3://unknown-bucket/file.parquet",
                FileFormat::Parquet,
                None,
            )
            .await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("S3") || err_msg.contains("storage"),
            "expected descriptive error about S3 storage, got: {err_msg}"
        );
    }

    // Need to re-import MemoryCatalog etc for integration tests
    use super::super::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
}
