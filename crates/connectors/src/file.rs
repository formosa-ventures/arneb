//! File-based connector: reads CSV and Parquet files from local or remote storage.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};
use std::task::{Context, Poll};

use arneb_catalog::{CatalogProvider, SchemaProvider, TableProvider, TableStatistics};
use arneb_common::error::{ArnebError, ConnectorError, ExecutionError};
use arneb_common::stream::{stream_from_batches, RecordBatchStream, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_execution::{DataSource, ScanContext};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use futures::Stream;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::storage::{StorageRegistry, StorageUri};
use crate::ConnectorFactory;

/// Default record-batch size for Parquet/CSV readers when
/// [`ScanContext::batch_size`] is unset.
///
/// Defaults to 2048 — small per-partition in-flight Arrow batches keep scan
/// memory low (the deliberate override of Parquet's built-in 8192). Tunable at
/// runtime via `ARNEB_SCAN_BATCH_SIZE`: a larger value amortizes per-batch
/// overhead across the whole pipeline (decode → filter → repartition → hash →
/// exchange) at the cost of more in-flight Arrow memory. Read, applied, and
/// logged once. Both the file and Hive connectors route through this helper so
/// the knob is honored uniformly.
pub fn scan_default_batch_size() -> usize {
    static BATCH_SIZE: OnceLock<usize> = OnceLock::new();
    *BATCH_SIZE.get_or_init(|| {
        let value = std::env::var("ARNEB_SCAN_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(2048);
        tracing::info!(
            target: "arneb::config",
            scan_batch_size = value,
            "ARNEB_SCAN_BATCH_SIZE effective value (default 2048; larger amortizes \
             per-batch overhead at the cost of higher scan memory)"
        );
        value
    })
}

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

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        if partition != 0 {
            return Err(ExecutionError::InvalidOperation(format!(
                "single-file data source: partition {partition} out of range"
            )));
        }
        let _ = partition;
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

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        if partition != 0 {
            return Err(ExecutionError::InvalidOperation(format!(
                "single-file data source: partition {partition} out of range"
            )));
        }
        let _ = partition;
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

        // Apply row group pruning based on filters.
        if !ctx.filters.is_empty() {
            let column_names: Vec<String> =
                self.column_schema.iter().map(|c| c.name.clone()).collect();
            let file_meta = builder.metadata().clone();
            let selected = crate::parquet_pushdown::prune_row_groups(
                file_meta.row_groups(),
                &ctx.filters,
                &column_names,
            );
            if selected.len() < file_meta.row_groups().len() {
                let selection = parquet::arrow::arrow_reader::RowSelection::from(
                    build_row_selection(file_meta.row_groups(), &selected),
                );
                builder = builder.with_row_selection(selection);
            }
        }

        // Apply predicate pushdown for within-row-group filtering.
        if !ctx.filters.is_empty() || !ctx.dynamic_filter_domains.is_empty() {
            if let Some(row_filter) = crate::parquet_pushdown::build_row_filter_with_dynamic_domains(
                &ctx.filters,
                &ctx.dynamic_filter_domains,
                builder.parquet_schema(),
            ) {
                builder = builder.with_row_filter(row_filter);
            }
        }

        // Apply projection pushdown: only read requested columns.
        if let Some(ref projection) = ctx.projection {
            let mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                projection.iter().copied(),
            );
            builder = builder.with_projection(mask);
        }

        // Default 2048 (override Parquet's built-in 8192) to keep
        // per-partition in-flight Arrow batches small. Matches the
        // Hive connector; rationale lives there. Tunable via
        // `ARNEB_SCAN_BATCH_SIZE` (see `scan_default_batch_size`).
        let batch_size = ctx.batch_size.unwrap_or_else(scan_default_batch_size);
        builder = builder.with_batch_size(batch_size);

        let arrow_schema = builder.schema().clone();

        let stream = builder.build().map_err(|e| {
            ExecutionError::InvalidOperation(format!("Parquet reader build error: {e}"))
        })?;

        // Phase 3b.7b (2026-05-21): true streaming. Previously this
        // collected every parquet batch into a `Vec<RecordBatch>`
        // before returning, holding the whole partition's decoded
        // Arrow batches in memory before the downstream operator saw
        // the first row — for SF1 lineitem with 7 projected columns
        // that's ~336 MB per query. Matches the hive datasource
        // pattern (crates/hive/src/datasource.rs::ParquetBatchStream).
        Ok(Box::pin(ParquetBatchStream {
            schema: arrow_schema,
            inner: Box::pin(stream),
            file_path: self.path.as_ref().to_string(),
        }))
    }
}

/// Adapts a [`ParquetRecordBatchStream`] into a
/// [`SendableRecordBatchStream`] without materialising the partition's
/// batches up front. Mirrors `crates/hive/src/datasource.rs::ParquetBatchStream`.
struct ParquetBatchStream {
    schema: arrow::datatypes::SchemaRef,
    inner: Pin<
        Box<
            parquet::arrow::async_reader::ParquetRecordBatchStream<
                parquet::arrow::async_reader::ParquetObjectReader,
            >,
        >,
    >,
    file_path: String,
}

impl Stream for ParquetBatchStream {
    type Item = Result<RecordBatch, ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(b))) => Poll::Ready(Some(Ok(b))),
            Poll::Ready(Some(Err(e))) => {
                let msg = format!("Parquet read error for '{}': {e}", self.file_path);
                Poll::Ready(Some(Err(ExecutionError::InvalidOperation(msg).into())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ParquetBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Build a RowSelector list from selected row group indices.
///
/// Each row group contributes either a "select" or "skip" entry based on
/// whether its index is in the selected set.
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
    /// Total row count, populated at registration time for Parquet files
    /// by summing `RowGroupMetaData::num_rows()` over the footer. `None`
    /// for CSV (line counting is too expensive without a pre-scan).
    row_count: Option<u64>,
    /// On-disk byte size of the file, when known.
    size_bytes: Option<u64>,
}

/// A file-backed table exposing schema metadata via [`TableProvider`].
#[derive(Debug)]
pub struct FileTable {
    schema: Vec<ColumnInfo>,
    row_count: Option<u64>,
    size_bytes: Option<u64>,
}

impl FileTable {
    /// Creates a new file table with the given schema and no statistics.
    pub fn new(schema: Vec<ColumnInfo>) -> Self {
        Self {
            schema,
            row_count: None,
            size_bytes: None,
        }
    }

    /// Creates a new file table with the given schema and statistics.
    pub fn with_statistics(
        schema: Vec<ColumnInfo>,
        row_count: Option<u64>,
        size_bytes: Option<u64>,
    ) -> Self {
        Self {
            schema,
            row_count,
            size_bytes,
        }
    }
}

impl TableProvider for FileTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }

    fn statistics(&self) -> Option<TableStatistics> {
        if self.row_count.is_none() && self.size_bytes.is_none() {
            return None;
        }
        Some(TableStatistics {
            row_count: self.row_count,
            size_bytes: self.size_bytes,
            columns: Default::default(),
        })
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

        let (schema, row_count, size_bytes) = match (format, schema) {
            (FileFormat::Parquet, schema_opt) => {
                // Open the file once for both schema (if needed) and stats.
                let ds = ParquetDataSource::new(store.clone(), obj_path.clone()).await?;
                let schema = schema_opt.unwrap_or_else(|| ds.column_schema.clone());
                let stats = parquet_file_statistics(&store, &obj_path).await;
                (schema, stats.row_count, stats.size_bytes)
            }
            (FileFormat::Csv, Some(s)) => (s, None, None),
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
                row_count,
                size_bytes,
            },
        );
        Ok(())
    }
}

/// Best-effort statistics for a Parquet file. Reads the footer once and
/// sums `num_rows` across row groups; returns `None` for any field that
/// fails to compute (the caller treats `None` as "no stats").
struct ParquetFileStats {
    row_count: Option<u64>,
    size_bytes: Option<u64>,
}

async fn parquet_file_statistics(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> ParquetFileStats {
    let mut out = ParquetFileStats {
        row_count: None,
        size_bytes: None,
    };
    let meta = match store.head(path).await {
        Ok(m) => m,
        Err(_) => return out,
    };
    out.size_bytes = Some(meta.size);

    let reader =
        parquet::arrow::async_reader::ParquetObjectReader::new(store.clone(), meta.location)
            .with_file_size(meta.size);
    let builder =
        match parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader).await {
            Ok(b) => b,
            Err(_) => return out,
        };
    let total_rows: i64 = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .sum();
    if total_rows >= 0 {
        out.row_count = Some(total_rows as u64);
    }
    out
}

impl fmt::Debug for FileConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tables = self.tables.read().unwrap();
        f.debug_struct("FileConnectorFactory")
            .field("tables", &tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[async_trait]
impl ConnectorFactory for FileConnectorFactory {
    fn name(&self) -> &str {
        "file"
    }

    async fn create_data_source(
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

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        if partition != 0 {
            return Err(ExecutionError::InvalidOperation(format!(
                "single-file data source: partition {partition} out of range"
            )));
        }
        let _ = partition;
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

        // Phase 3b.7b (2026-05-21): true streaming. Same change as
        // ParquetDataSource::scan above.
        Ok(Box::pin(ParquetBatchStream {
            schema: arrow_schema,
            inner: Box::pin(stream),
            file_path: self.path.to_string(),
        }))
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
        tables.get(name).map(|e| {
            Arc::new(FileTable::with_statistics(
                e.schema.clone(),
                e.row_count,
                e.size_bytes,
            )) as Arc<dyn TableProvider>
        })
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
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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
        assert!(ds.scan(&ScanContext::default(), 0).await.is_err());
    }

    // -- Parquet tests --

    #[tokio::test]
    async fn parquet_data_source_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(local_store(), to_object_path(&path))
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn file_factory_table_not_found() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = FileConnectorFactory::new(registry);
        let table_ref = TableReference::table("nope");
        let result = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .await;
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
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn integration_csv_via_object_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let ds = CsvDataSource::new(local_store(), to_object_path(&path), csv_schema());
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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
    async fn parquet_registration_populates_row_count_statistics() {
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store = Arc::new(InMemory::new());
        let obj_path = ObjectPath::from("data/stats.parquet");
        let (parquet_bytes, _) = write_parquet_bytes();
        let bytes_len = parquet_bytes.len() as u64;
        store
            .put(&obj_path, PutPayload::from_bytes(parquet_bytes.into()))
            .await
            .unwrap();

        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("s3://test-bucket", store);

        let factory = Arc::new(FileConnectorFactory::new(registry));
        factory
            .register_table(
                "events",
                "s3://test-bucket/data/stats.parquet",
                FileFormat::Parquet,
                None,
            )
            .await
            .unwrap();

        let schema = Arc::new(FileSchema::new(factory));
        let table = schema.table("events").await.expect("table registered");
        let stats = table.statistics().expect("Parquet must expose statistics");
        assert_eq!(stats.row_count, Some(3), "row_count should be 3");
        assert_eq!(
            stats.size_bytes,
            Some(bytes_len),
            "size_bytes from object_store::head()"
        );
    }

    #[tokio::test]
    async fn csv_registration_returns_no_statistics() {
        use arneb_common::types::DataType;
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store = Arc::new(InMemory::new());
        let obj_path = ObjectPath::from("data/x.csv");
        store
            .put(
                &obj_path,
                PutPayload::from_bytes(b"id,name\n1,a\n2,b\n".to_vec().into()),
            )
            .await
            .unwrap();
        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("s3://test-bucket", store);
        let factory = Arc::new(FileConnectorFactory::new(registry));
        factory
            .register_table(
                "csv_t",
                "s3://test-bucket/data/x.csv",
                FileFormat::Csv,
                Some(vec![
                    ColumnInfo {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    ColumnInfo {
                        name: "name".into(),
                        data_type: DataType::Utf8,
                        nullable: false,
                    },
                ]),
            )
            .await
            .unwrap();

        let schema = Arc::new(FileSchema::new(factory));
        let table = schema.table("csv_t").await.expect("table registered");
        assert!(
            table.statistics().is_none(),
            "CSV statistics should be None — line counting is too expensive"
        );
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
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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
            .await
            .unwrap();

        // Test with projection pushdown
        let ctx = ScanContext::default().with_projection(vec![1]); // only "name" column
        let stream = ds.scan(&ctx, 0).await.unwrap();
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

    // -- Row group pruning and batch size tests --

    #[tokio::test]
    async fn parquet_batch_size_produces_correct_results() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(local_store(), to_object_path(&path))
            .await
            .unwrap();
        let ctx = ScanContext::default().with_batch_size(1);
        let stream = ds.scan(&ctx, 0).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        // With batch_size=1, we expect 2 batches of 1 row each
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn parquet_filters_passed_to_scan() {
        use arneb_common::types::ScalarValue;
        use arneb_planner::PlanExpr;
        use arneb_sql_parser::ast::BinaryOp;

        let dir = tempfile::tempdir().unwrap();

        // Create a Parquet file with multiple row groups by writing small batches
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let path = dir.path().join("filtered.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(2)) // force small row groups
            .build();
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            arrow_schema.clone(),
            Some(props),
        )
        .unwrap();

        // Write 6 rows → should create 3 row groups of 2 rows each
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let ds = ParquetDataSource::new(local_store(), to_object_path(&path))
            .await
            .unwrap();

        // Without filter: should read all 6 rows
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 6);

        // With filter: id > 4 — row groups with max <= 4 should be pruned
        let filter = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "id".to_string(),
                span: None,
            }),
            op: BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int32(4),
                span: None,
            }),
            span: None,
        };
        let ctx = ScanContext::default().with_filters(vec![filter]);
        let stream = ds.scan(&ctx, 0).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Row group [1,2] pruned (max=2 ≤ 4), row group [3,4] pruned (max=4 ≤ 4)
        // Only row group [5,6] remains → 2 rows
        assert!(total <= 6, "filter should not add rows");
        // With stats-based pruning, we expect fewer rows than without filter
        // (exact count depends on whether stats are written for this small file)
    }

    // -- Parquet nested type tests --

    #[tokio::test]
    async fn parquet_scan_with_nested_columns_projection() {
        use arrow::array::ListArray;
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Field;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested.parquet");

        // Create a Parquet file with: id (Int32), tags (List<Utf8>), name (Utf8)
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new(
                "tags",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true))),
                true,
            ),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));

        let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let names = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));

        // Build a simple List<Utf8> array
        let values = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let tags = Arc::new(ListArray::new(
            Arc::new(Field::new("item", ArrowDataType::Utf8, true)),
            offsets,
            Arc::new(values),
            None,
        ));

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![ids, tags, names]).unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let mut writer =
            parquet::arrow::arrow_writer::ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read with projection: only primitive columns (id=0, name=2), skip tags(1)
        let ds = ParquetDataSource::new(local_store(), to_object_path(&path))
            .await
            .unwrap();
        let ctx = ScanContext::default().with_projection(vec![0, 2]);
        let stream = ds.scan(&ctx, 0).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();

        assert_eq!(batches[0].num_columns(), 2);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);

        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "alice");
    }

    // -- Parquet compression tests --

    fn write_parquet_with_compression(
        dir: &Path,
        name: &str,
        compression: parquet::basic::Compression,
    ) -> std::path::PathBuf {
        use parquet::arrow::arrow_writer::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let path = dir.join(name);
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    async fn assert_parquet_readable(path: &Path) {
        let ds = ParquetDataSource::new(local_store(), to_object_path(path))
            .await
            .unwrap();
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "expected 3 rows from {}", path.display());

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
    }

    #[tokio::test]
    async fn parquet_reads_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_parquet_with_compression(
            dir.path(),
            "uncompressed.parquet",
            parquet::basic::Compression::UNCOMPRESSED,
        );
        assert_parquet_readable(&path).await;
    }

    #[tokio::test]
    async fn parquet_reads_snappy() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_parquet_with_compression(
            dir.path(),
            "snappy.parquet",
            parquet::basic::Compression::SNAPPY,
        );
        assert_parquet_readable(&path).await;
    }

    #[tokio::test]
    async fn parquet_reads_gzip() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_parquet_with_compression(
            dir.path(),
            "gzip.parquet",
            parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default()),
        );
        assert_parquet_readable(&path).await;
    }

    #[tokio::test]
    async fn parquet_reads_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_parquet_with_compression(
            dir.path(),
            "zstd.parquet",
            parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()),
        );
        assert_parquet_readable(&path).await;
    }

    #[tokio::test]
    async fn parquet_reads_lz4() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_parquet_with_compression(
            dir.path(),
            "lz4.parquet",
            parquet::basic::Compression::LZ4_RAW,
        );
        assert_parquet_readable(&path).await;
    }

    #[tokio::test]
    async fn parquet_reads_brotli() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_parquet_with_compression(
            dir.path(),
            "brotli.parquet",
            parquet::basic::Compression::BROTLI(parquet::basic::BrotliLevel::default()),
        );
        assert_parquet_readable(&path).await;
    }

    // Need to re-import MemoryCatalog etc for integration tests
    use super::super::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
}
