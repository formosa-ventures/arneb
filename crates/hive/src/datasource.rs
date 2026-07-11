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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::debug;

use arneb_common::error::{ArnebError, ConnectorError, ExecutionError};
use arneb_common::stream::{stream_from_batches, RecordBatchStream, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_connectors::storage::{StorageRegistry, StorageUri};
use arneb_connectors::ConnectorFactory;
use arneb_execution::{DataSource, ScanContext};

// ---------------------------------------------------------------------------
// HiveDataSource
// ---------------------------------------------------------------------------

/// Data source that reads Parquet files from a Hive table location.
///
/// The file list is computed once at construction time (by
/// [`HiveConnectorFactory::create_data_source`]) and stored in
/// `file_paths`. Each value of [`partition_count`](DataSource::partition_count)
/// corresponds to one file; [`scan`](DataSource::scan) for partition `i`
/// reads `file_paths[i]` only.
pub struct HiveDataSource {
    /// Object store backend (local, S3, GCS, Azure, etc.).
    store: Arc<dyn ObjectStore>,
    /// Column schema from HMS metadata.
    column_schema: Vec<ColumnInfo>,
    /// Pre-listed Parquet files under the table's storage prefix.
    file_paths: Vec<ObjectPath>,
    /// Number of logical sub-partitions per file (1 = legacy
    /// one-partition-per-file). Each sub-partition reads a row-range
    /// slice via `with_row_selection`, exposing more parallel scan
    /// tasks than the raw file count when the workload has many CPU
    /// cores and few files.
    splits_per_file: usize,
}

impl HiveDataSource {
    /// Create a new Hive data source with a pre-listed file set.
    ///
    /// - `store`: the object store for the table location.
    /// - `column_schema`: column metadata from HMS.
    /// - `file_paths`: pre-listed Parquet files (one per partition). The
    ///   caller (usually [`HiveConnectorFactory::create_data_source`])
    ///   resolves this via [`list_parquet_files`] during async construction.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        column_schema: Vec<ColumnInfo>,
        file_paths: Vec<ObjectPath>,
    ) -> Self {
        // Pick a `splits_per_file` so the resulting partition count
        // saturates available CPU cores. For a 4-file table on a
        // 14-core machine that's `ceil(14/4) = 4` splits per file →
        // 16 scan partitions. For a 16-file table we already have
        // enough — keep splits_per_file=1.
        let n_files = file_paths.len().max(1);
        let target = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        let splits_per_file = if n_files >= target {
            1
        } else {
            target.div_ceil(n_files)
        };
        Self {
            store,
            column_schema,
            file_paths,
            splits_per_file,
        }
    }

    /// Async constructor that lists the Parquet files under `prefix`
    /// before building the source. Convenience for callers and tests
    /// that don't already have the file list in hand.
    pub async fn from_prefix(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        column_schema: Vec<ColumnInfo>,
    ) -> Result<Self, ExecutionError> {
        let file_paths = list_parquet_files(&store, &prefix).await?;
        Ok(Self::new(store, column_schema, file_paths))
    }
}

impl fmt::Debug for HiveDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveDataSource")
            .field("files", &self.file_paths.len())
            .field("columns", &self.column_schema.len())
            .finish()
    }
}

#[async_trait]
impl DataSource for HiveDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    fn partition_count(&self) -> usize {
        (self.file_paths.len() * self.splits_per_file).max(1)
    }

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
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

        if self.file_paths.is_empty() {
            debug!("no Parquet files registered on HiveDataSource");
            return Ok(stream_from_batches(output_schema, vec![]));
        }

        let total_partitions = self.file_paths.len() * self.splits_per_file;
        if partition >= total_partitions {
            return Err(ExecutionError::InvalidOperation(format!(
                "HiveDataSource: partition {partition} out of range (have {total_partitions} \
                 partitions = {} files × {} splits)",
                self.file_paths.len(),
                self.splits_per_file
            )));
        }

        let file_idx = partition / self.splits_per_file;
        let split_idx = partition % self.splits_per_file;
        let file_path = &self.file_paths[file_idx];
        let stream = read_one_file_split(
            &self.store,
            file_path,
            ctx,
            &self.column_schema,
            split_idx,
            self.splits_per_file,
        )
        .await?;

        // True pipelined streaming: yield Parquet batches as they're
        // produced instead of collecting the whole partition's output
        // into a Vec first. The earlier `collect → stream_from_batches`
        // path held an entire partition's worth of decoded Arrow
        // batches in memory before the downstream operator saw the
        // first row — for a 6M-row lineitem scan with 7 projected
        // columns that's ~336 MB per query, which dominated the
        // single-table-aggregate work-memory delta vs Trino.
        Ok(Box::pin(ParquetBatchStream {
            schema: output_schema,
            inner: Box::pin(stream),
            file_path: file_path.to_string(),
        }))
    }
}

/// Adapts a [`ParquetRecordBatchStream`] into a
/// [`SendableRecordBatchStream`] without materialising the partition's
/// batches up front. Errors are converted to [`ExecutionError::InvalidOperation`]
/// with file-path context.
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

/// Single-file Parquet reader for one (split_idx, splits_per_file)
/// sub-partition. When `splits_per_file == 1`, this behaves identically
/// to the legacy `read_one_file` (no row-range slicing). Otherwise it
/// caps the read to a contiguous `total_rows / splits_per_file` slice
/// via `with_row_selection` so the file's CPU work spreads across
/// `splits_per_file` parallel tasks.
async fn read_one_file_split(
    store: &Arc<dyn ObjectStore>,
    file_path: &ObjectPath,
    ctx: &ScanContext,
    column_schema: &[ColumnInfo],
    split_idx: usize,
    splits_per_file: usize,
) -> Result<
    parquet::arrow::async_reader::ParquetRecordBatchStream<
        parquet::arrow::async_reader::ParquetObjectReader,
    >,
    ExecutionError,
> {
    use parquet::arrow::arrow_reader::RowSelection;
    let mut builder = open_parquet_builder(store, file_path).await?;
    let total_rows: usize = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum();

    // Apply row-group pruning (min/max) and predicate filters BEFORE
    // building the slice — the slice should reflect the user-visible
    // logical row count. Row-group pruning is OK because all splits
    // see the same min/max-pruned row groups.
    if !ctx.filters.is_empty() {
        let column_names: Vec<String> = column_schema.iter().map(|c| c.name.clone()).collect();
        let file_meta = builder.metadata().clone();
        let selected = arneb_connectors::parquet_pushdown::prune_row_groups(
            file_meta.row_groups(),
            &ctx.filters,
            &column_names,
        );
        if selected.len() < file_meta.row_groups().len() {
            let selectors = build_row_selection(file_meta.row_groups(), &selected);
            // For the slice path below we need to merge this pruning
            // with the slice's RowSelection; simplest is to start
            // from this and intersect.
            let pruning_selection = RowSelection::from(selectors);
            // If we'll also slice, intersect later; if not, apply directly.
            if splits_per_file > 1 {
                let slice = compute_split_selection(total_rows, split_idx, splits_per_file);
                let combined = pruning_selection.intersection(&slice);
                builder = builder.with_row_selection(combined);
            } else {
                builder = builder.with_row_selection(pruning_selection);
            }
        } else if splits_per_file > 1 {
            let slice = compute_split_selection(total_rows, split_idx, splits_per_file);
            builder = builder.with_row_selection(slice);
        }
    } else if splits_per_file > 1 {
        let slice = compute_split_selection(total_rows, split_idx, splits_per_file);
        builder = builder.with_row_selection(slice);
    }

    // Within-row-group predicate pushdown.
    if !ctx.filters.is_empty() {
        if let Some(row_filter) = arneb_connectors::parquet_pushdown::build_row_filter(
            &ctx.filters,
            builder.parquet_schema(),
        ) {
            builder = builder.with_row_filter(row_filter);
        }
    }

    // Column projection pushdown.
    if let Some(ref projection) = ctx.projection {
        let mask = parquet::arrow::ProjectionMask::roots(
            builder.parquet_schema(),
            projection.iter().copied(),
        );
        builder = builder.with_projection(mask);
    }

    // Default 2048 (override Parquet's built-in 8192) to keep per-
    // partition in-flight Arrow batches small. Per Trino architecture
    // research + arrow-rs issue #623: in-flight working set scales
    // linearly with batch_size × pipeline_depth × partition_count;
    // smaller default = lower memory floor for small queries (TPC-H
    // Q01/Q06/Q10/Q12/Q14 baseline). Override via `ctx.batch_size`, or
    // tune the default at runtime via `ARNEB_SCAN_BATCH_SIZE`.
    let batch_size = ctx
        .batch_size
        .unwrap_or_else(arneb_connectors::file::scan_default_batch_size);
    builder = builder.with_batch_size(batch_size);

    builder.build().map_err(|e| {
        ExecutionError::InvalidOperation(format!(
            "Parquet reader build error for '{file_path}': {e}"
        ))
    })
}

/// Build a `RowSelection` that picks rows `[split_idx*chunk, (split_idx+1)*chunk)`
/// out of `total_rows` (clamped). `chunk = ceil(total_rows / splits)`.
fn compute_split_selection(
    total_rows: usize,
    split_idx: usize,
    splits: usize,
) -> parquet::arrow::arrow_reader::RowSelection {
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    let chunk = total_rows.div_ceil(splits);
    let start = (split_idx * chunk).min(total_rows);
    let end = ((split_idx + 1) * chunk).min(total_rows);
    let mut selectors = Vec::with_capacity(3);
    if start > 0 {
        selectors.push(RowSelector::skip(start));
    }
    if end > start {
        selectors.push(RowSelector::select(end - start));
    }
    if total_rows > end {
        selectors.push(RowSelector::skip(total_rows - end));
    }
    RowSelection::from(selectors)
}

/// Open the Parquet stream builder for a file; common entry point
/// for `read_one_file_split` and the legacy `read_one_file` wrapper.
async fn open_parquet_builder(
    store: &Arc<dyn ObjectStore>,
    file_path: &ObjectPath,
) -> Result<
    parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder<
        parquet::arrow::async_reader::ParquetObjectReader,
    >,
    ExecutionError,
> {
    let meta = store.head(file_path).await.map_err(|e| {
        ExecutionError::InvalidOperation(format!("failed to stat Parquet file '{file_path}': {e}"))
    })?;
    let reader =
        parquet::arrow::async_reader::ParquetObjectReader::new(store.clone(), meta.location)
            .with_file_size(meta.size);
    parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| {
            ExecutionError::InvalidOperation(format!("Parquet reader error for '{file_path}': {e}"))
        })
}

/// Legacy single-file reader; kept for tests/back-compat. Equivalent
/// to `read_one_file_split(.., 0, 1)`.
#[allow(dead_code)]
async fn read_one_file(
    store: &Arc<dyn ObjectStore>,
    file_path: &ObjectPath,
    ctx: &ScanContext,
    column_schema: &[ColumnInfo],
) -> Result<
    parquet::arrow::async_reader::ParquetRecordBatchStream<
        parquet::arrow::async_reader::ParquetObjectReader,
    >,
    ExecutionError,
> {
    let meta = store.head(file_path).await.map_err(|e| {
        ExecutionError::InvalidOperation(format!("failed to stat Parquet file '{file_path}': {e}"))
    })?;
    let reader =
        parquet::arrow::async_reader::ParquetObjectReader::new(store.clone(), meta.location)
            .with_file_size(meta.size);
    let mut builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| {
            ExecutionError::InvalidOperation(format!("Parquet reader error for '{file_path}': {e}"))
        })?;

    // Row-group pruning via min/max statistics.
    if !ctx.filters.is_empty() {
        let column_names: Vec<String> = column_schema.iter().map(|c| c.name.clone()).collect();
        let file_meta = builder.metadata().clone();
        let selected = arneb_connectors::parquet_pushdown::prune_row_groups(
            file_meta.row_groups(),
            &ctx.filters,
            &column_names,
        );
        if selected.len() < file_meta.row_groups().len() {
            let selectors = build_row_selection(file_meta.row_groups(), &selected);
            let selection = parquet::arrow::arrow_reader::RowSelection::from(selectors);
            builder = builder.with_row_selection(selection);
        }
    }

    // Within-row-group predicate pushdown.
    if !ctx.filters.is_empty() {
        if let Some(row_filter) = arneb_connectors::parquet_pushdown::build_row_filter(
            &ctx.filters,
            builder.parquet_schema(),
        ) {
            builder = builder.with_row_filter(row_filter);
        }
    }

    // Column projection pushdown.
    if let Some(ref projection) = ctx.projection {
        let mask = parquet::arrow::ProjectionMask::roots(
            builder.parquet_schema(),
            projection.iter().copied(),
        );
        builder = builder.with_projection(mask);
    }

    // Per-batch row-count tuning.
    // Default 2048 (override Parquet's built-in 8192) to keep per-
    // partition in-flight Arrow batches small. Per Trino architecture
    // research + arrow-rs issue #623: in-flight working set scales
    // linearly with batch_size × pipeline_depth × partition_count;
    // smaller default = lower memory floor for small queries (TPC-H
    // Q01/Q06/Q10/Q12/Q14 baseline). Override via `ctx.batch_size`, or
    // tune the default at runtime via `ARNEB_SCAN_BATCH_SIZE`.
    let batch_size = ctx
        .batch_size
        .unwrap_or_else(arneb_connectors::file::scan_default_batch_size);
    builder = builder.with_batch_size(batch_size);

    builder.build().map_err(|e| {
        ExecutionError::InvalidOperation(format!(
            "Parquet reader build error for '{file_path}': {e}"
        ))
    })
}

/// List all data files under a given prefix in an object store.
///
/// Skips hidden files following the Hadoop/Hive convention: any filename
/// whose first character is `.` or `_` is treated as hidden (e.g. `_SUCCESS`,
/// `_committed_*`, `.part-xxx.parquet.crc`). This matches Trino's
/// `HiveFileIterator`. No extension-based filtering is applied — the Hive
/// table's `InputFormat` (currently Parquet-only in arneb) decides how to
/// read the remaining files.
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
        if filename.starts_with('.') || filename.starts_with('_') {
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

#[async_trait]
impl ConnectorFactory for HiveConnectorFactory {
    fn name(&self) -> &str {
        "hive"
    }

    async fn create_data_source(
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
        let (location, column_schema) = {
            let locations = self.locations.read().unwrap();
            match locations.get(&table.table) {
                Some(entry) => entry.clone(),
                None => {
                    return Err(ConnectorError::TableNotFound(format!(
                        "Hive table '{}' location not available in properties or pre-registered map",
                        table.table
                    )));
                }
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

        // Pre-list the Parquet files under this prefix so the resulting
        // HiveDataSource exposes one partition per file (phase 3.3).
        let file_paths = list_parquet_files(&store, &prefix)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("hive list files: {e}")))?;

        Ok(Arc::new(HiveDataSource::new(
            store,
            effective_schema,
            file_paths,
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

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // splits_per_file may be > 1 on this machine; sum across all
        // partitions for a stable row-count assertion.
        let mut total_rows = 0;
        let mut all_batches = Vec::new();
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
            all_batches.extend(batches);
        }
        assert_eq!(total_rows, 3);

        let id_col = all_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // Partition 0 holds the FIRST slice of the file. With 1 file +
        // many splits, that slice starts at row 0 → value(0) == 1.
        assert_eq!(id_col.value(0), 1);
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

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // 2 files × splits_per_file partitions; sum across all = 4 rows.
        assert_eq!(ds.partition_count() % 2, 0);
        let mut total_rows = 0;
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn scan_skips_hidden_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes = write_parquet_bytes(vec![10], vec!["x"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/data.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        // Hadoop/Hive hidden-file markers — should be skipped.
        for hidden in ["_SUCCESS", "_committed_abc", ".hidden"] {
            store
                .put(
                    &ObjectPath::from(format!("warehouse/db/table/{hidden}")),
                    PutPayload::from_static(b""),
                )
                .await
                .unwrap();
        }

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn scan_empty_directory() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/empty_table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // Empty directory → 0 file partitions → partition_count clamps to 1
        // but scan returns immediately with the empty-batches stream below.
        assert!(ds.partition_count() == 1);
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // Project only the "name" column (index 1).
        let ctx = ScanContext::default().with_projection(vec![1]);
        let mut all_batches = Vec::new();
        let mut total_rows = 0;
        let mut all_names: Vec<String> = Vec::new();
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ctx, p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            for b in &batches {
                total_rows += b.num_rows();
                let name_col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                for r in 0..b.num_rows() {
                    all_names.push(name_col.value(r).to_string());
                }
            }
            all_batches.extend(batches);
        }
        assert_eq!(total_rows, 2);
        assert_eq!(all_batches[0].num_columns(), 1);
        all_names.sort();
        assert_eq!(all_names, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn hive_data_source_debug() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();
        let debug_str = format!("{ds:?}");
        assert!(debug_str.contains("HiveDataSource"));
        // Debug now reports file count instead of prefix.
        assert!(debug_str.contains("files"));
    }

    #[tokio::test]
    async fn hive_data_source_schema() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();
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
            .await
            .unwrap();

        let mut total_rows = 0;
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn factory_unregistered_table() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);

        let table_ref = TableReference::table("nonexistent");
        let result = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .await;
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

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        let mut total_rows = 0;
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
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
            .await
            .unwrap();

        // Scan will find no files (directory doesn't exist), returning empty stream.
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert!(batches.is_empty());
    }
}
