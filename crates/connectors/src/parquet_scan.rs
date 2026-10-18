//! Shared split-based Parquet scan used by the Hive and Iceberg connectors.
//!
//! A table's files are exposed as `files × splits_per_file` scan
//! partitions; each partition reads one contiguous row-range slice of one
//! file with row-group pruning, `RowFilter` predicate pushdown, projection
//! pushdown, and the scan batch size applied. Output batches are streamed
//! (never collected per partition), optionally through a per-batch adapter
//! (Iceberg uses it to cast promoted columns and null-fill added ones).

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::Stream;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder};

/// The object-store-backed Parquet reader used by every scan path.
///
/// parquet 59.2 deprecated `ParquetObjectReader` in favour of hand-written
/// `AsyncFileReader` impls (apache/arrow-rs#10308). It still works, and it is
/// the I/O path our benchmarks are measured on, so the dependency bump keeps it
/// unchanged; replacing it is a separate, separately-benchmarked change. The
/// alias keeps the deprecation allowance in this one place.
#[allow(deprecated)]
pub(crate) type ParquetObjectReader = parquet::arrow::async_reader::ParquetObjectReader;

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use arneb_common::stream::RecordBatchStream;
use arneb_planner::PlanExpr;
use parquet::file::metadata::ParquetMetaData;

/// `TableScan` property carrying the scan-parallelism target the planning
/// node chose (see [`scan_parallelism`]).
pub const SCAN_PARALLELISM_PROPERTY: &str = "scan_parallelism";

/// This node's scan-parallelism target: its available CPU cores.
pub fn local_scan_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

/// The scan-parallelism target for a table scan: the planning node's value
/// from [`SCAN_PARALLELISM_PROPERTY`], or this node's own when the plan
/// carries none.
///
/// In distributed mode every worker builds its own data source and the scan
/// tasks stride over its `files × splits_per_file` partitions. Those only
/// cover every partition exactly once when all nodes compute the same count,
/// so the count must not depend on each worker's core count (#157). Catalogs
/// therefore record the planning node's target in the scan's properties.
pub fn scan_parallelism(properties: &std::collections::HashMap<String, String>) -> usize {
    properties
        .get(SCAN_PARALLELISM_PROPERTY)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(local_scan_parallelism)
}

/// Pick a `splits_per_file` so the resulting partition count reaches
/// `target` (normally [`scan_parallelism`]). For a 4-file table and a target
/// of 14 that's `ceil(14/4) = 4` splits per file → 16 scan partitions. For a
/// 16-file table we already have enough — keep splits_per_file=1.
pub fn splits_per_file(n_files: usize, target: usize) -> usize {
    let n_files = n_files.max(1);
    let target = target.max(1);
    if n_files >= target {
        1
    } else {
        target.div_ceil(n_files)
    }
}

/// Map a scan partition to `(file_idx, split_idx)`, or fail when it is out
/// of range. `source` names the data source in the error message.
pub fn split_index(
    source: &str,
    partition: usize,
    n_files: usize,
    splits_per_file: usize,
) -> Result<(usize, usize), ExecutionError> {
    let total_partitions = n_files * splits_per_file;
    if partition >= total_partitions {
        return Err(ExecutionError::InvalidOperation(format!(
            "{source}: partition {partition} out of range (have {total_partitions} \
             partitions = {n_files} files × {splits_per_file} splits)"
        )));
    }
    Ok((partition / splits_per_file, partition % splits_per_file))
}

/// Open the Parquet stream builder for a file.
pub async fn open_parquet_builder(
    store: &Arc<dyn ObjectStore>,
    file_path: &ObjectPath,
) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, ExecutionError> {
    let meta = store.head(file_path).await.map_err(|e| {
        ExecutionError::InvalidOperation(format!("failed to stat Parquet file '{file_path}': {e}"))
    })?;
    let reader = ParquetObjectReader::new(store.clone(), meta.location).with_file_size(meta.size);
    ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| {
            ExecutionError::InvalidOperation(format!("Parquet reader error for '{file_path}': {e}"))
        })
}

/// Configure an opened file for one `(split_idx, splits_per_file)`
/// sub-partition and build its stream.
///
/// - `filters` must reference **file leaf column indices** (for Hive the
///   table schema equals the file schema; Iceberg remaps by field ID).
/// - `projection_roots`: file root columns to read (`None` = all).
/// - `batch_size`: `None` uses [`crate::file::scan_default_batch_size`].
///
/// When `splits_per_file == 1` no row-range slicing is applied. Otherwise
/// the read is capped to a contiguous `total_rows / splits_per_file` slice
/// via `with_row_selection` so the file's CPU work spreads across
/// `splits_per_file` parallel tasks.
#[allow(clippy::too_many_arguments)]
pub fn build_split_stream(
    mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    file_path: &dyn fmt::Display,
    filters: &[PlanExpr],
    dynamic_domains: &[arneb_execution::DynamicFilterDomain],
    projection_roots: Option<&[usize]>,
    batch_size: Option<usize>,
    split_idx: usize,
    splits_per_file: usize,
) -> Result<ParquetRecordBatchStream<ParquetObjectReader>, ExecutionError> {
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
    if !filters.is_empty() {
        let file_meta = builder.metadata().clone();
        let selected =
            crate::parquet_pushdown::prune_row_groups(file_meta.row_groups(), filters, &[]);
        if selected.len() < file_meta.row_groups().len() {
            let selectors = build_row_selection(file_meta.row_groups(), &selected);
            let pruning_selection = RowSelection::from(selectors);
            // If we'll also slice, intersect; if not, apply directly.
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

    // Within-row-group predicate pushdown, including cross-fragment dynamic
    // filter domains (Bloom) resolved by the scan before it started.
    if !filters.is_empty() || !dynamic_domains.is_empty() {
        if let Some(row_filter) = crate::parquet_pushdown::build_row_filter_with_dynamic_domains(
            filters,
            dynamic_domains,
            builder.parquet_schema(),
        ) {
            builder = builder.with_row_filter(row_filter);
        }
    }

    // Column projection pushdown.
    if let Some(roots) = projection_roots {
        let mask =
            parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
        builder = builder.with_projection(mask);
    }

    // Default 2048 (override Parquet's built-in 8192) to keep per-
    // partition in-flight Arrow batches small. Per Trino architecture
    // research + arrow-rs issue #623: in-flight working set scales
    // linearly with batch_size × pipeline_depth × partition_count;
    // smaller default = lower memory floor for small queries (TPC-H
    // Q01/Q06/Q10/Q12/Q14 baseline). Override via `ctx.batch_size`, or
    // tune the default at runtime via `ARNEB_SCAN_BATCH_SIZE`.
    let batch_size = batch_size.unwrap_or_else(crate::file::scan_default_batch_size);
    builder = builder.with_batch_size(batch_size);

    builder.build().map_err(|e| {
        ExecutionError::InvalidOperation(format!(
            "Parquet reader build error for '{file_path}': {e}"
        ))
    })
}

/// Bytes a scan of this file holds in its decode buffer: the compressed
/// size of the projected columns of its largest row group.
///
/// The async reader fetches a whole row group's projected column chunks
/// into memory before decoding it, and decoding copies into fresh Arrow
/// buffers, so this memory is separate from the batches the scan yields.
/// Row groups are read one at a time, so the largest one bounds the scan.
pub fn row_group_buffer_bytes(
    metadata: &ParquetMetaData,
    projection_roots: Option<&[usize]>,
) -> usize {
    let schema = metadata.file_metadata().schema_descr();
    let projected = |leaf: usize| {
        projection_roots.is_none_or(|roots| roots.contains(&schema.get_column_root_idx(leaf)))
    };
    metadata
        .row_groups()
        .iter()
        .map(|rg| {
            rg.columns()
                .iter()
                .enumerate()
                .filter(|(leaf, _)| projected(*leaf))
                .map(|(_, col)| col.compressed_size().max(0) as usize)
                .sum::<usize>()
        })
        .max()
        .unwrap_or(0)
}

/// Build a `RowSelection` that picks rows `[split_idx*chunk, (split_idx+1)*chunk)`
/// out of `total_rows` (clamped). `chunk = ceil(total_rows / splits)`.
pub fn compute_split_selection(total_rows: usize, split_idx: usize, splits: usize) -> RowSelection {
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

/// Build a RowSelector list from selected row group indices.
pub fn build_row_selection(
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    selected: &[usize],
) -> Vec<RowSelector> {
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

/// Per-batch transformation applied by [`ParquetBatchStream`].
pub type BatchAdapter = Box<dyn Fn(RecordBatch) -> Result<RecordBatch, ArnebError> + Send + Sync>;

/// Adapts a [`ParquetRecordBatchStream`] into a
/// [`arneb_common::stream::SendableRecordBatchStream`] without
/// materialising the partition's batches up front. Errors are converted to
/// [`ExecutionError::InvalidOperation`] with file-path context.
pub struct ParquetBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<ParquetRecordBatchStream<ParquetObjectReader>>>,
    file_path: String,
    adapter: Option<BatchAdapter>,
    memory: Option<ScanMemory>,
}

/// Decode-buffer accounting for one scan stream.
struct ScanMemory {
    reservation: MemoryReservation,
    pending: Option<usize>,
}

impl ParquetBatchStream {
    /// Stream `inner`'s batches as-is; `schema` is the output schema.
    pub fn new(
        schema: SchemaRef,
        inner: ParquetRecordBatchStream<ParquetObjectReader>,
        file_path: String,
    ) -> Self {
        Self {
            schema,
            inner: Box::pin(inner),
            file_path,
            adapter: None,
            memory: None,
        }
    }

    /// Account the scan's decode buffer (`bytes`, see
    /// [`row_group_buffer_bytes`]) against `pool` from the first poll until
    /// the stream ends, fails, or is dropped.
    ///
    /// A scan cannot spill or shrink its buffer, so the bytes are recorded
    /// with the infallible [`MemoryReservation::grow`] even when that takes
    /// the pool past its limit: the scan never fails on the reservation, and
    /// spillable operators see the pool as full rather than an untracked gap.
    pub fn with_memory_pool(mut self, pool: &Arc<dyn MemoryPool>, bytes: usize) -> Self {
        if bytes > 0 {
            self.memory = Some(ScanMemory {
                reservation: MemoryConsumer::new("ParquetScan").register(pool.clone()),
                pending: Some(bytes),
            });
        }
        self
    }

    /// Account the decode buffer against the scan context's memory pool; see
    /// [`Self::with_memory_pool`].
    pub fn with_scan_memory(self, ctx: &arneb_execution::ScanContext, bytes: usize) -> Self {
        self.with_memory_pool(&ctx.memory_pool, bytes)
    }

    /// Transform every batch with `adapter` before yielding it.
    pub fn with_adapter(mut self, adapter: BatchAdapter) -> Self {
        self.adapter = Some(adapter);
        self
    }
}

impl Stream for ParquetBatchStream {
    type Item = Result<RecordBatch, ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mem) = self.memory.as_mut() {
            if let Some(bytes) = mem.pending.take() {
                mem.reservation.grow(bytes);
            }
        }
        let polled = self.inner.as_mut().poll_next(cx);
        if matches!(polled, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            if let Some(mem) = self.memory.as_mut() {
                mem.reservation.free();
            }
        }
        match polled {
            Poll::Ready(Some(Ok(b))) => Poll::Ready(Some(match &self.adapter {
                Some(adapt) => adapt(b),
                None => Ok(b),
            })),
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
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_per_file_depends_only_on_its_inputs() {
        assert_eq!(splits_per_file(4, 14), 4);
        assert_eq!(splits_per_file(16, 14), 1);
        assert_eq!(splits_per_file(0, 0), 1);
    }

    #[test]
    fn scan_parallelism_prefers_the_plan_property() {
        let mut props = std::collections::HashMap::new();
        assert_eq!(scan_parallelism(&props), local_scan_parallelism());
        props.insert(SCAN_PARALLELISM_PROPERTY.to_string(), "3".to_string());
        assert_eq!(scan_parallelism(&props), 3);
        props.insert(SCAN_PARALLELISM_PROPERTY.to_string(), "0".to_string());
        assert_eq!(scan_parallelism(&props), local_scan_parallelism());
    }
    use arneb_common::memory_pool::{GreedyMemoryPool, MemoryPool};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::PutPayload;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// Write a 3-column file with `rows` rows in row groups of `rg_rows`
    /// into an in-memory store.
    async fn store_with_parquet(rows: i64, rg_rows: usize) -> (Arc<dyn ObjectStore>, ObjectPath) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i * 7))),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("row-{i}")),
                )),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rg_rows))
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("t.parquet");
        store.put(&path, PutPayload::from(buf)).await.unwrap();
        (store, path)
    }

    #[tokio::test]
    async fn row_group_buffer_bytes_counts_only_projected_columns() {
        let (store, path) = store_with_parquet(10_000, 2_500).await;
        let builder = open_parquet_builder(&store, &path).await.unwrap();
        let all = row_group_buffer_bytes(builder.metadata(), None);
        let one = row_group_buffer_bytes(builder.metadata(), Some(&[0]));
        assert!(one > 0);
        assert!(
            one < all,
            "projecting 1 of 3 columns must shrink the estimate"
        );
        // The estimate is one row group, not the whole file.
        let file_bytes: i64 = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.compressed_size())
            .sum();
        assert!((all as i64) < file_bytes);
    }

    #[tokio::test]
    async fn scan_stream_reserves_while_open_and_releases_at_end() {
        let (store, path) = store_with_parquet(10_000, 2_500).await;
        let builder = open_parquet_builder(&store, &path).await.unwrap();
        let bytes = row_group_buffer_bytes(builder.metadata(), None);
        let schema = builder.schema().clone();
        let inner = build_split_stream(builder, &path, &[], &[], None, None, 0, 1).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let mut stream =
            ParquetBatchStream::new(schema, inner, path.to_string()).with_memory_pool(&pool, bytes);
        assert_eq!(pool.reserved(), 0, "nothing reserved before the first poll");

        let first = stream.next().await.unwrap().unwrap();
        assert!(first.num_rows() > 0);
        assert_eq!(pool.reserved(), bytes);

        let mut rows = first.num_rows();
        while let Some(b) = stream.next().await {
            rows += b.unwrap().num_rows();
        }
        assert_eq!(rows, 10_000);
        assert_eq!(pool.reserved(), 0, "released once the stream is exhausted");
    }

    #[tokio::test]
    async fn scan_stream_releases_on_drop_mid_stream() {
        let (store, path) = store_with_parquet(10_000, 2_500).await;
        let builder = open_parquet_builder(&store, &path).await.unwrap();
        let bytes = row_group_buffer_bytes(builder.metadata(), None);
        let schema = builder.schema().clone();
        let inner = build_split_stream(builder, &path, &[], &[], None, None, 0, 1).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let mut stream =
            ParquetBatchStream::new(schema, inner, path.to_string()).with_memory_pool(&pool, bytes);
        stream.next().await.unwrap().unwrap();
        assert_eq!(pool.reserved(), bytes);
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn scan_stream_records_bytes_when_pool_is_full() {
        let (store, path) = store_with_parquet(10_000, 2_500).await;
        let builder = open_parquet_builder(&store, &path).await.unwrap();
        let bytes = row_group_buffer_bytes(builder.metadata(), None);
        let schema = builder.schema().clone();
        let inner = build_split_stream(builder, &path, &[], &[], None, None, 0, 1).unwrap();
        // Fill the pool to its limit with another consumer: the scan must
        // still succeed, and its buffer must be recorded past the limit.
        let limit = bytes / 2;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let mut other = MemoryConsumer::new("Other").register(pool.clone());
        other.try_grow(limit).unwrap();
        let mut stream =
            ParquetBatchStream::new(schema, inner, path.to_string()).with_memory_pool(&pool, bytes);

        let mut rows = stream.next().await.unwrap().unwrap().num_rows();
        assert_eq!(pool.reserved(), limit + bytes, "recorded over the limit");
        assert!(pool.try_grow(&MemoryConsumer::new("Join"), 1).is_err());
        while let Some(b) = stream.next().await {
            rows += b.unwrap().num_rows();
        }
        assert_eq!(rows, 10_000);
        assert_eq!(pool.reserved(), limit, "scan bytes released at end");
        drop(other);
        assert_eq!(pool.reserved(), 0);
    }
}
