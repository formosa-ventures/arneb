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
/// - `filters` and `dynamic_domains` reference **file root column indices**
///   (for Hive and registered files the table schema equals the file schema;
///   Iceberg remaps by field ID). They are mapped to the leaf indices the
///   Parquet pushdown works on here; one on a nested root is not pushed
///   (see [`roots_to_leaves`]).
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
    let (filters, dynamic_domains) =
        roots_to_leaves(builder.parquet_schema(), filters, dynamic_domains);
    let (filters, dynamic_domains) = (&filters[..], &dynamic_domains[..]);
    let total_rows: usize = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum();

    // A file with at least as many row groups as splits is split by whole
    // row groups: a row range that ends inside a row group would make two
    // splits fetch and decode that row group's column chunks.
    if let Some(groups) =
        split_row_groups(builder.metadata().row_groups(), split_idx, splits_per_file)
    {
        let groups = if filters.is_empty() {
            groups
        } else {
            let pruned = crate::parquet_pushdown::prune_row_groups(
                builder.metadata().row_groups(),
                filters,
                &[],
            );
            groups.into_iter().filter(|g| pruned.contains(g)).collect()
        };
        builder = builder.with_row_groups(groups);
    }
    // Otherwise apply row-group pruning (min/max) and predicate filters
    // BEFORE building the slice — the slice should reflect the
    // user-visible logical row count. Row-group pruning is OK because all
    // splits see the same min/max-pruned row groups.
    else if !filters.is_empty() {
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

    // `ctx.batch_size`, else `scan_default_batch_size()` (16384 unless
    // `ARNEB_SCAN_BATCH_SIZE` says otherwise). The in-flight working set
    // scales with batch_size × pipeline_depth × partition_count.
    let batch_size = batch_size.unwrap_or_else(crate::file::scan_default_batch_size);
    builder = builder.with_batch_size(batch_size);

    builder.build().map_err(|e| {
        ExecutionError::InvalidOperation(format!(
            "Parquet reader build error for '{file_path}': {e}"
        ))
    })
}

/// Map filters and dynamic-filter domains from file root column indices to
/// the leaf column indices the Parquet pushdown (row-group statistics and
/// `RowFilter`) reads.
///
/// Root and leaf indices differ as soon as a nested column (a struct or a
/// map, which have several leaves) comes before the filtered one. A filter
/// or domain on a nested root, or on a column the file does not have, is
/// dropped: it still runs above the scan, which is what keeps this safe.
pub fn roots_to_leaves(
    schema: &parquet::schema::types::SchemaDescriptor,
    filters: &[PlanExpr],
    dynamic_domains: &[arneb_execution::DynamicFilterDomain],
) -> (Vec<PlanExpr>, Vec<arneb_execution::DynamicFilterDomain>) {
    let roots = schema.root_schema().get_fields();
    let mut leaf_by_root: Vec<Option<usize>> = vec![None; roots.len()];
    for leaf in 0..schema.num_columns() {
        let root = schema.get_column_root_idx(leaf);
        if roots[root].is_primitive() {
            leaf_by_root[root] = Some(leaf);
        }
    }
    let leaf_of = |root: usize| leaf_by_root.get(root).copied().flatten();
    let filters = filters
        .iter()
        .filter_map(|f| crate::scan_adapter::remap_filter(f, &leaf_of))
        .collect();
    let dynamic_domains = dynamic_domains
        .iter()
        .filter_map(|d| {
            Some(arneb_execution::DynamicFilterDomain {
                column_index: leaf_of(d.column_index)?,
                ..d.clone()
            })
        })
        .collect();
    (filters, dynamic_domains)
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
    let all: Vec<usize> = (0..metadata.row_groups().len()).collect();
    buffer_bytes_of(metadata, projection_roots, &all)
}

/// The largest projected compressed size among `row_groups`.
fn buffer_bytes_of(
    metadata: &ParquetMetaData,
    projection_roots: Option<&[usize]>,
    row_groups: &[usize],
) -> usize {
    let schema = metadata.file_metadata().schema_descr();
    let projected = |leaf: usize| {
        projection_roots.is_none_or(|roots| roots.contains(&schema.get_column_root_idx(leaf)))
    };
    row_groups
        .iter()
        .map(|&idx| {
            let rg = metadata.row_group(idx);
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

/// The row groups split `split_idx` of `splits` reads when the file is
/// split by whole row groups, or `None` when it is split by row ranges
/// (one split, or fewer row groups than splits).
///
/// A row group belongs to the split its first row falls in when the file's
/// rows are cut into `splits` equal ranges, so splits get about the same
/// number of rows and every row group exactly one split. The assignment
/// depends only on the file's metadata, so every node agrees on it.
pub fn split_row_groups(
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    split_idx: usize,
    splits: usize,
) -> Option<Vec<usize>> {
    if splits <= 1 || row_groups.len() < splits {
        return None;
    }
    let total: usize = row_groups.iter().map(|rg| rg.num_rows() as usize).sum();
    if total == 0 {
        return None;
    }
    let mut start = 0usize;
    let mut groups = Vec::new();
    for (idx, rg) in row_groups.iter().enumerate() {
        // u128: `start * splits` can overflow usize on a huge file.
        let owner = (start as u128 * splits as u128 / total as u128) as usize;
        if owner == split_idx {
            groups.push(idx);
        }
        start += rg.num_rows() as usize;
    }
    Some(groups)
}

/// [`row_group_buffer_bytes`] for split `split_idx` of `splits`: the largest
/// row group that split reads, instead of the file's largest. A split by
/// row range counts the row groups its range overlaps.
pub fn split_buffer_bytes(
    metadata: &ParquetMetaData,
    projection_roots: Option<&[usize]>,
    split_idx: usize,
    splits: usize,
) -> usize {
    let row_groups = metadata.row_groups();
    let read: Vec<usize> = match split_row_groups(row_groups, split_idx, splits) {
        Some(groups) => groups,
        None if splits <= 1 => (0..row_groups.len()).collect(),
        None => {
            let total: usize = row_groups.iter().map(|rg| rg.num_rows() as usize).sum();
            let chunk = total.div_ceil(splits);
            let (lo, hi) = (
                (split_idx * chunk).min(total),
                ((split_idx + 1) * chunk).min(total),
            );
            let mut start = 0usize;
            let mut overlapping = Vec::new();
            for (idx, rg) in row_groups.iter().enumerate() {
                let end = start + rg.num_rows() as usize;
                if start < hi && lo < end {
                    overlapping.push(idx);
                }
                start = end;
            }
            overlapping
        }
    };
    buffer_bytes_of(metadata, projection_roots, &read)
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

    /// The values of column `a` split `split` of `splits` reads, with
    /// `filters` pushed down.
    async fn read_split(
        store: &Arc<dyn ObjectStore>,
        path: &ObjectPath,
        filters: &[PlanExpr],
        split: usize,
        splits: usize,
    ) -> Vec<i64> {
        let builder = open_parquet_builder(store, path).await.unwrap();
        let mut stream =
            build_split_stream(builder, path, filters, &[], None, None, split, splits).unwrap();
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.extend(a.values().iter().copied());
        }
        values
    }

    #[tokio::test]
    async fn split_row_groups_gives_every_row_group_to_one_split() {
        let (store, path) = store_with_parquet(10_000, 1_000).await;
        let builder = open_parquet_builder(&store, &path).await.unwrap();
        let rgs = builder.metadata().row_groups();
        assert_eq!(rgs.len(), 10);
        // One split, or fewer row groups than splits: split by row range.
        assert_eq!(split_row_groups(rgs, 0, 1), None);
        assert_eq!(split_row_groups(rgs, 0, 11), None);
        let assigned: Vec<Vec<usize>> = (0..4)
            .map(|split| split_row_groups(rgs, split, 4).unwrap())
            .collect();
        assert_eq!(
            assigned,
            vec![vec![0, 1, 2], vec![3, 4], vec![5, 6, 7], vec![8, 9]]
        );
    }

    #[tokio::test]
    async fn splits_by_whole_row_groups_cover_the_file_once() {
        let (store, path) = store_with_parquet(10_000, 1_000).await;
        let mut all = Vec::new();
        for split in 0..4 {
            let values = read_split(&store, &path, &[], split, 4).await;
            // Whole row groups only: no row group is shared with another split.
            assert_eq!(values.len() % 1_000, 0, "split {split}");
            all.extend(values);
        }
        assert_eq!(all, (0..10_000).collect::<Vec<_>>());

        // A filter still prunes inside each split's row groups:
        // 4_500..5_500 lies in row groups 4 and 5, split 1 and split 2.
        let col = PlanExpr::Column {
            index: 0,
            name: "a".to_string(),
            span: None,
        };
        let cmp = |op, v| PlanExpr::BinaryOp {
            left: Box::new(col.clone()),
            op,
            right: Box::new(PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Int64(v),
                span: None,
            }),
            span: None,
        };
        use arneb_sql_parser::ast::BinaryOp;
        let filters = [cmp(BinaryOp::GtEq, 4_500), cmp(BinaryOp::Lt, 5_500)];
        let mut matched = Vec::new();
        for split in 0..4 {
            let values = read_split(&store, &path, &filters, split, 4).await;
            if split == 0 || split == 3 {
                assert!(
                    values.is_empty(),
                    "split {split} reads no matching row group"
                );
            }
            matched.extend(values);
        }
        assert_eq!(matched, (4_500..5_500).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn fewer_row_groups_than_splits_still_split_by_row_range() {
        let (store, path) = store_with_parquet(10_000, 5_000).await;
        let mut all = Vec::new();
        for split in 0..4 {
            let values = read_split(&store, &path, &[], split, 4).await;
            assert_eq!(values.len(), 2_500, "split {split}");
            all.extend(values);
        }
        assert_eq!(all, (0..10_000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_split_reserves_only_the_row_groups_it_reads() {
        // Split by row group: the largest of the split's own row groups.
        let (store, path) = store_with_parquet(10_000, 1_000).await;
        let builder = open_parquet_builder(&store, &path).await.unwrap();
        let meta = builder.metadata();
        for split in 0..4 {
            let groups = split_row_groups(meta.row_groups(), split, 4).unwrap();
            assert_eq!(
                split_buffer_bytes(meta, None, split, 4),
                buffer_bytes_of(meta, None, &groups),
                "split {split}"
            );
        }
        // One split reads the whole file: the file's largest row group.
        assert_eq!(
            split_buffer_bytes(meta, None, 0, 1),
            row_group_buffer_bytes(meta, None)
        );

        // Split by row range: the row groups the range overlaps. Rows
        // 0..2_500 lie in row group 0, 2_500..5_000 too, 5_000.. in 1.
        let (store, path) = store_with_parquet(10_000, 5_000).await;
        let builder = open_parquet_builder(&store, &path).await.unwrap();
        let meta = builder.metadata();
        for (split, group) in [(0, 0), (1, 0), (2, 1), (3, 1)] {
            assert_eq!(
                split_buffer_bytes(meta, None, split, 4),
                buffer_bytes_of(meta, None, &[group]),
                "split {split}"
            );
        }
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
