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
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::stream::RecordBatchStream;
use arneb_planner::PlanExpr;

/// Pick a `splits_per_file` so the resulting partition count saturates
/// available CPU cores. For a 4-file table on a 14-core machine that's
/// `ceil(14/4) = 4` splits per file → 16 scan partitions. For a 16-file
/// table we already have enough — keep splits_per_file=1.
pub fn splits_per_file(n_files: usize) -> usize {
    let n_files = n_files.max(1);
    let target = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
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
pub fn build_split_stream(
    mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    file_path: &dyn fmt::Display,
    filters: &[PlanExpr],
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

    // Within-row-group predicate pushdown.
    if !filters.is_empty() {
        if let Some(row_filter) =
            crate::parquet_pushdown::build_row_filter(filters, builder.parquet_schema())
        {
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
        }
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
        match self.inner.as_mut().poll_next(cx) {
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
