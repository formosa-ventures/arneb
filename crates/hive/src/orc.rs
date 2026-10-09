//! ORC file reading for Hive tables (via the `orc-rust` crate).
//!
//! One scan partition reads one split of one ORC file. File columns are
//! matched to the table's HMS columns by name, case-insensitively (Hive
//! semantics); files written by old Hive versions with positional
//! `_col0, _col1, ...` names are matched by position instead. Columns the
//! file does not have read as NULL, narrower file types are widened where
//! Hive allows it, and anything else is a clear error — never a silently
//! wrong value.
//!
//! Timestamps: ORC `timestamp` values are wall-clock values relative to the
//! writer time zone recorded in each stripe footer; orc-rust converts them
//! back to the writer's wall clock, which is what Hive and Trino return for
//! a `timestamp` (without time zone) column.

use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType as ArrowDataType, SchemaRef};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use orc_rust::async_arrow_reader::ArrowStreamReader;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::AsyncChunkReader;
use orc_rust::row_selection::{RowSelection, RowSelector};
use orc_rust::{ArrowReaderBuilder, ArrowSchemaOptions, TimestampPrecision};

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::stream::{stream_from_batches, RecordBatchStream, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arneb_connectors::scan_adapter::{adapt_batch, ColumnSource};

/// Column layout of a Hive ACID (transactional) ORC data file.
const ACID_COLUMNS: [&str; 6] = [
    "operation",
    "originalTransaction",
    "bucket",
    "rowId",
    "currentTransaction",
    "row",
];

/// [`AsyncChunkReader`] over an object-store file: `len` is a cached HEAD,
/// `get_bytes` a ranged GET.
struct ObjectStoreChunkReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    size: Option<u64>,
}

impl AsyncChunkReader for ObjectStoreChunkReader {
    fn len(&mut self) -> BoxFuture<'_, io::Result<u64>> {
        async move {
            if let Some(size) = self.size {
                return Ok(size);
            }
            let meta = self
                .store
                .head(&self.path)
                .await
                .map_err(io::Error::other)?;
            self.size = Some(meta.size);
            Ok(meta.size)
        }
        .boxed()
    }

    fn get_bytes(&mut self, offset: u64, length: u64) -> BoxFuture<'_, io::Result<Bytes>> {
        async move {
            // orc-rust asks for empty streams too; object stores reject
            // empty ranges.
            if length == 0 {
                return Ok(Bytes::new());
            }
            self.store
                .get_range(&self.path, offset..offset + length)
                .await
                .map_err(io::Error::other)
        }
        .boxed()
    }
}

/// Which part of a file one scan split reads.
#[derive(Debug, PartialEq)]
enum SplitPlan {
    /// The whole file.
    All,
    /// Nothing (more splits than stripes/rows).
    Empty,
    /// The stripes whose offsets fall in `bytes`; with `rows`, only the
    /// row slice that starts `skip` rows into the first of those stripes
    /// and is `len` rows long.
    Range {
        bytes: Range<usize>,
        rows: Option<RowSlice>,
    },
}

/// A row slice within the stripes of a [`SplitPlan::Range`].
#[derive(Debug, PartialEq)]
struct RowSlice {
    skip: usize,
    len: usize,
}

/// Plan split `split_idx` of `splits` over a file's stripes, given as
/// `(offset, row_count)`. With at least as many stripes as splits, each
/// split reads a contiguous run of whole stripes. Otherwise each split
/// reads a contiguous `total_rows / splits` row slice, fetching only the
/// stripes that overlap it.
fn plan_split(stripes: &[(u64, u64)], split_idx: usize, splits: usize) -> SplitPlan {
    if splits <= 1 {
        return SplitPlan::All;
    }
    let n = stripes.len();
    let byte_range =
        |first: usize, last: usize| stripes[first].0 as usize..stripes[last].0 as usize + 1;
    if n >= splits {
        let (lo, hi) = (split_idx * n / splits, (split_idx + 1) * n / splits);
        if lo == hi {
            return SplitPlan::Empty;
        }
        return SplitPlan::Range {
            bytes: byte_range(lo, hi - 1),
            rows: None,
        };
    }

    let total: u64 = stripes.iter().map(|s| s.1).sum();
    let chunk = total.div_ceil(splits as u64);
    let start = (split_idx as u64 * chunk).min(total);
    let end = ((split_idx as u64 + 1) * chunk).min(total);
    if start >= end {
        return SplitPlan::Empty;
    }
    // Stripes overlapping [start, end) and the row offset of the first one.
    let (mut first, mut last, mut base) = (None, 0, 0);
    let mut cum = 0u64;
    for (i, &(_, rows)) in stripes.iter().enumerate() {
        let (s, e) = (cum, cum + rows);
        if e > start && s < end {
            if first.is_none() {
                first = Some(i);
                base = s;
            }
            last = i;
        }
        cum = e;
    }
    let Some(first) = first else {
        return SplitPlan::Empty;
    };
    SplitPlan::Range {
        bytes: byte_range(first, last),
        rows: Some(RowSlice {
            skip: (start - base) as usize,
            len: (end - start) as usize,
        }),
    }
}

/// `true` when a file column of type `file` can be read as table type
/// `table` (identical, or a widening Hive allows).
fn can_read_as(file: &ArrowDataType, table: &ArrowDataType) -> bool {
    use ArrowDataType::*;
    let int_rank = |t: &ArrowDataType| match t {
        Int8 => Some(1),
        Int16 => Some(2),
        Int32 => Some(3),
        Int64 => Some(4),
        _ => None,
    };
    if file == table {
        return true;
    }
    match (file, table) {
        (f, t) if int_rank(f).is_some() && int_rank(t).is_some() => int_rank(f) <= int_rank(t),
        (f, Float32 | Float64 | Decimal128(..)) if int_rank(f).is_some() => true,
        (Float32, Float64) => true,
        // Precision/scale changes; values that do not fit fail the cast.
        (Decimal128(..), Decimal128(..)) => true,
        (LargeUtf8, Utf8) => true,
        _ => false,
    }
}

/// Old Hive versions name ORC columns `_col0`, `_col1`, ...
fn is_positional_name(name: &str) -> bool {
    name.strip_prefix("_col")
        .is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
}

/// Inputs for scanning one split of one ORC file.
pub(crate) struct OrcScan<'a> {
    pub store: &'a Arc<dyn ObjectStore>,
    pub path: &'a ObjectPath,
    /// Table data columns (HMS order, partition columns excluded).
    pub data_columns: &'a [ColumnInfo],
    /// Output columns as table column indices; indices at or past
    /// `data_columns.len()` are partition columns.
    pub projection: &'a [usize],
    /// One-row value per partition column, already in the table type.
    pub partition_values: &'a [ArrayRef],
    pub output_schema: SchemaRef,
    pub batch_size: usize,
    pub split_idx: usize,
    pub splits_per_file: usize,
}

/// Open one split of an ORC file as a table-schema batch stream.
pub(crate) async fn scan(s: OrcScan<'_>) -> Result<SendableRecordBatchStream, ExecutionError> {
    let path = s.path;
    let err = |msg: String| ExecutionError::InvalidOperation(format!("ORC file '{path}': {msg}"));
    let reader = ObjectStoreChunkReader {
        store: s.store.clone(),
        path: path.clone(),
        size: None,
    };
    let builder = ArrowReaderBuilder::try_new_async(reader)
        .await
        .map_err(|e| err(e.to_string()))?
        .with_timestamp_precision(TimestampPrecision::Microsecond);
    let meta = builder.file_metadata();
    let root = meta.root_data_type();
    let file_cols = root.children();

    if file_cols.len() == ACID_COLUMNS.len()
        && file_cols
            .iter()
            .zip(ACID_COLUMNS)
            .all(|(c, n)| c.name() == n)
    {
        return Err(err(
            "Hive ACID (transactional) ORC files are not supported".to_string()
        ));
    }

    // Table data column index -> file column position.
    let lower = |s: &str| s.to_ascii_lowercase();
    let by_name: std::collections::HashMap<String, usize> = file_cols
        .iter()
        .enumerate()
        .map(|(i, c)| (lower(c.name()), i))
        .collect();
    let positional = !file_cols.is_empty()
        && file_cols.iter().all(|c| is_positional_name(c.name()))
        && !s
            .data_columns
            .iter()
            .any(|c| by_name.contains_key(&lower(&c.name)));
    let file_pos_of = |table_idx: usize| -> Option<usize> {
        if positional {
            (table_idx < file_cols.len()).then_some(table_idx)
        } else {
            by_name
                .get(&lower(&s.data_columns[table_idx].name))
                .copied()
        }
    };

    let n_data = s.data_columns.len();
    let mut file_positions: Vec<usize> = s
        .projection
        .iter()
        .filter(|&&i| i < n_data)
        .filter_map(|&i| file_pos_of(i))
        .collect();
    file_positions.sort_unstable();
    file_positions.dedup();

    let options =
        ArrowSchemaOptions::new().with_timestamp_precision(TimestampPrecision::Microsecond);
    let mut sources = Vec::with_capacity(s.projection.len());
    for &i in s.projection {
        if i >= n_data {
            let value = s
                .partition_values
                .get(i - n_data)
                .ok_or_else(|| err(format!("projection index {i} out of range")))?;
            sources.push(ColumnSource::Constant(value.clone()));
            continue;
        }
        let Some(pos) = file_pos_of(i) else {
            sources.push(ColumnSource::Null);
            continue;
        };
        let col = &s.data_columns[i];
        let file_type = file_cols[pos]
            .data_type()
            .to_arrow_data_type_with_options(options.clone());
        let table_type: ArrowDataType = col.data_type.clone().into();
        if !can_read_as(&file_type, &table_type) {
            return Err(err(format!(
                "column '{}' has ORC type {} which cannot be read as the table type {}",
                col.name,
                file_cols[pos].data_type(),
                col.data_type
            )));
        }
        let rank = file_positions.binary_search(&pos).unwrap_or_default();
        sources.push(ColumnSource::File(rank));
    }

    let stripes: Vec<(u64, u64)> = meta
        .stripe_metadatas()
        .iter()
        .map(|st| (st.offset(), st.number_of_rows()))
        .collect();
    let mask = ProjectionMask::roots(
        root,
        file_positions
            .iter()
            .map(|&p| file_cols[p].data_type().column_index()),
    );
    let mut builder = builder.with_projection(mask).with_batch_size(s.batch_size);
    let mut remaining = None;
    match plan_split(&stripes, s.split_idx, s.splits_per_file) {
        SplitPlan::All => {}
        SplitPlan::Empty => return Ok(stream_from_batches(s.output_schema, vec![])),
        SplitPlan::Range { bytes, rows } => {
            builder = builder.with_file_byte_range(bytes);
            if let Some(RowSlice { skip, len }) = rows {
                // Skip to the slice start, then cut the output at `len`
                // rows ourselves: orc-rust keeps decoding a `select` run
                // longer than one batch until the stripe ends instead of
                // honouring a trailing `skip`, so the slice end cannot be
                // expressed as a selection.
                builder = builder.with_row_selection(RowSelection::from(vec![
                    RowSelector::skip(skip),
                    RowSelector::select(len),
                ]));
                remaining = Some(len);
            }
        }
    }

    Ok(Box::pin(OrcBatchStream {
        schema: s.output_schema,
        inner: Box::pin(builder.build_async()),
        file_path: path.to_string(),
        sources,
        remaining,
    }))
}

/// Streams an ORC reader's batches, adapted to the table schema.
struct OrcBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<ArrowStreamReader<ObjectStoreChunkReader>>>,
    file_path: String,
    sources: Vec<ColumnSource>,
    /// Rows left in this split's row slice (`None` = unbounded).
    remaining: Option<usize>,
}

impl Stream for OrcBatchStream {
    type Item = Result<RecordBatch, ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == Some(0) {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(mut batch))) => {
                if let Some(left) = self.remaining {
                    if batch.num_rows() > left {
                        batch = batch.slice(0, left);
                    }
                    self.remaining = Some(left - batch.num_rows());
                }
                Poll::Ready(Some(adapt_batch(
                    &self.schema,
                    &self.sources,
                    &self.file_path,
                    batch,
                )))
            }
            Poll::Ready(Some(Err(e))) => {
                let msg = format!("ORC read error for '{}': {e}", self.file_path);
                Poll::Ready(Some(Err(ExecutionError::InvalidOperation(msg).into())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for OrcBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_split_reads_whole_file() {
        assert_eq!(plan_split(&[(3, 10)], 0, 1), SplitPlan::All);
    }

    #[test]
    fn many_stripes_split_by_whole_stripes() {
        let stripes = [(3, 10), (100, 10), (200, 10), (300, 10)];
        let plans: Vec<_> = (0..2).map(|i| plan_split(&stripes, i, 2)).collect();
        assert_eq!(
            plans,
            vec![
                SplitPlan::Range {
                    bytes: 3..101,
                    rows: None
                },
                SplitPlan::Range {
                    bytes: 200..301,
                    rows: None
                },
            ]
        );
    }

    #[test]
    fn few_stripes_split_by_row_slices_over_overlapping_stripes() {
        // 2 stripes x 10 rows, 4 splits of 5 rows.
        let stripes = [(3, 10), (100, 10)];
        let SplitPlan::Range { bytes, rows } = plan_split(&stripes, 1, 4) else {
            panic!()
        };
        assert_eq!(bytes, 3..4);
        assert_eq!(rows, Some(RowSlice { skip: 5, len: 5 }));
        // A slice spanning both stripes reads both, relative to stripe 0.
        let SplitPlan::Range { bytes, rows } = plan_split(&stripes, 1, 3) else {
            panic!()
        };
        assert_eq!(bytes, 3..101);
        assert_eq!(rows, Some(RowSlice { skip: 7, len: 7 }));
    }

    #[test]
    fn extra_splits_are_empty() {
        assert_eq!(plan_split(&[(3, 2)], 5, 8), SplitPlan::Empty);
        assert_eq!(plan_split(&[], 0, 2), SplitPlan::Empty);
    }

    #[test]
    fn widening_rules() {
        use ArrowDataType::*;
        assert!(can_read_as(&Int32, &Int64));
        assert!(can_read_as(&Int8, &Int16));
        assert!(!can_read_as(&Int64, &Int32));
        assert!(can_read_as(&Float32, &Float64));
        assert!(can_read_as(&Int32, &Float64));
        assert!(can_read_as(&Decimal128(10, 2), &Decimal128(12, 2)));
        assert!(!can_read_as(&Utf8, &Int64));
        assert!(!can_read_as(&Float64, &Float32));
        assert!(!can_read_as(&Date32, &Utf8));
    }

    #[test]
    fn positional_names() {
        assert!(is_positional_name("_col0"));
        assert!(is_positional_name("_col12"));
        assert!(!is_positional_name("_col"));
        assert!(!is_positional_name("col0"));
    }
}
