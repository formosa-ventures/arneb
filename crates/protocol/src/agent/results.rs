//! Bounded results: retention of at most `max_rows` rows at completion, and
//! pages over the retained rows limited by bytes and cell size.

use arrow::array::UInt32Array;
use arrow::compute::take_record_batch;
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde_json::Value;

use crate::trino::{encode_cell, materialize};

/// The rows a finished statement keeps. Cloning is cheap: the batches share
/// their Arrow buffers.
#[derive(Debug, Default, Clone)]
pub(crate) struct Retained {
    /// At most `max_rows` rows.
    pub(crate) batches: Vec<RecordBatch>,
    /// Rows in the result when none were dropped; `None` when the result
    /// was truncated, since execution stops after `max_rows + 1` rows and
    /// never learns the full count.
    pub(crate) total_rows: Option<u64>,
    /// Whether rows beyond `max_rows` were dropped.
    pub(crate) truncated: bool,
}

impl Retained {
    /// Number of rows kept.
    pub(crate) fn kept_rows(&self) -> u64 {
        self.batches.iter().map(|b| b.num_rows() as u64).sum()
    }
}

/// Copies `batch` into freshly allocated buffers, so the kept rows no longer
/// pin the (possibly much larger) buffers they were sliced from. A batch that
/// cannot be copied is kept as is.
fn compact(batch: RecordBatch) -> RecordBatch {
    let indices = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
    take_record_batch(&batch, &indices).unwrap_or(batch)
}

/// Keeps the first `max_rows` rows of `batches`, dropping the rest. The kept
/// rows are copied out of the source buffers, so a statement retains memory
/// for at most `max_rows` rows even when an operator emitted one large batch.
pub(crate) fn retain(batches: Vec<RecordBatch>, max_rows: usize) -> Retained {
    let produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let truncated = produced > max_rows as u64;
    let mut kept = Vec::new();
    let mut remaining = max_rows;
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            remaining -= batch.num_rows();
            kept.push(compact(batch));
        } else {
            kept.push(compact(batch.slice(0, remaining)));
            remaining = 0;
        }
    }
    Retained {
        batches: kept,
        total_rows: (!truncated).then_some(produced),
        truncated,
    }
}

/// One page of rows.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResultPage {
    /// Rows as Trino-encoded JSON values.
    pub rows: Vec<Vec<Value>>,
    /// Row offset to pass as `cursor` for the next page; `None` at the end.
    pub next_cursor: Option<u64>,
    /// Cells in this page replaced by a shortened string.
    pub truncated_cells: u64,
}

/// Replaces `cell` by its first `max_cell_bytes` bytes (plus `…`) when its
/// serialized form is longer. Returns whether it was cut.
fn cut_cell(cell: &mut Value, max_cell_bytes: usize) -> bool {
    let serialized = cell.to_string();
    if serialized.len() <= max_cell_bytes {
        return false;
    }
    let text = match cell {
        Value::String(s) => s.as_str(),
        _ => serialized.as_str(),
    };
    let mut end = max_cell_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    *cell = Value::String(format!("{}…", &text[..end]));
    true
}

/// The page of `retained` starting at row `cursor`. `cursor` must not exceed
/// the number of kept rows.
pub(crate) fn page(
    retained: &Retained,
    cursor: u64,
    max_bytes: usize,
    max_cell_bytes: usize,
) -> ResultPage {
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut bytes = 0usize;
    let mut truncated_cells = 0u64;
    let mut position = 0u64; // global row offset of the current batch start
    let mut next = cursor;
    'batches: for batch in &retained.batches {
        let n = batch.num_rows() as u64;
        if position + n <= cursor {
            position += n;
            continue;
        }
        let columns: Vec<_> = batch.columns().iter().map(materialize).collect();
        for r in (cursor.max(position) - position) as usize..n as usize {
            let mut row: Vec<Value> = columns.iter().map(|c| encode_cell(c.as_ref(), r)).collect();
            let cut = row
                .iter_mut()
                .map(|c| cut_cell(c, max_cell_bytes))
                .filter(|cut| *cut)
                .count() as u64;
            let size = serde_json::to_string(&row).map_or(0, |s| s.len()) + 1;
            if !rows.is_empty() && bytes + size > max_bytes {
                break 'batches;
            }
            bytes += size;
            truncated_cells += cut;
            rows.push(row);
            next = position + r as u64 + 1;
        }
        position += n;
    }
    ResultPage {
        rows,
        next_cursor: (next < retained.kept_rows()).then_some(next),
        truncated_cells,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(start: i64, n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let col: ArrayRef = Arc::new(Int64Array::from_iter_values(start..start + n));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn retain_slices_across_batches_and_counts_untruncated_total() {
        let r = retain(vec![batch(0, 3), batch(3, 3), batch(6, 3)], 4);
        assert_eq!(r.kept_rows(), 4);
        assert_eq!(r.total_rows, None);
        assert!(r.truncated);
        let r = retain(vec![batch(0, 3)], 3);
        assert_eq!(
            (r.kept_rows(), r.total_rows, r.truncated),
            (3, Some(3), false)
        );
    }

    #[test]
    fn retain_compacts_rows_out_of_a_large_batch() {
        let source = batch(0, 1_000_000);
        let source_bytes = source.get_array_memory_size();
        let r = retain(vec![source], 10);
        assert_eq!((r.kept_rows(), r.total_rows, r.truncated), (10, None, true));
        let kept_bytes: usize = r.batches.iter().map(|b| b.get_array_memory_size()).sum();
        assert!(
            kept_bytes * 100 < source_bytes,
            "kept {kept_bytes} bytes of a {source_bytes}-byte source"
        );
        let p = page(&r, 0, 1 << 20, 8192);
        assert_eq!(p.rows[9][0], Value::from(9));
    }

    #[test]
    fn cursor_walks_across_batches() {
        let r = retain(vec![batch(0, 3), batch(3, 3)], 100);
        let p = page(&r, 4, 1 << 20, 8192);
        assert_eq!(p.rows.len(), 2);
        assert_eq!(p.rows[0][0], Value::from(4));
        assert_eq!(p.next_cursor, None);
        let p = page(&r, 0, 6, 8192); // each row "[0]" is 3 + 1 bytes
        assert_eq!(p.rows.len(), 1);
        assert_eq!(p.next_cursor, Some(1));
    }

    #[test]
    fn cell_cut_is_char_boundary_safe() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let col: ArrayRef = Arc::new(StringArray::from(vec!["é".repeat(100)]));
        let r = retain(vec![RecordBatch::try_new(schema, vec![col]).unwrap()], 10);
        let p = page(&r, 0, 1 << 20, 11);
        assert_eq!(p.truncated_cells, 1);
        assert_eq!(p.rows[0][0], Value::from(format!("{}…", "é".repeat(5))));
    }
}
