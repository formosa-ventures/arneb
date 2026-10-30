//! Bounded results: retention of at most `max_rows` rows at completion, and
//! pages over the retained rows limited by bytes and cell size.

use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde_json::Value;

use crate::trino::{encode_cell, materialize};

/// The rows a finished statement keeps.
#[derive(Debug, Default)]
pub(crate) struct Retained {
    /// At most `max_rows` rows.
    pub(crate) batches: Vec<RecordBatch>,
    /// Rows the statement produced before truncation.
    pub(crate) total_rows: u64,
    /// Whether rows beyond `max_rows` were dropped.
    pub(crate) truncated: bool,
}

impl Retained {
    /// Number of rows kept.
    pub(crate) fn kept_rows(&self) -> u64 {
        self.batches.iter().map(|b| b.num_rows() as u64).sum()
    }
}

/// Keeps the first `max_rows` rows of `batches`, dropping the rest.
pub(crate) fn retain(batches: Vec<RecordBatch>, max_rows: usize) -> Retained {
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let mut kept = Vec::new();
    let mut remaining = max_rows;
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            remaining -= batch.num_rows();
            kept.push(batch);
        } else {
            kept.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }
    Retained {
        batches: kept,
        total_rows,
        truncated: total_rows > max_rows as u64,
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
    fn retain_slices_across_batches_and_counts_total() {
        let r = retain(vec![batch(0, 3), batch(3, 3), batch(6, 3)], 4);
        assert_eq!(r.kept_rows(), 4);
        assert_eq!(r.total_rows, 9);
        assert!(r.truncated);
        let r = retain(vec![batch(0, 3)], 3);
        assert_eq!((r.kept_rows(), r.total_rows, r.truncated), (3, 3, false));
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
