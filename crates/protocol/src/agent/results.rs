//! Bounded results: retention of at most `max_rows` rows and
//! `max_retained_bytes` bytes at completion, charged to a process-wide
//! budget, and pages over the retained rows limited by bytes and cell size.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::compute::take_record_batch;
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde_json::Value;

use crate::trino::{encode_cell, materialize};

/// Why a finished statement kept fewer rows than its query produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncatedBy {
    /// The query produced more than the statement's `max_rows` rows. Ask
    /// for fewer rows (`LIMIT`, a filter, an aggregate) or a larger
    /// `max_rows`.
    MaxRows,
    /// The kept rows reached the server's per-statement
    /// `max_retained_bytes`. Select fewer or narrower columns.
    MaxRetainedBytes,
    /// The server's budget for retained results across all statements
    /// (`max_total_retained_bytes`) was exhausted. Fetch and release (let
    /// expire) earlier results, or retry later.
    ServerBudget,
}

/// The process-wide budget for retained result bytes, shared by every
/// statement of one service.
#[derive(Debug)]
pub(crate) struct RetainedBudget {
    capacity: usize,
    used: AtomicUsize,
}

impl RetainedBudget {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            used: AtomicUsize::new(0),
        })
    }

    /// Bytes currently held by retained results.
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    fn available(&self) -> usize {
        self.capacity.saturating_sub(self.used())
    }

    /// Takes `bytes` from the budget, or nothing when they do not fit.
    fn try_take(&self, bytes: usize) -> bool {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(bytes).filter(|n| *n <= self.capacity) else {
                return false;
            };
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => used = actual,
            }
        }
    }
}

/// Bytes one statement's retained rows hold against the budget; given back
/// when the last clone of the [`Retained`] drops (the statement is evicted
/// or reaped, and no page is being encoded from it).
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<RetainedBudget>,
    bytes: usize,
}

impl Reservation {
    fn grow(&mut self, bytes: usize) -> bool {
        let ok = self.budget.try_take(bytes);
        if ok {
            self.bytes += bytes;
        }
        ok
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// The rows a finished statement keeps. Cloning is cheap: the batches share
/// their Arrow buffers, and the clones share one budget reservation.
#[derive(Debug, Default, Clone)]
pub(crate) struct Retained {
    /// At most `max_rows` rows and `max_retained_bytes` bytes.
    pub(crate) batches: Vec<RecordBatch>,
    /// Rows in the result when none were dropped; `None` when the result
    /// was truncated, since execution stops after `max_rows + 1` rows and
    /// never learns the full count.
    pub(crate) total_rows: Option<u64>,
    /// Why rows were dropped; `None` when every produced row was kept.
    pub(crate) truncated_by: Option<TruncatedBy>,
    /// Rows the query produced (at most `max_rows + 1`, since execution
    /// stops there), whether kept or not.
    pub(crate) produced: u64,
    /// Arrow memory of the kept batches, charged to the budget.
    pub(crate) bytes: usize,
    _reservation: Option<Arc<Reservation>>,
}

impl Retained {
    /// Number of rows kept.
    pub(crate) fn kept_rows(&self) -> u64 {
        self.batches.iter().map(|b| b.num_rows() as u64).sum()
    }

    /// Whether any produced row was dropped.
    pub(crate) fn truncated(&self) -> bool {
        self.truncated_by.is_some()
    }
}

/// Copies `batch` into freshly allocated buffers, so the kept rows no longer
/// pin the (possibly much larger) buffers they were sliced from. A batch that
/// cannot be copied is kept as is.
fn compact(batch: RecordBatch) -> RecordBatch {
    let indices = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
    take_record_batch(&batch, &indices).unwrap_or(batch)
}

/// The longest prefix of `batch` (compacted) whose Arrow memory is at most
/// `avail` bytes, with that size; `None` when not even one row fits. Starts
/// from a proportional estimate and shrinks it.
fn prefix_within(batch: &RecordBatch, size: usize, avail: usize) -> Option<(RecordBatch, usize)> {
    let n = batch.num_rows();
    let mut k = ((n as u128 * avail as u128) / size.max(1) as u128) as usize;
    k = k.min(n.saturating_sub(1));
    while k > 0 {
        let piece = compact(batch.slice(0, k));
        let piece_size = piece.get_array_memory_size();
        if piece_size <= avail {
            return Some((piece, piece_size));
        }
        k = (k * 3 / 4).min(k - 1);
    }
    None
}

/// Keeps the first rows of `batches`, at most `max_rows` of them and at most
/// `max_bytes` of Arrow memory (`get_array_memory_size`), each kept batch
/// charged to `budget` as it is kept; stops when either limit or the budget
/// runs out and records why. The kept rows are copied out of the source
/// buffers, so a statement retains memory only for what it keeps even when
/// an operator emitted one large batch.
pub(crate) fn retain(
    batches: Vec<RecordBatch>,
    max_rows: usize,
    max_bytes: usize,
    budget: &Arc<RetainedBudget>,
) -> Retained {
    let produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let mut truncated_by = (produced > max_rows as u64).then_some(TruncatedBy::MaxRows);
    let mut reservation = Reservation {
        budget: Arc::clone(budget),
        bytes: 0,
    };
    let mut kept = Vec::new();
    let mut remaining = max_rows;
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = batch.num_rows().min(remaining);
        if take == 0 {
            continue;
        }
        let piece = compact(if take == batch.num_rows() {
            batch
        } else {
            batch.slice(0, take)
        });
        let size = piece.get_array_memory_size();
        let cap_left = max_bytes - reservation.bytes;
        if size <= cap_left && reservation.grow(size) {
            remaining -= take;
            kept.push(piece);
            continue;
        }
        // Out of room: keep the prefix that still fits, then stop.
        let available = budget.available();
        let reason = if cap_left <= available {
            TruncatedBy::MaxRetainedBytes
        } else {
            TruncatedBy::ServerBudget
        };
        if let Some((prefix, prefix_size)) = prefix_within(&piece, size, cap_left.min(available)) {
            if reservation.grow(prefix_size) {
                kept.push(prefix);
            }
        }
        truncated_by = Some(reason);
        break;
    }
    let bytes = reservation.bytes;
    Retained {
        batches: kept,
        total_rows: truncated_by.is_none().then_some(produced),
        truncated_by,
        produced,
        bytes,
        _reservation: Some(Arc::new(reservation)),
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

/// Shortens `cell` to its first `max_cell_bytes` bytes (cut on a char
/// boundary, plus `…`) when it is longer, and returns whether it was cut. A
/// string is measured and cut as its raw UTF-8 text, so JSON quoting and
/// escapes never count; any other value (number, array, map, …) is measured
/// and cut as its serialized JSON, becoming a string when cut.
fn cut_cell(cell: &mut Value, max_cell_bytes: usize) -> bool {
    let text = match &mut *cell {
        Value::String(s) if s.len() <= max_cell_bytes => return false,
        Value::String(s) => std::mem::take(s),
        other => {
            let serialized = other.to_string();
            if serialized.len() <= max_cell_bytes {
                return false;
            }
            serialized
        }
    };
    let mut end = max_cell_bytes;
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

    /// `retain` with no byte limit.
    fn keep(batches: Vec<RecordBatch>, max_rows: usize) -> Retained {
        retain(
            batches,
            max_rows,
            usize::MAX,
            &RetainedBudget::new(usize::MAX),
        )
    }

    fn batch(start: i64, n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let col: ArrayRef = Arc::new(Int64Array::from_iter_values(start..start + n));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn retain_slices_across_batches_and_counts_untruncated_total() {
        let r = keep(vec![batch(0, 3), batch(3, 3), batch(6, 3)], 4);
        assert_eq!(r.kept_rows(), 4);
        assert_eq!(r.total_rows, None);
        assert!(r.truncated());
        let r = keep(vec![batch(0, 3)], 3);
        assert_eq!(
            (r.kept_rows(), r.total_rows, r.truncated()),
            (3, Some(3), false)
        );
    }

    #[test]
    fn retain_compacts_rows_out_of_a_large_batch() {
        let source = batch(0, 1_000_000);
        let source_bytes = source.get_array_memory_size();
        let r = keep(vec![source], 10);
        assert_eq!(
            (r.kept_rows(), r.total_rows, r.truncated()),
            (10, None, true)
        );
        let kept_bytes: usize = r.batches.iter().map(|b| b.get_array_memory_size()).sum();
        assert!(
            kept_bytes * 100 < source_bytes,
            "kept {kept_bytes} bytes of a {source_bytes}-byte source"
        );
        let p = page(&r, 0, 1 << 20, 8192);
        assert_eq!(p.rows[9][0], Value::from(9));
    }

    fn wide_batch(rows: usize, width: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let col: ArrayRef = Arc::new(StringArray::from(vec!["w".repeat(width); rows]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn per_statement_byte_cap_truncates_without_total() {
        let budget = RetainedBudget::new(usize::MAX);
        // 100 rows of 10 KB: about 1 MB, capped at 100 KB.
        let r = retain(vec![wide_batch(100, 10_000)], 1000, 100_000, &budget);
        assert_eq!(r.truncated_by, Some(TruncatedBy::MaxRetainedBytes));
        assert_eq!(r.total_rows, None);
        assert_eq!(r.produced, 100);
        assert!(r.kept_rows() > 0 && r.kept_rows() < 10, "{}", r.kept_rows());
        assert!(r.bytes <= 100_000, "{}", r.bytes);
        let measured: usize = r.batches.iter().map(|b| b.get_array_memory_size()).sum();
        assert_eq!(r.bytes, measured);
        assert_eq!(budget.used(), r.bytes);
        // Under the cap nothing is dropped.
        let r = retain(vec![wide_batch(5, 100)], 1000, 1 << 20, &budget);
        assert_eq!((r.truncated_by, r.total_rows), (None, Some(5)));
    }

    #[test]
    fn row_cap_reports_max_rows() {
        let r = keep(vec![batch(0, 10)], 3);
        assert_eq!(r.truncated_by, Some(TruncatedBy::MaxRows));
    }

    #[test]
    fn exhausted_server_budget_truncates_and_is_released_on_drop() {
        let one = keep(vec![wide_batch(10, 1000)], 1000).bytes;
        let budget = RetainedBudget::new(one + one / 2);
        let first = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(first.truncated_by, None);
        assert_eq!(budget.used(), first.bytes);
        // The second statement gets only what is left of the budget.
        let second = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(second.truncated_by, Some(TruncatedBy::ServerBudget));
        assert_eq!(second.total_rows, None);
        assert!(second.kept_rows() < 10);
        assert_eq!(budget.used(), first.bytes + second.bytes);
        // Clones share the reservation: it is released with the last one.
        let clone = first.clone();
        drop(first);
        assert_eq!(budget.used(), clone.bytes + second.bytes);
        drop(clone);
        drop(second);
        assert_eq!(budget.used(), 0);
        let third = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(third.truncated_by, None);
    }

    #[test]
    fn cursor_walks_across_batches() {
        let r = keep(vec![batch(0, 3), batch(3, 3)], 100);
        let p = page(&r, 4, 1 << 20, 8192);
        assert_eq!(p.rows.len(), 2);
        assert_eq!(p.rows[0][0], Value::from(4));
        assert_eq!(p.next_cursor, None);
        let p = page(&r, 0, 6, 8192); // each row "[0]" is 3 + 1 bytes
        assert_eq!(p.rows.len(), 1);
        assert_eq!(p.next_cursor, Some(1));
    }

    fn string_retained(values: Vec<String>) -> Retained {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let col: ArrayRef = Arc::new(StringArray::from(values));
        keep(vec![RecordBatch::try_new(schema, vec![col]).unwrap()], 10)
    }

    #[test]
    fn string_just_under_the_cell_limit_is_untouched() {
        // Serialized it is 8193 bytes (quotes), but the raw text fits.
        let value = "x".repeat(8191);
        let p = page(&string_retained(vec![value.clone()]), 0, 1 << 20, 8192);
        assert_eq!(p.truncated_cells, 0);
        assert_eq!(p.rows[0][0], Value::from(value));
    }

    #[test]
    fn escapes_do_not_count_toward_the_cell_limit() {
        // 5000 raw bytes, 10002 serialized (`\"` per quote).
        let value = "\"".repeat(5000);
        let p = page(&string_retained(vec![value.clone()]), 0, 1 << 20, 8192);
        assert_eq!(p.truncated_cells, 0);
        assert_eq!(p.rows[0][0], Value::from(value));
    }

    #[test]
    fn long_string_is_cut_to_the_cell_limit() {
        let p = page(&string_retained(vec!["x".repeat(9000)]), 0, 1 << 20, 8192);
        assert_eq!(p.truncated_cells, 1);
        assert_eq!(p.rows[0][0], Value::from(format!("{}…", "x".repeat(8192))));
    }

    #[test]
    fn non_string_cell_is_measured_serialized() {
        let mut short = Value::from(123_456);
        assert!(!cut_cell(&mut short, 6));
        assert_eq!(short, Value::from(123_456));
        let mut long = serde_json::json!([1, 2, 3, 4]);
        assert!(cut_cell(&mut long, 4));
        assert_eq!(long, Value::from("[1,2…"));
    }

    #[test]
    fn cell_cut_is_char_boundary_safe() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let col: ArrayRef = Arc::new(StringArray::from(vec!["é".repeat(100)]));
        let r = keep(vec![RecordBatch::try_new(schema, vec![col]).unwrap()], 10);
        let p = page(&r, 0, 1 << 20, 11);
        assert_eq!(p.truncated_cells, 1);
        assert_eq!(p.rows[0][0], Value::from(format!("{}…", "é".repeat(5))));
    }
}
