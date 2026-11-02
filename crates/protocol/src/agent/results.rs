//! Bounded results: retention of at most `max_rows` rows and
//! `max_retained_bytes` bytes, collected as the query produces them and
//! charged to a per-user and a process-wide budget, and pages over the
//! retained rows limited by bytes and cell size.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, GenericBinaryArray, GenericStringArray,
    OffsetSizeTrait, StringArray, StringViewArray, UInt32Array,
};
use arrow::compute::take_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde_json::Value;

use arneb_common::memory_pool::BatchMemorySizer;

use crate::handler::BatchSink;
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
    /// The user's results together reached `max_user_retained_bytes`, and
    /// none of the user's ended statements was left to evict. Delete
    /// statements whose results you no longer need, or wait for running
    /// ones to finish.
    UserBudget,
    /// The server's budget for retained results across all users
    /// (`max_total_retained_bytes`) was exhausted, and no ended statement
    /// was left to evict. Retry later.
    ServerBudget,
}

impl TruncatedBy {
    /// The name `truncated_by` serializes as (`max_rows`, `user_budget`,
    /// ...), for messages that must match it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MaxRows => "max_rows",
            Self::MaxRetainedBytes => "max_retained_bytes",
            Self::UserBudget => "user_budget",
            Self::ServerBudget => "server_budget",
        }
    }
}

/// Which budget a reservation could not grow within.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shortfall {
    /// The user's share (`max_user_retained_bytes`).
    User,
    /// The server budget (`max_total_retained_bytes`).
    Server,
}

/// The budget for retained result bytes shared by every statement of one
/// service: at most `capacity` in all and `user_capacity` per user.
#[derive(Debug)]
pub(crate) struct RetainedBudget {
    capacity: usize,
    user_capacity: usize,
    state: Mutex<BudgetState>,
}

#[derive(Debug, Default)]
struct BudgetState {
    used: usize,
    by_user: HashMap<String, usize>,
}

impl RetainedBudget {
    pub(crate) fn new(capacity: usize, user_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            user_capacity,
            state: Mutex::default(),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, BudgetState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Bytes currently held by retained results.
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.state().used
    }

    /// Bytes currently held by `user`'s retained results.
    #[cfg(test)]
    pub(crate) fn used_by(&self, user: &str) -> usize {
        self.state().by_user.get(user).copied().unwrap_or(0)
    }

    /// Bytes `user` may still take.
    fn room(&self, user: &str) -> usize {
        let state = self.state();
        let mine = state.by_user.get(user).copied().unwrap_or(0);
        self.capacity
            .saturating_sub(state.used)
            .min(self.user_capacity.saturating_sub(mine))
    }

    /// Takes `bytes` for `user`, or nothing and the budget that is short.
    fn try_take(&self, user: &str, bytes: usize) -> Result<(), Shortfall> {
        let mut state = self.state();
        let mine = state.by_user.get(user).copied().unwrap_or(0);
        if mine.saturating_add(bytes) > self.user_capacity {
            return Err(Shortfall::User);
        }
        if state.used.saturating_add(bytes) > self.capacity {
            return Err(Shortfall::Server);
        }
        state.used += bytes;
        *state.by_user.entry(user.to_string()).or_default() += bytes;
        Ok(())
    }

    /// How many bytes would have to be freed in the `short` budget for
    /// `user` to take `bytes`, as of now (0 once they fit).
    pub(crate) fn missing(&self, user: &str, bytes: usize, short: Shortfall) -> usize {
        let state = self.state();
        match short {
            Shortfall::User => {
                let mine = state.by_user.get(user).copied().unwrap_or(0);
                (mine + bytes).saturating_sub(self.user_capacity)
            }
            Shortfall::Server => (state.used + bytes).saturating_sub(self.capacity),
        }
    }

    fn give_back(&self, user: &str, bytes: usize) {
        let mut state = self.state();
        state.used = state.used.saturating_sub(bytes);
        if let Some(mine) = state.by_user.get_mut(user) {
            *mine = mine.saturating_sub(bytes);
            if *mine == 0 {
                state.by_user.remove(user);
            }
        }
    }
}

/// Bytes one statement's retained rows hold against the budget; given back
/// when the last clone of the [`Retained`] drops (the statement is deleted,
/// evicted or reaped, and no page is being encoded from it), or when the
/// [`Collector`] is dropped unfinished (the statement failed or was
/// cancelled).
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<RetainedBudget>,
    user: String,
    bytes: usize,
}

impl Reservation {
    fn try_grow(&mut self, bytes: usize) -> Result<(), Shortfall> {
        self.budget.try_take(&self.user, bytes)?;
        self.bytes += bytes;
        Ok(())
    }

    /// Gives every byte back (the collector restarts).
    fn clear(&mut self) {
        self.budget.give_back(&self.user, self.bytes);
        self.bytes = 0;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.give_back(&self.user, self.bytes);
    }
}

/// The rows a finished statement keeps. Cloning is cheap: the batches share
/// their Arrow buffers, and the clones share one budget reservation.
#[derive(Debug, Default, Clone)]
pub(crate) struct Retained {
    /// At most `max_rows` rows and `max_retained_bytes` bytes.
    pub(crate) batches: Vec<RecordBatch>,
    /// Rows in the result when execution was not capped, kept or not;
    /// `None` when the query produced more than `max_rows` rows, since
    /// execution stops after `max_rows + 1` and never learns the full count.
    pub(crate) total_rows: Option<u64>,
    /// Why rows were dropped; `None` when every produced row was kept.
    pub(crate) truncated_by: Option<TruncatedBy>,
    /// Rows the query produced (at most `max_rows + 1`, since execution
    /// stops there), whether kept or not.
    pub(crate) produced: u64,
    /// Arrow memory of the kept batches, charged to the budget.
    pub(crate) bytes: usize,
    /// Why not even the first row was kept, when that happened.
    pub(crate) hint: Option<String>,
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

    /// Whether a clone (a page being encoded) still holds the reservation,
    /// so dropping this one would free nothing yet.
    pub(crate) fn pinned(&self) -> bool {
        self._reservation
            .as_ref()
            .is_some_and(|r| Arc::strong_count(r) > 1)
    }
}

/// Copies `batch` into freshly allocated buffers, so the kept rows no longer
/// pin the (possibly much larger) buffers they were sliced from. A
/// dictionary column keeps sharing its dictionary. A batch that cannot be
/// copied is kept as is.
fn compact(batch: RecordBatch) -> RecordBatch {
    let indices = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
    take_record_batch(&batch, &indices).unwrap_or(batch)
}

/// `s`, or its prefix up to the first character boundary past
/// `max_cell_bytes` when it is longer: still longer than the limit, so the
/// page cuts it exactly as it would the whole value, and counts it.
fn cell_prefix(s: &str, max_cell_bytes: usize) -> &str {
    if s.len() <= max_cell_bytes {
        return s;
    }
    let mut end = max_cell_bytes + 1;
    while !s.is_char_boundary(end) {
        end += 1;
    }
    &s[..end]
}

/// Raw bytes kept of a longer binary value: a multiple of 3, so their
/// (padded, standard) base64 is an exact prefix of the whole value's, and
/// longer than `max_cell_bytes`, so the page cuts it exactly as it would the
/// whole value, and counts it.
fn binary_prefix_len(max_cell_bytes: usize) -> usize {
    3 * (max_cell_bytes / 4 + 1)
}

fn cut_strings<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    max_cell_bytes: usize,
) -> Option<ArrayRef> {
    if !array.iter().flatten().any(|s| s.len() > max_cell_bytes) {
        return None;
    }
    let cut: GenericStringArray<O> = array
        .iter()
        .map(|v| v.map(|s| cell_prefix(s, max_cell_bytes)))
        .collect();
    Some(Arc::new(cut))
}

fn cut_binaries<O: OffsetSizeTrait>(
    array: &GenericBinaryArray<O>,
    keep: usize,
) -> Option<ArrayRef> {
    if !array.iter().flatten().any(|b| b.len() > keep) {
        return None;
    }
    let cut: GenericBinaryArray<O> = array
        .iter()
        .map(|v| v.map(|b| &b[..b.len().min(keep)]))
        .collect();
    Some(Arc::new(cut))
}

/// `column` with every value longer than a page can show shortened to what
/// it can still show, or `None` when nothing needs cutting (or the type is
/// not cut: nested, dictionary and other values are kept whole). A
/// fixed-size binary column becomes a variable-size one, which encodes the
/// same.
fn cut_column(column: &ArrayRef, max_cell_bytes: usize) -> Option<ArrayRef> {
    let keep = binary_prefix_len(max_cell_bytes);
    match column.data_type() {
        DataType::Utf8 => cut_strings(column.as_string::<i32>(), max_cell_bytes),
        DataType::LargeUtf8 => cut_strings(column.as_string::<i64>(), max_cell_bytes),
        DataType::Utf8View => {
            let a = column.as_string_view();
            a.iter()
                .flatten()
                .any(|s| s.len() > max_cell_bytes)
                .then(|| {
                    let cut: StringViewArray = a
                        .iter()
                        .map(|v| v.map(|s| cell_prefix(s, max_cell_bytes)))
                        .collect();
                    Arc::new(cut) as ArrayRef
                })
        }
        DataType::Binary => cut_binaries(column.as_binary::<i32>(), keep),
        DataType::LargeBinary => cut_binaries(column.as_binary::<i64>(), keep),
        DataType::BinaryView => {
            let a = column.as_binary_view();
            a.iter().flatten().any(|b| b.len() > keep).then(|| {
                let cut: BinaryViewArray = a
                    .iter()
                    .map(|v| v.map(|b| &b[..b.len().min(keep)]))
                    .collect();
                Arc::new(cut) as ArrayRef
            })
        }
        DataType::FixedSizeBinary(width) if *width as usize > keep => {
            let a = column.as_fixed_size_binary();
            let cut: BinaryArray = a.iter().map(|v| v.map(|b| &b[..keep])).collect();
            Some(Arc::new(cut))
        }
        _ => None,
    }
}

/// `batch` with `columns` in place of its own, the schema following their
/// types.
fn with_columns(batch: RecordBatch, columns: Vec<ArrayRef>) -> RecordBatch {
    let schema = batch.schema();
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(&columns)
        .map(|(f, c)| f.as_ref().clone().with_data_type(c.data_type().clone()))
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(schema, columns).unwrap_or(batch)
}

/// `batch` with every string and binary value longer than a page can show
/// shortened to what it can still show ([`cut_column`]), so an oversized
/// value never stays in memory past retention.
fn cut_long_values(batch: RecordBatch, max_cell_bytes: usize) -> RecordBatch {
    let cut: Vec<Option<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|c| cut_column(c, max_cell_bytes))
        .collect();
    if cut.iter().all(Option::is_none) {
        return batch;
    }
    let columns = cut
        .into_iter()
        .zip(batch.columns())
        .map(|(cut, c)| cut.unwrap_or_else(|| Arc::clone(c)))
        .collect();
    with_columns(batch, columns)
}

/// A one-row `row` as small as the page output allows: a cell longer than
/// `max_cell_bytes` once encoded (a list, map, struct, …) becomes the cut
/// string the page would show (cutting it again on a page changes
/// nothing), and a dictionary column is decoded so the row no longer pins
/// the dictionary. Every other cell is kept as is.
fn render_first_row(row: RecordBatch, max_cell_bytes: usize) -> RecordBatch {
    let columns = row
        .columns()
        .iter()
        .map(|column| {
            let plain = materialize(column);
            let mut cell = encode_cell(plain.as_ref(), 0);
            if cut_cell(&mut cell, max_cell_bytes) {
                let text = cell.as_str().unwrap_or_default();
                Arc::new(StringArray::from(vec![text])) as ArrayRef
            } else {
                plain
            }
        })
        .collect();
    compact(with_columns(row, columns))
}

/// Frees retained bytes for a collector by evicting ended statements from
/// the `short` budget (only the collecting user's own for
/// [`Shortfall::User`]) until nothing is missing. The second argument
/// reports the bytes still missing, read from the budget each time it is
/// called, so a shortfall that went away meanwhile evicts nothing. Returns
/// whether retrying may now succeed; `false` (after evicting nothing, when
/// the candidates cannot cover the shortfall) makes the collector give up.
pub(crate) type Evict = Box<dyn FnMut(Shortfall, &dyn Fn() -> usize) -> bool + Send>;

/// Retains a statement's rows as the query produces them: at most
/// `max_rows` rows and `max_retained_bytes` bytes (measured with
/// [`BatchMemorySizer`], so a dictionary shared by several batches counts
/// once), each kept batch charged to the budget as it arrives. Strings and
/// binaries are cut to what a page can show as they are kept. When the
/// budget is short it evicts ended statements (only when that covers the
/// shortfall) and retries; once a limit stops it, it keeps counting the
/// rows the query produces but drops their data, so the memory held never
/// exceeds the limits. A first row that does not fit as is is rendered to
/// its cut page form and kept if that fits.
pub(crate) struct Collector {
    max_rows: usize,
    max_bytes: usize,
    max_cell_bytes: usize,
    reservation: Reservation,
    evict: Evict,
    sizer: BatchMemorySizer,
    kept: Vec<RecordBatch>,
    kept_rows: usize,
    produced: u64,
    /// The byte limit that stopped retention, once one has.
    stopped_by: Option<TruncatedBy>,
    hint: Option<String>,
}

impl Collector {
    pub(crate) fn new(
        user: &str,
        max_rows: usize,
        max_bytes: usize,
        max_cell_bytes: usize,
        budget: &Arc<RetainedBudget>,
        evict: Evict,
    ) -> Self {
        Self {
            max_rows,
            max_bytes,
            max_cell_bytes,
            reservation: Reservation {
                budget: Arc::clone(budget),
                user: user.to_string(),
                bytes: 0,
            },
            evict,
            sizer: BatchMemorySizer::default(),
            kept: Vec::new(),
            kept_rows: 0,
            produced: 0,
            stopped_by: None,
            hint: None,
        }
    }

    /// Bytes retained so far.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.reservation.bytes
    }

    /// Charges `size` more bytes, evicting ended statements when that frees
    /// enough of a short budget; the limit that refused it otherwise.
    fn grow(&mut self, size: usize) -> Result<(), TruncatedBy> {
        if size > self.max_bytes.saturating_sub(self.reservation.bytes) {
            return Err(TruncatedBy::MaxRetainedBytes);
        }
        loop {
            let short = match self.reservation.try_grow(size) {
                Ok(()) => return Ok(()),
                Err(short) => short,
            };
            let (budget, user) = (&self.reservation.budget, &self.reservation.user);
            let missing = || budget.missing(user, size, short);
            if !(self.evict)(short, &missing) {
                return Err(match short {
                    Shortfall::User => TruncatedBy::UserBudget,
                    Shortfall::Server => TruncatedBy::ServerBudget,
                });
            }
        }
    }

    /// Bytes this statement may still take without evicting.
    fn room(&self) -> usize {
        self.max_bytes
            .saturating_sub(self.reservation.bytes)
            .min(self.reservation.budget.room(&self.reservation.user))
    }

    fn keep(&mut self, piece: RecordBatch) {
        self.sizer.add(&piece);
        self.kept_rows += piece.num_rows();
        self.kept.push(piece);
    }

    /// The longest proper prefix of `piece` (compacted) that fits in `room`
    /// bytes, with its size; `None` when not even one row fits. Starts from
    /// a proportional estimate and shrinks it.
    fn prefix_within(
        &self,
        piece: &RecordBatch,
        size: usize,
        room: usize,
    ) -> Option<(RecordBatch, usize)> {
        let n = piece.num_rows();
        let mut k = ((n as u128 * room as u128) / size.max(1) as u128) as usize;
        k = k.min(n.saturating_sub(1));
        while k > 0 {
            let prefix = compact(piece.slice(0, k));
            let prefix_size = self.sizer.measure(&prefix);
            if prefix_size <= room {
                return Some((prefix, prefix_size));
            }
            k = (k * 3 / 4).min(k - 1);
        }
        None
    }

    /// The retained rows, with why any were dropped.
    pub(crate) fn finish(self) -> Retained {
        let truncated_by = (self.produced > self.kept_rows as u64)
            .then(|| self.stopped_by.unwrap_or(TruncatedBy::MaxRows));
        Retained {
            total_rows: (self.produced <= self.max_rows as u64).then_some(self.produced),
            truncated_by,
            produced: self.produced,
            bytes: self.reservation.bytes,
            hint: self.hint,
            batches: self.kept,
            _reservation: Some(Arc::new(self.reservation)),
        }
    }
}

impl BatchSink for Collector {
    fn push(&mut self, batch: RecordBatch) {
        let n = batch.num_rows();
        self.produced += n as u64;
        let take = n.min(self.max_rows.saturating_sub(self.kept_rows));
        if self.stopped_by.is_some() || take == 0 {
            return;
        }
        let sliced = if take == n {
            batch
        } else {
            batch.slice(0, take)
        };
        let piece = compact(cut_long_values(sliced, self.max_cell_bytes));
        let size = self.sizer.measure(&piece);
        let reason = match self.grow(size) {
            Ok(()) => return self.keep(piece),
            Err(reason) => reason,
        };
        // Out of room: keep the prefix that still fits, then stop.
        if let Some((prefix, prefix_size)) = self.prefix_within(&piece, size, self.room()) {
            if self.grow(prefix_size).is_ok() {
                self.keep(prefix);
            }
        }
        let mut reason = reason;
        if self.kept_rows == 0 {
            // Not even one row fits as is: keep the first row in the form a
            // page shows it, if that fits.
            let first = render_first_row(compact(piece.slice(0, 1)), self.max_cell_bytes);
            let first_size = self.sizer.measure(&first);
            match self.grow(first_size) {
                Ok(()) => self.keep(first),
                Err(why) => {
                    reason = why;
                    self.hint = Some(format!(
                        "the first row alone needs {first_size} bytes even with long values \
                         cut to max_cell_bytes, more than this statement could retain \
                         ({}); select fewer columns, or release other results",
                        why.as_str()
                    ));
                }
            }
        }
        self.stopped_by = Some(reason);
    }

    fn reset(&mut self) {
        self.reservation.clear();
        self.sizer = BatchMemorySizer::default();
        self.kept.clear();
        self.kept_rows = 0;
        self.produced = 0;
        self.stopped_by = None;
        self.hint = None;
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

/// Retains `batches` as `alice` with nothing to evict, and cells kept whole.
#[cfg(test)]
pub(crate) fn retain(
    batches: Vec<RecordBatch>,
    max_rows: usize,
    max_bytes: usize,
    budget: &Arc<RetainedBudget>,
) -> Retained {
    let mut c = Collector::new(
        "alice",
        max_rows,
        max_bytes,
        usize::MAX,
        budget,
        Box::new(|_, _| false),
    );
    batches.into_iter().for_each(|b| c.push(b));
    c.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema, UInt32Type};

    use arneb_common::memory_pool::batch_memory_size;

    fn unbounded() -> Arc<RetainedBudget> {
        RetainedBudget::new(usize::MAX, usize::MAX)
    }

    /// `retain` with no byte limit.
    fn keep(batches: Vec<RecordBatch>, max_rows: usize) -> Retained {
        retain(batches, max_rows, usize::MAX, &unbounded())
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
    fn per_statement_byte_cap_truncates_but_keeps_the_known_total() {
        let budget = unbounded();
        // 100 rows of 10 KB: about 1 MB, capped at 100 KB.
        let r = retain(vec![wide_batch(100, 10_000)], 1000, 100_000, &budget);
        assert_eq!(r.truncated_by, Some(TruncatedBy::MaxRetainedBytes));
        // Execution was not capped (100 <= max_rows), so the count is exact.
        assert_eq!(r.total_rows, Some(100));
        assert_eq!(r.produced, 100);
        assert!(r.kept_rows() > 0 && r.kept_rows() < 10, "{}", r.kept_rows());
        assert!(r.bytes <= 100_000, "{}", r.bytes);
        let measured: usize = r.batches.iter().map(batch_memory_size).sum();
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
        let budget = RetainedBudget::new(one + one / 2, usize::MAX);
        let first = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(first.truncated_by, None);
        assert_eq!(budget.used(), first.bytes);
        // The second statement gets only what is left of the budget.
        let second = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(second.truncated_by, Some(TruncatedBy::ServerBudget));
        assert_eq!(second.total_rows, Some(10));
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
    fn total_rows_is_null_only_when_execution_was_capped() {
        let budget = unbounded();
        // 200 rows cut to a few by bytes: the count is still exact.
        let r = retain(vec![wide_batch(200, 1000)], 1000, 20_000, &budget);
        assert_eq!(r.truncated_by, Some(TruncatedBy::MaxRetainedBytes));
        assert_eq!(r.total_rows, Some(200));
        // max_rows + 1 produced: execution stopped, so no count.
        let r = retain(vec![wide_batch(11, 10)], 10, usize::MAX, &budget);
        assert_eq!(r.truncated_by, Some(TruncatedBy::MaxRows));
        assert_eq!(r.total_rows, None);
        // Exactly max_rows, nothing cut.
        let r = retain(vec![wide_batch(10, 10)], 10, usize::MAX, &budget);
        assert_eq!((r.truncated_by, r.total_rows), (None, Some(10)));
    }

    #[test]
    fn collection_never_holds_more_than_the_byte_cap() {
        let budget = unbounded();
        let cap = 200_000;
        let mut c = Collector::new("alice", 10_000, cap, 8192, &budget, Box::new(|_, _| false));
        // 100 batches of 10 rows of 5 KB: about 5 MB produced in all.
        for _ in 0..100 {
            c.push(wide_batch(10, 5_000));
            assert!(c.bytes() <= cap, "{} > {cap}", c.bytes());
            assert_eq!(budget.used(), c.bytes());
            let held: usize = c.kept.iter().map(batch_memory_size).sum();
            assert!(held <= cap, "{held} > {cap}");
        }
        let r = c.finish();
        assert_eq!(r.produced, 1000);
        assert_eq!(r.total_rows, Some(1000));
        assert_eq!(r.truncated_by, Some(TruncatedBy::MaxRetainedBytes));
        assert!(r.kept_rows() > 0 && r.kept_rows() < 50, "{}", r.kept_rows());
    }

    #[test]
    fn a_single_oversized_cell_comes_back_as_one_cut_row() {
        let budget = unbounded();
        // One 80 MB value, over the default 64 MiB statement cap.
        let r = {
            let mut c = Collector::new(
                "alice",
                1000,
                64 << 20,
                8192,
                &budget,
                Box::new(|_, _| false),
            );
            c.push(wide_batch(1, 80 << 20));
            c.finish()
        };
        assert_eq!(r.kept_rows(), 1);
        assert_eq!((r.truncated_by, r.total_rows), (None, Some(1)));
        // Only what a page can show of the cell is retained.
        assert!(r.bytes < 64 << 10, "{}", r.bytes);
        let p = page(&r, 0, 1 << 20, 8192);
        assert_eq!(p.truncated_cells, 1);
        assert_eq!(p.rows[0][0], Value::from(format!("{}…", "w".repeat(8192))));
        // A statement cap smaller than even the cut row keeps no row, never
        // more than the cap, and says why.
        let mut c = Collector::new("alice", 1000, 10, 8192, &budget, Box::new(|_, _| false));
        c.push(wide_batch(3, 100_000));
        let r = c.finish();
        assert_eq!(r.kept_rows(), 0);
        assert_eq!(r.bytes, 0);
        assert_eq!(r.truncated_by, Some(TruncatedBy::MaxRetainedBytes));
        assert!(r.hint.unwrap().contains("first row alone"));
    }

    fn one_column(array: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    /// The first page of `batch` when kept whole, and when collected under
    /// a 1 MiB statement cap.
    fn whole_and_collected(batch: RecordBatch) -> (ResultPage, Retained) {
        let whole = page(&keep(vec![batch.clone()], 10), 0, 1 << 20, 8192);
        let mut c = Collector::new(
            "alice",
            10,
            1 << 20,
            8192,
            &unbounded(),
            Box::new(|_, _| false),
        );
        c.push(batch);
        (whole, c.finish())
    }

    #[test]
    fn an_80_mb_binary_is_cut_at_keep_time_with_the_same_page() {
        let value = vec![7u8; 80 << 20];
        for array in [
            Arc::new(BinaryArray::from(vec![value.as_slice()])) as ArrayRef,
            Arc::new(arrow::array::LargeBinaryArray::from(vec![value.as_slice()])),
            Arc::new(BinaryViewArray::from(vec![value.as_slice()])),
        ] {
            let ty = array.data_type().clone();
            let (whole, r) = whole_and_collected(one_column(array));
            assert_eq!((r.kept_rows(), r.truncated_by), (1, None), "{ty}");
            assert!(r.bytes < 64 << 10, "{ty}: {}", r.bytes);
            let p = page(&r, 0, 1 << 20, 8192);
            assert_eq!(p, whole, "{ty}: the page is unchanged");
            assert_eq!(p.truncated_cells, 1);
        }
        // A short binary is kept whole and not cut.
        let (whole, r) = whole_and_collected(one_column(Arc::new(BinaryArray::from(vec![
            b"abc".as_ref()
        ]))));
        assert_eq!(page(&r, 0, 1 << 20, 8192), whole);
        assert_eq!(whole.truncated_cells, 0);
    }

    #[test]
    fn a_large_list_first_row_is_kept_as_its_cut_page_form() {
        use arrow::array::{Int64Builder, ListBuilder};
        // One list of 2 million bigints (16 MB) under a 1 MiB cap.
        let mut b = ListBuilder::new(Int64Builder::new());
        b.values().append_slice(&vec![123_456_789; 2_000_000]);
        b.append(true);
        let list: ArrayRef = Arc::new(b.finish());
        let id: ArrayRef = Arc::new(Int64Array::from(vec![42]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("l", list.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![id, list]).unwrap();
        let (whole, r) = whole_and_collected(batch);
        assert_eq!((r.kept_rows(), r.truncated_by), (1, None));
        assert!(r.bytes < 64 << 10, "{}", r.bytes);
        let p = page(&r, 0, 1 << 20, 8192);
        assert_eq!(p, whole);
        assert_eq!(p.rows[0][0], Value::from(42), "uncut cells keep their type");
        assert_eq!(p.truncated_cells, 1);
    }

    #[test]
    fn the_first_row_never_bypasses_the_user_share() {
        let one = keep(vec![wide_batch(10, 1000)], 1000).bytes;
        let budget = RetainedBudget::new(usize::MAX, one);
        let full = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(full.truncated_by, None);
        let mut c = Collector::new(
            "alice",
            1000,
            usize::MAX,
            8192,
            &budget,
            Box::new(|_, _| false),
        );
        c.push(wide_batch(1, 80 << 20));
        let r = c.finish();
        assert_eq!((r.kept_rows(), r.bytes), (0, 0));
        assert_eq!(r.truncated_by, Some(TruncatedBy::UserBudget));
        // The hint names the limit as truncated_by serializes it.
        let hint = r.hint.unwrap();
        assert!(hint.contains("(user_budget)"), "{hint}");
        assert_eq!(budget.used_by("alice"), full.bytes);
    }

    #[test]
    fn truncated_by_names_match_their_serialized_form() {
        for by in [
            TruncatedBy::MaxRows,
            TruncatedBy::MaxRetainedBytes,
            TruncatedBy::UserBudget,
            TruncatedBy::ServerBudget,
        ] {
            assert_eq!(serde_json::to_value(by).unwrap(), Value::from(by.as_str()));
        }
    }

    #[test]
    fn eviction_is_asked_for_the_shortfall_only() {
        let one = keep(vec![wide_batch(10, 1000)], 1000).bytes;
        let budget = RetainedBudget::new(usize::MAX, one + one / 2);
        let _first = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        let asked = Arc::new(Mutex::new(Vec::new()));
        let evict: Evict = {
            let asked = Arc::clone(&asked);
            Box::new(move |short, missing| {
                asked.lock().unwrap().push((short, missing()));
                false
            })
        };
        let mut c = Collector::new("alice", 1000, usize::MAX, 8192, &budget, evict);
        c.push(wide_batch(10, 1000));
        let asked = asked.lock().unwrap();
        // The whole batch first: what is missing beyond the user's room.
        assert_eq!(asked[0], (Shortfall::User, one - one / 2));
    }

    #[test]
    fn reset_starts_a_retried_attempt_clean() {
        let budget = unbounded();
        let mut c = Collector::new(
            "alice",
            5,
            usize::MAX,
            8192,
            &budget,
            Box::new(|_, _| false),
        );
        c.push(batch(0, 10));
        assert!(budget.used() > 0);
        c.reset();
        assert_eq!(budget.used(), 0);
        c.push(batch(100, 3));
        let r = c.finish();
        assert_eq!((r.produced, r.kept_rows(), r.truncated_by), (3, 3, None));
        assert_eq!(page(&r, 0, 1 << 20, 8192).rows[0][0], Value::from(100));
        assert_eq!(budget.used(), r.bytes);
    }

    #[test]
    fn retention_cut_matches_the_page_cut_on_multibyte_text() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let col: ArrayRef = Arc::new(StringArray::from(vec!["é".repeat(100)]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
        let whole = page(&keep(vec![batch.clone()], 10), 0, 1 << 20, 11);
        let mut c = Collector::new(
            "alice",
            10,
            usize::MAX,
            11,
            &unbounded(),
            Box::new(|_, _| false),
        );
        c.push(batch);
        assert_eq!(page(&c.finish(), 0, 1 << 20, 11), whole);
    }

    #[test]
    fn a_shared_dictionary_is_charged_once() {
        // 100 distinct 10 KB values: a 1 MB dictionary every batch shares.
        let values = StringArray::from_iter_values((0..100).map(|i| format!("{i:0>10000}")));
        let keys = arrow::array::UInt32Array::from_iter_values((0..100).map(|i| i % 100));
        let dict: ArrayRef =
            Arc::new(DictionaryArray::<UInt32Type>::try_new(keys, Arc::new(values)).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            dict.data_type().clone(),
            false,
        )]));
        let batches: Vec<RecordBatch> = (0..20)
            .map(|i| RecordBatch::try_new(Arc::clone(&schema), vec![dict.slice(i * 5, 5)]).unwrap())
            .collect();
        // Charged per batch, 20 batches would need about 20 MB.
        let r = retain(batches, 1000, 4 << 20, &unbounded());
        assert_eq!(r.truncated_by, None);
        assert_eq!(r.kept_rows(), 100);
        assert!(r.bytes < 2 << 20, "{}", r.bytes);
    }

    #[test]
    fn user_share_truncates_and_evicts_before_giving_up() {
        let one = keep(vec![wide_batch(10, 1000)], 1000).bytes;
        let budget = RetainedBudget::new(usize::MAX, one + one / 2);
        let first = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(first.truncated_by, None);
        let second = retain(vec![wide_batch(10, 1000)], 1000, usize::MAX, &budget);
        assert_eq!(second.truncated_by, Some(TruncatedBy::UserBudget));
        assert_eq!(budget.used_by("alice"), budget.used());
        drop(second);
        // An evictor that drops `first` when asked makes room for a full result.
        let held = Arc::new(Mutex::new(Some(first)));
        let asked = Arc::new(Mutex::new(Vec::new()));
        let evict: Evict = {
            let (held, asked) = (Arc::clone(&held), Arc::clone(&asked));
            Box::new(move |short, _| {
                asked.lock().unwrap().push(short);
                held.lock().unwrap().take().is_some()
            })
        };
        let mut c = Collector::new("alice", 1000, usize::MAX, 8192, &budget, evict);
        c.push(wide_batch(10, 1000));
        let third = c.finish();
        assert_eq!(third.truncated_by, None);
        assert_eq!(*asked.lock().unwrap(), vec![Shortfall::User]);
        assert_eq!(budget.used(), third.bytes);
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
