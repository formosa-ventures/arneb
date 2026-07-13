//! Physical execution operators.
//!
//! Each operator implements [`ExecutionPlan`] and produces a
//! [`SendableRecordBatchStream`] from its children. Operators are assembled
//! into a tree by the physical planner in [`super::planner`].

use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
#[cfg(not(test))]
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use arneb_common::error::ExecutionError;
use arneb_common::memory_profile::LiveBytesGuard;
use arneb_common::stream::{
    collect_stream, stream_from_batches, RecordBatchStream, SendableRecordBatchStream,
};
use arneb_common::types::{ColumnInfo, ScalarValue};
use arneb_planner::{LogicalPlan, PlanExpr, SortExpr};
use arneb_sql_parser::ast;
use arrow::array::{
    self, Array, ArrayRef, AsArray, BooleanArray, Date32Array, DictionaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::compute;
use arrow::datatypes::{self, DataType as ArrowDataType, Field, Schema, UInt32Type};
use async_trait::async_trait;
use futures::stream::Stream;
use futures::StreamExt;

use crate::aggregate::{self, Accumulator, GroupedAccumulator};
use crate::datasource::DataSource;
use crate::dynamic_filter_collector::{domain_to_filter_expr, DynamicFilterCollector};
use crate::expression;
use crate::fast_hash::FastHashMap;
use crate::group_by_hash::{
    agg_presize_adaptive_enabled, group_partition_assignments, GroupByHash,
};
use crate::group_key::GroupKey;
use crate::scan_context::{DynamicFilterDomain, ScanContext};
use crate::spill::PartitionedSpillWriter;

/// A physical execution operator that produces a stream of record batches.
#[async_trait]
pub trait ExecutionPlan: Any + Send + Sync + Debug {
    /// The output schema of this operator.
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Executes the operator and returns a stream of result batches.
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError>;

    /// A short display name for EXPLAIN output.
    fn display_name(&self) -> &str;

    /// Describes how this operator's output is partitioned across `n`
    /// independent streams. Phase 3.1 defaults to a single sequential
    /// partition — matching the pre-3.1 execution model. Operators that
    /// want intra-query parallelism override this in later phases.
    fn output_partitioning(&self) -> crate::partitioning::Partitioning {
        crate::partitioning::Partitioning::UnknownPartitioning(1)
    }

    /// Per-child partitioning requirement, one entry per child input.
    /// The physical planner inserts `RepartitionExec` /
    /// `CoalescePartitionsExec` whenever a child's
    /// [`output_partitioning`](Self::output_partitioning) does not
    /// [`satisfy`](crate::partitioning::Partitioning::satisfies) the
    /// declared requirement. Defaults to an empty vector — Phase 3.1
    /// is structural only, so no operator yet asserts a requirement.
    fn required_input_partitioning(&self) -> Vec<crate::partitioning::Partitioning> {
        Vec::new()
    }

    /// Inject a runtime-derived filter into this operator's deepest
    /// `ScanExec`(s). Used by `HashJoinExec` after its build phase
    /// completes to push a min/max (or future: sorted-range-set)
    /// filter derived from the build keys down into the probe-side
    /// scan — Trino-style dynamic filters.
    ///
    /// `target_index` is the column the filter applies to, expressed as an
    /// index in THIS operator's OUTPUT schema (provenance routing, not by
    /// name). Composite operators (Filter, Projection, Limit, HashJoin, …)
    /// override to remap `target_index` into the owning child and recurse;
    /// `ScanExec` overrides to apply the filter at that local index.
    ///
    /// Default impl: no-op (drop). Best-effort: if the index cannot be traced
    /// to a scan (e.g. through a computed projection or an unhandled operator)
    /// the filter is silently dropped — correctness is unaffected, only a
    /// pruning opportunity is missed.
    ///
    /// Must only be called BEFORE the first `execute()`. Calling it
    /// after a partition has been opened may or may not affect that
    /// partition's behavior depending on the data source's filter
    /// application timing.
    fn inject_dynamic_filter(&self, _filter: PlanExpr, _target_index: usize) {}

    /// Whether this subtree is "leaf-scan-shaped" — a single scan with
    /// optional filtering/projection/repartition wrapping, no joins or
    /// aggregates. Used by `HashJoinExec` to decide whether it's safe
    /// to overlap its build phase with collecting the left input
    /// (parallel-build, Step PB-v2): when both sides are leaf scans,
    /// the overlap saves one full scan's worth of wall-time without
    /// the task-count blow-up that universal PB caused. Joins,
    /// aggregates, and other compound operators return `false` so we
    /// don't multiply concurrent tasks at every join level.
    fn is_leaf_scan_subtree(&self) -> bool {
        false
    }

    /// Peak bytes the operator has reserved for its build/hash/group
    /// state during execution. Trino-aligned with `LocalMemoryContext.
    /// setBytes()` — see `HashBuilderOperator.java:329` for the build
    /// hash-table accounting, `JoinHash.getInMemorySizeInBytes` for
    /// what it counts.
    ///
    /// Returns 0 for operators that haven't executed yet or that don't
    /// retain significant memory (the default for streaming operators).
    /// Operators that hold a large per-execute structure (right-side
    /// hash table, group-by state, sort buffer) override this to
    /// expose their peak.
    ///
    /// Used by `EXPLAIN ANALYZE` (future) and by the bench harness to
    /// compare arneb-vs-Trino memory profiles when chasing OOMs.
    fn peak_bytes_reserved(&self) -> usize {
        0
    }
}

#[cfg(test)]
impl dyn ExecutionPlan {
    pub(crate) fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Sum the in-memory size of every column of a [`RecordBatch`] —
/// Trino-equivalent to `Page.getRetainedSizeInBytes()`. Includes
/// shared buffer overhead, so the value is "retained bytes",
/// stable across batch slicing.
pub(crate) fn record_batch_bytes(batch: &RecordBatch) -> usize {
    (0..batch.num_columns())
        .map(|i| batch.column(i).get_array_memory_size())
        .sum()
}

/// Pass-through stream that times the entire produce-to-drain lifetime
/// of an operator and emits a single `target: "arneb::profile"` log on
/// completion. Used by Step 1 of the Q09 profile session to map
/// per-operator wall-time, row count, and retained-bytes.
///
/// Enable with the server binary's `--profile` flag (or set
/// `RUST_LOG=arneb::profile=info` manually).
pub(crate) struct ProfileStream {
    inner: SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
    op: &'static str,
    partition: usize,
    start: Instant,
    first_batch_ms: Option<u64>,
    total_rows: u64,
    total_bytes: u64,
    batches: u64,
    completed: bool,
}

impl ProfileStream {
    fn new(op: &'static str, partition: usize, inner: SendableRecordBatchStream) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            op,
            partition,
            start: Instant::now(),
            first_batch_ms: None,
            total_rows: 0,
            total_bytes: 0,
            batches: 0,
            completed: false,
        }
    }
}

impl Stream for ProfileStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if self.first_batch_ms.is_none() {
                    self.first_batch_ms = Some(self.start.elapsed().as_millis() as u64);
                }
                self.total_rows = self.total_rows.saturating_add(batch.num_rows() as u64);
                self.total_bytes = self
                    .total_bytes
                    .saturating_add(record_batch_bytes(&batch) as u64);
                self.batches = self.batches.saturating_add(1);
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                if !self.completed {
                    self.completed = true;
                    let elapsed_ms = self.start.elapsed().as_millis() as u64;
                    tracing::info!(
                        target: "arneb::profile",
                        op = self.op,
                        partition = self.partition,
                        elapsed_ms,
                        first_batch_ms = self.first_batch_ms.unwrap_or(elapsed_ms),
                        rows = self.total_rows,
                        bytes = self.total_bytes,
                        batches = self.batches,
                        "operator finished"
                    );
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ProfileStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Wrap `inner` with a [`ProfileStream`] that emits one `arneb::profile`
/// log when the stream terminates (clean end or after the consumer drops
/// it). Cheap: just a pin + atomic-free counters on the producer thread.
pub(crate) fn profile_stream(
    op: &'static str,
    partition: usize,
    inner: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    Box::pin(ProfileStream::new(op, partition, inner))
}

struct LiveBatchStream {
    inner: SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
    label: &'static str,
    current: Option<LiveBytesGuard>,
}

impl LiveBatchStream {
    fn new(label: &'static str, inner: SendableRecordBatchStream) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            label,
            current: None,
        }
    }
}

impl Stream for LiveBatchStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.current.take();
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let bytes = batch.get_array_memory_size() as u64;
                self.current = Some(LiveBytesGuard::new(self.label, bytes));
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for LiveBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

fn live_batch_stream(
    label: &'static str,
    inner: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    Box::pin(LiveBatchStream::new(label, inner))
}

/// Default coalescing target — typical Arrow batch size. Small batches
/// (e.g. the ~850-row fragments that survive a join→filter→exchange
/// pipeline) carry fixed per-batch overhead (serialization, array
/// allocation, operator dispatch); coalescing them up to this many rows
/// before handing off to the next operator amortizes that overhead.
/// N2-Q18 (2026-05-29): Q18 SF10's 30M-row ExchangeExec→ProjectionExec
/// path ran 35257 batches @ ~850 rows; ProjectionExec spent 7.2 s mostly
/// on per-batch overhead. Coalescing to 8192 cuts the batch count ~10×.
pub(crate) const COALESCE_TARGET_ROWS: usize = 8192;

/// Buffers consecutive small `RecordBatch`es from `inner` and emits them
/// concatenated once at least `target_rows` rows have accumulated (or at
/// end-of-stream). Preserves row order and content — purely a batching
/// transform, so it never changes query results. Empty batches are
/// dropped. On `Pending` it yields `Pending` (keeping the buffer) rather
/// than emitting a partial batch — safe for pull-based upstreams like
/// `ExchangeExec`'s Flight stream where batches arrive back-to-back.
pub(crate) struct CoalescingStream {
    inner: SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
    target_rows: usize,
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    done: bool,
}

impl CoalescingStream {
    fn new(inner: SendableRecordBatchStream, target_rows: usize) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            target_rows,
            buffer: Vec::new(),
            buffered_rows: 0,
            done: false,
        }
    }

    /// Concatenate and clear the buffer. Returns `None` when empty; for a
    /// single buffered batch, returns it directly (no concat cost).
    fn flush(&mut self) -> Option<Result<RecordBatch, arneb_common::error::ArnebError>> {
        self.buffered_rows = 0;
        if self.buffer.is_empty() {
            return None;
        }
        if self.buffer.len() == 1 {
            return Some(Ok(self.buffer.drain(..).next().unwrap()));
        }
        let res = compute::concat_batches(&self.schema, self.buffer.iter())
            .map_err(|e| arneb_common::error::ArnebError::from(ExecutionError::from(e)));
        self.buffer.clear();
        Some(res)
    }
}

impl Stream for CoalescingStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                return Poll::Ready(self.flush());
            }
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.buffered_rows += batch.num_rows();
                    self.buffer.push(batch);
                    if self.buffered_rows >= self.target_rows {
                        if let Some(out) = self.flush() {
                            return Poll::Ready(Some(out));
                        }
                    }
                    // otherwise keep pulling to fill the buffer
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.done = true;
                    return match self.flush() {
                        Some(out) => Poll::Ready(Some(out)),
                        None => Poll::Ready(None),
                    };
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for CoalescingStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Wrap `inner` so consecutive small batches are merged up to
/// `COALESCE_TARGET_ROWS` rows before being emitted. See
/// [`CoalescingStream`].
pub(crate) fn coalesce_stream(inner: SendableRecordBatchStream) -> SendableRecordBatchStream {
    Box::pin(CoalescingStream::new(inner, COALESCE_TARGET_ROWS))
}

/// Pool-tracked counterpart to [`collect_stream`]: pulls batches one
/// at a time, calls [`MemoryReservation::try_grow`] for each batch's
/// retained bytes, and accumulates them into a `Vec`. Returns a
/// `ResourceExhausted` error (cleanly propagated) the moment the
/// configured [`MemoryPool`] refuses growth — instead of letting the
/// kernel OOM-kill the worker.
///
/// The reservation is dropped (and bytes returned to the pool) when
/// the returned `Vec<RecordBatch>` is itself dropped — keep it alive
/// for as long as the batches are in use downstream by binding the
/// `_reservation` to a wider scope.
///
/// Phase M.3 (2026-05-21): used by `SortExec`, `TopKExec`, and
/// `StreamingHashAggregateExec` — each of which must materialise the
/// full input before producing output, but should fail fast rather
/// than OOM the worker.
pub(crate) async fn collect_stream_pool_tracked(
    mut stream: SendableRecordBatchStream,
    pool: Arc<dyn crate::memory_pool::MemoryPool>,
    consumer_name: &'static str,
) -> Result<(Vec<RecordBatch>, crate::memory_pool::MemoryReservation), ExecutionError> {
    let consumer = crate::memory_pool::MemoryConsumer::new(consumer_name).with_can_spill(false);
    let mut reservation = consumer.register(pool);
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch_res) = stream.next().await {
        let batch = batch_res.map_err(|e| {
            ExecutionError::InvalidOperation(format!("{consumer_name}: stream error: {e}"))
        })?;
        let bytes = record_batch_bytes(&batch);
        reservation.try_grow(bytes)?;
        batches.push(batch);
    }
    Ok((batches, reservation))
}

/// Outcome of [`collect_probe_within_budget`].
pub(crate) enum ProbeCollect {
    /// The whole probe fit within the pool budget — fully materialised with
    /// a live reservation. Safe for the fast parallel/in-memory probe.
    Fits {
        batches: Vec<RecordBatch>,
        reservation: crate::memory_pool::MemoryReservation,
    },
    /// The probe exceeded the budget mid-collect. Returns the already-pulled
    /// prefix (reservation released) plus the un-consumed remainder so the
    /// caller can switch to a bounded STREAMING probe without losing rows.
    Overflow {
        prefix: Vec<RecordBatch>,
        rest: SendableRecordBatchStream,
    },
}

/// Like [`collect_stream_pool_tracked`], but on the first `try_grow` refusal
/// it returns the collected prefix + the remaining stream (releasing the
/// reservation) instead of erroring. Lets a cache-fit hash join take the fast
/// in-memory/parallel probe when the probe fits the budget, and fall back to a
/// bounded streaming probe when it doesn't (e.g. q18's ~10 GB lineitem⋈orders
/// probe intermediate, which otherwise OOM-kills the worker — see the
/// 2026-06-08 heap-profile attribution).
///
/// `max_bytes` is an OPTIONAL dedicated cap on the collected prefix,
/// independent of the (typically large) pool budget. When `Some(m)`, a
/// probe whose collected size would exceed `m` overflows to the streaming
/// path even if the pool still has room. This bounds the no-spill probe
/// PEAK without shrinking the build's spill headroom — TPC-H Q08 otherwise
/// collected its whole ~3.4 GB lineitem-derived probe under a 5 GB pool
/// (heap-profile attribution 2026-06-09). `None` preserves the prior
/// pool-only behaviour.
pub(crate) async fn collect_probe_within_budget(
    mut stream: SendableRecordBatchStream,
    pool: Arc<dyn crate::memory_pool::MemoryPool>,
    consumer_name: &'static str,
    max_bytes: Option<usize>,
) -> Result<ProbeCollect, ExecutionError> {
    let consumer = crate::memory_pool::MemoryConsumer::new(consumer_name).with_can_spill(false);
    let mut reservation = consumer.register(pool);
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut collected: usize = 0;
    while let Some(batch_res) = stream.next().await {
        let batch = batch_res.map_err(|e| {
            ExecutionError::InvalidOperation(format!("{consumer_name}: stream error: {e}"))
        })?;
        let bytes = record_batch_bytes(&batch);
        // Overflow when EITHER the dedicated `max_bytes` cap would be
        // exceeded OR the pool refuses the growth. Either way: keep the
        // batch we just pulled, release the prefix's reservation (the
        // streaming probe consumes + drops the prefix incrementally), and
        // hand back prefix + remainder.
        let cap_exceeded = max_bytes.is_some_and(|m| collected.saturating_add(bytes) > m);
        if cap_exceeded || reservation.try_grow(bytes).is_err() {
            batches.push(batch);
            drop(reservation);
            return Ok(ProbeCollect::Overflow {
                prefix: batches,
                rest: stream,
            });
        }
        collected = collected.saturating_add(bytes);
        batches.push(batch);
    }
    Ok(ProbeCollect::Fits {
        batches,
        reservation,
    })
}

/// Build a [`SendableRecordBatchStream`] that yields `prefix` batches first,
/// then the batches from `rest`. Used to feed a streaming probe the
/// already-collected prefix in front of the un-consumed remainder (see
/// [`collect_probe_within_budget`]).
pub(crate) fn prepend_batches(
    schema: Arc<Schema>,
    prefix: Vec<RecordBatch>,
    mut rest: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let inner = async_stream::try_stream! {
        for batch in prefix {
            yield batch;
        }
        while let Some(batch_res) = futures::StreamExt::next(&mut rest).await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!("prepend_batches rest stream: {e}"))
            })?;
            yield batch;
        }
    };
    Box::pin(LimitOutputStream {
        schema,
        inner: Box::pin(inner),
    })
}

// ===========================================================================
// OneRowExec
// ===========================================================================

/// Emits exactly one empty (zero-column) row. Backs the synthetic FROM
/// source planner builds for `SELECT <expr>, ...` queries without a
/// FROM clause — e.g. `SELECT 1`, `pg_isready`-style health probes.
/// The surrounding `ProjectionExec` evaluates literal expressions
/// against this single-row batch to produce the actual output.
#[derive(Debug)]
pub(crate) struct OneRowExec;

#[async_trait]
impl ExecutionPlan for OneRowExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        Vec::new()
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Arrow's RecordBatch::try_new rejects zero-column batches
        // because it can't infer row count. Use try_new_with_options
        // to set row_count explicitly.
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
        let options = arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1));
        let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options)?;
        Ok(stream_from_batches(schema, vec![batch]))
    }

    fn display_name(&self) -> &str {
        "OneRowExec"
    }
}

// ===========================================================================
// ScanExec
// ===========================================================================

/// Reads all data from a [`DataSource`].
#[derive(Debug)]
pub(crate) struct ScanExec {
    pub(crate) source: Arc<dyn DataSource>,
    pub(crate) _table_name: String,
    pub(crate) scan_context: ScanContext,
    /// Runtime-injected filters from upstream HashJoinExec build phases
    /// (Trino-style dynamic filters). Accumulated via
    /// `ExecutionPlan::inject_dynamic_filter` AFTER the planner has
    /// constructed the tree but BEFORE the first `execute()` call.
    /// Merged with `scan_context.filters` at scan time so the
    /// connector sees a single combined filter list.
    pub(crate) dynamic_filters: std::sync::Mutex<Vec<PlanExpr>>,
    /// A1.4 (2026-05-27): cross-fragment dynamic filters this scan
    /// should await before reading. Each entry is one DF id that an
    /// upstream join's build phase populates on the coordinator
    /// (which then pushes the resolved Domain to this worker via the
    /// `notify_dynamic_filter` Flight action). Populated from
    /// `LogicalPlan::TableScan::dynamic_filters_consumed` at plan
    /// time. The wait is gated by `dynamic_filtering_enabled`; when
    /// off (default through A1.4–A1.5) the scan starts immediately
    /// and behaves exactly as pre-A1.
    pub(crate) dynamic_filters_consumed: Vec<arneb_planner::DynamicFilterConsumer>,
    /// A1.4: handle to the per-task DF storage. The scan calls
    /// `take_receiver(df_id)` for each consumed id and awaits with a
    /// timeout. `None` on coord / standalone / tests — scan skips the
    /// cross-fragment path regardless of the feature flag.
    pub(crate) dynamic_filter_collector: Option<DynamicFilterCollector>,
    /// A1.4: feature flag mirroring `ExecutionContext::dynamic_filtering_enabled`.
    /// `false` keeps the runtime identical to pre-A1.
    pub(crate) dynamic_filtering_enabled: bool,
    /// A1.4: per-DF wait timeout. Fallback on timeout is "no filter",
    /// so this is purely a performance cap.
    pub(crate) dynamic_filtering_wait_timeout: std::time::Duration,
    /// Multi-worker scan stride: this task is `scan_task_index` of
    /// `scan_task_count`. The task reads only the DataSource partitions
    /// `{index, index+count, index+2*count, …}` so M parallel scan tasks
    /// (one per worker) each read a disjoint 1/M of the table — no
    /// partition read twice or dropped. The default `(0, 1)` reads every
    /// partition (single-task scan, pre-multi-worker behavior).
    pub(crate) scan_task_index: usize,
    pub(crate) scan_task_count: usize,
}

/// Number of DataSource partitions assigned to scan task `task_index` of
/// `task_count` under the round-robin stride `{index, index+count, …}`.
/// `(0, 1)` (the default) yields all `n`. Local partition `p` maps to the
/// global DataSource partition `task_index + p * task_count`. The strides
/// partition `0..n` into `task_count` disjoint covers (the sum over all
/// task indices equals `n`), so no partition is read twice or dropped.
fn strided_partition_count(n: usize, task_index: usize, task_count: usize) -> usize {
    if task_count <= 1 {
        return n;
    }
    if task_index >= n {
        return 0;
    }
    (n - task_index).div_ceil(task_count)
}

impl ScanExec {
    /// A1.4 helper: gather any cross-fragment DF Domains that have
    /// arrived (or wait up to the configured timeout for them) and
    /// convert each into pushdown state ready to merge with
    /// `scan_context`.
    ///
    /// Returns an empty Vec when:
    /// - the feature flag is off,
    /// - no consumed DFs are declared on this scan,
    /// - or the collector is absent (coord / tests).
    ///
    /// On `recv` error or timeout for an individual DF, that DF is
    /// silently skipped — the scan proceeds with the static filters
    /// plus any other DFs that did arrive. This matches the
    /// soundness contract spelled out in design.md D4.
    pub(crate) async fn collect_cross_fragment_filters(&self) -> Vec<DynamicFilterDomain> {
        if !self.dynamic_filtering_enabled || self.dynamic_filters_consumed.is_empty() {
            return Vec::new();
        }
        let Some(collector) = &self.dynamic_filter_collector else {
            return Vec::new();
        };
        let timeout = self.dynamic_filtering_wait_timeout;
        let mut domains = Vec::with_capacity(self.dynamic_filters_consumed.len());
        for consumer in &self.dynamic_filters_consumed {
            eprintln!(
                "[DFRPC] scan subscribe df_id={} column_index={} column_name={}",
                consumer.id, consumer.column_index, consumer.column_name
            );
            let recv = collector.take_receiver(consumer.id).await;
            match tokio::time::timeout(timeout, recv).await {
                Ok(Ok(domain)) => {
                    let domain_variant = match &domain {
                        arneb_common::Domain::DistinctValues(values) => {
                            format!("DistinctValues(len={})", values.len())
                        }
                        arneb_common::Domain::Range { .. } => "Range".to_string(),
                        arneb_common::Domain::Bloom(_) => "Bloom".to_string(),
                        arneb_common::Domain::All => "All".to_string(),
                    };
                    eprintln!(
                        "[DFRPC] scan resolved subscribed_df_id={} domain={}",
                        consumer.id, domain_variant
                    );
                    let src_index = match &self.scan_context.projection {
                        Some(proj) => match proj.get(consumer.column_index) {
                            Some(&s) => s,
                            None => continue,
                        },
                        None => consumer.column_index,
                    };
                    let our_schema = self.source.schema();
                    if src_index >= our_schema.len() {
                        continue;
                    }
                    let column_name = our_schema
                        .get(src_index)
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| consumer.column_name.clone());
                    domains.push(DynamicFilterDomain {
                        column_index: src_index,
                        column_name,
                        domain,
                    });
                }
                Ok(Err(_)) => {
                    eprintln!("[DFRPC] scan recv_closed subscribed_df_id={}", consumer.id);
                    // Sender dropped before resolution because the
                    // query/worker-side collector went away. Fall
                    // through to static-filter-only behaviour.
                    tracing::debug!(
                        df_id = %consumer.id,
                        "dynamic filter sender dropped before resolution"
                    );
                }
                Err(_) => {
                    eprintln!(
                        "[DFRPC] scan timeout subscribed_df_id={} timeout_ms={}",
                        consumer.id,
                        timeout.as_millis()
                    );
                    // Timeout. Treat as "no filter" — the scan reads
                    // every row that passes static filters, which is
                    // a correct (just slower) result.
                    tracing::debug!(
                        df_id = %consumer.id,
                        timeout_ms = timeout.as_millis() as u64,
                        "dynamic filter wait timed out"
                    );
                }
            }
        }
        domains
    }
}

#[async_trait]
impl ExecutionPlan for ScanExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.source.schema()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        // A1.4: gather cross-fragment Domains first (potentially
        // awaiting on `oneshot::Receiver<Domain>`s with timeout).
        // No-op when the feature flag is off or no annotations exist,
        // so the pre-A1 fast path stays a single mutex lock + clone.
        let cross_fragment = self.collect_cross_fragment_filters().await;

        let ctx = {
            let dynamic = self.dynamic_filters.lock().unwrap();
            if dynamic.is_empty() && cross_fragment.is_empty() {
                self.scan_context.clone()
            } else {
                let mut merged = self.scan_context.clone();
                merged.filters.extend(dynamic.iter().cloned());
                for df in cross_fragment {
                    if let Some(expr) =
                        domain_to_filter_expr(&df.domain, df.column_index, &df.column_name)
                    {
                        merged.filters.push(expr);
                    }
                    merged.dynamic_filter_domains.push(df);
                }
                merged
            }
        };
        // Multi-worker scan stride: this task owns DataSource partitions
        // {index, index+count, …}; local output partition `p` maps to the
        // global partition `index + p*count`. Default (0, 1) → identity.
        let global_partition = self.scan_task_index + partition * self.scan_task_count;
        let stream = self.source.scan(&ctx, global_partition).await?;
        Ok(profile_stream(
            "ScanExec",
            partition,
            live_batch_stream("ScanExec.live", stream),
        ))
    }

    fn display_name(&self) -> &str {
        "ScanExec"
    }

    fn is_leaf_scan_subtree(&self) -> bool {
        true
    }

    fn output_partitioning(&self) -> crate::partitioning::Partitioning {
        // One output partition per source partition this task owns. With the
        // multi-worker stride (scan_task_count > 1) the task owns only its
        // strided 1/M of the source partitions; default (0, 1) owns all.
        // Downstream operators that need a single stream pull through
        // `CoalescePartitionsExec`, inserted by the physical planner.
        crate::partitioning::Partitioning::UnknownPartitioning(strided_partition_count(
            self.source.partition_count(),
            self.scan_task_index,
            self.scan_task_count,
        ))
    }

    fn inject_dynamic_filter(&self, filter: PlanExpr, target_index: usize) {
        // `target_index` is in this scan's OUTPUT (post-projection) space —
        // the schema the parent operators saw and the provenance descent
        // routed against. Map it back to the SOURCE-schema index via the
        // pushdown projection (identity when none), because the filter is
        // evaluated against the full source schema. Drop on out-of-bounds.
        let src_index = match &self.scan_context.projection {
            Some(proj) => match proj.get(target_index) {
                Some(&s) => s,
                None => return,
            },
            None => target_index,
        };
        let our_schema = self.source.schema();
        if src_index >= our_schema.len() {
            return;
        }
        let Some(rewritten) = rewrite_filter_at_index(&filter, src_index, &our_schema) else {
            return;
        };
        if let Ok(mut filters) = self.dynamic_filters.lock() {
            filters.push(rewritten);
        }
    }
}

/// Set the dynamic-filter column's index to `target_index` — the local scan
/// column the provenance descent resolved it to (NOT a name lookup). The list
/// elements are literals, so only the single `Column` is retargeted. A
/// `debug_assert` checks the resolved column's name matches the filter's name:
/// a mismatch means the descent misrouted (a real bug), and it fires loudly in
/// debug rather than silently pruning the wrong column. Returns `None` on an
/// unexpected filter shape (drop).
fn rewrite_filter_at_index(
    expr: &PlanExpr,
    target_index: usize,
    local_schema: &[ColumnInfo],
) -> Option<PlanExpr> {
    use PlanExpr as E;
    Some(match expr {
        E::Column { name, span, .. } => {
            debug_assert_eq!(
                local_schema.get(target_index).map(|c| c.name.as_str()),
                Some(name.as_str()),
                "dynamic-filter provenance descent misrouted: target index {target_index} \
                 resolves to {:?}, expected column {:?}",
                local_schema.get(target_index).map(|c| c.name.clone()),
                name
            );
            E::Column {
                index: target_index,
                name: name.clone(),
                span: *span,
            }
        }
        E::Literal { value, span } => E::Literal {
            value: value.clone(),
            span: *span,
        },
        E::InList {
            expr,
            list,
            negated,
            span,
        } => E::InList {
            expr: Box::new(rewrite_filter_at_index(expr, target_index, local_schema)?),
            list: list
                .iter()
                .map(|e| rewrite_filter_at_index(e, target_index, local_schema))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
            span: *span,
        },
        // Other variants shouldn't appear in dynamic filters; drop.
        _ => return None,
    })
}

/// Walk a `PlanExpr` collecting every `Column { name }` it references.
/// Used to decide whether a dynamic filter applies to a given scan's
/// schema. Returns a fresh Vec per call (the filter trees are small
/// — typically `col >= literal AND col <= literal` with one column).
///
/// Currently unused — paired with the dynamic-filter infrastructure
/// dormant pending the SortedRangeSet revisit.
#[allow(dead_code)]
fn filter_column_names(expr: &PlanExpr) -> Vec<String> {
    let mut out = Vec::new();
    fn walk(e: &PlanExpr, out: &mut Vec<String>) {
        match e {
            PlanExpr::Column { name, .. } => out.push(name.clone()),
            PlanExpr::BinaryOp { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            PlanExpr::UnaryOp { expr, .. } => walk(expr, out),
            PlanExpr::Cast { expr, .. } => walk(expr, out),
            PlanExpr::Between {
                expr, low, high, ..
            } => {
                walk(expr, out);
                walk(low, out);
                walk(high, out);
            }
            PlanExpr::InList { expr, list, .. } => {
                walk(expr, out);
                for e in list {
                    walk(e, out);
                }
            }
            PlanExpr::IsNull { expr, .. } | PlanExpr::IsNotNull { expr, .. } => walk(expr, out),
            _ => {}
        }
    }
    walk(expr, &mut out);
    out
}

// ===========================================================================
// ProjectionExec
// ===========================================================================

/// Evaluates expressions to produce new columns.
#[derive(Debug)]
pub(crate) struct ProjectionExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) exprs: Vec<PlanExpr>,
    pub(crate) output_schema: Vec<ColumnInfo>,
}

#[async_trait]
impl ExecutionPlan for ProjectionExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    fn output_partitioning(&self) -> crate::partitioning::Partitioning {
        // Stateless per-row pass-through: inherit the input's partitioning.
        self.input.output_partitioning()
    }

    fn inject_dynamic_filter(&self, filter: PlanExpr, target_index: usize) {
        // Map the output-column index back to the input column it projects.
        // Only a direct `Column` passthrough can carry the filter further; a
        // computed/renamed expression cannot be targeted, so drop.
        if let Some(PlanExpr::Column { index, .. }) = self.exprs.get(target_index) {
            self.input.inject_dynamic_filter(filter, *index);
        }
    }

    fn is_leaf_scan_subtree(&self) -> bool {
        self.input.is_leaf_scan_subtree()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        let input_stream = self.input.execute(partition).await?;
        let exprs = self.exprs.clone();
        let output_schema = self.output_schema.clone();
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&output_schema);

        let stream: SendableRecordBatchStream = Box::pin(MapStream::new(
            input_stream,
            arrow_schema.clone(),
            move |batch| {
                let columns: Vec<ArrayRef> = exprs
                    .iter()
                    .map(|e| expression::evaluate(e, &batch, None))
                    .collect::<Result<_, _>>()?;

                let columns = columns
                    .into_iter()
                    .zip(arrow_schema.fields())
                    .map(|(col, field)| {
                        if col.data_type() != field.data_type() {
                            compute::cast(&col, field.data_type()).map_err(ExecutionError::from)
                        } else {
                            Ok(col)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(RecordBatch::try_new(arrow_schema.clone(), columns)?)
            },
        ));
        Ok(profile_stream("ProjectionExec", partition, stream))
    }

    fn display_name(&self) -> &str {
        "ProjectionExec"
    }
}

// ===========================================================================
// FilterExec
// ===========================================================================

/// Filters rows by a boolean predicate.
#[derive(Debug)]
pub(crate) struct FilterExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) predicate: PlanExpr,
}

#[async_trait]
impl ExecutionPlan for FilterExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    fn output_partitioning(&self) -> crate::partitioning::Partitioning {
        // Stateless per-row pass-through: inherit input partitioning.
        self.input.output_partitioning()
    }

    fn inject_dynamic_filter(&self, filter: PlanExpr, target_index: usize) {
        // FilterExec is schema-preserving: same column layout, pass through.
        self.input.inject_dynamic_filter(filter, target_index);
    }

    fn is_leaf_scan_subtree(&self) -> bool {
        self.input.is_leaf_scan_subtree()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        let input_stream = self.input.execute(partition).await?;
        let predicate = self.predicate.clone();
        let schema = input_stream.schema();

        let stream: SendableRecordBatchStream =
            Box::pin(FilterMapStream::new(input_stream, schema, move |batch| {
                let mask_arr = expression::evaluate(&predicate, &batch, None)?;
                let mask = mask_arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(
                            "filter predicate must produce a boolean array".to_string(),
                        )
                    })?;

                let filtered = compute::filter_record_batch(&batch, mask)?;
                if filtered.num_rows() > 0 {
                    Ok(Some(filtered))
                } else {
                    Ok(None)
                }
            }));
        Ok(profile_stream("FilterExec", partition, stream))
    }

    fn display_name(&self) -> &str {
        "FilterExec"
    }
}

// ===========================================================================
// NestedLoopJoinExec
// ===========================================================================

/// Nested-loop join for all join types.
#[derive(Debug)]
pub(crate) struct NestedLoopJoinExec {
    pub(crate) left: Arc<dyn ExecutionPlan>,
    pub(crate) right: Arc<dyn ExecutionPlan>,
    pub(crate) join_type: ast::JoinType,
    pub(crate) condition: arneb_planner::JoinCondition,
}

#[async_trait]
impl ExecutionPlan for NestedLoopJoinExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let left_batches = self.collect_input(&self.left).await?;
        let right_batches = self.collect_input(&self.right).await?;

        let left_batch = match left_batches {
            Some(b) => b,
            None => {
                let schema = crate::datasource::column_info_to_arrow_schema(&self.schema());
                return Ok(stream_from_batches(schema, vec![]));
            }
        };
        let right_batch = match right_batches {
            Some(b) => b,
            None => {
                return match self.join_type {
                    ast::JoinType::Left | ast::JoinType::Full => {
                        let result =
                            self.left_unmatched_output(&left_batch, self.right.schema().len())?;
                        let schema = result.first().map(|b| b.schema()).unwrap_or_else(|| {
                            crate::datasource::column_info_to_arrow_schema(&self.schema())
                        });
                        Ok(stream_from_batches(schema, result))
                    }
                    _ => {
                        let schema = crate::datasource::column_info_to_arrow_schema(&self.schema());
                        Ok(stream_from_batches(schema, vec![]))
                    }
                };
            }
        };

        let result = self.execute_join(&left_batch, &right_batch)?;
        let schema = result
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| crate::datasource::column_info_to_arrow_schema(&self.schema()));
        Ok(stream_from_batches(schema, result))
    }

    fn display_name(&self) -> &str {
        "NestedLoopJoinExec"
    }
}

impl NestedLoopJoinExec {
    /// Collect all input batches into a single concatenated batch.
    /// Nested-loop join materializes both sides; v1 always reads
    /// partition 0 of each child. (Parallel nested-loop is out of scope.)
    async fn collect_input(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<RecordBatch>, ExecutionError> {
        let stream = plan.execute(0).await?;
        let batches = collect_stream(stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect input: {e}"))
        })?;
        if batches.is_empty() {
            return Ok(None);
        }
        let batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&batches[0].schema(), batches.iter())?
        };
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    fn execute_join(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let left_rows = left_batch.num_rows();
        let right_rows = right_batch.num_rows();
        let output_schema = self.build_output_schema(left_batch, right_batch);

        let mut left_matched = vec![false; left_rows];
        let mut right_matched = vec![false; right_rows];
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        for (l, l_matched) in left_matched.iter_mut().enumerate() {
            for (r, r_matched) in right_matched.iter_mut().enumerate() {
                let pass = self.eval_join_condition(l, r, left_batch, right_batch)?;
                if pass {
                    left_indices.push(l as u32);
                    right_indices.push(r as u32);
                    *l_matched = true;
                    *r_matched = true;
                }
            }
        }

        let mut result_columns = Vec::new();
        let left_idx_arr = UInt32Array::from(left_indices.clone());
        let right_idx_arr = UInt32Array::from(right_indices.clone());

        for col_i in 0..left_batch.num_columns() {
            let col = compute::take(left_batch.column(col_i), &left_idx_arr, None)?;
            result_columns.push(col);
        }
        for col_i in 0..right_batch.num_columns() {
            let col = compute::take(right_batch.column(col_i), &right_idx_arr, None)?;
            result_columns.push(col);
        }

        let mut all_batches = Vec::new();

        if !result_columns.is_empty() && !left_indices.is_empty() {
            all_batches.push(RecordBatch::try_new(output_schema.clone(), result_columns)?);
        }

        // Handle unmatched rows for outer joins.
        match self.join_type {
            ast::JoinType::Left | ast::JoinType::Full => {
                let unmatched: Vec<u32> = left_matched
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| !**m)
                    .map(|(i, _)| i as u32)
                    .collect();
                if !unmatched.is_empty() {
                    let idx = UInt32Array::from(unmatched);
                    let mut cols: Vec<ArrayRef> = Vec::new();
                    for col_i in 0..left_batch.num_columns() {
                        cols.push(compute::take(left_batch.column(col_i), &idx, None)?);
                    }
                    let null_len = idx.len();
                    for col_i in 0..right_batch.num_columns() {
                        cols.push(arrow::array::new_null_array(
                            right_batch.column(col_i).data_type(),
                            null_len,
                        ));
                    }
                    all_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
                }
            }
            _ => {}
        }

        match self.join_type {
            ast::JoinType::Right | ast::JoinType::Full => {
                let unmatched: Vec<u32> = right_matched
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| !**m)
                    .map(|(i, _)| i as u32)
                    .collect();
                if !unmatched.is_empty() {
                    let idx = UInt32Array::from(unmatched);
                    let null_len = idx.len();
                    let mut cols: Vec<ArrayRef> = Vec::new();
                    for col_i in 0..left_batch.num_columns() {
                        cols.push(arrow::array::new_null_array(
                            left_batch.column(col_i).data_type(),
                            null_len,
                        ));
                    }
                    for col_i in 0..right_batch.num_columns() {
                        cols.push(compute::take(right_batch.column(col_i), &idx, None)?);
                    }
                    all_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
                }
            }
            _ => {}
        }

        Ok(all_batches)
    }

    fn build_output_schema(&self, left: &RecordBatch, right: &RecordBatch) -> Arc<Schema> {
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| {
                if matches!(self.join_type, ast::JoinType::Right | ast::JoinType::Full) {
                    Field::new(f.name(), f.data_type().clone(), true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        fields.extend(right.schema().fields().iter().map(|f| {
            if matches!(self.join_type, ast::JoinType::Left | ast::JoinType::Full) {
                Field::new(f.name(), f.data_type().clone(), true)
            } else {
                f.as_ref().clone()
            }
        }));
        Arc::new(Schema::new(fields))
    }

    fn eval_join_condition(
        &self,
        left_row: usize,
        right_row: usize,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
    ) -> Result<bool, ExecutionError> {
        match &self.condition {
            arneb_planner::JoinCondition::None => Ok(true),
            arneb_planner::JoinCondition::On(expr) => {
                let combined =
                    self.build_combined_row(left_row, right_row, left_batch, right_batch)?;
                let result = expression::evaluate(expr, &combined, None)?;
                let bool_arr = result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(
                            "join condition must produce boolean".to_string(),
                        )
                    })?;
                Ok(bool_arr.value(0))
            }
        }
    }

    fn build_combined_row(
        &self,
        left_row: usize,
        right_row: usize,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
    ) -> Result<RecordBatch, ExecutionError> {
        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for (i, field) in left_batch.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(left_batch.column(i).slice(left_row, 1));
        }
        for (i, field) in right_batch.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(right_batch.column(i).slice(right_row, 1));
        }

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn left_unmatched_output(
        &self,
        left_batch: &RecordBatch,
        right_cols: usize,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let schema = self.schema();
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&schema);
        let mut cols: Vec<ArrayRef> = Vec::new();
        for i in 0..left_batch.num_columns() {
            cols.push(left_batch.column(i).clone());
        }
        for i in 0..right_cols {
            let dt: ArrowDataType = schema[left_batch.num_columns() + i]
                .data_type
                .clone()
                .into();
            cols.push(arrow::array::new_null_array(&dt, left_batch.num_rows()));
        }
        Ok(vec![RecordBatch::try_new(arrow_schema, cols)?])
    }
}

// ===========================================================================
// HashAggregateExec
// ===========================================================================

/// Hash-based grouping and aggregation.
#[derive(Debug)]
pub(crate) struct HashAggregateExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) group_by: Vec<PlanExpr>,
    pub(crate) aggr_exprs: Vec<PlanExpr>,
    pub(crate) output_schema: Vec<ColumnInfo>,
    pub(crate) output_order: Option<Vec<AggregateOutputColumn>>,
    /// Best-effort estimate of output groups. Used only by the
    /// default-off `ARNEB_AGG_PRESIZE` path to reserve hash/key storage.
    pub(crate) estimated_groups: Option<usize>,
    /// exec-memory-accounting D3: pool the GROUP BY group state is reserved
    /// against, so a large aggregate is visible to the global pool (and the
    /// D2 exchange spill can balance) rather than growing untracked into an
    /// OOM-kill. Defaults to `UnboundedMemoryPool` (single-node / tests = no
    /// tracking); the distributed worker installs the cgroup-derived pool via
    /// the physical planner's `ExecutionContext`.
    pub(crate) memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateOutputColumn {
    Group(usize),
    Aggregate(usize),
}

#[async_trait]
impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        // arneb::profile (2026-06-16): HashAggregateExec is BLOCKING — it
        // collects the full result Vec before emitting — so the output-stream
        // ProfileStream wrapper used by other operators captures ~0ms here
        // (compute already done). Time execute() directly + emit one
        // `arneb::profile` event so the agg's true compute cost (and, for the
        // FINAL merge of a high-cardinality GROUP BY, any single-threaded
        // merge bottleneck) is visible in the per-operator profile.
        let __t_agg = Instant::now();
        let input_partitioning = self.input.output_partitioning();
        let n = input_partitioning.partition_count();
        // DISTINCT aggregates stay on the legacy per-row `Accumulator`
        // path; non-DISTINCT use the batch-aware `GroupedAccumulator`
        // path that removes the per-row dyn dispatch and slice alloc.
        let use_batch = !self.has_distinct();
        let result = if n <= 1 {
            // Single-partition path: drain the input stream batch-by-batch
            // into the group hash table without collecting the full input
            // into RAM first. Phase 3b.5 (2026-05-21) fix for TPC-H Q09:
            // previously this collected the join's 3 M-row output into a
            // single Vec<RecordBatch> before aggregating, contributing the
            // 1.8 GB peak from audit Hotspot 4.
            let stream = self.input.execute(0).await?;
            if use_batch {
                self.execute_streaming_batch_aware(stream).await?
            } else {
                self.execute_streaming(stream).await?
            }
        } else if use_batch && self.input_hash_partitioned_on_group_by(&input_partitioning) {
            self.execute_hash_partitioned_batch_aware(n).await?
        } else if use_batch {
            self.execute_parallel_batch_aware(n).await?
        } else {
            self.execute_parallel(n).await?
        };
        let __agg_ms = __t_agg.elapsed().as_millis() as u64;
        let __agg_rows: u64 = result.iter().map(|b| b.num_rows() as u64).sum();
        tracing::info!(
            target: "arneb::profile",
            op = "HashAggregateExec",
            partition = _partition,
            elapsed_ms = __agg_ms,
            first_batch_ms = __agg_ms,
            rows = __agg_rows,
            n_input_partitions = n,
            "operator finished"
        );
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        Ok(stream_from_batches(arrow_schema, result))
    }

    fn display_name(&self) -> &str {
        "HashAggregateExec"
    }
}

#[derive(Debug)]
struct AggrInfo {
    name: String,
    args: Vec<PlanExpr>,
    is_count_star: bool,
    distinct: bool,
}

fn agg_spill_enabled() -> bool {
    #[cfg(test)]
    {
        let enabled = std::env::var("ARNEB_AGG_SPILL")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            enabled,
            "ARNEB_AGG_SPILL"
        );
        enabled
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_AGG_SPILL")
                .map(|v| {
                    matches!(
                        v.as_str(),
                        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                    )
                })
                .unwrap_or(false);
            tracing::info!(
                target: "arneb::config",
                enabled,
                "ARNEB_AGG_SPILL"
            );
            enabled
        })
    }
}

fn dict_probe_build_enabled() -> bool {
    #[cfg(test)]
    {
        match DICT_PROBE_BUILD_TEST_OVERRIDE.load(Ordering::SeqCst) {
            1 => return false,
            2 => return true,
            _ => {}
        }
        std::env::var("ARNEB_DICT_PROBE_BUILD")
            .ok()
            .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on" | "ON"))
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_DICT_PROBE_BUILD")
                .ok()
                .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on" | "ON"));
            tracing::info!(
                target: "arneb::profile",
                knob = "ARNEB_DICT_PROBE_BUILD",
                enabled,
                "HashAggregateExec dict probe-build gate"
            );
            enabled
        })
    }
}

#[cfg(test)]
static DICT_PROBE_BUILD_TEST_OVERRIDE: AtomicU8 = AtomicU8::new(0);

#[cfg(test)]
fn set_dict_probe_build_for_test(enabled: Option<bool>) {
    let value = match enabled {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    };
    DICT_PROBE_BUILD_TEST_OVERRIDE.store(value, Ordering::SeqCst);
}

// The group's accumulator vector. The group's `Vec<ScalarValue>` lives
// inside the `GroupKey` that maps to this state, so we don't duplicate
// it here.
type GroupState = Vec<Box<dyn Accumulator>>;

impl HashAggregateExec {
    fn input_hash_partitioned_on_group_by(
        &self,
        partitioning: &crate::partitioning::Partitioning,
    ) -> bool {
        matches!(
            partitioning,
            crate::partitioning::Partitioning::Hash(exprs, n)
                if *n > 1 && !self.group_by.is_empty() && exprs == &self.group_by
        )
    }

    /// Stream the input batch-by-batch into the group hash table.
    /// Replaces the pre-3b.5 collect-then-iterate path. Per-batch
    /// peak RAM = one input batch + groups state, instead of the
    /// full input × wide-columns retained for the duration of the
    /// aggregate.
    async fn execute_streaming(
        &self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let aggr_info: Vec<AggrInfo> = self
            .aggr_exprs
            .iter()
            .map(|e| match e {
                PlanExpr::Function {
                    name,
                    args,
                    distinct,
                    ..
                } => {
                    let is_count_star =
                        args.is_empty() || args.iter().any(|a| matches!(a, PlanExpr::Wildcard));
                    Ok(AggrInfo {
                        name: name.clone(),
                        args: args.clone(),
                        is_count_star,
                        distinct: *distinct,
                    })
                }
                other => Err(ExecutionError::InvalidOperation(format!(
                    "expected aggregate function, got {other:?}"
                ))),
            })
            .collect::<Result<_, _>>()?;

        if self.group_by.is_empty() {
            return self.execute_no_grouping_streaming(stream, &aggr_info).await;
        }

        let mut groups: FastHashMap<GroupKey, GroupState> = FastHashMap::default();

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate input stream: {e}"))
            })?;
            let group_cols: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|e| expression::evaluate(e, &batch, None).and_then(|c| group_key_array(&c)))
                .collect::<Result<_, _>>()?;

            let aggr_input_cols: Vec<ArrayRef> = aggr_info
                .iter()
                .map(|info| {
                    if info.is_count_star {
                        count_star_input_array(batch.column(0))
                    } else {
                        expression::evaluate(&info.args[0], &batch, None)
                            .and_then(|c| materialize_dictionary_array(&c))
                    }
                })
                .collect::<Result<_, _>>()?;

            for row in 0..batch.num_rows() {
                let group_values: Vec<ScalarValue> = group_cols
                    .iter()
                    .map(|col| extract_scalar(col, row))
                    .collect::<Result<_, _>>()?;
                let key = GroupKey(group_values);

                let accumulators = groups.entry(key).or_insert_with(|| {
                    aggr_info
                        .iter()
                        .map(|info| {
                            aggregate::create_accumulator(
                                &info.name,
                                info.is_count_star,
                                info.distinct,
                            )
                            .unwrap()
                        })
                        .collect::<Vec<_>>()
                });

                for (acc_i, acc) in accumulators.iter_mut().enumerate() {
                    let col = &aggr_input_cols[acc_i];
                    let slice = col.slice(row, 1);
                    acc.update_batch(&slice)?;
                }
            }
        }

        self.build_aggregate_output(groups)
    }

    /// Streaming no-grouping fold: drain the stream into per-aggregate
    /// accumulators without holding any prior batch in memory.
    async fn execute_no_grouping_streaming(
        &self,
        mut stream: SendableRecordBatchStream,
        aggr_info: &[AggrInfo],
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let mut accumulators: Vec<Box<dyn Accumulator>> = aggr_info
            .iter()
            .map(|info| {
                aggregate::create_accumulator(&info.name, info.is_count_star, info.distinct)
            })
            .collect::<Result<_, _>>()?;

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate input stream: {e}"))
            })?;
            for (i, info) in aggr_info.iter().enumerate() {
                let col = if info.is_count_star {
                    batch.column(0).clone()
                } else {
                    expression::evaluate(&info.args[0], &batch, None)?
                };
                accumulators[i].update_batch(&col)?;
            }
        }

        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        let columns: Vec<ArrayRef> = accumulators
            .iter()
            .map(|acc| {
                let val = acc.evaluate()?;
                expression::scalar_to_array(&val, 1)
            })
            .collect::<Result<_, _>>()?;

        Ok(vec![RecordBatch::try_new(arrow_schema, columns)?])
    }

    /// Multi-partition aggregate: spawn one tokio task per input partition,
    /// each builds its own partial `GroupKey → accumulators` map; then
    /// the calling task merges all partials sequentially via the
    /// [`Accumulator::merge`] hook and emits the result batch.
    async fn execute_parallel(&self, n: usize) -> Result<Vec<RecordBatch>, ExecutionError> {
        let aggr_info: Arc<Vec<AggrInfo>> = Arc::new(
            self.aggr_exprs
                .iter()
                .map(|e| match e {
                    PlanExpr::Function {
                        name,
                        args,
                        distinct,
                        ..
                    } => {
                        let is_count_star =
                            args.is_empty() || args.iter().any(|a| matches!(a, PlanExpr::Wildcard));
                        Ok(AggrInfo {
                            name: name.clone(),
                            args: args.clone(),
                            is_count_star,
                            distinct: *distinct,
                        })
                    }
                    other => Err(ExecutionError::InvalidOperation(format!(
                        "expected aggregate function, got {other:?}"
                    ))),
                })
                .collect::<Result<_, _>>()?,
        );

        let mut handles = Vec::with_capacity(n);
        for p in 0..n {
            let input = Arc::clone(&self.input);
            let group_by = self.group_by.clone();
            let aggr_info = Arc::clone(&aggr_info);
            handles.push(tokio::spawn(async move {
                build_partial_groups(input, p, group_by, aggr_info).await
            }));
        }

        // Wait for all partials and merge sequentially.
        let mut merged: FastHashMap<GroupKey, GroupState> = FastHashMap::default();
        let mut global_no_group: Option<GroupState> = None;
        let no_grouping = self.group_by.is_empty();

        for handle in handles {
            let partial = handle.await.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate partial task: {e}"))
            })??;
            if no_grouping {
                // Each partial returns a single GroupState under an empty key.
                let local_accs = partial.into_iter().next().map(|(_, v)| v);
                if let Some(local_accs) = local_accs {
                    if let Some(existing) = global_no_group.as_mut() {
                        for (i, acc) in local_accs.iter().enumerate() {
                            existing[i].merge(acc.as_ref())?;
                        }
                    } else {
                        global_no_group = Some(local_accs);
                    }
                }
            } else {
                for (key, local_accs) in partial {
                    if let Some(existing) = merged.get_mut(&key) {
                        for (i, acc) in local_accs.iter().enumerate() {
                            existing[i].merge(acc.as_ref())?;
                        }
                    } else {
                        merged.insert(key, local_accs);
                    }
                }
            }
        }

        if no_grouping {
            // Build the single-row output from `global_no_group`.
            let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
            let accumulators = global_no_group.unwrap_or_else(|| {
                aggr_info
                    .iter()
                    .map(|info| {
                        aggregate::create_accumulator(&info.name, info.is_count_star, info.distinct)
                            .unwrap()
                    })
                    .collect()
            });
            let columns: Vec<ArrayRef> = accumulators
                .iter()
                .map(|acc| {
                    let val = acc.evaluate()?;
                    expression::scalar_to_array(&val, 1)
                })
                .collect::<Result<_, _>>()?;
            return Ok(vec![RecordBatch::try_new(arrow_schema, columns)?]);
        }

        self.build_aggregate_output(merged)
    }

    /// True if any aggregate in this operator has DISTINCT semantics.
    /// Used to dispatch between the legacy per-row `Accumulator` path
    /// (DISTINCT) and the batch-aware `GroupedAccumulator` path.
    fn has_distinct(&self) -> bool {
        self.aggr_exprs
            .iter()
            .any(|e| matches!(e, PlanExpr::Function { distinct: true, .. }))
    }

    /// Batch-aware single-partition aggregate. Counterpart to
    /// [`Self::execute_streaming`], but does one
    /// `GroupByHash::get_group_ids` + one `add_input` per aggregate per
    /// batch instead of per-row dispatch. Streams the input.
    async fn execute_streaming_batch_aware(
        &self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let aggr_info: Vec<AggrInfo> = self
            .aggr_exprs
            .iter()
            .map(|e| match e {
                PlanExpr::Function {
                    name,
                    args,
                    distinct,
                    ..
                } => {
                    let is_count_star =
                        args.is_empty() || args.iter().any(|a| matches!(a, PlanExpr::Wildcard));
                    Ok(AggrInfo {
                        name: name.clone(),
                        args: args.clone(),
                        is_count_star,
                        distinct: *distinct,
                    })
                }
                other => Err(ExecutionError::InvalidOperation(format!(
                    "expected aggregate function, got {other:?}"
                ))),
            })
            .collect::<Result<_, _>>()?;

        if self.group_by.is_empty() {
            return self
                .execute_no_grouping_batch_aware_streaming(stream, &aggr_info)
                .await;
        }

        if agg_spill_enabled() {
            return self
                .execute_partitioned_spill_batch_aware(stream, &aggr_info)
                .await;
        }

        let mut gbh = GroupByHash::with_estimated_groups(self.estimated_groups);
        let mut accs: Vec<Box<dyn GroupedAccumulator>> = aggr_info
            .iter()
            .map(|info| {
                aggregate::create_grouped_accumulator(&info.name, info.is_count_star, info.distinct)
            })
            .collect::<Result<_, _>>()?;

        // exec-memory-accounting D3: reserve the growing group state against
        // the global pool. `try_resize` fails fast (ResourceExhausted) when
        // the pool refuses — far better than the worker OOM-kill seen on q18
        // SF30. With the default Unbounded pool this never fails (no behaviour
        // change). The reservation lives until the method returns (after
        // `build_output_batch_aware` consumes `gbh`/`accs`), releasing on drop.
        let consumer =
            crate::memory_pool::MemoryConsumer::new("HashAggregateExec").with_can_spill(false);
        let mut reservation = consumer.register(self.memory_pool.clone());

        let mut rows_seen = 0usize;
        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate input stream: {e}"))
            })?;
            let group_cols: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|e| expression::evaluate(e, &batch, None).and_then(|c| group_key_array(&c)))
                .collect::<Result<_, _>>()?;
            let aggr_input_cols: Vec<ArrayRef> = aggr_info
                .iter()
                .map(|info| {
                    if info.is_count_star {
                        count_star_input_array(batch.column(0))
                    } else {
                        expression::evaluate(&info.args[0], &batch, None)
                            .and_then(|c| materialize_dictionary_array(&c))
                    }
                })
                .collect::<Result<_, _>>()?;

            let group_ids = gbh.get_group_ids(&group_cols)?;
            rows_seen = rows_seen.saturating_add(batch.num_rows());
            gbh.adaptive_reserve_after_batch(rows_seen);
            let n_groups = gbh.num_groups();
            for (i, acc) in accs.iter_mut().enumerate() {
                acc.ensure_capacity(n_groups);
                acc.add_input(&group_ids, &aggr_input_cols[i])?;
            }
            // Group key/table bytes (exact-ish) + a coarse per-group
            // accumulator estimate (~16 B/group/aggregate).
            let state_bytes = gbh.heap_bytes() + n_groups * accs.len() * 16;
            reservation.try_resize(state_bytes)?;
        }

        self.build_output_batch_aware(&gbh, &accs)
    }

    async fn execute_partitioned_spill_batch_aware(
        &self,
        mut stream: SendableRecordBatchStream,
        aggr_info: &[AggrInfo],
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        const N_PARTITIONS: usize = 64;

        let input_schema = crate::datasource::column_info_to_arrow_schema(&self.input.schema());
        let mut writer =
            PartitionedSpillWriter::new(input_schema.clone(), N_PARTITIONS, "agg_grace_input");
        let mut input_rows = 0usize;
        let mut input_bytes = 0usize;

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate input stream: {e}"))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            input_rows += batch.num_rows();
            input_bytes += record_batch_bytes(&batch);

            let group_cols: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|e| expression::evaluate(e, &batch, None).and_then(|c| group_key_array(&c)))
                .collect::<Result<_, _>>()?;
            let assignments = group_partition_assignments(&group_cols, N_PARTITIONS)?;
            let mut buckets: Vec<Vec<u32>> = (0..N_PARTITIONS).map(|_| Vec::new()).collect();
            for (row, &partition) in assignments.iter().enumerate() {
                buckets[partition as usize].push(row as u32);
            }
            let schema = batch.schema();
            for (partition, indices) in buckets.into_iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let idx_arr = UInt32Array::from(indices);
                let cols: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|i| {
                        compute::take(batch.column(i), &idx_arr, None).map_err(ExecutionError::from)
                    })
                    .collect::<Result<_, _>>()?;
                let partition_batch = RecordBatch::try_new(schema.clone(), cols)?;
                writer.write_partition(partition, &partition_batch)?;
            }
        }

        let mut spill_file = writer.finish()?;
        tracing::info!(
            target: "arneb::mem",
            operator = "HashAggregateExec",
            n_partitions = N_PARTITIONS,
            input_rows,
            input_bytes,
            spilled_bytes = spill_file.total_bytes(),
            "partitioned-agg-spill: starting partition reload"
        );

        let mut output = Vec::new();
        for partition in 0..spill_file.n_partitions() {
            let Some(partition_file) = spill_file.take_partition(partition) else {
                continue;
            };

            let mut gbh = GroupByHash::with_estimated_groups(
                self.estimated_groups
                    .map(|groups| groups.div_ceil(N_PARTITIONS)),
            );
            let mut accs: Vec<Box<dyn GroupedAccumulator>> = aggr_info
                .iter()
                .map(|info| {
                    aggregate::create_grouped_accumulator(
                        &info.name,
                        info.is_count_star,
                        info.distinct,
                    )
                })
                .collect::<Result<_, _>>()?;
            let consumer = crate::memory_pool::MemoryConsumer::new(format!(
                "HashAggregateExec.spill.p{partition}"
            ))
            .with_can_spill(false);
            let mut reservation = consumer.register(self.memory_pool.clone());

            let reader = partition_file.open_reader()?;
            for batch_res in reader {
                let batch = batch_res?;
                let group_cols: Vec<ArrayRef> = self
                    .group_by
                    .iter()
                    .map(|e| {
                        expression::evaluate(e, &batch, None).and_then(|c| group_key_array(&c))
                    })
                    .collect::<Result<_, _>>()?;
                let aggr_input_cols: Vec<ArrayRef> = aggr_info
                    .iter()
                    .map(|info| {
                        if info.is_count_star {
                            count_star_input_array(batch.column(0))
                        } else {
                            expression::evaluate(&info.args[0], &batch, None)
                                .and_then(|c| materialize_dictionary_array(&c))
                        }
                    })
                    .collect::<Result<_, _>>()?;

                let group_ids = gbh.get_group_ids(&group_cols)?;
                let n_groups = gbh.num_groups();
                for (i, acc) in accs.iter_mut().enumerate() {
                    acc.ensure_capacity(n_groups);
                    acc.add_input(&group_ids, &aggr_input_cols[i])?;
                }
                let state_bytes = gbh.heap_bytes() + n_groups * accs.len() * 16;
                reservation.try_resize(state_bytes)?;
            }

            output.extend(self.build_output_batch_aware(&gbh, &accs)?);
        }

        Ok(output)
    }

    /// Streaming batch-aware no-grouping aggregate. All rows fold into group 0.
    async fn execute_no_grouping_batch_aware_streaming(
        &self,
        mut stream: SendableRecordBatchStream,
        aggr_info: &[AggrInfo],
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let mut accs: Vec<Box<dyn GroupedAccumulator>> = aggr_info
            .iter()
            .map(|info| {
                aggregate::create_grouped_accumulator(&info.name, info.is_count_star, info.distinct)
            })
            .collect::<Result<_, _>>()?;
        for acc in accs.iter_mut() {
            acc.ensure_capacity(1);
        }

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate input stream: {e}"))
            })?;
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let group_ids: Vec<u32> = vec![0u32; n];
            for (i, info) in aggr_info.iter().enumerate() {
                let col = if info.is_count_star {
                    batch.column(0).clone()
                } else {
                    expression::evaluate(&info.args[0], &batch, None)?
                };
                accs[i].add_input(&group_ids, &col)?;
            }
        }

        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        let columns: Vec<ArrayRef> = accs
            .iter()
            .map(|acc| {
                let val = acc.evaluate(0)?;
                expression::scalar_to_array(&val, 1)
            })
            .collect::<Result<_, _>>()?;
        Ok(vec![RecordBatch::try_new(arrow_schema, columns)?])
    }

    /// Build the output `RecordBatch` for the grouped batch-aware path.
    ///
    /// Group columns come from `GroupByHash::build_group_arrays`, which
    /// uses the Bigint fast path's flat `Vec<i64>` directly when the
    /// key is a single Int64 — no per-group `ScalarValue` materialise.
    fn build_output_batch_aware(
        &self,
        gbh: &GroupByHash,
        accs: &[Box<dyn GroupedAccumulator>],
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let n = gbh.num_groups();
        if n == 0 {
            return Ok(vec![]);
        }
        let num_aggr_cols = self.aggr_exprs.len();

        // Group key columns (one ArrayRef per group_by expression).
        let group_columns: Vec<ArrayRef> = gbh.build_group_arrays()?;

        // Aggregate result columns — still walk per group_id via the
        // accumulator's per-group `evaluate`; this path is unchanged.
        let mut aggr_values: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(n); num_aggr_cols];
        for g_usize in 0..n {
            let g = g_usize as u32;
            for (i, acc) in accs.iter().enumerate() {
                aggr_values[i].push(acc.evaluate(g)?);
            }
        }
        let aggr_columns: Vec<ArrayRef> = aggr_values
            .iter()
            .map(|col_vals| scalars_to_array(col_vals, n))
            .collect::<Result<_, _>>()?;
        let columns = self.order_grouped_output_columns(group_columns, aggr_columns)?;

        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        Ok(vec![RecordBatch::try_new(arrow_schema, columns)?])
    }

    /// Batch-aware multi-partition aggregate. Spawns per-partition
    /// partial builders, then merges via `GroupedAccumulator::merge_from`
    /// with a per-partial `group_remap`.
    async fn execute_parallel_batch_aware(
        &self,
        n: usize,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let aggr_info: Arc<Vec<AggrInfo>> = Arc::new(
            self.aggr_exprs
                .iter()
                .map(|e| match e {
                    PlanExpr::Function {
                        name,
                        args,
                        distinct,
                        ..
                    } => {
                        let is_count_star =
                            args.is_empty() || args.iter().any(|a| matches!(a, PlanExpr::Wildcard));
                        Ok(AggrInfo {
                            name: name.clone(),
                            args: args.clone(),
                            is_count_star,
                            distinct: *distinct,
                        })
                    }
                    other => Err(ExecutionError::InvalidOperation(format!(
                        "expected aggregate function, got {other:?}"
                    ))),
                })
                .collect::<Result<_, _>>()?,
        );

        let no_grouping = self.group_by.is_empty();

        let mut handles = Vec::with_capacity(n);
        for p in 0..n {
            let input = Arc::clone(&self.input);
            let group_by = self.group_by.clone();
            let aggr_info = Arc::clone(&aggr_info);
            let estimated_groups = partition_estimated_groups(self.estimated_groups, n);
            handles.push(tokio::spawn(async move {
                build_partial_groups_batch_aware(input, p, group_by, aggr_info, estimated_groups)
                    .await
            }));
        }

        if no_grouping {
            // Each partial returns an empty GroupByHash + accs sized 1.
            // Merge by collapsing each partial's group 0 into final's group 0.
            let mut final_accs: Vec<Box<dyn GroupedAccumulator>> = aggr_info
                .iter()
                .map(|info| {
                    aggregate::create_grouped_accumulator(
                        &info.name,
                        info.is_count_star,
                        info.distinct,
                    )
                })
                .collect::<Result<_, _>>()?;
            for acc in final_accs.iter_mut() {
                acc.ensure_capacity(1);
            }
            for h in handles {
                let (_partial_gbh, partial_accs) = h.await.map_err(|e| {
                    ExecutionError::InvalidOperation(format!("aggregate partial task: {e}"))
                })??;
                let remap = vec![0u32; 1];
                for (i, pa) in partial_accs.iter().enumerate() {
                    final_accs[i].merge_from(pa.as_ref(), &remap)?;
                }
            }
            let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
            let columns: Vec<ArrayRef> = final_accs
                .iter()
                .map(|acc| {
                    let val = acc.evaluate(0)?;
                    expression::scalar_to_array(&val, 1)
                })
                .collect::<Result<_, _>>()?;
            return Ok(vec![RecordBatch::try_new(arrow_schema, columns)?]);
        }

        // Grouped case: build global GroupByHash by inserting each
        // partial's keys, computing a per-partial remap, then call
        // merge_from once per partial per aggregate.
        let mut global_gbh = GroupByHash::with_estimated_groups(self.estimated_groups);
        let mut final_accs: Vec<Box<dyn GroupedAccumulator>> = aggr_info
            .iter()
            .map(|info| {
                aggregate::create_grouped_accumulator(&info.name, info.is_count_star, info.distinct)
            })
            .collect::<Result<_, _>>()?;

        // exec-memory-accounting D3: reserve the merged global group state
        // against the pool (the parallel path's 45 M-group accumulator is the
        // q18 SF30 OOM site). Fail-fast on pool exhaustion; Unbounded default
        // never fails. Reservation drops (releases) when the method returns.
        let consumer = crate::memory_pool::MemoryConsumer::new("HashAggregateExec.merge")
            .with_can_spill(false);
        let mut reservation = consumer.register(self.memory_pool.clone());

        let mut partials = Vec::with_capacity(handles.len());
        for h in handles {
            partials.push(h.await.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate partial task: {e}"))
            })??);
        }
        if agg_presize_adaptive_enabled() {
            let final_groups_upper_bound = partials
                .iter()
                .map(|(partial_gbh, _)| partial_gbh.num_groups())
                .sum();
            global_gbh.reserve_groups(final_groups_upper_bound);
        }

        for (partial_gbh, partial_accs) in partials {
            // Re-hash the partial's keys (typed Array columns) into the
            // global GroupByHash. This goes through the same Bigint
            // fast path the partial used, with zero `ScalarValue`
            // round-trip — replacing the old `insert_or_get(clone)`
            // loop that allocated a `GroupKey` per partial group.
            let partial_arrays = partial_gbh.build_group_arrays()?;
            let remap = global_gbh.get_group_ids(&partial_arrays)?;
            let n_global = global_gbh.num_groups();
            for (i, pa) in partial_accs.iter().enumerate() {
                final_accs[i].ensure_capacity(n_global);
                final_accs[i].merge_from(pa.as_ref(), &remap)?;
            }
            let state_bytes = global_gbh.heap_bytes() + n_global * final_accs.len() * 16;
            reservation.try_resize(state_bytes)?;
        }

        self.build_output_batch_aware(&global_gbh, &final_accs)
    }

    /// Hash-partitioned grouped input should colocate every equal group key in
    /// one input partition. Still merge the per-partition states by key before
    /// emitting: the partitioning metadata can be stale or optimistic at a
    /// fragment boundary, and finalizing partitions independently would produce
    /// duplicate group rows with partial aggregate values.
    async fn execute_hash_partitioned_batch_aware(
        &self,
        n: usize,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let aggr_info: Arc<Vec<AggrInfo>> = Arc::new(
            self.aggr_exprs
                .iter()
                .map(|e| match e {
                    PlanExpr::Function {
                        name,
                        args,
                        distinct,
                        ..
                    } => {
                        let is_count_star =
                            args.is_empty() || args.iter().any(|a| matches!(a, PlanExpr::Wildcard));
                        Ok(AggrInfo {
                            name: name.clone(),
                            args: args.clone(),
                            is_count_star,
                            distinct: *distinct,
                        })
                    }
                    other => Err(ExecutionError::InvalidOperation(format!(
                        "expected aggregate function, got {other:?}"
                    ))),
                })
                .collect::<Result<_, _>>()?,
        );

        let mut handles = Vec::with_capacity(n);
        for p in 0..n {
            let input = Arc::clone(&self.input);
            let group_by = self.group_by.clone();
            let aggr_info = Arc::clone(&aggr_info);
            let estimated_groups = partition_estimated_groups(self.estimated_groups, n);
            handles.push(tokio::spawn(async move {
                build_partial_groups_batch_aware(input, p, group_by, aggr_info, estimated_groups)
                    .await
            }));
        }

        let mut global_gbh = GroupByHash::with_estimated_groups(self.estimated_groups);
        let mut final_accs: Vec<Box<dyn GroupedAccumulator>> = aggr_info
            .iter()
            .map(|info| {
                aggregate::create_grouped_accumulator(&info.name, info.is_count_star, info.distinct)
            })
            .collect::<Result<_, _>>()?;

        let mut partials = Vec::with_capacity(handles.len());
        for h in handles {
            partials.push(h.await.map_err(|e| {
                ExecutionError::InvalidOperation(format!("aggregate partition task: {e}"))
            })??);
        }
        if agg_presize_adaptive_enabled() {
            let final_groups_upper_bound = partials
                .iter()
                .map(|(partial_gbh, _)| partial_gbh.num_groups())
                .sum();
            global_gbh.reserve_groups(final_groups_upper_bound);
        }

        for (partial_gbh, partial_accs) in partials {
            let partial_arrays = partial_gbh.build_group_arrays()?;
            let remap = global_gbh.get_group_ids(&partial_arrays)?;
            let n_global = global_gbh.num_groups();
            for (i, pa) in partial_accs.iter().enumerate() {
                final_accs[i].ensure_capacity(n_global);
                final_accs[i].merge_from(pa.as_ref(), &remap)?;
            }
        }
        self.build_output_batch_aware(&global_gbh, &final_accs)
    }

    fn build_aggregate_output(
        &self,
        groups: FastHashMap<GroupKey, GroupState>,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        if groups.is_empty() {
            return Ok(vec![]);
        }

        let num_groups = groups.len();
        let num_group_cols = self.group_by.len();
        let num_aggr_cols = self.aggr_exprs.len();

        let mut group_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_group_cols];
        let mut aggr_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_aggr_cols];

        for (key, accumulators) in groups {
            for (i, v) in key.0.into_iter().enumerate() {
                group_values[i].push(v);
            }
            for (i, acc) in accumulators.iter().enumerate() {
                aggr_values[i].push(acc.evaluate()?);
            }
        }

        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        let group_columns: Vec<ArrayRef> = group_values
            .iter()
            .map(|col_vals| scalars_to_array(col_vals, num_groups))
            .collect::<Result<_, _>>()?;
        let aggr_columns: Vec<ArrayRef> = aggr_values
            .iter()
            .map(|col_vals| scalars_to_array(col_vals, num_groups))
            .collect::<Result<_, _>>()?;
        let columns = self.order_grouped_output_columns(group_columns, aggr_columns)?;

        Ok(vec![RecordBatch::try_new(arrow_schema, columns)?])
    }

    fn order_grouped_output_columns(
        &self,
        group_columns: Vec<ArrayRef>,
        aggr_columns: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, ExecutionError> {
        let natural_order;
        let output_order = if let Some(output_order) = &self.output_order {
            output_order.as_slice()
        } else {
            natural_order = (0..group_columns.len())
                .map(AggregateOutputColumn::Group)
                .chain((0..aggr_columns.len()).map(AggregateOutputColumn::Aggregate))
                .collect::<Vec<_>>();
            natural_order.as_slice()
        };
        if output_order.len() != self.output_schema.len() {
            return Err(ExecutionError::InvalidOperation(format!(
                "aggregate output order has {} columns but output schema has {}",
                output_order.len(),
                self.output_schema.len()
            )));
        }

        let mut columns = Vec::with_capacity(output_order.len());
        for (out_idx, source) in output_order.iter().enumerate() {
            let col = match *source {
                AggregateOutputColumn::Group(group_idx) => group_columns.get(group_idx),
                AggregateOutputColumn::Aggregate(aggr_idx) => aggr_columns.get(aggr_idx),
            }
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(format!(
                    "aggregate output column {out_idx} references missing source {source:?}"
                ))
            })?;
            let expected_type: ArrowDataType = self.output_schema[out_idx].data_type.clone().into();
            if col.data_type() != &expected_type {
                return Err(ExecutionError::InvalidOperation(format!(
                    "aggregate output column {out_idx} has type {:?}, expected {:?}",
                    col.data_type(),
                    expected_type
                )));
            }
            columns.push(col.clone());
        }
        Ok(columns)
    }
}

fn partition_estimated_groups(estimated_groups: Option<usize>, partitions: usize) -> Option<usize> {
    estimated_groups
        .map(|groups| groups.saturating_add(partitions.saturating_sub(1)) / partitions.max(1))
}

/// Build a partial `GroupKey → accumulators` map by consuming a single
/// input partition. Free function (not a method) so it can be moved into
/// a spawned task without borrowing `&self`.
async fn build_partial_groups(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    group_by: Vec<PlanExpr>,
    aggr_info: Arc<Vec<AggrInfo>>,
) -> Result<FastHashMap<GroupKey, GroupState>, ExecutionError> {
    let mut stream = input.execute(partition).await?;

    let mut groups: FastHashMap<GroupKey, GroupState> = FastHashMap::default();
    let no_grouping = group_by.is_empty();

    if no_grouping {
        // Use an empty `GroupKey` as the single bucket.
        let bucket = groups.entry(GroupKey(Vec::new())).or_insert_with(|| {
            aggr_info
                .iter()
                .map(|info| {
                    aggregate::create_accumulator(&info.name, info.is_count_star, info.distinct)
                        .unwrap()
                })
                .collect::<Vec<_>>()
        });
        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "partial aggregate input stream (p={partition}): {e}"
                ))
            })?;
            for (i, info) in aggr_info.iter().enumerate() {
                let col = if info.is_count_star {
                    batch.column(0).clone()
                } else {
                    expression::evaluate(&info.args[0], &batch, None)?
                };
                bucket[i].update_batch(&col)?;
            }
        }
        return Ok(groups);
    }

    while let Some(batch_res) = stream.next().await {
        let batch = batch_res.map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "partial aggregate input stream (p={partition}): {e}"
            ))
        })?;
        let group_cols: Vec<ArrayRef> = group_by
            .iter()
            .map(|e| expression::evaluate(e, &batch, None).and_then(|c| group_key_array(&c)))
            .collect::<Result<_, _>>()?;

        let aggr_input_cols: Vec<ArrayRef> = aggr_info
            .iter()
            .map(|info| {
                if info.is_count_star {
                    count_star_input_array(batch.column(0))
                } else {
                    expression::evaluate(&info.args[0], &batch, None)
                        .and_then(|c| materialize_dictionary_array(&c))
                }
            })
            .collect::<Result<_, _>>()?;

        for row in 0..batch.num_rows() {
            let group_values: Vec<ScalarValue> = group_cols
                .iter()
                .map(|col| extract_scalar(col, row))
                .collect::<Result<_, _>>()?;
            let key = GroupKey(group_values);
            let accumulators = groups.entry(key).or_insert_with(|| {
                aggr_info
                    .iter()
                    .map(|info| {
                        aggregate::create_accumulator(&info.name, info.is_count_star, info.distinct)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            });
            for (acc_i, acc) in accumulators.iter_mut().enumerate() {
                let col = &aggr_input_cols[acc_i];
                let slice = col.slice(row, 1);
                acc.update_batch(&slice)?;
            }
        }
    }

    Ok(groups)
}

/// Batch-aware counterpart to [`build_partial_groups`]. Returns a
/// per-partition `(GroupByHash, Vec<Box<dyn GroupedAccumulator>>)` that
/// the merge step in `execute_parallel_batch_aware` collapses via
/// `insert_or_get` + `merge_from`.
async fn build_partial_groups_batch_aware(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    group_by: Vec<PlanExpr>,
    aggr_info: Arc<Vec<AggrInfo>>,
    estimated_groups: Option<usize>,
) -> Result<(GroupByHash, Vec<Box<dyn GroupedAccumulator>>), ExecutionError> {
    let mut stream = input.execute(partition).await?;

    let mut gbh = GroupByHash::with_estimated_groups(estimated_groups);
    let mut accs: Vec<Box<dyn GroupedAccumulator>> = aggr_info
        .iter()
        .map(|info| {
            aggregate::create_grouped_accumulator(&info.name, info.is_count_star, info.distinct)
        })
        .collect::<Result<_, _>>()?;

    let no_grouping = group_by.is_empty();

    if no_grouping {
        for acc in accs.iter_mut() {
            acc.ensure_capacity(1);
        }
        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "partial aggregate input stream (p={partition}): {e}"
                ))
            })?;
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let group_ids: Vec<u32> = vec![0u32; n];
            for (i, info) in aggr_info.iter().enumerate() {
                let col = if info.is_count_star {
                    batch.column(0).clone()
                } else {
                    expression::evaluate(&info.args[0], &batch, None)?
                };
                accs[i].add_input(&group_ids, &col)?;
            }
        }
        return Ok((gbh, accs));
    }

    let mut rows_seen = 0usize;
    while let Some(batch_res) = stream.next().await {
        let batch = batch_res.map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "partial aggregate input stream (p={partition}): {e}"
            ))
        })?;
        let group_cols: Vec<ArrayRef> = group_by
            .iter()
            .map(|e| expression::evaluate(e, &batch, None).and_then(|c| group_key_array(&c)))
            .collect::<Result<_, _>>()?;
        let aggr_input_cols: Vec<ArrayRef> = aggr_info
            .iter()
            .map(|info| {
                if info.is_count_star {
                    count_star_input_array(batch.column(0))
                } else {
                    expression::evaluate(&info.args[0], &batch, None)
                        .and_then(|c| materialize_dictionary_array(&c))
                }
            })
            .collect::<Result<_, _>>()?;
        let group_ids = gbh.get_group_ids(&group_cols)?;
        rows_seen = rows_seen.saturating_add(batch.num_rows());
        gbh.adaptive_reserve_after_batch(rows_seen);
        let n_groups = gbh.num_groups();
        for (i, acc) in accs.iter_mut().enumerate() {
            acc.ensure_capacity(n_groups);
            acc.add_input(&group_ids, &aggr_input_cols[i])?;
        }
    }

    Ok((gbh, accs))
}

// ===========================================================================
// SortExec
// ===========================================================================

/// In-memory sort operator.
#[derive(Debug)]
pub(crate) struct SortExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) order_by: Vec<SortExpr>,
    /// Pool the input collection reserves against. Phase M.3
    /// (2026-05-21): SortExec must collect-then-sort by nature
    /// (O(n log n) lexsort), but should fail fast on
    /// `ResourceExhausted` instead of OOM-killing the worker.
    /// Defaults to `UnboundedMemoryPool` so unit-test construction
    /// via struct literal stays a no-op.
    #[allow(dead_code)]
    pub(crate) memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
}

#[async_trait]
impl ExecutionPlan for SortExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        let t_total = Instant::now();
        let stream = self.input.execute(partition).await?;
        let t_collect = Instant::now();
        let (batches, _reservation) =
            collect_stream_pool_tracked(stream, Arc::clone(&self.memory_pool), "SortExec.input")
                .await?;
        let collect_ms = t_collect.elapsed().as_millis() as u64;
        let n_batches = batches.len();

        if batches.is_empty() {
            let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.input.schema());
            tracing::info!(
                target: "arneb::profile",
                op = "SortExec.phases",
                partition,
                collect_ms,
                concat_ms = 0u64,
                sort_ms = 0u64,
                take_ms = 0u64,
                rows = 0u64,
                batches = 0u64,
                "SortExec phases (empty)"
            );
            return Ok(profile_stream(
                "SortExec",
                partition,
                stream_from_batches(arrow_schema, vec![]),
            ));
        }

        let schema = batches[0].schema();
        let t_concat = Instant::now();
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&schema, batches.iter())?
        };
        let concat_ms = t_concat.elapsed().as_millis() as u64;
        let n_rows = combined.num_rows();

        if combined.num_rows() == 0 {
            tracing::info!(
                target: "arneb::profile",
                op = "SortExec.phases",
                partition,
                collect_ms,
                concat_ms,
                sort_ms = 0u64,
                take_ms = 0u64,
                rows = 0u64,
                batches = n_batches as u64,
                "SortExec phases (zero rows)"
            );
            return Ok(profile_stream(
                "SortExec",
                partition,
                stream_from_batches(schema, vec![combined]),
            ));
        }

        let t_sort = Instant::now();
        let sort_columns: Vec<arrow::compute::SortColumn> = self
            .order_by
            .iter()
            .map(|s| {
                let col = expression::evaluate(&s.expr, &combined, None)
                    .and_then(|c| materialize_dictionary_array(&c))?;
                Ok(arrow::compute::SortColumn {
                    values: col,
                    options: Some(arrow::compute::SortOptions {
                        descending: !s.asc,
                        nulls_first: s.nulls_first,
                    }),
                })
            })
            .collect::<Result<_, ExecutionError>>()?;

        let indices = compute::lexsort_to_indices(&sort_columns, None)?;
        let sort_ms = t_sort.elapsed().as_millis() as u64;

        let t_take = Instant::now();
        let sorted_columns: Vec<ArrayRef> = (0..combined.num_columns())
            .map(|i| compute::take(combined.column(i), &indices, None).map_err(Into::into))
            .collect::<Result<_, ExecutionError>>()?;
        let take_ms = t_take.elapsed().as_millis() as u64;

        let result = RecordBatch::try_new(schema.clone(), sorted_columns)?;
        tracing::info!(
            target: "arneb::profile",
            op = "SortExec.phases",
            partition,
            total_ms = t_total.elapsed().as_millis() as u64,
            collect_ms,
            concat_ms,
            sort_ms,
            take_ms,
            rows = n_rows as u64,
            batches = n_batches as u64,
            "SortExec phases"
        );
        Ok(profile_stream(
            "SortExec",
            partition,
            stream_from_batches(schema, vec![result]),
        ))
    }

    fn display_name(&self) -> &str {
        "SortExec"
    }
}

// ===========================================================================
// LimitExec
// ===========================================================================

/// Applies LIMIT and OFFSET to the input.
#[derive(Debug)]
pub(crate) struct LimitExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

#[async_trait]
impl ExecutionPlan for LimitExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Phase 3b.7c (2026-05-21): short-circuit streaming. Walk input
        // batches incrementally, advance past OFFSET rows, emit slices
        // covering up to LIMIT rows, then stop pulling. Previously this
        // collected the entire input + concat'd into one batch before
        // slicing — wasting ~640 MB for SF1-scale inputs on a 10-row
        // LIMIT query.
        let offset = self.offset.unwrap_or(0);
        let limit = self.limit;
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.input.schema());
        let mut input = self.input.execute(partition).await?;

        let stream = async_stream::try_stream! {
            let mut skipped: usize = 0;
            let mut emitted: usize = 0;
            while let Some(batch_res) = futures::StreamExt::next(&mut input).await {
                let batch = batch_res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!("limit input stream: {e}"))
                })?;
                let rows = batch.num_rows();
                if rows == 0 {
                    continue;
                }
                // Skip whole batches that fall entirely before OFFSET.
                if skipped + rows <= offset {
                    skipped += rows;
                    continue;
                }
                // Partial-skip within this batch if OFFSET lands mid-batch.
                let local_start = offset.saturating_sub(skipped);
                skipped += local_start;
                let available = rows - local_start;
                let take = match limit {
                    Some(lim) => available.min(lim - emitted),
                    None => available,
                };
                if take > 0 {
                    let sliced = batch.slice(local_start, take);
                    emitted += take;
                    yield sliced;
                }
                if let Some(lim) = limit {
                    if emitted >= lim {
                        break;
                    }
                }
            }
        };

        let out: SendableRecordBatchStream = Box::pin(LimitOutputStream {
            schema: arrow_schema,
            inner: Box::pin(stream),
        });
        Ok(profile_stream("LimitExec", partition, out))
    }

    fn display_name(&self) -> &str {
        "LimitExec"
    }
}

/// `RecordBatchStream` wrapper for `LimitExec`'s short-circuit stream.
struct LimitOutputStream {
    schema: Arc<Schema>,
    inner: Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, ExecutionError>> + Send>>,
}

impl futures::Stream for LimitOutputStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(arneb_common::error::ArnebError::Execution)))
    }
}

impl RecordBatchStream for LimitOutputStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// ===========================================================================
// TopKExec
// ===========================================================================

/// Heap-style ORDER BY ... LIMIT k. Avoids the full O(n log n) sort
/// that `SortExec` + `LimitExec` would perform — `select_nth_unstable`
/// partitions the rows into "top k" and "rest" in O(n), then sorts
/// only those k rows in O(k log k).
///
/// Comparison goes through Arrow's `RowConverter`, which encodes each
/// row's sort columns into a packed byte slice that compares
/// lexicographically with per-column asc/desc + nulls-first/last
/// honoured.
///
/// Planner emits this in place of `LimitExec(SortExec)` whenever
/// `limit.is_some()` and `offset.is_none()`. Falls back to a full sort
/// when `n_rows <= fetch` (heap optimisation has no benefit).
#[derive(Debug)]
pub(crate) struct TopKExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) order_by: Vec<SortExpr>,
    /// Number of rows to keep. Always > 0.
    pub(crate) fetch: usize,
    /// See [`SortExec::memory_pool`]. Phase M.3 (2026-05-21).
    #[allow(dead_code)]
    pub(crate) memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
}

#[async_trait]
impl ExecutionPlan for TopKExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        use arrow::row::{RowConverter, SortField};

        let t_total = Instant::now();
        let stream = self.input.execute(partition).await?;
        let t_collect = Instant::now();
        let (batches, _reservation) =
            collect_stream_pool_tracked(stream, Arc::clone(&self.memory_pool), "TopKExec.input")
                .await?;
        let collect_ms = t_collect.elapsed().as_millis() as u64;

        if batches.is_empty() {
            let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.input.schema());
            return Ok(profile_stream(
                "TopKExec",
                partition,
                stream_from_batches(arrow_schema, vec![]),
            ));
        }

        let schema = batches[0].schema();
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&schema, batches.iter())?
        };
        let n_rows = combined.num_rows();
        if n_rows == 0 {
            return Ok(profile_stream(
                "TopKExec",
                partition,
                stream_from_batches(schema, vec![combined]),
            ));
        }
        tracing::debug!(
            target: "arneb::topk",
            fetch = self.fetch,
            n_rows,
            order_cols = self.order_by.len(),
            "TopKExec materialising",
        );

        // Evaluate sort expressions once over the concatenated input.
        let sort_arrays: Vec<ArrayRef> = self
            .order_by
            .iter()
            .map(|s| {
                expression::evaluate(&s.expr, &combined, None)
                    .and_then(|c| materialize_dictionary_array(&c))
            })
            .collect::<Result<_, ExecutionError>>()?;

        // RowConverter packs each row's sort columns into a single
        // byte slice that compares lexicographically — much faster
        // than per-column scalar dispatch in a tight inner loop.
        let fields: Vec<SortField> = self
            .order_by
            .iter()
            .zip(sort_arrays.iter())
            .map(|(s, a)| {
                SortField::new_with_options(
                    a.data_type().clone(),
                    arrow::compute::SortOptions {
                        descending: !s.asc,
                        nulls_first: s.nulls_first,
                    },
                )
            })
            .collect();
        let converter = RowConverter::new(fields).map_err(|e| {
            ExecutionError::InvalidOperation(format!("TopKExec RowConverter init: {e}"))
        })?;
        let rows = converter.convert_columns(&sort_arrays).map_err(|e| {
            ExecutionError::InvalidOperation(format!("TopKExec convert_columns: {e}"))
        })?;

        let k = self.fetch.min(n_rows);

        // Build the index buffer (one u32 per input row). Partial
        // sort: `select_nth_unstable` places the k-th smallest at
        // position k-1, with all "<= it" before and all "> it" after,
        // in O(n) expected time. Then we sort just the k-element
        // prefix.
        let mut indices: Vec<u32> = (0..n_rows as u32).collect();
        if k < n_rows {
            indices.select_nth_unstable_by(k - 1, |&a, &b| {
                rows.row(a as usize).cmp(&rows.row(b as usize))
            });
        }
        indices[..k].sort_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));

        let take_indices = UInt32Array::from(indices[..k].to_vec());
        let cols: Vec<ArrayRef> = (0..combined.num_columns())
            .map(|i| compute::take(combined.column(i), &take_indices, None).map_err(Into::into))
            .collect::<Result<_, ExecutionError>>()?;

        let result = RecordBatch::try_new(schema.clone(), cols)?;
        tracing::info!(
            target: "arneb::profile",
            op = "TopKExec.phases",
            partition,
            total_ms = t_total.elapsed().as_millis() as u64,
            collect_ms,
            n_rows = n_rows as u64,
            k = k as u64,
            "TopKExec phases"
        );
        Ok(profile_stream(
            "TopKExec",
            partition,
            stream_from_batches(schema, vec![result]),
        ))
    }

    fn display_name(&self) -> &str {
        "TopKExec"
    }
}

// ===========================================================================
// ExplainExec
// ===========================================================================

/// Produces the textual plan description as a single-column Utf8 batch.
///
/// In the default `EXPLAIN` form, only the static logical plan is
/// rendered. In `EXPLAIN ANALYZE` form (`analyze_inner = Some(_)`),
/// the inner physical plan is executed, the root output stream is
/// drained to count actual rows + measure wall time, and the plan
/// text is prefixed with an `Actual rows / wall-ms` summary line.
/// Inner per-operator stats can be enabled with the `--profile` CLI
/// flag (or `RUST_LOG=arneb::profile=info`) for now — embedding them
/// in the EXPLAIN output is a follow-up.
#[derive(Debug)]
pub(crate) struct ExplainExec {
    pub(crate) plan: LogicalPlan,
    /// `Some(physical)` when this came from `EXPLAIN ANALYZE`. The
    /// inner plan is executed once at `execute()` time and the root
    /// stream's row count is folded into the rendered output.
    pub(crate) analyze_inner: Option<Arc<dyn ExecutionPlan>>,
}

#[async_trait]
impl ExecutionPlan for ExplainExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            name: "plan".to_string(),
            data_type: arneb_common::types::DataType::Utf8,
            nullable: false,
        }]
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let mut output = String::new();
        if let Some(inner) = &self.analyze_inner {
            let started = Instant::now();
            let mut stream = inner.execute(0).await?;
            let mut rows: u64 = 0;
            let mut batches: u64 = 0;
            while let Some(b) = stream.next().await {
                let batch = b.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "EXPLAIN ANALYZE: inner stream error: {e}"
                    ))
                })?;
                rows = rows.saturating_add(batch.num_rows() as u64);
                batches = batches.saturating_add(1);
            }
            let elapsed_ms = started.elapsed().as_millis();
            output.push_str(&format!(
                "Actual: rows={rows}, batches={batches}, wall_ms={elapsed_ms}\n"
            ));
            output.push_str("---\n");
        }
        output.push_str(&format!("{}", self.plan));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "plan",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![output.as_str()]))],
        )?;
        Ok(stream_from_batches(schema, vec![batch]))
    }

    fn display_name(&self) -> &str {
        "ExplainExec"
    }
}

// ===========================================================================
// StreamingHashAggregateExec
// ===========================================================================

/// Fold-style aggregate for plans whose input is already partitioned by
/// one of the group-by columns. Runs of equal-key rows are contiguous
/// in the input stream, so we maintain ONE active group at a time and
/// flush it whenever the key changes — no hash table, no row-hashing,
/// O(group_by × aggregate_count) memory.
///
/// Mirrors Trino's `StreamingAggregationOperator`. Activated by the
/// physical planner when an `Aggregate` node's `group_by` contains a
/// column produced by an `AssignUniqueIdExec` somewhere in its input
/// subtree — the unique-id column is monotone non-decreasing within
/// each input partition (and equal-key rows are always contiguous,
/// even after hash-repartition), so streaming aggregation is sound.
///
/// Trade-off vs `HashAggregateExec`: zero hash overhead for the common
/// case where one group-by column is provably unique per source row
/// (LeftJoin × AssignUniqueId pattern from F-Perf11c). Used by Q21
/// after `CorrelatedExistsToLeftJoin` rewrites the SemiJoin{residual}
/// into a LEFT JOIN + Aggregate(BOOL_OR) form.
#[derive(Debug)]
pub(crate) struct StreamingHashAggregateExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) group_by: Vec<PlanExpr>,
    pub(crate) aggr_exprs: Vec<PlanExpr>,
    pub(crate) output_schema: Vec<ColumnInfo>,
    /// Index into `group_by` of the column that's provably unique per
    /// source row (typically the `__semi_rowid_N` column from
    /// `AssignUniqueIdExec`).
    pub(crate) unique_key_idx: usize,
    /// See [`SortExec::memory_pool`]. Phase M.3 (2026-05-21).
    #[allow(dead_code)]
    pub(crate) memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
}

#[async_trait]
impl ExecutionPlan for StreamingHashAggregateExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    fn output_partitioning(&self) -> crate::partitioning::Partitioning {
        crate::partitioning::Partitioning::UnknownPartitioning(1)
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        use arrow::array::Int64Array;
        use arrow::compute::take;

        let t_total = Instant::now();
        let stream = self.input.execute(0).await?;
        let t_collect = Instant::now();
        let (batches, _reservation) = collect_stream_pool_tracked(
            stream,
            Arc::clone(&self.memory_pool),
            "StreamingHashAggregateExec.input",
        )
        .await?;
        let collect_ms = t_collect.elapsed().as_millis() as u64;
        let n_input_batches = batches.len() as u64;
        let n_input_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

        let aggr_info: Vec<AggrInfo> = self
            .aggr_exprs
            .iter()
            .map(|e| match e {
                PlanExpr::Function {
                    name,
                    args,
                    distinct,
                    ..
                } => {
                    let is_count_star =
                        args.is_empty() || args.iter().any(|a| matches!(a, PlanExpr::Wildcard));
                    Ok(AggrInfo {
                        name: name.clone(),
                        args: args.clone(),
                        is_count_star,
                        distinct: *distinct,
                    })
                }
                other => Err(ExecutionError::InvalidOperation(format!(
                    "expected aggregate function, got {other:?}"
                ))),
            })
            .collect::<Result<_, _>>()?;

        let num_group_cols = self.group_by.len();
        let num_aggr_cols = self.aggr_exprs.len();

        let mut accs: Vec<Box<dyn Accumulator>> = aggr_info
            .iter()
            .map(|info| {
                aggregate::create_accumulator(&info.name, info.is_count_star, info.distinct)
            })
            .collect::<Result<_, _>>()?;

        // For each completed group we record the (batch_idx, row_idx) of
        // its FIRST row — at the end, we use Arrow's `take` kernel to
        // materialise group_by output columns in bulk. This is the
        // critical perf win vs per-cell `extract_scalar`: 35 Utf8
        // `to_string()` allocations per group → 0.
        let mut group_first_rows_per_batch: Vec<Vec<u32>> = vec![Vec::new(); batches.len()];
        // Per-batch evaluated group_by + aggr-input columns, indexed by
        // batch_idx. We keep the group_by ArrayRefs so the final `take`
        // can run against them (the `Aggregate`'s `group_by` may be
        // computed expressions, not just direct Column refs).
        let mut group_cols_per_batch: Vec<Vec<ArrayRef>> = Vec::with_capacity(batches.len());

        // Per-aggregate output scalars (one per completed group). Aggr
        // outputs are typically tiny (Int64 / Float64 / Boolean) so the
        // ScalarValue path is fine here.
        let mut aggr_out: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_aggr_cols];

        // Open-group state — (batch_idx, row_idx) of the open group's
        // first row, plus the key value for run detection.
        let mut open_group: Option<(usize, u32)> = None;
        let mut open_key: Option<i64> = None;

        for (batch_idx, batch) in batches.iter().enumerate() {
            if batch.num_rows() == 0 {
                group_cols_per_batch.push(Vec::new());
                continue;
            }
            let group_cols: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|e| expression::evaluate(e, batch, None).and_then(|c| group_key_array(&c)))
                .collect::<Result<_, _>>()?;

            let aggr_input_cols: Vec<ArrayRef> = aggr_info
                .iter()
                .map(|info| {
                    if info.is_count_star {
                        count_star_input_array(batch.column(0))
                    } else {
                        expression::evaluate(&info.args[0], batch, None)
                            .and_then(|c| materialize_dictionary_array(&c))
                    }
                })
                .collect::<Result<_, _>>()?;

            // Typed access to the unique-key column. We require it to
            // be Int64 — `AssignUniqueIdExec` always emits Int64.
            let key_arr = group_cols[self.unique_key_idx]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation(
                        "StreamingHashAggregateExec: unique key must be Int64".to_string(),
                    )
                })?;

            let n = batch.num_rows();
            let mut run_start = 0usize;
            while run_start < n {
                let key_at_start = key_arr.value(run_start);

                let new_group = match open_key {
                    Some(k) => k != key_at_start,
                    None => true,
                };

                if new_group && open_group.is_some() {
                    // Flush previous group's accumulators.
                    for (i, acc) in accs.iter_mut().enumerate() {
                        aggr_out[i].push(acc.evaluate()?);
                        acc.reset();
                    }
                }

                if new_group {
                    open_group = Some((batch_idx, run_start as u32));
                    open_key = Some(key_at_start);
                    group_first_rows_per_batch[batch_idx].push(run_start as u32);
                }

                // Find the run end (where the key changes within this batch).
                let mut run_end = run_start + 1;
                while run_end < n && key_arr.value(run_end) == key_at_start {
                    run_end += 1;
                }

                // Feed accumulators with the run's slice.
                let run_len = run_end - run_start;
                for (i, acc) in accs.iter_mut().enumerate() {
                    let slice = aggr_input_cols[i].slice(run_start, run_len);
                    acc.update_batch(&slice)?;
                }

                run_start = run_end;
            }

            group_cols_per_batch.push(group_cols);
        }

        // Flush the final open group, if any.
        if open_group.is_some() {
            for (i, acc) in accs.iter_mut().enumerate() {
                aggr_out[i].push(acc.evaluate()?);
            }
        }

        // Materialise group_by output columns using Arrow's `take`
        // kernel: for each input batch, take the rows we recorded as
        // group boundaries; then concat across batches.
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        let num_groups: usize = group_first_rows_per_batch.iter().map(|v| v.len()).sum();

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_group_cols + num_aggr_cols);

        // `gc_idx` indexes both the per-batch column slice and the output schema;
        // `enumerate` on either side doesn't simplify, so we drive by index here.
        #[allow(clippy::needless_range_loop)]
        for gc_idx in 0..num_group_cols {
            let mut per_batch_taken: Vec<ArrayRef> = Vec::with_capacity(batches.len());
            for (batch_idx, indices) in group_first_rows_per_batch.iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let idx_arr = arrow::array::UInt32Array::from(indices.clone());
                let col = &group_cols_per_batch[batch_idx][gc_idx];
                let taken = take(col.as_ref(), &idx_arr, None)?;
                per_batch_taken.push(taken);
            }
            // concat all per-batch taken arrays into one.
            let col = if per_batch_taken.is_empty() {
                arrow::array::new_empty_array(arrow_schema.field(gc_idx).data_type())
            } else if per_batch_taken.len() == 1 {
                per_batch_taken.into_iter().next().unwrap()
            } else {
                let refs: Vec<&dyn arrow::array::Array> =
                    per_batch_taken.iter().map(|a| a.as_ref()).collect();
                arrow::compute::concat(&refs)?
            };
            columns.push(col);
        }

        // Aggregate outputs are small per-group ScalarValues — convert
        // via `scalars_to_array`.
        for col_vals in &aggr_out {
            columns.push(scalars_to_array(col_vals, num_groups)?);
        }

        let out_batch = if num_groups == 0 {
            let empty_cols: Vec<ArrayRef> = arrow_schema
                .fields()
                .iter()
                .map(|f| arrow::array::new_empty_array(f.data_type()))
                .collect();
            RecordBatch::try_new(arrow_schema.clone(), empty_cols)?
        } else {
            RecordBatch::try_new(arrow_schema.clone(), columns)?
        };

        tracing::info!(
            target: "arneb::profile",
            op = "StreamingHashAggregateExec.phases",
            total_ms = t_total.elapsed().as_millis() as u64,
            collect_ms,
            input_batches = n_input_batches,
            input_rows = n_input_rows,
            n_groups = num_groups as u64,
            "StreamingHashAggregateExec phases"
        );
        Ok(profile_stream(
            "StreamingHashAggregateExec",
            0,
            stream_from_batches(arrow_schema, vec![out_batch]),
        ))
    }

    fn display_name(&self) -> &str {
        "StreamingHashAggregateExec"
    }
}

// ===========================================================================
// AssignUniqueIdExec
// ===========================================================================

/// Appends a monotonically increasing Int64 column to each batch. The
/// counter is shared across all partitions via `Arc<AtomicI64>`, so even
/// when the input is multi-partitioned every emitted row gets a globally
/// unique id (starting at 0, monotonically per call to `execute`).
///
/// Trino's `AssignUniqueIdOperator` encodes `(stageId<<54 |
/// partitionId<<40 | rowId)` to avoid contention across drivers. For a
/// single-coordinator arneb we use a plain `AtomicI64` — Relaxed loads
/// on the hot path keep the per-batch overhead negligible (the alloc of
/// the new Int64 array dominates).
#[derive(Debug)]
pub(crate) struct AssignUniqueIdExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) id_column: String,
    pub(crate) counter: Arc<std::sync::atomic::AtomicI64>,
}

impl AssignUniqueIdExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, id_column: String) -> Self {
        Self {
            input,
            id_column,
            counter: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        }
    }
}

#[async_trait]
impl ExecutionPlan for AssignUniqueIdExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut s = self.input.schema();
        s.push(ColumnInfo {
            name: self.id_column.clone(),
            data_type: arneb_common::types::DataType::Int64,
            nullable: false,
        });
        s
    }

    fn output_partitioning(&self) -> crate::partitioning::Partitioning {
        // Stateless append; inherit partitioning.
        self.input.output_partitioning()
    }

    fn inject_dynamic_filter(&self, filter: PlanExpr, target_index: usize) {
        // Output = input columns followed by the appended unique-id column.
        // A dynamic-filter target is always an input column; pass it through
        // unchanged (the appended id column at `input.width` is never targeted).
        if target_index < self.input.schema().len() {
            self.input.inject_dynamic_filter(filter, target_index);
        }
    }

    fn is_leaf_scan_subtree(&self) -> bool {
        false
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        use arrow::array::Int64Array;
        use std::sync::atomic::Ordering;

        let input_stream = self.input.execute(partition).await?;
        let input_schema = input_stream.schema();
        let id_name = self.id_column.clone();
        let counter = self.counter.clone();

        // Build the output Arrow schema once (input cols + id col).
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        fields.push(Field::new(&id_name, ArrowDataType::Int64, false));
        let out_schema = Arc::new(Schema::new(fields));
        let out_schema_for_stream = out_schema.clone();

        Ok(Box::pin(FilterMapStream::new(
            input_stream,
            out_schema_for_stream,
            move |batch| {
                let n = batch.num_rows();
                if n == 0 {
                    // Preserve empty batch but with the appended column.
                    let empty_ids = Int64Array::from(Vec::<i64>::new());
                    let mut cols: Vec<ArrayRef> = (0..batch.num_columns())
                        .map(|i| batch.column(i).clone())
                        .collect();
                    cols.push(Arc::new(empty_ids));
                    let out = RecordBatch::try_new(out_schema.clone(), cols)?;
                    return Ok(Some(out));
                }
                // Reserve a contiguous [start, start+n) range. Relaxed
                // is fine here — we don't synchronize with anything
                // beyond "different batches get different ranges".
                let start = counter.fetch_add(n as i64, Ordering::Relaxed);
                let ids: Vec<i64> = (start..start + n as i64).collect();
                let id_arr = Int64Array::from(ids);
                let mut cols: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|i| batch.column(i).clone())
                    .collect();
                cols.push(Arc::new(id_arr));
                let out = RecordBatch::try_new(out_schema.clone(), cols)?;
                Ok(Some(out))
            },
        )))
    }

    fn display_name(&self) -> &str {
        "AssignUniqueIdExec"
    }
}

// ===========================================================================
// Stream adapters
// ===========================================================================

/// A stream that applies a mapping function to each batch from an input stream.
struct MapStream<F> {
    input: SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
    map_fn: F,
}

impl<F> MapStream<F>
where
    F: FnMut(RecordBatch) -> Result<RecordBatch, ExecutionError> + Send + Unpin,
{
    fn new(
        input: SendableRecordBatchStream,
        schema: arrow::datatypes::SchemaRef,
        map_fn: F,
    ) -> Self {
        Self {
            input,
            schema,
            map_fn,
        }
    }
}

impl<F> Stream for MapStream<F>
where
    F: FnMut(RecordBatch) -> Result<RecordBatch, ExecutionError> + Send + Unpin,
{
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = (self.map_fn)(batch).map_err(arneb_common::error::ArnebError::from);
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> RecordBatchStream for MapStream<F>
where
    F: FnMut(RecordBatch) -> Result<RecordBatch, ExecutionError> + Send + Unpin,
{
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// A stream that applies a filter-map function (returning `Option<RecordBatch>`)
/// to each input batch, skipping `None` results.
struct FilterMapStream<F> {
    input: SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
    map_fn: F,
}

impl<F> FilterMapStream<F>
where
    F: FnMut(RecordBatch) -> Result<Option<RecordBatch>, ExecutionError> + Send + Unpin,
{
    fn new(
        input: SendableRecordBatchStream,
        schema: arrow::datatypes::SchemaRef,
        map_fn: F,
    ) -> Self {
        Self {
            input,
            schema,
            map_fn,
        }
    }
}

impl<F> Stream for FilterMapStream<F>
where
    F: FnMut(RecordBatch) -> Result<Option<RecordBatch>, ExecutionError> + Send + Unpin,
{
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.input).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    match (self.map_fn)(batch) {
                        Ok(Some(result)) => {
                            return Poll::Ready(Some(Ok(result)));
                        }
                        Ok(None) => {
                            // Skip this batch, try next
                            continue;
                        }
                        Err(e) => {
                            return Poll::Ready(Some(Err(arneb_common::error::ArnebError::from(
                                e,
                            ))));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<F> RecordBatchStream for FilterMapStream<F>
where
    F: FnMut(RecordBatch) -> Result<Option<RecordBatch>, ExecutionError> + Send + Unpin,
{
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

pub(crate) fn extract_scalar(arr: &ArrayRef, index: usize) -> Result<ScalarValue, ExecutionError> {
    if matches!(arr.data_type(), ArrowDataType::Dictionary(_, _)) {
        let materialized = materialize_dictionary_array(arr)?;
        return extract_scalar(&materialized, index);
    }

    if arr.is_null(index) {
        return Ok(ScalarValue::Null);
    }
    match arr.data_type() {
        ArrowDataType::Int32 => {
            let a = arr.as_primitive::<datatypes::Int32Type>();
            Ok(ScalarValue::Int32(a.value(index)))
        }
        ArrowDataType::Int64 => {
            let a = arr.as_primitive::<datatypes::Int64Type>();
            Ok(ScalarValue::Int64(a.value(index)))
        }
        ArrowDataType::Float32 => {
            let a = arr.as_primitive::<datatypes::Float32Type>();
            Ok(ScalarValue::Float32(a.value(index)))
        }
        ArrowDataType::Float64 => {
            let a = arr.as_primitive::<datatypes::Float64Type>();
            Ok(ScalarValue::Float64(a.value(index)))
        }
        ArrowDataType::Utf8 => {
            let a = arr.as_string::<i32>();
            Ok(ScalarValue::Utf8(a.value(index).to_string()))
        }
        ArrowDataType::Boolean => {
            let a = arr.as_boolean();
            Ok(ScalarValue::Boolean(a.value(index)))
        }
        ArrowDataType::Date32 => {
            let a = arr.as_primitive::<datatypes::Date32Type>();
            Ok(ScalarValue::Date32(a.value(index)))
        }
        dt => Err(ExecutionError::InvalidOperation(format!(
            "cannot extract scalar from type {dt:?}"
        ))),
    }
}

pub(crate) fn materialize_dictionary_array(arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
    match arr.data_type() {
        ArrowDataType::Dictionary(_, value_type) => {
            compute::cast(arr, value_type.as_ref()).map_err(ExecutionError::from)
        }
        _ => Ok(arr.clone()),
    }
}

fn group_key_array(arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
    if !dict_probe_build_enabled() {
        return materialize_dictionary_array(arr);
    }
    dictionary_values_for_group_key(arr)
}

fn count_star_input_array(arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
    if dict_probe_build_enabled() {
        Ok(arr.clone())
    } else {
        materialize_dictionary_array(arr)
    }
}

fn dictionary_values_for_group_key(arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
    match arr.data_type() {
        ArrowDataType::Dictionary(key_type, _) if key_type.as_ref() == &ArrowDataType::UInt32 => {
            let dict = arr
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation(
                        "dictionary group key had UInt32 type but failed downcast".to_string(),
                    )
                })?;
            compute::take(dict.values().as_ref(), dict.keys(), None).map_err(ExecutionError::from)
        }
        ArrowDataType::Dictionary(_, _) => materialize_dictionary_array(arr),
        _ => Ok(arr.clone()),
    }
}

pub(crate) fn scalars_to_array(
    values: &[ScalarValue],
    _len: usize,
) -> Result<ArrayRef, ExecutionError> {
    if values.is_empty() {
        return Ok(Arc::new(array::NullArray::new(0)));
    }

    let first_type = values.iter().find(|v| !matches!(v, ScalarValue::Null));
    match first_type {
        Some(ScalarValue::Int32(_)) => {
            let arr: Int32Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Int32(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Int64(_)) => {
            let arr: Int64Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Int64(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Float32(_)) => {
            let arr: Float32Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Float32(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Float64(_)) => {
            let arr: Float64Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Float64(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Utf8(_)) => {
            let arr: StringArray = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Utf8(s) => Some(s.as_str()),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Boolean(_)) => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Boolean(b) => Some(*b),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Date32(_)) => {
            let arr: Date32Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Date32(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        _ => Ok(Arc::new(array::NullArray::new(values.len()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::DataType;

    #[test]
    fn strided_partition_count_covers_without_overlap() {
        // Default (0, 1) reads every partition.
        assert_eq!(strided_partition_count(4, 0, 1), 4);
        // 2 tasks over 4 partitions: each owns 2, disjoint {0,2} / {1,3}.
        assert_eq!(strided_partition_count(4, 0, 2), 2);
        assert_eq!(strided_partition_count(4, 1, 2), 2);
        // 2 tasks over 5: 3 ({0,2,4}) + 2 ({1,3}).
        assert_eq!(strided_partition_count(5, 0, 2), 3);
        assert_eq!(strided_partition_count(5, 1, 2), 2);
        // A task index past the partition count owns nothing.
        assert_eq!(strided_partition_count(2, 3, 4), 0);
        // Coverage invariant: across all task indices, every partition is
        // owned exactly once (sum == n) — no row dropped or double-read.
        for (n, m) in [(10usize, 3usize), (7, 4), (8, 8), (3, 5), (0, 2)] {
            let total: usize = (0..m).map(|t| strided_partition_count(n, t, m)).sum();
            assert_eq!(total, n, "stride coverage failed for n={n} m={m}");
        }
    }

    fn make_test_source() -> Arc<dyn DataSource> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        Arc::new(InMemoryDataSource::new(
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
                ColumnInfo {
                    name: "value".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        ))
    }

    #[test]
    fn extract_scalar_supports_date32() {
        // TPC-H Q3 groups by a date column; before this fix the aggregate
        // operator failed with "cannot extract scalar from type Date32".
        let arr: ArrayRef = Arc::new(Date32Array::from(vec![Some(19000), None, Some(19500)]));
        assert_eq!(extract_scalar(&arr, 0).unwrap(), ScalarValue::Date32(19000));
        assert_eq!(extract_scalar(&arr, 1).unwrap(), ScalarValue::Null);
        assert_eq!(extract_scalar(&arr, 2).unwrap(), ScalarValue::Date32(19500));
    }

    #[test]
    fn scalars_to_array_supports_date32() {
        let values = vec![
            ScalarValue::Date32(19000),
            ScalarValue::Null,
            ScalarValue::Date32(19500),
        ];
        let arr = scalars_to_array(&values, 3).unwrap();
        let date_arr = arr.as_primitive::<datatypes::Date32Type>();
        assert_eq!(date_arr.value(0), 19000);
        assert!(date_arr.is_null(1));
        assert_eq!(date_arr.value(2), 19500);
    }

    #[tokio::test]
    async fn scan_exec() {
        let source = make_test_source();
        let scan = ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        };
        let stream = scan.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn filter_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let filter = FilterExec {
            input: scan,
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                    span: None,
                }),
                op: ast::BinaryOp::Gt,
                right: Box::new(PlanExpr::Literal {
                    value: ScalarValue::Int32(1),
                    span: None,
                }),
                span: None,
            },
        };
        let stream = filter.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn projection_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let proj = ProjectionExec {
            input: scan,
            exprs: vec![PlanExpr::Column {
                index: 1,
                name: "name".to_string(),
                span: None,
            }],
            output_schema: vec![ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            }],
        };
        let stream = proj.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[tokio::test]
    async fn assign_unique_id_appends_int64_column() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let assign = AssignUniqueIdExec::new(scan, "__rowid".to_string());

        // Schema appends one column.
        let schema = assign.schema();
        assert_eq!(schema.len(), 4);
        assert_eq!(schema[3].name, "__rowid");
        assert_eq!(schema[3].data_type, DataType::Int64);
        assert!(!schema[3].nullable);

        let stream = assign.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 4);
        assert_eq!(batches[0].num_rows(), 3);

        let ids = batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 0);
        assert_eq!(ids.value(1), 1);
        assert_eq!(ids.value(2), 2);
    }

    #[tokio::test]
    async fn assign_unique_id_counter_unique_across_executes() {
        // Two consecutive execute() calls must yield disjoint id ranges
        // since the counter is shared across partitions/calls.
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let assign = AssignUniqueIdExec::new(scan, "id".to_string());

        let s1 = assign.execute(0).await.unwrap();
        let b1 = collect_stream(s1).await.unwrap();
        let s2 = assign.execute(0).await.unwrap();
        let b2 = collect_stream(s2).await.unwrap();

        let ids1 = b1[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids2 = b2[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids1.value(0), 0);
        assert_eq!(ids2.value(0), 3);
        assert_eq!(ids2.value(2), 5);
    }

    #[tokio::test]
    async fn limit_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let limit = LimitExec {
            input: scan,
            limit: Some(2),
            offset: None,
        };
        let stream = limit.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn limit_with_offset() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let limit = LimitExec {
            input: scan,
            limit: Some(1),
            offset: Some(1),
        };
        let stream = limit.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
    }

    #[tokio::test]
    async fn sort_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let sort = SortExec {
            input: scan,
            order_by: vec![SortExpr {
                expr: PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                    span: None,
                },
                asc: false,
                nulls_first: false,
            }],
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
        };
        let stream = sort.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 1);
    }

    #[tokio::test]
    async fn aggregate_no_grouping() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let agg = HashAggregateExec {
            input: scan,
            group_by: vec![],
            aggr_exprs: vec![
                PlanExpr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                    distinct: false,
                    span: None,
                },
                PlanExpr::Function {
                    name: "SUM".to_string(),
                    args: vec![PlanExpr::Column {
                        index: 2,
                        name: "value".to_string(),
                        span: None,
                    }],
                    distinct: false,
                    span: None,
                },
            ],
            output_schema: vec![
                ColumnInfo {
                    name: "count".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "sum".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            output_order: None,
            estimated_groups: None,
            memory_pool: std::sync::Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
        };
        let stream = agg.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 3);
        let sum = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum.value(0), 600);
    }

    #[tokio::test]
    async fn partial_sum_sum_final_matches_single_phase_sum_sum() {
        let input_columns = vec![
            ColumnInfo {
                name: "o_year".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "case_volume".to_string(),
                data_type: DataType::Float64,
                nullable: false,
            },
            ColumnInfo {
                name: "volume".to_string(),
                data_type: DataType::Float64,
                nullable: false,
            },
        ];
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("o_year", ArrowDataType::Int32, false),
            Field::new("case_volume", ArrowDataType::Float64, false),
            Field::new("volume", ArrowDataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![
                    1993, 1994, 1993, 1995, 1994, 1993, 1995, 1994,
                ])),
                Arc::new(Float64Array::from(vec![
                    10.0, 0.0, 2.5, 7.0, 1.0, 0.0, 3.0, 4.0,
                ])),
                Arc::new(Float64Array::from(vec![
                    10.0, 20.0, 5.0, 7.0, 2.0, 8.0, 3.0, 4.0,
                ])),
            ],
        )
        .unwrap();
        let partial_schema = vec![
            input_columns[0].clone(),
            ColumnInfo {
                name: "partial_num".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
            ColumnInfo {
                name: "partial_den".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ];
        let group_by = vec![PlanExpr::Column {
            index: 0,
            name: "o_year".to_string(),
            span: None,
        }];
        let partial_aggr_exprs = vec![
            PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "case_volume".to_string(),
                    span: None,
                }],
                distinct: false,
                span: None,
            },
            PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![PlanExpr::Column {
                    index: 2,
                    name: "volume".to_string(),
                    span: None,
                }],
                distinct: false,
                span: None,
            },
        ];
        let final_aggr_exprs = vec![
            PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "partial_num".to_string(),
                    span: None,
                }],
                distinct: false,
                span: None,
            },
            PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![PlanExpr::Column {
                    index: 2,
                    name: "partial_den".to_string(),
                    span: None,
                }],
                distinct: false,
                span: None,
            },
        ];

        fn scan_for(columns: Vec<ColumnInfo>, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
            Arc::new(ScanExec {
                source: Arc::new(InMemoryDataSource::new(columns, batches)),
                _table_name: "q08_agg_test".to_string(),
                scan_context: ScanContext::default(),
                dynamic_filters: Default::default(),
                dynamic_filters_consumed: Vec::new(),
                dynamic_filter_collector: None,
                dynamic_filtering_enabled: false,
                dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
                scan_task_index: 0,
                scan_task_count: 1,
            })
        }

        let make_agg = |input: Arc<dyn ExecutionPlan>,
                        aggr_exprs: Vec<PlanExpr>,
                        output_schema: Vec<ColumnInfo>| {
            HashAggregateExec {
                input,
                group_by: group_by.clone(),
                aggr_exprs,
                output_schema,
                output_order: None,
                estimated_groups: None,
                memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            }
        };

        let single_stream = make_agg(
            scan_for(input_columns.clone(), vec![batch.clone()]),
            partial_aggr_exprs.clone(),
            partial_schema.clone(),
        )
        .execute(0)
        .await
        .unwrap();
        let single = collect_stream(single_stream).await.unwrap();

        let left_partial_stream = make_agg(
            scan_for(input_columns.clone(), vec![batch.slice(0, 4)]),
            partial_aggr_exprs.clone(),
            partial_schema.clone(),
        )
        .execute(0)
        .await
        .unwrap();
        let mut partials = collect_stream(left_partial_stream).await.unwrap();
        let right_partial_stream = make_agg(
            scan_for(input_columns, vec![batch.slice(4, 4)]),
            partial_aggr_exprs,
            partial_schema.clone(),
        )
        .execute(0)
        .await
        .unwrap();
        partials.extend(collect_stream(right_partial_stream).await.unwrap());

        let final_stream = make_agg(
            scan_for(partial_schema.clone(), partials),
            final_aggr_exprs,
            partial_schema,
        )
        .execute(0)
        .await
        .unwrap();
        let final_batches = collect_stream(final_stream).await.unwrap();

        fn to_map(batches: &[RecordBatch]) -> std::collections::BTreeMap<i32, (f64, f64)> {
            let mut out = std::collections::BTreeMap::new();
            for batch in batches {
                let years = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let nums = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let dens = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    out.insert(years.value(row), (nums.value(row), dens.value(row)));
                }
            }
            out
        }

        let single = to_map(&single);
        let final_map = to_map(&final_batches);
        assert_eq!(single.len(), final_map.len());
        for (year, (single_num, single_den)) in single {
            let (final_num, final_den) = final_map.get(&year).copied().unwrap();
            assert!((single_num - final_num).abs() <= 1e-9);
            assert!((single_den - final_den).abs() <= 1e-9);
        }
    }

    #[tokio::test]
    async fn aggregate_dictionary_group_key_canonicalizes_values_across_batches() {
        fn scan_for(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
            Arc::new(ScanExec {
                source: Arc::new(InMemoryDataSource::new(
                    vec![ColumnInfo {
                        name: "s_name".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                    }],
                    batches,
                )),
                _table_name: "supplier_names".to_string(),
                scan_context: ScanContext::default(),
                dynamic_filters: Default::default(),
                dynamic_filters_consumed: Vec::new(),
                dynamic_filter_collector: None,
                dynamic_filtering_enabled: false,
                dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
                scan_task_index: 0,
                scan_task_count: 1,
            })
        }

        fn make_agg(input: Arc<dyn ExecutionPlan>) -> HashAggregateExec {
            HashAggregateExec {
                input,
                group_by: vec![PlanExpr::Column {
                    index: 0,
                    name: "s_name".to_string(),
                    span: None,
                }],
                aggr_exprs: vec![PlanExpr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                    distinct: false,
                    span: None,
                }],
                output_schema: vec![
                    ColumnInfo {
                        name: "s_name".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                    },
                    ColumnInfo {
                        name: "cnt".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                ],
                output_order: None,
                estimated_groups: None,
                memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            }
        }

        fn result_map(batches: &[RecordBatch]) -> std::collections::BTreeMap<String, i64> {
            let mut out = std::collections::BTreeMap::new();
            for batch in batches {
                let names = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    out.insert(names.value(row).to_string(), counts.value(row));
                }
            }
            out
        }

        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "s_name",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt32),
                Box::new(ArrowDataType::Utf8),
            ),
            false,
        )]));
        let dict_batch_1 = RecordBatch::try_new(
            dict_schema.clone(),
            vec![Arc::new(
                DictionaryArray::<UInt32Type>::try_new(
                    UInt32Array::from(vec![0, 1, 0]),
                    Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
                )
                .unwrap(),
            )],
        )
        .unwrap();
        let dict_batch_2 = RecordBatch::try_new(
            dict_schema,
            vec![Arc::new(
                DictionaryArray::<UInt32Type>::try_new(
                    UInt32Array::from(vec![0, 1, 0]),
                    Arc::new(StringArray::from(vec!["Bob", "Carol"])) as ArrayRef,
                )
                .unwrap(),
            )],
        )
        .unwrap();

        let plain_schema = Arc::new(Schema::new(vec![Field::new(
            "s_name",
            ArrowDataType::Utf8,
            false,
        )]));
        let plain_batch_1 = RecordBatch::try_new(
            plain_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["Alice", "Bob", "Alice"]))],
        )
        .unwrap();
        let plain_batch_2 = RecordBatch::try_new(
            plain_schema,
            vec![Arc::new(StringArray::from(vec!["Bob", "Carol", "Bob"]))],
        )
        .unwrap();

        set_dict_probe_build_for_test(Some(true));
        let dict_stream = make_agg(scan_for(vec![dict_batch_1, dict_batch_2]))
            .execute(0)
            .await
            .unwrap();
        let dict_result = collect_stream(dict_stream).await.unwrap();
        set_dict_probe_build_for_test(None);

        let plain_stream = make_agg(scan_for(vec![plain_batch_1, plain_batch_2]))
            .execute(0)
            .await
            .unwrap();
        let plain_result = collect_stream(plain_stream).await.unwrap();

        assert_eq!(result_map(&dict_result), result_map(&plain_result));
        assert_eq!(
            result_map(&dict_result),
            std::collections::BTreeMap::from([
                ("Alice".to_string(), 2),
                ("Bob".to_string(), 3),
                ("Carol".to_string(), 1),
            ])
        );
    }

    #[tokio::test]
    async fn hash_partitioned_grouped_final_matches_single_gather() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", ArrowDataType::Int32, false),
            Field::new("partial_v", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![
                    0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 100, 101, 102, 103,
                ])),
                Arc::new(Int64Array::from(vec![
                    1, 10, 100, 1000, 7, 8, 2, 20, 200, 2000, 70, 80, 5, 6, 7, 8,
                ])),
            ],
        )
        .unwrap();
        let columns = vec![
            ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "partial_v".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let group_by = vec![PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        }];
        let aggr_exprs = vec![PlanExpr::Function {
            name: "SUM".to_string(),
            args: vec![PlanExpr::Column {
                index: 1,
                name: "partial_v".to_string(),
                span: None,
            }],
            distinct: false,
            span: None,
        }];
        let output_schema = vec![
            ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "sum_v".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let make_scan = || -> Arc<dyn ExecutionPlan> {
            Arc::new(ScanExec {
                source: Arc::new(InMemoryDataSource::new(
                    columns.clone(),
                    vec![batch.clone()],
                )),
                _table_name: "partials".to_string(),
                scan_context: ScanContext::default(),
                dynamic_filters: Default::default(),
                dynamic_filters_consumed: Vec::new(),
                dynamic_filter_collector: None,
                dynamic_filtering_enabled: false,
                dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
                scan_task_index: 0,
                scan_task_count: 1,
            })
        };
        let make_agg = |input: Arc<dyn ExecutionPlan>| HashAggregateExec {
            input,
            group_by: group_by.clone(),
            aggr_exprs: aggr_exprs.clone(),
            output_schema: output_schema.clone(),
            output_order: None,
            estimated_groups: None,
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
        };

        let single_stream = make_agg(make_scan()).execute(0).await.unwrap();
        let single = collect_stream(single_stream).await.unwrap();

        let repartitioned: Arc<dyn ExecutionPlan> = Arc::new(crate::RepartitionExec::new(
            make_scan(),
            crate::partitioning::Partitioning::Hash(group_by.clone(), 4),
        ));
        let hash_stream = make_agg(repartitioned).execute(0).await.unwrap();
        let hash_partitioned = collect_stream(hash_stream).await.unwrap();

        fn to_map(batches: &[RecordBatch]) -> std::collections::BTreeMap<i32, i64> {
            let mut out = std::collections::BTreeMap::new();
            for batch in batches {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let vals = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    out.insert(keys.value(row), vals.value(row));
                }
            }
            out
        }

        assert_eq!(to_map(&hash_partitioned), to_map(&single));
    }

    #[derive(Debug)]
    struct PartitionedBatchInput {
        columns: Vec<ColumnInfo>,
        batches: Vec<Vec<RecordBatch>>,
        partitioning: crate::partitioning::Partitioning,
    }

    #[async_trait]
    impl ExecutionPlan for PartitionedBatchInput {
        fn schema(&self) -> Vec<ColumnInfo> {
            self.columns.clone()
        }

        fn output_partitioning(&self) -> crate::partitioning::Partitioning {
            self.partitioning.clone()
        }

        async fn execute(
            &self,
            partition: usize,
        ) -> Result<SendableRecordBatchStream, ExecutionError> {
            let batches = self.batches.get(partition).cloned().ok_or_else(|| {
                ExecutionError::InvalidOperation(format!("partition {partition} out of range"))
            })?;
            let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.columns);
            Ok(stream_from_batches(arrow_schema, batches))
        }

        fn display_name(&self) -> &str {
            "PartitionedBatchInput"
        }
    }

    #[tokio::test]
    async fn hash_partitioned_batch_aware_merges_duplicate_groups_across_partitions() {
        let columns = vec![
            ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "v".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("k", ArrowDataType::Int32, false),
            Field::new("v", ArrowDataType::Int64, false),
        ]));
        let batch = |keys: Vec<i32>, vals: Vec<i64>| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(keys)),
                    Arc::new(Int64Array::from(vals)),
                ],
            )
            .unwrap()
        };
        let partitioned_batches = vec![
            vec![
                batch(vec![1, 2, 1], vec![10, 20, 30]),
                batch(vec![3, 1], vec![300, 40]),
            ],
            vec![
                batch(vec![1, 2], vec![50, 25]),
                batch(vec![3, 2], vec![350, 35]),
            ],
            vec![
                batch(vec![4, 1], vec![400, 60]),
                batch(vec![4, 3], vec![450, 375]),
            ],
        ];
        let single_batches = partitioned_batches
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let group_by = vec![PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        }];
        let value_col = || PlanExpr::Column {
            index: 1,
            name: "v".to_string(),
            span: None,
        };
        let aggr_exprs = vec![
            PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![value_col()],
                distinct: false,
                span: None,
            },
            PlanExpr::Function {
                name: "COUNT".to_string(),
                args: vec![value_col()],
                distinct: false,
                span: None,
            },
            PlanExpr::Function {
                name: "AVG".to_string(),
                args: vec![value_col()],
                distinct: false,
                span: None,
            },
            PlanExpr::Function {
                name: "MIN".to_string(),
                args: vec![value_col()],
                distinct: false,
                span: None,
            },
            PlanExpr::Function {
                name: "MAX".to_string(),
                args: vec![value_col()],
                distinct: false,
                span: None,
            },
        ];
        let output_schema = vec![
            ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "sum_v".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "count_v".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "avg_v".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
            ColumnInfo {
                name: "min_v".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
            ColumnInfo {
                name: "max_v".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ];
        let make_agg = |input: Arc<dyn ExecutionPlan>| HashAggregateExec {
            input,
            group_by: group_by.clone(),
            aggr_exprs: aggr_exprs.clone(),
            output_schema: output_schema.clone(),
            output_order: None,
            estimated_groups: None,
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
        };

        let pfa_input: Arc<dyn ExecutionPlan> = Arc::new(PartitionedBatchInput {
            columns: columns.clone(),
            batches: partitioned_batches,
            partitioning: crate::partitioning::Partitioning::Hash(group_by.clone(), 3),
        });
        let single_input: Arc<dyn ExecutionPlan> = Arc::new(PartitionedBatchInput {
            columns,
            batches: vec![single_batches],
            partitioning: crate::partitioning::Partitioning::UnknownPartitioning(1),
        });

        let pfa = collect_stream(make_agg(pfa_input).execute(0).await.unwrap())
            .await
            .unwrap();
        let single = collect_stream(make_agg(single_input).execute(0).await.unwrap())
            .await
            .unwrap();

        fn results(
            batches: &[RecordBatch],
        ) -> std::collections::BTreeMap<i32, (i64, i64, u64, i64, i64)> {
            let mut out = std::collections::BTreeMap::new();
            for batch in batches {
                let keys = batch.column(0).as_primitive::<datatypes::Int32Type>();
                let sums = batch.column(1).as_primitive::<datatypes::Int64Type>();
                let counts = batch.column(2).as_primitive::<datatypes::Int64Type>();
                let avgs = batch.column(3).as_primitive::<datatypes::Float64Type>();
                let mins = batch.column(4).as_primitive::<datatypes::Int64Type>();
                let maxes = batch.column(5).as_primitive::<datatypes::Int64Type>();
                for row in 0..batch.num_rows() {
                    let prior = out.insert(
                        keys.value(row),
                        (
                            sums.value(row),
                            counts.value(row),
                            avgs.value(row).to_bits(),
                            mins.value(row),
                            maxes.value(row),
                        ),
                    );
                    assert!(
                        prior.is_none(),
                        "duplicate group key {} in aggregate output",
                        keys.value(row)
                    );
                }
            }
            out
        }

        assert_eq!(results(&pfa), results(&single));
    }

    #[tokio::test]
    async fn hash_partitioned_batch_aware_preserves_interleaved_arithmetic_aggregate_order() {
        let columns = vec![
            ColumnInfo {
                name: "k1".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "k2".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "a".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "b".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("k1", ArrowDataType::Int32, false),
            Field::new("k2", ArrowDataType::Int32, false),
            Field::new("a", ArrowDataType::Int64, false),
            Field::new("b", ArrowDataType::Int64, false),
        ]));
        let batch = |k1: Vec<i32>, k2: Vec<i32>, a: Vec<i64>, b: Vec<i64>| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(k1)),
                    Arc::new(Int32Array::from(k2)),
                    Arc::new(Int64Array::from(a)),
                    Arc::new(Int64Array::from(b)),
                ],
            )
            .unwrap()
        };
        let partitioned_batches = vec![
            vec![batch(
                vec![1, 1, 2],
                vec![10, 10, 20],
                vec![2, 3, 4],
                vec![5, 7, 11],
            )],
            vec![batch(vec![1, 3], vec![10, 30], vec![5, 6], vec![13, 17])],
            vec![batch(vec![2, 3], vec![20, 30], vec![7, 8], vec![19, 23])],
        ];
        let single_batches = partitioned_batches
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let group_by = vec![
            PlanExpr::Column {
                index: 0,
                name: "k1".to_string(),
                span: None,
            },
            PlanExpr::Column {
                index: 1,
                name: "k2".to_string(),
                span: None,
            },
        ];
        let aggr_exprs = vec![PlanExpr::Function {
            name: "SUM".to_string(),
            args: vec![PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "a".to_string(),
                    span: None,
                }),
                op: arneb_sql_parser::ast::BinaryOp::Multiply,
                right: Box::new(PlanExpr::Column {
                    index: 3,
                    name: "b".to_string(),
                    span: None,
                }),
                span: None,
            }],
            distinct: false,
            span: None,
        }];
        let output_schema = vec![
            ColumnInfo {
                name: "k1".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "revenue".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "k2".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ];
        let make_agg = |input: Arc<dyn ExecutionPlan>| HashAggregateExec {
            input,
            group_by: group_by.clone(),
            aggr_exprs: aggr_exprs.clone(),
            output_schema: output_schema.clone(),
            output_order: Some(vec![
                AggregateOutputColumn::Group(0),
                AggregateOutputColumn::Aggregate(0),
                AggregateOutputColumn::Group(1),
            ]),
            estimated_groups: None,
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
        };

        let pfa_input: Arc<dyn ExecutionPlan> = Arc::new(PartitionedBatchInput {
            columns: columns.clone(),
            batches: partitioned_batches,
            partitioning: crate::partitioning::Partitioning::Hash(group_by.clone(), 3),
        });
        let single_input: Arc<dyn ExecutionPlan> = Arc::new(PartitionedBatchInput {
            columns,
            batches: vec![single_batches],
            partitioning: crate::partitioning::Partitioning::UnknownPartitioning(1),
        });

        let pfa = collect_stream(make_agg(pfa_input).execute(0).await.unwrap())
            .await
            .unwrap();
        let single = collect_stream(make_agg(single_input).execute(0).await.unwrap())
            .await
            .unwrap();

        fn schema_names(batch: &RecordBatch) -> Vec<String> {
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        }

        fn results(batches: &[RecordBatch]) -> std::collections::BTreeMap<(i32, i32), i64> {
            let mut out = std::collections::BTreeMap::new();
            for batch in batches {
                assert_eq!(schema_names(batch), vec!["k1", "revenue", "k2"]);
                let k1 = batch.column(0).as_primitive::<datatypes::Int32Type>();
                let sums = batch.column(1).as_primitive::<datatypes::Int64Type>();
                let k2 = batch.column(2).as_primitive::<datatypes::Int32Type>();
                for row in 0..batch.num_rows() {
                    let prior = out.insert((k1.value(row), k2.value(row)), sums.value(row));
                    assert!(
                        prior.is_none(),
                        "duplicate group key ({}, {}) in aggregate output",
                        k1.value(row),
                        k2.value(row)
                    );
                }
            }
            out
        }

        assert_eq!(results(&pfa), results(&single));
        assert_eq!(
            results(&pfa),
            std::collections::BTreeMap::from([((1, 10), 96), ((2, 20), 177), ((3, 30), 286)])
        );
    }

    /// Regression for PB-003: `COUNT(DISTINCT ...)` must deduplicate
    /// before counting. Historically the `distinct` flag was dropped
    /// on the way from `PlanExpr::Function` into the accumulator, so
    /// this test counted 6 (total rows) instead of 3 (distinct keys).
    #[tokio::test]
    async fn count_distinct_groups_deduplicates() {
        // Two groups (k=0 / k=1), each with six rows where the
        // distinct cardinality of `v` is 3 (values 10, 20, 30 appear
        // twice each). COUNT(v) would return 6 for each group;
        // COUNT(DISTINCT v) must return 3.
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", ArrowDataType::Int32, false),
            Field::new("v", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![
                    10, 20, 10, 30, 20, 30, 10, 20, 10, 30, 20, 30,
                ])),
            ],
        )
        .unwrap();
        let source: Arc<dyn DataSource> = Arc::new(InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        ));

        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "t".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let agg = HashAggregateExec {
            input: scan,
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "k".to_string(),
                span: None,
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".to_string(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "v".to_string(),
                    span: None,
                }],
                distinct: true,
                span: None,
            }],
            output_schema: vec![
                ColumnInfo {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            output_order: None,
            estimated_groups: None,
            memory_pool: std::sync::Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
        };

        let stream = agg.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        let counts = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Both groups must produce 3 distinct values.
        for i in 0..counts.len() {
            assert_eq!(counts.value(i), 3, "row {i} should see 3 distinct values");
        }
    }

    /// exec-memory-accounting D3: the batch-aware grouping path reserves its
    /// growing group state against the pool and fails fast (ResourceExhausted)
    /// under a tight pool — instead of growing untracked into the worker
    /// OOM-kill observed on q18 SF30. (With the default Unbounded pool the
    /// reservation never fails, so the existing aggregate tests are unaffected.)
    #[tokio::test]
    async fn hash_aggregate_group_state_fails_fast_under_tight_pool() {
        use crate::memory_pool::{GreedyMemoryPool, MemoryPool};
        let k: Vec<i32> = (0..2000).collect();
        let v: Vec<i64> = vec![1; 2000];
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int32Array::from(k)),
                Arc::new(arrow::array::Int64Array::from(v)),
            ],
        )
        .unwrap();
        let source: Arc<dyn DataSource> = Arc::new(InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        ));
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "t".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        // 256 B limit ≪ 2000 groups' state → try_resize fails on the first batch.
        let tiny_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256));
        let agg = HashAggregateExec {
            input: scan,
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "k".to_string(),
                span: None,
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".to_string(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "v".to_string(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }],
            output_schema: vec![
                ColumnInfo {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            output_order: None,
            estimated_groups: None,
            memory_pool: tiny_pool,
        };
        match agg.execute(0).await {
            Err(ExecutionError::ResourceExhausted(_)) => {}
            Err(other) => panic!("expected ResourceExhausted, got {other:?}"),
            Ok(_) => panic!("expected ResourceExhausted under a tight pool, got Ok (no fail-fast)"),
        }
    }

    #[tokio::test]
    async fn cross_join() {
        let schema1 = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            false,
        )]));
        let batch1 =
            RecordBatch::try_new(schema1, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let src1 = Arc::new(InMemoryDataSource::from_batch(batch1).unwrap()) as Arc<dyn DataSource>;

        let schema2 = Arc::new(Schema::new(vec![Field::new(
            "b",
            ArrowDataType::Int32,
            false,
        )]));
        let batch2 =
            RecordBatch::try_new(schema2, vec![Arc::new(Int32Array::from(vec![10, 20, 30]))])
                .unwrap();
        let src2 = Arc::new(InMemoryDataSource::from_batch(batch2).unwrap()) as Arc<dyn DataSource>;

        let left: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: src1,
            _table_name: "t1".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let right: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: src2,
            _table_name: "t2".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });

        let join = NestedLoopJoinExec {
            left,
            right,
            join_type: ast::JoinType::Cross,
            condition: arneb_planner::JoinCondition::None,
        };

        let stream = join.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6);
    }

    #[tokio::test]
    async fn explain_exec() {
        let plan = LogicalPlan::TableScan {
            table: arneb_common::types::TableReference::table("test"),
            schema: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        };
        let explain = ExplainExec {
            plan,
            analyze_inner: None,
        };
        let stream = explain.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        let text = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(text.value(0).contains("TableScan"));
        // Plain EXPLAIN must not emit the runtime header.
        assert!(!text.value(0).contains("Actual:"));
    }

    #[tokio::test]
    async fn explain_analyze_emits_actual_rows() {
        // Use a OneRowExec inner — it deterministically produces one
        // empty row — so the analyze path can be exercised without
        // pulling in a real DataSource.
        let plan = LogicalPlan::TableScan {
            table: arneb_common::types::TableReference::table("test"),
            schema: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        };
        let analyze_inner: Arc<dyn ExecutionPlan> = Arc::new(OneRowExec);
        let explain = ExplainExec {
            plan,
            analyze_inner: Some(analyze_inner),
        };
        let stream = explain.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        let text = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let out = text.value(0);
        assert!(out.contains("Actual: rows=1"), "missing header: {out}");
        assert!(out.contains("batches=1"), "missing batches: {out}");
        assert!(out.contains("---"), "missing separator: {out}");
        // The original plan still follows.
        assert!(out.contains("TableScan"), "missing inner plan: {out}");
    }

    // -----------------------------------------------------------
    // A1.4: cross-fragment dynamic filter scan-side tests
    // -----------------------------------------------------------

    fn build_scan_with_df(
        consumed: Vec<arneb_planner::DynamicFilterConsumer>,
        collector: Option<crate::dynamic_filter_collector::DynamicFilterCollector>,
        enabled: bool,
        timeout: std::time::Duration,
    ) -> ScanExec {
        ScanExec {
            source: make_test_source(),
            _table_name: "t".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: consumed,
            dynamic_filter_collector: collector,
            dynamic_filtering_enabled: enabled,
            dynamic_filtering_wait_timeout: timeout,
            scan_task_index: 0,
            scan_task_count: 1,
        }
    }

    fn consumer(
        df_id: u32,
        col_index: usize,
        col_name: &str,
    ) -> arneb_planner::DynamicFilterConsumer {
        arneb_planner::DynamicFilterConsumer {
            id: arneb_common::DynamicFilterId(df_id),
            column_index: col_index,
            column_name: col_name.to_string(),
        }
    }

    #[tokio::test]
    async fn df_flag_off_skips_wait_even_with_annotations() {
        // Flag off → no wait, no filter added even if collector has Domains ready.
        let collector = crate::dynamic_filter_collector::DynamicFilterCollector::with_pending([(
            arneb_common::DynamicFilterId(0),
            arneb_common::Domain::All,
        )]);
        let scan = build_scan_with_df(
            vec![consumer(0, 0, "id")],
            Some(collector),
            false, // flag OFF
            std::time::Duration::from_millis(50),
        );
        let filters = scan.collect_cross_fragment_filters().await;
        assert!(filters.is_empty(), "flag off must skip the wait");
        // Scan still executes successfully — feature is inert.
        let stream = scan.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn df_no_collector_skips_wait() {
        let scan = build_scan_with_df(
            vec![consumer(0, 0, "id")],
            None, // no collector
            true, // flag ON, but nothing to wait on
            std::time::Duration::from_millis(50),
        );
        let filters = scan.collect_cross_fragment_filters().await;
        assert!(filters.is_empty());
    }

    #[tokio::test]
    async fn df_resolved_domain_becomes_dynamic_filter_domain() {
        let domain = arneb_common::Domain::DistinctValues(vec![ScalarValue::Int32(2)]);
        let collector = crate::dynamic_filter_collector::DynamicFilterCollector::with_pending([(
            arneb_common::DynamicFilterId(0),
            domain.clone(),
        )]);
        let scan = build_scan_with_df(
            vec![consumer(0, 0, "id")],
            Some(collector),
            true,
            std::time::Duration::from_millis(50),
        );
        let domains = scan.collect_cross_fragment_filters().await;
        assert_eq!(domains.len(), 1, "one domain expected, got {domains:?}");
        assert_eq!(domains[0].column_index, 0);
        assert_eq!(domains[0].column_name, "id");
        assert_eq!(domains[0].domain, domain);
    }

    #[tokio::test]
    async fn df_wait_times_out_and_scan_proceeds() {
        // Flag ON, collector exists, but the Domain never arrives.
        // The wait must time out within the configured timeout AND
        // the scan must still execute successfully (sound fallback).
        let collector = crate::dynamic_filter_collector::DynamicFilterCollector::new();
        let scan = build_scan_with_df(
            vec![consumer(0, 0, "id")],
            Some(collector),
            true,
            std::time::Duration::from_millis(50),
        );
        let started = std::time::Instant::now();
        let filters = scan.collect_cross_fragment_filters().await;
        let elapsed = started.elapsed();
        assert!(
            filters.is_empty(),
            "timeout must drop the filter, not synthesise one"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "wait did not respect timeout (elapsed: {elapsed:?})"
        );
        // Scan still works, returning all rows since no filter was applied.
        let stream = scan.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn df_pending_then_resolved_via_insert() {
        let collector = crate::dynamic_filter_collector::DynamicFilterCollector::new();
        let collector_for_send = collector.clone();

        // Insert the Domain shortly after the scan starts waiting.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            collector_for_send
                .insert(
                    arneb_common::DynamicFilterId(0),
                    arneb_common::Domain::DistinctValues(vec![ScalarValue::Int32(1)]),
                )
                .await;
        });

        let scan = build_scan_with_df(
            vec![consumer(0, 0, "id")],
            Some(collector),
            true,
            std::time::Duration::from_millis(500),
        );
        let filters = scan.collect_cross_fragment_filters().await;
        assert_eq!(filters.len(), 1);
    }

    // ---- CoalescingStream (N2-Q18) ----

    fn int_batch(schema: &arrow::datatypes::SchemaRef, vals: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vals.to_vec()))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn coalescing_merges_small_batches_preserving_order() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        // 10 single-row batches → target 4 rows.
        let batches: Vec<RecordBatch> = (0..10).map(|i| int_batch(&schema, &[i])).collect();
        let inner = stream_from_batches(schema.clone(), batches);
        let coalesced = Box::pin(CoalescingStream::new(inner, 4));
        let out = collect_stream(coalesced).await.unwrap();
        // 10 rows / target 4 → batches of 4,4,2.
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].num_rows(), 4);
        assert_eq!(out[1].num_rows(), 4);
        assert_eq!(out[2].num_rows(), 2);
        // Order + content preserved: flatten and compare to 0..10.
        let mut seen = Vec::new();
        for b in &out {
            let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..col.len() {
                seen.push(col.value(i));
            }
        }
        assert_eq!(seen, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn coalescing_drops_empty_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        let batches = vec![
            int_batch(&schema, &[1, 2]),
            int_batch(&schema, &[]),
            int_batch(&schema, &[3]),
        ];
        let inner = stream_from_batches(schema.clone(), batches);
        let coalesced = Box::pin(CoalescingStream::new(inner, 8192));
        let out = collect_stream(coalesced).await.unwrap();
        // All fits under target → one coalesced batch, 3 rows, no empty.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn coalescing_passes_through_single_large_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        let big: Vec<i32> = (0..1000).collect();
        let inner = stream_from_batches(schema.clone(), vec![int_batch(&schema, &big)]);
        let coalesced = Box::pin(CoalescingStream::new(inner, 8192));
        let out = collect_stream(coalesced).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1000);
    }

    // ---- collect_probe_within_budget / prepend_batches (q18 OOM fix) ----

    fn i32_batch(vals: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals))]).unwrap()
    }

    fn i32_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        stream_from_batches(schema, batches)
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn collect_probe_within_budget_fits_under_generous_pool() {
        let batches = vec![
            i32_batch((0..1000).collect()),
            i32_batch((0..1000).collect()),
            i32_batch((0..1000).collect()),
        ];
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(1_000_000));
        let outcome = collect_probe_within_budget(i32_stream(batches), pool, "test.probe", None)
            .await
            .unwrap();
        match outcome {
            ProbeCollect::Fits { batches, .. } => assert_eq!(total_rows(&batches), 3000),
            ProbeCollect::Overflow { .. } => panic!("generous pool should not overflow"),
        }
    }

    #[tokio::test]
    async fn collect_probe_within_budget_overflows_and_preserves_all_rows() {
        let batches = vec![
            i32_batch((0..1000).collect()),
            i32_batch((0..1000).collect()),
            i32_batch((0..1000).collect()),
        ];
        // One 1000-row Int32 batch ≈ 4 KB; a 5 KB pool admits the first and
        // refuses the second → Overflow, with no rows lost.
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(5_000));
        let outcome = collect_probe_within_budget(i32_stream(batches), pool, "test.probe", None)
            .await
            .unwrap();
        match outcome {
            ProbeCollect::Overflow { prefix, rest } => {
                let rest_batches = collect_stream(rest).await.unwrap();
                assert_eq!(
                    total_rows(&prefix) + total_rows(&rest_batches),
                    3000,
                    "prefix + rest must preserve every probe row"
                );
                assert!(
                    !prefix.is_empty(),
                    "prefix holds the batches pulled before overflow"
                );
                assert!(
                    !rest_batches.is_empty(),
                    "rest holds the un-consumed remainder"
                );
            }
            ProbeCollect::Fits { .. } => panic!("tight pool must overflow"),
        }
    }

    #[tokio::test]
    async fn collect_probe_within_budget_streams_when_cap_exceeded_despite_generous_pool() {
        // The q08 peak fix: a GENEROUS pool would collect the whole probe,
        // but a small dedicated `max_bytes` cap forces the large probe to
        // stream (Overflow) — bounding the no-spill probe peak without
        // touching the pool / build-spill headroom. All rows preserved.
        let batches = vec![
            i32_batch((0..1000).collect()),
            i32_batch((0..1000).collect()),
            i32_batch((0..1000).collect()),
        ];
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(1_000_000)); // generous
                                                                            // One 1000-row Int32 batch ≈ 4 KB; a 5 KB cap admits the first and
                                                                            // refuses the second → Overflow despite the roomy pool.
        let outcome =
            collect_probe_within_budget(i32_stream(batches), pool, "test.probe", Some(5_000))
                .await
                .unwrap();
        match outcome {
            ProbeCollect::Overflow { prefix, rest } => {
                let rest_batches = collect_stream(rest).await.unwrap();
                assert_eq!(
                    total_rows(&prefix) + total_rows(&rest_batches),
                    3000,
                    "cap overflow must still preserve every probe row"
                );
                assert!(!prefix.is_empty());
                assert!(!rest_batches.is_empty());
            }
            ProbeCollect::Fits { .. } => {
                panic!("a small max_bytes cap must overflow even under a generous pool")
            }
        }
    }

    #[tokio::test]
    async fn prepend_batches_yields_prefix_then_rest_in_order() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        let prefix = vec![i32_batch(vec![1, 2]), i32_batch(vec![3])];
        let rest = i32_stream(vec![i32_batch(vec![4, 5]), i32_batch(vec![6])]);
        let chained = prepend_batches(schema, prefix, rest);
        let out = collect_stream(chained).await.unwrap();
        let all: Vec<i32> = out
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(all, vec![1, 2, 3, 4, 5, 6]);
    }
}
