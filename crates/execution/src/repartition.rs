//! `RepartitionExec`: re-distribute rows from `M` input partitions into
//! `N` output partitions using either round-robin or hash partitioning.
//!
//! Architecture:
//! - On the first call to `execute(p)`, the operator builds `N`
//!   `tokio::sync::mpsc` channels (capacity 4 each) and spawns one tokio
//!   task per input partition. Each task drains its input partition's
//!   stream and routes batches into the appropriate output channel:
//!   - `RoundRobinBatch(N)` increments a shared atomic counter and
//!     dispatches the whole batch to `counter % N`.
//!   - `Hash(exprs, N)` evaluates `exprs` on every row, hashes the
//!     resulting values, then splits the batch into `N` sub-batches by
//!     `(row hash) % N` and sends each sub-batch to its target channel.
//! - Subsequent `execute(p)` calls hand out the pre-built `Receiver` for
//!   partition `p`. Each receiver may be drained at most once.
//!
//! Bounded channel capacity gives back-pressure: when a slow downstream
//! consumer's channel fills, the producer task `await`s on `send` and
//! stops pulling its input, propagating the back-pressure upstream.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::inflight_budget::InflightBudget;
use arneb_common::memory_profile::{record_live_alloc, record_live_free};
use arneb_common::stream::{RecordBatchStream, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arneb_planner::PlanExpr;
use arrow::array::{ArrayRef, RecordBatch};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;

use crate::expression;
use crate::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
use crate::operator::{materialize_dictionary_array, ExecutionPlan};
use crate::partitioning::Partitioning;

const DEFAULT_CHANNEL_CAPACITY: usize = 4;

static SCAN_DECODE_CONCURRENCY: OnceLock<usize> = OnceLock::new();

fn scan_decode_concurrency() -> usize {
    *SCAN_DECODE_CONCURRENCY.get_or_init(|| {
        // Default = effectively unbounded (preserves pre-semaphore behavior):
        // the SF30 A/B showed bounding to K=4 was null-to-slightly-worse on
        // memory (the untracked anon is NOT the concurrent producer decodes),
        // so the cap is OFF by default and kept only as an experiment knob.
        // NOT usize::MAX — tokio Semaphore panics above MAX_PERMITS
        // (usize::MAX>>3); 1<<20 is far above any real scan-partition count
        // (≤ low hundreds) so the semaphore never actually blocks.
        let default = 1 << 20;
        let k = std::env::var("ARNEB_SCAN_DECODE_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(default);
        tracing::info!(
            target: "arneb::config",
            scan_decode_concurrency = k,
            "ARNEB_SCAN_DECODE_CONCURRENCY effective value"
        );
        k
    })
}

/// Releases an exchange-channel memory reservation when the batch it
/// accompanies leaves the bounded channel — whether the consumer pulls
/// it (released in `MpscStream::poll_next`) or the channel is dropped
/// with the batch still buffered (released here on `Drop`). This keeps
/// the global pool's reserved total balanced with no leak on either path.
///
/// `bytes` is the amount actually granted by `try_grow` (0 if the pool
/// declined — D1 tracks best-effort and never gates; gating/spill is D2).
struct ChannelReservationGuard {
    pool: Arc<dyn MemoryPool>,
    consumer: MemoryConsumer,
    bytes: usize,
    inflight_budget: Arc<InflightBudget>,
    budget_bytes: u64,
    live_bytes: u64,
}

impl Drop for ChannelReservationGuard {
    fn drop(&mut self) {
        if self.live_bytes > 0 {
            record_live_free("RepartitionExec.live", self.live_bytes);
            self.live_bytes = 0;
        }
        if self.bytes > 0 {
            self.pool.shrink(&self.consumer, self.bytes);
            self.bytes = 0;
        }
        if self.budget_bytes > 0 {
            self.inflight_budget.release(self.budget_bytes);
            self.budget_bytes = 0;
        }
    }
}

/// Channel item: a routed batch plus its reservation guard, or an error.
type ChannelItem = Result<(RecordBatch, ChannelReservationGuard), ArnebError>;

/// Fan rows from `input` into `N` output partitions per the configured
/// [`Partitioning`].
pub struct RepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
    state: tokio::sync::Mutex<RouterState>,
    schema: Vec<ColumnInfo>,
    /// Global pool the in-flight channel batches are accounted against.
    /// Defaults to [`UnboundedMemoryPool`] (single-node / tests); the
    /// distributed worker installs the cgroup-derived pool via
    /// [`Self::with_memory_pool`]. Phase 1 (2026-06-04, exec-memory-accounting).
    memory_pool: Arc<dyn MemoryPool>,
    inflight_budget: Arc<InflightBudget>,
}

struct RouterState {
    receivers: Vec<Option<Receiver<ChannelItem>>>,
    started: bool,
}

impl std::fmt::Debug for RepartitionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepartitionExec")
            .field("partitioning", &self.partitioning)
            .finish()
    }
}

impl RepartitionExec {
    /// Construct a new repartition operator. The partitioning's
    /// partition count `N` determines the number of output streams; it
    /// must be `>= 1` (a debug assert fires otherwise in debug builds).
    pub fn new(input: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        debug_assert!(
            partitioning.partition_count() >= 1,
            "RepartitionExec partition count must be >= 1"
        );
        let schema = input.schema();
        let n = partitioning.partition_count();
        Self {
            input,
            partitioning,
            state: tokio::sync::Mutex::new(RouterState {
                receivers: (0..n).map(|_| None).collect(),
                started: false,
            }),
            schema,
            memory_pool: Arc::new(UnboundedMemoryPool::new()),
            inflight_budget: Arc::new(InflightBudget::new(0)),
        }
    }

    /// Install the global [`MemoryPool`] the in-flight channel batches are
    /// reserved against, so the exchange's buffered bytes are visible to
    /// the worker memory limit. Without this, the operator tracks against
    /// an [`UnboundedMemoryPool`] (no-op accounting). Phase 1.
    pub fn with_memory_pool(mut self, memory_pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = memory_pool;
        self
    }

    /// Install the shared per-stage in-flight byte budget used to block
    /// producers before batches enter repartition output channels.
    pub fn with_inflight_budget(mut self, inflight_budget: Arc<InflightBudget>) -> Self {
        self.inflight_budget = inflight_budget;
        self
    }
}

#[async_trait]
impl ExecutionPlan for RepartitionExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }

    fn display_name(&self) -> &str {
        "RepartitionExec"
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn inject_dynamic_filter(&self, filter: arneb_planner::PlanExpr, target_index: usize) {
        // Repartition is a pure shuffle: same column layout, pass through.
        self.input.inject_dynamic_filter(filter, target_index);
    }

    fn is_leaf_scan_subtree(&self) -> bool {
        self.input.is_leaf_scan_subtree()
    }

    fn required_input_partitioning(&self) -> Vec<Partitioning> {
        // `Repartition` accepts any input shape — its job is to bridge.
        vec![self.input.output_partitioning()]
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        let n = self.partitioning.partition_count();
        if partition >= n {
            return Err(ExecutionError::InvalidOperation(format!(
                "RepartitionExec: partition {partition} out of range (have {n} output partitions)"
            )));
        }

        let mut state = self.state.lock().await;
        if !state.started {
            // Build N (sender, receiver) pairs and stash the receivers.
            let mut senders: Vec<Sender<ChannelItem>> = Vec::with_capacity(n);
            let mut new_receivers: Vec<Option<Receiver<ChannelItem>>> = Vec::with_capacity(n);
            for _ in 0..n {
                let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_CAPACITY);
                senders.push(tx);
                new_receivers.push(Some(rx));
            }
            state.receivers = new_receivers;
            state.started = true;

            // Spawn one producer task per input partition. Each routes
            // its batches to the appropriate output channel(s) per the
            // partitioning mode. A shared counter coordinates round-
            // robin distribution across producers.
            let input = Arc::clone(&self.input);
            let partitioning = self.partitioning.clone();
            let senders = Arc::new(senders);
            let counter = Arc::new(AtomicUsize::new(0));
            let memory_pool = Arc::clone(&self.memory_pool);
            let inflight_budget = Arc::clone(&self.inflight_budget);
            let decode_sem = Arc::new(Semaphore::new(scan_decode_concurrency()));
            let m = input.output_partitioning().partition_count();
            for input_partition in 0..m {
                let input = Arc::clone(&input);
                let senders = Arc::clone(&senders);
                let counter = Arc::clone(&counter);
                let partitioning = partitioning.clone();
                let memory_pool = Arc::clone(&memory_pool);
                let inflight_budget = Arc::clone(&inflight_budget);
                let decode_sem = Arc::clone(&decode_sem);
                tokio::spawn(async move {
                    let _permit = decode_sem.acquire_owned().await.unwrap();
                    route_one_input_partition(
                        input,
                        input_partition,
                        senders,
                        counter,
                        partitioning,
                        memory_pool,
                        inflight_budget,
                    )
                    .await;
                });
            }
        }

        let rx = state.receivers[partition].take().ok_or_else(|| {
            ExecutionError::InvalidOperation(format!(
                "RepartitionExec: partition {partition} already taken by a previous execute() call"
            ))
        })?;
        drop(state);

        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.schema);
        let inner: SendableRecordBatchStream = Box::pin(MpscStream {
            rx,
            schema: arrow_schema,
        });
        Ok(crate::operator::profile_stream(
            "RepartitionExec",
            partition,
            inner,
        ))
    }
}

async fn route_one_input_partition(
    input: Arc<dyn ExecutionPlan>,
    input_partition: usize,
    senders: Arc<Vec<Sender<ChannelItem>>>,
    counter: Arc<AtomicUsize>,
    partitioning: Partitioning,
    memory_pool: Arc<dyn MemoryPool>,
    inflight_budget: Arc<InflightBudget>,
) {
    // Account each routed batch against the global pool while it sits in
    // the bounded channel; the guard releases when the consumer pulls it
    // (or the channel drops). D1 tracks best-effort — `try_grow` failure
    // does not gate or error (granted = 0); gating/spill is D2.
    let channel_consumer = MemoryConsumer::new("RepartitionExec.channel");
    let t_route = std::time::Instant::now();
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_batches: u64 = 0;
    let mut blocked_ms: u64 = 0;
    let n_outputs = senders.len();
    let partitioning_label = match &partitioning {
        Partitioning::Hash(_, _) => "hash",
        Partitioning::RoundRobinBatch(_) => "rr",
        Partitioning::UnknownPartitioning(_) => "unknown",
    };
    let mut stream = match input.execute(input_partition).await {
        Ok(s) => s,
        Err(e) => {
            // Failed to even open the input; broadcast the error to
            // partition 0 (the first reader observes it).
            let _ = senders[0].send(Err(ArnebError::Execution(e))).await;
            return;
        }
    };

    while let Some(item) = stream.next().await {
        let batch = match item {
            Ok(b) => b,
            Err(e) => {
                let _ = senders[0].send(Err(e)).await;
                return;
            }
        };
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows += batch.num_rows() as u64;
        total_bytes += crate::operator::record_batch_bytes(&batch) as u64;
        total_batches += 1;

        match &partitioning {
            Partitioning::RoundRobinBatch(_) | Partitioning::UnknownPartitioning(_) => {
                // Send the whole batch to a single output partition.
                let n = senders.len();
                let target = counter.fetch_add(1, Ordering::Relaxed) % n;
                let t_send = std::time::Instant::now();
                let item =
                    make_channel_item(batch, &memory_pool, &channel_consumer, &inflight_budget)
                        .await;
                if senders[target].send(item).await.is_err() {
                    // Receiver dropped — graceful shutdown.
                    return;
                }
                blocked_ms = blocked_ms.saturating_add(t_send.elapsed().as_millis() as u64);
            }
            Partitioning::Hash(exprs, _) => {
                let n = senders.len();
                let sub_batches = match split_by_hash(&batch, exprs, n) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = senders[0].send(Err(ArnebError::Execution(e))).await;
                        return;
                    }
                };
                for (out_partition, sub) in sub_batches.into_iter().enumerate() {
                    if sub.num_rows() == 0 {
                        continue;
                    }
                    let t_send = std::time::Instant::now();
                    let item =
                        make_channel_item(sub, &memory_pool, &channel_consumer, &inflight_budget)
                            .await;
                    if senders[out_partition].send(item).await.is_err() {
                        return;
                    }
                    blocked_ms = blocked_ms.saturating_add(t_send.elapsed().as_millis() as u64);
                }
            }
        }
    }

    tracing::info!(
        target: "arneb::profile",
        op = "RepartitionExec.route_input",
        partitioning = partitioning_label,
        input_partition,
        n_outputs = n_outputs as u64,
        total_rows,
        total_bytes,
        total_batches,
        blocked_ms,
        total_ms = t_route.elapsed().as_millis() as u64,
        "RepartitionExec input partition routed"
    );
}

async fn make_channel_item(
    batch: RecordBatch,
    memory_pool: &Arc<dyn MemoryPool>,
    channel_consumer: &MemoryConsumer,
    inflight_budget: &Arc<InflightBudget>,
) -> ChannelItem {
    let bytes = crate::operator::record_batch_bytes(&batch);
    inflight_budget.acquire(bytes as u64).await;
    let live_bytes = batch.get_array_memory_size() as u64;
    record_live_alloc("RepartitionExec.live", live_bytes);
    let granted = if memory_pool.try_grow(channel_consumer, bytes).is_ok() {
        bytes
    } else {
        0
    };
    Ok((
        batch,
        ChannelReservationGuard {
            pool: Arc::clone(memory_pool),
            consumer: channel_consumer.clone(),
            bytes: granted,
            inflight_budget: Arc::clone(inflight_budget),
            budget_bytes: bytes as u64,
            live_bytes,
        },
    ))
}

/// Split `batch` into `n` sub-batches by hashing the per-row evaluation
/// of `exprs`. Rows with the same key hash mod `n` land in the same
/// sub-batch, so a downstream hash-aggregate can build per-partition
/// state without cross-partition merging.
///
/// **Cross-process determinism (W3-Hash.6, 2026-05-20)**: the hasher
/// MUST produce the same `hash(value) -> u64` on every worker process,
/// otherwise partitioned hash joins are silently broken — worker A
/// routes row `K` to partition 3, worker B routes the same `K` to
/// partition 7, the join never sees matching rows together. We use
/// `ahash::RandomState::with_seeds(0, 0, 0, 0)` — fixed seeds, no
/// per-process randomization (DataFusion + Ballista do the same; cf.
/// `datafusion/physical-plan/src/repartition/mod.rs`). Trino's
/// equivalent is the per-type `XxHash64` from
/// `TypeOperators.getXxHash64Operator`. Either works; what does NOT
/// work is `std::DefaultHasher` (SipHash seeded by `RandomState`) or
/// default `ahash::RandomState::new()` — both are per-process random.
/// Stateful, reusable hash partitioner. Holds the evaluated key
/// expressions + the seeded `ahash` builder so callers can route many
/// batches into the same `N` buckets with one shared assignment
/// function. Both `RepartitionExec` (inter-stage shuffle) and Grace
/// Hash Join's build + probe sides go through this so they agree on
/// `(row -> bucket)`.
///
/// Seed is fixed `(0, 0, 0, 0)`; changing it silently breaks
/// partitioned probe matching against build files spilled by a
/// different binary version.
#[derive(Debug)]
pub(crate) struct HashPartitioner {
    exprs: Vec<PlanExpr>,
    n: usize,
    build_hasher: ahash::RandomState,
}

impl HashPartitioner {
    pub(crate) fn new(exprs: Vec<PlanExpr>, n: usize) -> Result<Self, ExecutionError> {
        if exprs.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "Hash partitioner requires at least one key expression".to_string(),
            ));
        }
        if n == 0 {
            return Err(ExecutionError::InvalidOperation(
                "Hash partitioner requires N >= 1".to_string(),
            ));
        }
        Ok(Self {
            exprs,
            n,
            build_hasher: ahash::RandomState::with_seeds(0, 0, 0, 0),
        })
    }

    #[allow(dead_code)] // Used in Phase 3b.5d (Grace HJ probe loop)
    pub(crate) fn n_partitions(&self) -> usize {
        self.n
    }

    /// Per-row partition id (in `0..n`) for `batch`. Avoids materialising
    /// sub-batches when the caller only needs assignments (e.g. Grace HJ
    /// probe Pass 1, which routes per row to either an in-memory hash
    /// table or a per-partition spill writer).
    pub(crate) fn assignments(&self, batch: &RecordBatch) -> Result<Vec<u32>, ExecutionError> {
        use std::hash::{BuildHasher, Hasher};
        let num_rows = batch.num_rows();
        let key_arrays: Vec<ArrayRef> = self
            .exprs
            .iter()
            .map(|e| {
                expression::evaluate(e, batch, None).and_then(|c| materialize_dictionary_array(&c))
            })
            .collect::<Result<_, _>>()?;
        let mut out: Vec<u32> = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            let mut hasher = self.build_hasher.build_hasher();
            for col in &key_arrays {
                hash_one_cell_ahash(col, row, &mut hasher)?;
            }
            let h = hasher.finish();
            out.push((h % self.n as u64) as u32);
        }
        Ok(out)
    }

    /// Split `batch` into `n` sub-batches by hash partition. Used by
    /// `RepartitionExec` for inter-stage shuffles; Grace HJ probe Pass 1
    /// prefers `assignments` + per-bucket `take` so it can interleave
    /// emit / spill per partition without materialising every sub-batch.
    pub(crate) fn split(&self, batch: &RecordBatch) -> Result<Vec<RecordBatch>, ExecutionError> {
        let assignments = self.assignments(batch)?;
        let mut buckets: Vec<Vec<u32>> = (0..self.n).map(|_| Vec::new()).collect();
        for (row, &b) in assignments.iter().enumerate() {
            buckets[b as usize].push(row as u32);
        }
        let schema = batch.schema();
        let mut sub_batches = Vec::with_capacity(self.n);
        for indices in buckets {
            if indices.is_empty() {
                let empty = RecordBatch::new_empty(schema.clone());
                sub_batches.push(empty);
                continue;
            }
            let idx_array = arrow::array::UInt32Array::from(indices);
            let columns = (0..batch.num_columns())
                .map(|i| arrow::compute::take(batch.column(i), &idx_array, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ExecutionError::from)?;
            let sub =
                RecordBatch::try_new(schema.clone(), columns).map_err(ExecutionError::from)?;
            sub_batches.push(sub);
        }
        Ok(sub_batches)
    }
}

/// Backward-compat wrapper around [`HashPartitioner::split`]. Existing
/// `RepartitionExec` call sites keep working without churn while Grace
/// HJ uses the struct API directly.
fn split_by_hash(
    batch: &RecordBatch,
    exprs: &[PlanExpr],
    n: usize,
) -> Result<Vec<RecordBatch>, ExecutionError> {
    HashPartitioner::new(exprs.to_vec(), n)?.split(batch)
}

/// Hash a single cell into the running hasher. Used by the deterministic
/// `ahash`-based `split_by_hash` path (W3-Hash.6). Generic over the
/// hasher so the same byte-feed routine works for any cross-process
/// deterministic hasher we swap in (e.g. xxhash-rust if we ever move
/// off ahash).
fn hash_one_cell_ahash<H: std::hash::Hasher>(
    col: &ArrayRef,
    row: usize,
    hasher: &mut H,
) -> Result<(), ExecutionError> {
    use arrow::array::{
        Array, AsArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray,
    };
    use arrow::datatypes::DataType as ArrowDataType;
    use std::hash::Hash;

    if col.is_null(row) {
        // Encode null as a sentinel byte to differentiate from "0".
        0u8.hash(hasher);
        return Ok(());
    }
    match col.data_type() {
        ArrowDataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row)
            .hash(hasher),
        ArrowDataType::Int32 => i64::from(
            col.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        )
        .hash(hasher),
        ArrowDataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .hash(hasher),
        ArrowDataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .to_bits()
            .hash(hasher),
        ArrowDataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_bits()
            .hash(hasher),
        ArrowDataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .hash(hasher),
        ArrowDataType::Date32 => col
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row)
            .hash(hasher),
        ArrowDataType::Decimal128(_, _) => col
            .as_primitive::<arrow::datatypes::Decimal128Type>()
            .value(row)
            .hash(hasher),
        other => {
            return Err(ExecutionError::InvalidOperation(format!(
                "hash repartition: unsupported key type {other:?}"
            )));
        }
    }
    Ok(())
}

/// `RecordBatchStream` adapter over a tokio mpsc `Receiver`.
struct MpscStream {
    rx: Receiver<ChannelItem>,
    schema: arrow::datatypes::SchemaRef,
}

impl Stream for MpscStream {
    type Item = Result<RecordBatch, ArnebError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            // Dropping `_guard` here releases the exchange reservation:
            // the batch has left the bounded channel and is now the
            // downstream operator's memory.
            Poll::Ready(Some(Ok((batch, _guard)))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for MpscStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use crate::operator::ScanExec;
    use crate::scan_context::ScanContext;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::DataType;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use std::sync::Arc;

    fn three_rows() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn six_rows_two_batches() -> InMemoryDataSource {
        let schema_cols = vec![ColumnInfo {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }];
        InMemoryDataSource::new(
            schema_cols,
            vec![three_rows(), {
                let s = Arc::new(Schema::new(vec![Field::new(
                    "id",
                    ArrowDataType::Int32,
                    false,
                )]));
                RecordBatch::try_new(s, vec![Arc::new(Int32Array::from(vec![4, 5, 6]))]).unwrap()
            }],
        )
    }

    #[tokio::test]
    async fn roundrobin_distributes_batches_evenly() {
        let source: Arc<dyn crate::datasource::DataSource> = Arc::new(six_rows_two_batches());
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
        let rep = Arc::new(RepartitionExec::new(scan, Partitioning::RoundRobinBatch(2)));
        let mut rows_per_partition = Vec::new();
        for p in 0..2 {
            let stream = rep.execute(p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            rows_per_partition.push(batches.iter().map(|b| b.num_rows()).sum::<usize>());
        }
        // 2 input batches × 3 rows = 6 rows total; round-robin sends each
        // batch to a different output → 3 rows each.
        let total: usize = rows_per_partition.iter().sum();
        assert_eq!(total, 6);
        // Each output partition gets at most one batch (out of two).
        assert!(rows_per_partition.iter().all(|&n| n <= 3));
    }

    /// Spy pool that records the high-water mark of reserved bytes, so a
    /// test can assert "the channel reservation grew at some point" even
    /// though it returns to zero after a full drain.
    #[derive(Debug, Default)]
    struct PeakPool {
        used: std::sync::Mutex<usize>,
        peak: std::sync::Mutex<usize>,
    }
    impl crate::memory_pool::MemoryPool for PeakPool {
        fn register(&self, _c: &crate::memory_pool::MemoryConsumer) {}
        fn unregister(&self, _c: &crate::memory_pool::MemoryConsumer) {}
        fn try_grow(
            &self,
            _c: &crate::memory_pool::MemoryConsumer,
            additional: usize,
        ) -> Result<(), ExecutionError> {
            let mut u = self.used.lock().unwrap();
            *u += additional;
            let mut p = self.peak.lock().unwrap();
            *p = (*p).max(*u);
            Ok(())
        }
        fn shrink(&self, _c: &crate::memory_pool::MemoryConsumer, bytes: usize) {
            let mut u = self.used.lock().unwrap();
            *u = u.saturating_sub(bytes);
        }
        fn reserved(&self) -> usize {
            *self.used.lock().unwrap()
        }
    }

    #[tokio::test]
    async fn channel_bytes_are_pool_tracked_and_released() {
        let source: Arc<dyn crate::datasource::DataSource> = Arc::new(six_rows_two_batches());
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
        let peak_pool = Arc::new(PeakPool::default());
        let pool: Arc<dyn crate::memory_pool::MemoryPool> = peak_pool.clone();
        let rep = Arc::new(
            RepartitionExec::new(scan, Partitioning::RoundRobinBatch(2)).with_memory_pool(pool),
        );
        // Drain every output partition fully.
        for p in 0..2 {
            let stream = rep.execute(p).await.unwrap();
            let _ = collect_stream(stream).await.unwrap();
        }
        let peak = *peak_pool.peak.lock().unwrap();
        assert!(peak > 0, "channel bytes should be reserved while in flight");
        assert_eq!(
            peak_pool.reserved(),
            0,
            "all channel reservations must be released after a full drain (no leak)"
        );
    }

    #[tokio::test]
    async fn hash_repartition_same_key_same_partition() {
        // Build a single-batch source with keys {1, 1, 2, 2, 3, 3}.
        let s = Arc::new(Schema::new(vec![Field::new(
            "k",
            ArrowDataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(s, vec![Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3, 3]))])
                .unwrap();
        let source: Arc<dyn crate::datasource::DataSource> = Arc::new(InMemoryDataSource::new(
            vec![ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
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

        let key_expr = PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        };
        let rep = Arc::new(RepartitionExec::new(
            scan,
            Partitioning::Hash(vec![key_expr], 4),
        ));

        // Collect every output partition.
        let mut all = Vec::new();
        for p in 0..4 {
            let stream = rep.execute(p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            let mut values = Vec::new();
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..col.len() {
                    values.push(col.value(i));
                }
            }
            all.push(values);
        }

        // Every key value must live in exactly one partition.
        let mut owners = std::collections::HashMap::<i32, usize>::new();
        let mut total_rows = 0;
        for (p, vals) in all.iter().enumerate() {
            for &v in vals {
                total_rows += 1;
                if let Some(&prev) = owners.get(&v) {
                    assert_eq!(prev, p, "key {v} appeared in partitions {prev} and {p}");
                } else {
                    owners.insert(v, p);
                }
            }
        }
        assert_eq!(total_rows, 6, "all input rows must be present");
    }
}
