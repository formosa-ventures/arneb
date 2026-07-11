//! Bounded, partition-aware output buffer for task results.
//!
//! Each task writes its output to an [`OutputBuffer`] with one or more
//! partitions. Remote consumers (via Flight or ExchangeClient) read from
//! specific partitions.
//!
//! For broadcast joins (B.1, 2026-05-20) we expose a separate
//! [`BroadcastOutputBuffer`] whose producer writes once but whose batches
//! can be replayed independently to N consumers.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use arneb_common::error::ExecutionError;
use arneb_common::inflight_budget::InflightBudget;
use arneb_common::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
use arrow::array::{Array, RecordBatch};
use tokio::sync::{mpsc, Notify};

/// Bytes a batch holds, for memory accounting (Arrow array memory size).
fn batch_bytes(batch: &RecordBatch) -> usize {
    (0..batch.num_columns())
        .map(|i| batch.column(i).get_array_memory_size())
        .sum()
}

/// Releases an OutputBuffer reservation when the batch it accompanies
/// leaves the bounded channel — whether the consumer pulls it
/// ([`TrackedReceiver::recv`]) or the channel drops with the batch still
/// buffered (this `Drop`). Keeps the global pool's reserved total balanced
/// with no leak on either path. `bytes` is what `try_grow` granted (0 if
/// the pool declined — exec-memory-accounting D1 tracks best-effort and
/// never gates; gating/spill is D2).
struct OutputReservationGuard {
    pool: Arc<dyn MemoryPool>,
    consumer: MemoryConsumer,
    bytes: usize,
    inflight_budget: Arc<InflightBudget>,
    budget_bytes: u64,
}

impl Drop for OutputReservationGuard {
    fn drop(&mut self) {
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

/// A batch plus its in-flight reservation guard, as carried through the
/// OutputBuffer channel.
struct TrackedBatch {
    batch: RecordBatch,
    _guard: OutputReservationGuard,
}

/// Outcome of [`TrackedSender::try_send_pooled`] (exec-memory-accounting D2).
/// Distinguishes the two spill triggers — global pool pressure vs the fixed
/// channel cap — so the producer can route the overflow batch to disk in
/// either case. The rejected batch is handed back with NO reservation held.
#[derive(Debug)]
pub enum TrackedSendOutcome {
    /// Reserved against the pool and enqueued.
    Sent,
    /// `try_grow` would exceed the global pool — spill this batch (no reservation made).
    PoolFull(RecordBatch),
    /// Channel is at capacity — spill this batch (its reservation was released).
    ChannelFull(RecordBatch),
    /// Consumer dropped the receiver — cancel.
    Closed(RecordBatch),
}

/// Sender half that accounts each batch against the global [`MemoryPool`]
/// before enqueuing it. The public `send` API is identical to a raw
/// `mpsc::Sender<RecordBatch>` so producer call sites are unchanged.
pub struct TrackedSender {
    inner: mpsc::Sender<TrackedBatch>,
    pool: Arc<dyn MemoryPool>,
    consumer: MemoryConsumer,
    inflight_budget: Arc<InflightBudget>,
}

impl TrackedSender {
    /// Reserve the batch's bytes (best-effort) and wrap it with a release
    /// guard. D1: `try_grow` failure → granted 0, no gating.
    fn make_item(&self, batch: RecordBatch) -> TrackedBatch {
        let bytes = batch_bytes(&batch);
        let granted = if self.pool.try_grow(&self.consumer, bytes).is_ok() {
            bytes
        } else {
            0
        };
        TrackedBatch {
            batch,
            _guard: OutputReservationGuard {
                pool: Arc::clone(&self.pool),
                consumer: self.consumer.clone(),
                bytes: granted,
                inflight_budget: Arc::clone(&self.inflight_budget),
                budget_bytes: 0,
            },
        }
    }

    /// Wait for the shared byte budget, then reserve the batch's bytes
    /// (best-effort) and wrap it with a release guard.
    async fn make_item_budgeted(&self, batch: RecordBatch) -> TrackedBatch {
        let bytes = batch_bytes(&batch);
        self.inflight_budget.acquire(bytes as u64).await;
        let granted = if self.pool.try_grow(&self.consumer, bytes).is_ok() {
            bytes
        } else {
            0
        };
        TrackedBatch {
            batch,
            _guard: OutputReservationGuard {
                pool: Arc::clone(&self.pool),
                consumer: self.consumer.clone(),
                bytes: granted,
                inflight_budget: Arc::clone(&self.inflight_budget),
                budget_bytes: bytes as u64,
            },
        }
    }

    /// Reserve the batch's bytes (best-effort) and enqueue it. On channel
    /// failure the batch is returned (its reservation released on drop),
    /// mirroring `mpsc::Sender::send`'s `SendError<RecordBatch>`.
    pub async fn send(
        &self,
        batch: RecordBatch,
    ) -> Result<(), mpsc::error::SendError<RecordBatch>> {
        self.inner
            .send(self.make_item_budgeted(batch).await)
            .await
            .map_err(|e| mpsc::error::SendError(e.0.batch))
    }

    /// Non-blocking send, mirroring `mpsc::Sender::try_send`. On `Full` or
    /// `Closed` the batch is handed back (reservation released on drop) so
    /// the caller can spill or discard it.
    pub fn try_send(
        &self,
        batch: RecordBatch,
    ) -> Result<(), mpsc::error::TrySendError<RecordBatch>> {
        use mpsc::error::TrySendError;
        self.inner
            .try_send(self.make_item(batch))
            .map_err(|e| match e {
                TrySendError::Full(t) => TrySendError::Full(t.batch),
                TrySendError::Closed(t) => TrySendError::Closed(t.batch),
            })
    }

    /// exec-memory-accounting D2: non-blocking send that GATES on the global
    /// pool. Reserves the batch's bytes first; if `try_grow` would exceed the
    /// pool ([`TrackedSendOutcome::PoolFull`]) the batch is handed back with no
    /// reservation so the producer can spill it to disk — making pool pressure,
    /// not just the fixed channel cap, a spill trigger. A full channel still
    /// yields [`TrackedSendOutcome::ChannelFull`] (its reservation released on
    /// the returned guard's drop). With the default `UnboundedMemoryPool`
    /// (single-node / tests) `try_grow` never fails, so `PoolFull` never fires
    /// and the fast path is unchanged.
    pub fn try_send_pooled(&self, batch: RecordBatch) -> TrackedSendOutcome {
        let bytes = batch_bytes(&batch);
        if self.pool.try_grow(&self.consumer, bytes).is_err() {
            // No reservation made — hand the batch back to be spilled.
            return TrackedSendOutcome::PoolFull(batch);
        }
        let item = TrackedBatch {
            batch,
            _guard: OutputReservationGuard {
                pool: Arc::clone(&self.pool),
                consumer: self.consumer.clone(),
                bytes,
                inflight_budget: Arc::clone(&self.inflight_budget),
                budget_bytes: 0,
            },
        };
        use mpsc::error::TrySendError;
        match self.inner.try_send(item) {
            Ok(()) => TrackedSendOutcome::Sent,
            // Dropping the returned `TrackedBatch`'s guard releases the reservation.
            Err(TrySendError::Full(t)) => TrackedSendOutcome::ChannelFull(t.batch),
            Err(TrySendError::Closed(t)) => TrackedSendOutcome::Closed(t.batch),
        }
    }

    /// Async pooled send that blocks on the shared byte budget before trying
    /// to enqueue. The existing count cap and memory-pool spill outcomes are
    /// preserved.
    pub async fn send_pooled(&self, batch: RecordBatch) -> TrackedSendOutcome {
        let bytes = batch_bytes(&batch);
        self.inflight_budget.acquire(bytes as u64).await;
        if self.pool.try_grow(&self.consumer, bytes).is_err() {
            self.inflight_budget.release(bytes as u64);
            return TrackedSendOutcome::PoolFull(batch);
        }
        let item = TrackedBatch {
            batch,
            _guard: OutputReservationGuard {
                pool: Arc::clone(&self.pool),
                consumer: self.consumer.clone(),
                bytes,
                inflight_budget: Arc::clone(&self.inflight_budget),
                budget_bytes: bytes as u64,
            },
        };
        use mpsc::error::TrySendError;
        match self.inner.try_send(item) {
            Ok(()) => TrackedSendOutcome::Sent,
            Err(TrySendError::Full(t)) => TrackedSendOutcome::ChannelFull(t.batch),
            Err(TrySendError::Closed(t)) => TrackedSendOutcome::Closed(t.batch),
        }
    }
}

/// Receiver half that releases each batch's reservation as it is pulled
/// (the guard drops here). Yields bare `RecordBatch`, so consumer call
/// sites are unchanged.
pub struct TrackedReceiver {
    inner: mpsc::Receiver<TrackedBatch>,
}

impl TrackedReceiver {
    /// Receive the next batch, releasing its in-flight reservation.
    pub async fn recv(&mut self) -> Option<RecordBatch> {
        // Dropping the `TrackedBatch`'s guard (the field not moved out)
        // releases the reservation as the batch leaves the channel.
        self.inner.recv().await.map(|t| t.batch)
    }
}

/// A bounded buffer where a task writes output RecordBatches partitioned
/// by index, and remote consumers read from specific partitions.
///
/// **B-fix-3 (2026-05-22)**: carries a shared `failure` flag so a pumper
/// that errors mid-stream can record the cause BEFORE dropping its
/// sender. The Flight `do_get` handler in `flight_service::async_stream`
/// checks this flag after the receiver yields `None`; if set, the
/// stream emits a `tonic::Status::internal` error instead of clean EOF.
/// Without this, a worker task that crashed silently became a partial-
/// result query (Q05 returned 3/5 rows, Q09 0/25, see commit eea9b11
/// post-mortem).
pub struct OutputBuffer {
    senders: Vec<mpsc::Sender<TrackedBatch>>,
    receivers: Vec<Option<mpsc::Receiver<TrackedBatch>>>,
    schema: Arc<arrow::datatypes::Schema>,
    failure: Arc<Mutex<Option<String>>>,
    /// Global pool the in-flight buffered batches are accounted against.
    /// Defaults to [`UnboundedMemoryPool`] (tests / single-node); the
    /// distributed worker installs the cgroup-derived pool via
    /// [`Self::with_memory_pool`]. exec-memory-accounting D1 (2026-06-04).
    memory_pool: Arc<dyn MemoryPool>,
    inflight_budget: Arc<InflightBudget>,
}

impl OutputBuffer {
    /// Creates a new output buffer with the given number of partitions
    /// and per-partition channel capacity.
    pub fn new(
        num_partitions: usize,
        capacity: usize,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Self {
        let mut senders = Vec::with_capacity(num_partitions);
        let mut receivers = Vec::with_capacity(num_partitions);

        for _ in 0..num_partitions {
            let (tx, rx) = mpsc::channel(capacity);
            senders.push(tx);
            receivers.push(Some(rx));
        }

        Self {
            senders,
            receivers,
            schema,
            failure: Arc::new(Mutex::new(None)),
            memory_pool: Arc::new(UnboundedMemoryPool::new()),
            inflight_budget: Arc::new(InflightBudget::new(0)),
        }
    }

    /// Creates a single-partition buffer.
    pub fn single(capacity: usize, schema: Arc<arrow::datatypes::Schema>) -> Self {
        Self::new(1, capacity, schema)
    }

    /// Install the global [`MemoryPool`] the in-flight buffered batches are
    /// reserved against, so the exchange's staged bytes are visible to the
    /// worker memory limit. Builder; call right after construction (before
    /// any batch is written). exec-memory-accounting D1.
    pub fn with_memory_pool(mut self, memory_pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = memory_pool;
        self
    }

    /// Install the shared per-stage in-flight byte budget used to block
    /// producers before batches enter this output buffer.
    pub fn with_inflight_budget(mut self, inflight_budget: Arc<InflightBudget>) -> Self {
        self.inflight_budget = inflight_budget;
        self
    }

    /// Returns the output schema.
    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> usize {
        self.senders.len()
    }

    /// Write a batch to the specified partition.
    /// Returns error if the partition is invalid or the receiver has been dropped.
    pub async fn write_batch(
        &self,
        partition_id: usize,
        batch: RecordBatch,
    ) -> Result<(), ExecutionError> {
        let sender = self.senders.get(partition_id).ok_or_else(|| {
            ExecutionError::InvalidOperation(format!(
                "partition {partition_id} out of range (max {})",
                self.senders.len()
            ))
        })?;
        let num_rows = batch.num_rows();
        let num_bytes = batch_bytes(&batch);
        // Reserve + send via a TrackedSender so the in-flight batch is
        // accounted against the pool (released when the consumer pulls it).
        let tracked = TrackedSender {
            inner: sender.clone(),
            pool: Arc::clone(&self.memory_pool),
            consumer: MemoryConsumer::new("OutputBuffer.channel"),
            inflight_budget: Arc::clone(&self.inflight_budget),
        };
        let t_send = Instant::now();
        let res = tracked.send(batch).await.map_err(|_| {
            ExecutionError::InvalidOperation(format!(
                "output buffer partition {partition_id} receiver dropped"
            ))
        });
        let send_ms = t_send.elapsed().as_millis() as u64;
        // Only log slow sends — the cap=64 channel back-pressure is the
        // signal we care about for Q09 profiling.
        if send_ms >= 5 {
            tracing::info!(
                target: "arneb::profile",
                op = "OutputBuffer.write_batch",
                partition_id,
                num_rows,
                num_bytes,
                send_ms,
                "OutputBuffer back-pressure (>=5ms send)"
            );
        }
        res
    }

    /// Take the receiver for a partition. Can only be called once per partition.
    /// The receiver yields RecordBatches as they are written.
    pub fn take_receiver(&mut self, partition_id: usize) -> Option<TrackedReceiver> {
        let inner = self.receivers.get_mut(partition_id)?.take()?;
        Some(TrackedReceiver { inner })
    }

    /// True when every partition's receiver has been taken by a consumer.
    pub fn all_receivers_taken(&self) -> bool {
        self.receivers.iter().all(|r| r.is_none())
    }

    /// Take the senders out of this buffer so a producer can pump batches
    /// into the channels without holding the buffer's lock. The buffer is
    /// then safe to register with `FlightState::register_partitioned_buffer` — the
    /// remote consumer's `do_get` only needs `take_receiver`, which still
    /// works since the receivers stay inside.
    ///
    /// EOF semantics: when the returned senders are all dropped, every
    /// receiver yields `None` on its next `recv()`. That's how a streaming
    /// worker signals "task complete" without needing a separate close
    /// RPC.
    pub fn take_senders(&mut self) -> Vec<TrackedSender> {
        std::mem::take(&mut self.senders)
            .into_iter()
            .map(|inner| TrackedSender {
                inner,
                pool: Arc::clone(&self.memory_pool),
                consumer: MemoryConsumer::new("OutputBuffer.channel"),
                inflight_budget: Arc::clone(&self.inflight_budget),
            })
            .collect()
    }

    /// Clone the shared failure flag. Producers (worker pumper tasks)
    /// call `set_failure` on the returned handle when they error;
    /// consumers (Flight `do_get`) call `take_failure` after the EOF
    /// to detect that the stream is truncated because of a producer
    /// error rather than a normal end of input. B-fix-3 (2026-05-22).
    pub fn failure_handle(&self) -> Arc<Mutex<Option<String>>> {
        Arc::clone(&self.failure)
    }

    /// Returns and clears the recorded failure message, if any.
    pub fn take_failure(&self) -> Option<String> {
        self.failure.lock().ok()?.take()
    }

    /// Signal that no more data will be written. Drops all senders.
    pub fn finish(self) {
        // Dropping senders closes the channels, signaling EOF to receivers.
        drop(self.senders);
    }

    /// Close all senders without consuming self. Signals EOF to all receivers.
    pub fn close(&mut self) {
        self.senders.clear();
    }
}

impl std::fmt::Debug for OutputBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputBuffer")
            .field("num_partitions", &self.senders.len())
            .finish()
    }
}

// ===========================================================================
// BroadcastOutputBuffer
// ===========================================================================
//
// B.1 (2026-05-20): the producer side of a broadcast exchange. All batches
// are accumulated in memory; every subscribed consumer streams the full set
// independently. Ports the "broadcast: bool" pattern from Ballista's
// `shuffle_reader.rs` (Apache-2.0) but adapted to arneb's streaming Flight
// model — Ballista materialises the shuffle output to disk and lets multiple
// readers open the same file; arneb keeps batches in memory because
// broadcast is only used when the build side is known to be small.
//
// Producer contract: write batches via `write_batch`, then call `finish()`
// when the source is exhausted. Subscribers can join at any time; late
// subscribers replay the historical batches before tailing new ones.

struct BroadcastInner {
    state: Mutex<BroadcastState>,
    notify: Notify,
    /// B-fix-3 parity (A2.1.2, 2026-05-28): shared failure flag so a
    /// producer pumper that errors mid-stream can record the cause
    /// BEFORE calling `finish()`. The Flight `do_get` handler in
    /// `async_stream_broadcast` checks this after the `BroadcastStream`
    /// drains; if set, the stream emits a `tonic::Status::internal`
    /// error instead of clean EOF. Mirrors the `OutputBuffer::failure`
    /// field so partitioned + broadcast share the same propagation
    /// model.
    failure: Arc<Mutex<Option<String>>>,
}

struct BroadcastState {
    batches: Vec<RecordBatch>,
    closed: bool,
}

/// Output buffer where the producer writes a sequence of `RecordBatch`es
/// once and an arbitrary number of consumers each stream the full set
/// independently. Used for broadcast joins / replicated exchanges where
/// every downstream task needs the full upstream output.
pub struct BroadcastOutputBuffer {
    schema: Arc<arrow::datatypes::Schema>,
    inner: Arc<BroadcastInner>,
}

impl BroadcastOutputBuffer {
    /// Creates an empty broadcast buffer with the given output schema.
    pub fn new(schema: Arc<arrow::datatypes::Schema>) -> Self {
        Self {
            schema,
            inner: Arc::new(BroadcastInner {
                state: Mutex::new(BroadcastState {
                    batches: Vec::new(),
                    closed: false,
                }),
                notify: Notify::new(),
                failure: Arc::new(Mutex::new(None)),
            }),
        }
    }

    /// Output schema (shared with subscribers).
    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }

    /// Append a batch and notify all currently waiting subscribers.
    pub fn write_batch(&self, batch: RecordBatch) {
        self.inner.state.lock().unwrap().batches.push(batch);
        self.inner.notify.notify_waiters();
    }

    /// Signal that no more batches will arrive. Subscribers that have
    /// drained the in-memory buffer will receive `None` on their next
    /// `next()` call.
    pub fn finish(&self) {
        self.inner.state.lock().unwrap().closed = true;
        self.inner.notify.notify_waiters();
    }

    /// Create a new consumer stream. Each call returns an independent
    /// `BroadcastStream` that replays the historical batches before
    /// tailing new ones. Multiple consumers can be subscribed
    /// concurrently; they do not interfere.
    pub fn subscribe(&self) -> BroadcastStream {
        BroadcastStream {
            inner: Arc::clone(&self.inner),
            next_idx: 0,
        }
    }

    /// Clone the shared failure flag. B-fix-3 parity (A2.1.2, 2026-05-28):
    /// the producer pumper task calls `set` on the returned handle when
    /// it errors; the Flight `do_get` handler calls `take_failure()`
    /// after the consumer's `BroadcastStream` returns `None` to detect
    /// that the stream is truncated because of a producer error rather
    /// than a normal end of input.
    pub fn failure_handle(&self) -> Arc<Mutex<Option<String>>> {
        Arc::clone(&self.inner.failure)
    }

    /// Returns and clears the recorded failure message, if any.
    /// Mirror of `OutputBuffer::take_failure`.
    pub fn take_failure(&self) -> Option<String> {
        self.inner.failure.lock().ok()?.take()
    }
}

impl std::fmt::Debug for BroadcastOutputBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.state.lock().unwrap();
        f.debug_struct("BroadcastOutputBuffer")
            .field("batches", &state.batches.len())
            .field("closed", &state.closed)
            .finish()
    }
}

/// Independent consumer stream over a [`BroadcastOutputBuffer`].
pub struct BroadcastStream {
    inner: Arc<BroadcastInner>,
    next_idx: usize,
}

impl BroadcastStream {
    /// Returns the next batch, replaying historical ones first. Returns
    /// `None` when the producer has called `finish()` and the consumer
    /// has drained all batches.
    pub async fn next(&mut self) -> Option<RecordBatch> {
        loop {
            // Register interest in the next notification BEFORE inspecting
            // state, so a `notify_waiters()` that races with our lock
            // release is still captured.
            let notified = self.inner.notify.notified();
            {
                let state = self.inner.state.lock().unwrap();
                if let Some(batch) = state.batches.get(self.next_idx) {
                    let batch = batch.clone();
                    self.next_idx += 1;
                    return Some(batch);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(schema: &Arc<Schema>, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    /// Spy pool recording the high-water mark of reserved bytes.
    #[derive(Debug, Default)]
    struct PeakPool {
        used: Mutex<usize>,
        peak: Mutex<usize>,
    }
    impl MemoryPool for PeakPool {
        fn register(&self, _c: &MemoryConsumer) {}
        fn unregister(&self, _c: &MemoryConsumer) {}
        fn try_grow(&self, _c: &MemoryConsumer, additional: usize) -> Result<(), ExecutionError> {
            let mut u = self.used.lock().unwrap();
            *u += additional;
            let mut p = self.peak.lock().unwrap();
            *p = (*p).max(*u);
            Ok(())
        }
        fn shrink(&self, _c: &MemoryConsumer, bytes: usize) {
            let mut u = self.used.lock().unwrap();
            *u = u.saturating_sub(bytes);
        }
        fn reserved(&self) -> usize {
            *self.used.lock().unwrap()
        }
    }

    /// Pool that fails `try_grow` once `used + additional` would exceed `limit`.
    #[derive(Debug)]
    struct LimitPool {
        used: Mutex<usize>,
        limit: usize,
    }
    impl LimitPool {
        fn new(limit: usize) -> Self {
            Self {
                used: Mutex::new(0),
                limit,
            }
        }
    }
    impl MemoryPool for LimitPool {
        fn register(&self, _c: &MemoryConsumer) {}
        fn unregister(&self, _c: &MemoryConsumer) {}
        fn try_grow(&self, _c: &MemoryConsumer, additional: usize) -> Result<(), ExecutionError> {
            let mut u = self.used.lock().unwrap();
            if *u + additional > self.limit {
                return Err(ExecutionError::ResourceExhausted("limit".into()));
            }
            *u += additional;
            Ok(())
        }
        fn shrink(&self, _c: &MemoryConsumer, bytes: usize) {
            let mut u = self.used.lock().unwrap();
            *u = u.saturating_sub(bytes);
        }
        fn reserved(&self) -> usize {
            *self.used.lock().unwrap()
        }
    }

    #[tokio::test]
    async fn try_send_pooled_spills_on_pool_pressure() {
        // exec-memory-accounting D2: when the global pool would be exceeded,
        // `try_send_pooled` returns `PoolFull` (caller spills) instead of
        // enqueuing an un-reserved batch — the spill trigger is pool pressure,
        // not the fixed channel cap. The channel here is large (1000) so a
        // `ChannelFull` cannot fire first and confound the test.
        let schema = test_schema();
        let one = batch_bytes(&test_batch(&schema, vec![1, 2, 3]));
        // Limit fits exactly one batch.
        let pool: Arc<dyn MemoryPool> = Arc::new(LimitPool::new(one));
        let mut buf = OutputBuffer::new(1, 1000, schema.clone()).with_memory_pool(pool.clone());
        let _rx = buf.take_receiver(0); // keep the channel open (not closed)
        let sender = buf.take_senders().pop().unwrap();

        // First batch fits the pool → Sent.
        assert!(matches!(
            sender.try_send_pooled(test_batch(&schema, vec![1, 2, 3])),
            TrackedSendOutcome::Sent
        ));
        // Second batch would exceed the pool → PoolFull (spill), batch handed
        // back, NO reservation made (pool still at exactly one batch).
        match sender.try_send_pooled(test_batch(&schema, vec![4, 5, 6])) {
            TrackedSendOutcome::PoolFull(b) => assert_eq!(b.num_rows(), 3),
            other => panic!("expected PoolFull, got {other:?}"),
        }
        assert_eq!(
            pool.reserved(),
            one,
            "PoolFull must not reserve the rejected batch"
        );
    }

    #[tokio::test]
    async fn try_send_pooled_fast_path_unbounded_never_pool_full() {
        // Default Unbounded pool (single-node / tests): try_grow always
        // succeeds → never PoolFull, so the fast path is unchanged.
        let schema = test_schema();
        let mut buf = OutputBuffer::new(1, 1000, schema.clone()); // default Unbounded
        let _rx = buf.take_receiver(0);
        let sender = buf.take_senders().pop().unwrap();
        for v in 0..50 {
            assert!(matches!(
                sender.try_send_pooled(test_batch(&schema, vec![v])),
                TrackedSendOutcome::Sent
            ));
        }
    }

    #[tokio::test]
    async fn output_buffer_bytes_are_pool_tracked_and_released() {
        let schema = test_schema();
        let peak_pool = Arc::new(PeakPool::default());
        let pool: Arc<dyn MemoryPool> = peak_pool.clone();
        let mut buf = OutputBuffer::single(32, schema.clone()).with_memory_pool(pool);
        let mut rx = buf.take_receiver(0).unwrap();

        buf.write_batch(0, test_batch(&schema, vec![1, 2, 3]))
            .await
            .unwrap();
        // While buffered, the reservation is held.
        assert!(
            peak_pool.reserved() > 0,
            "batch buffered in channel should hold a reservation"
        );
        let peak_while_buffered = *peak_pool.peak.lock().unwrap();
        assert!(peak_while_buffered > 0);

        // Consuming releases it.
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            peak_pool.reserved(),
            0,
            "reservation released once the consumer pulls the batch (no leak)"
        );
    }

    #[tokio::test]
    async fn single_partition_write_read() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(32, schema.clone());
        let mut rx = buf.take_receiver(0).unwrap();

        buf.write_batch(0, test_batch(&schema, vec![1, 2, 3]))
            .await
            .unwrap();

        // Read from receiver.
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn multi_partition_write_read() {
        let schema = test_schema();
        let mut buf = OutputBuffer::new(3, 32, schema.clone());
        let mut rx0 = buf.take_receiver(0).unwrap();
        let mut rx1 = buf.take_receiver(1).unwrap();
        let mut rx2 = buf.take_receiver(2).unwrap();

        buf.write_batch(0, test_batch(&schema, vec![1]))
            .await
            .unwrap();
        buf.write_batch(1, test_batch(&schema, vec![2]))
            .await
            .unwrap();
        buf.write_batch(2, test_batch(&schema, vec![3]))
            .await
            .unwrap();

        assert_eq!(rx0.recv().await.unwrap().num_rows(), 1);
        assert_eq!(rx1.recv().await.unwrap().num_rows(), 1);
        assert_eq!(rx2.recv().await.unwrap().num_rows(), 1);
    }

    #[tokio::test]
    async fn finish_closes_channels() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(32, schema.clone());
        let mut rx = buf.take_receiver(0).unwrap();

        buf.write_batch(0, test_batch(&schema, vec![1]))
            .await
            .unwrap();
        buf.finish();

        // Should get the batch, then None (EOF).
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn invalid_partition_returns_error() {
        let schema = test_schema();
        let buf = OutputBuffer::single(32, schema.clone());
        let result = buf.write_batch(5, test_batch(&schema, vec![1])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn take_receiver_only_once() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(32, schema);
        assert!(buf.take_receiver(0).is_some());
        assert!(buf.take_receiver(0).is_none()); // second call returns None
    }

    // ----- BroadcastOutputBuffer -----

    #[tokio::test]
    async fn broadcast_single_consumer_reads_all() {
        let schema = test_schema();
        let buf = BroadcastOutputBuffer::new(schema.clone());
        let mut sub = buf.subscribe();

        buf.write_batch(test_batch(&schema, vec![1, 2]));
        buf.write_batch(test_batch(&schema, vec![3]));
        buf.finish();

        assert_eq!(sub.next().await.unwrap().num_rows(), 2);
        assert_eq!(sub.next().await.unwrap().num_rows(), 1);
        assert!(sub.next().await.is_none(), "EOF after finish + drain");
    }

    #[tokio::test]
    async fn broadcast_two_consumers_each_get_full_set() {
        let schema = test_schema();
        let buf = BroadcastOutputBuffer::new(schema.clone());
        buf.write_batch(test_batch(&schema, vec![10]));
        buf.write_batch(test_batch(&schema, vec![20, 30]));
        buf.finish();

        let mut a = buf.subscribe();
        let mut b = buf.subscribe();

        assert_eq!(a.next().await.unwrap().num_rows(), 1);
        assert_eq!(a.next().await.unwrap().num_rows(), 2);
        assert!(a.next().await.is_none());

        assert_eq!(b.next().await.unwrap().num_rows(), 1);
        assert_eq!(b.next().await.unwrap().num_rows(), 2);
        assert!(b.next().await.is_none());
    }

    #[tokio::test]
    async fn broadcast_late_subscriber_replays_history() {
        let schema = test_schema();
        let buf = BroadcastOutputBuffer::new(schema.clone());
        buf.write_batch(test_batch(&schema, vec![1]));
        buf.write_batch(test_batch(&schema, vec![2, 3]));

        let mut late = buf.subscribe();
        buf.finish();
        assert_eq!(late.next().await.unwrap().num_rows(), 1);
        assert_eq!(late.next().await.unwrap().num_rows(), 2);
        assert!(late.next().await.is_none());
    }

    #[tokio::test]
    async fn broadcast_consumer_wakes_on_new_batch() {
        let schema = test_schema();
        let buf = Arc::new(BroadcastOutputBuffer::new(schema.clone()));
        let mut sub = buf.subscribe();

        // Spawn a task that writes after a short delay.
        let buf2 = buf.clone();
        let schema2 = schema.clone();
        let producer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            buf2.write_batch(test_batch(&schema2, vec![42]));
            buf2.finish();
        });

        // Consumer blocks until producer writes.
        let batch = sub
            .next()
            .await
            .expect("should receive after producer wakes us");
        assert_eq!(batch.num_rows(), 1);
        assert!(sub.next().await.is_none());
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn broadcast_empty_finish_eofs_immediately() {
        let schema = test_schema();
        let buf = BroadcastOutputBuffer::new(schema);
        let mut sub = buf.subscribe();
        buf.finish();
        assert!(sub.next().await.is_none());
    }

    #[test]
    fn broadcast_schema_round_trip() {
        let schema = test_schema();
        let buf = BroadcastOutputBuffer::new(schema.clone());
        assert!(Arc::ptr_eq(&buf.schema(), &schema));
    }
}
