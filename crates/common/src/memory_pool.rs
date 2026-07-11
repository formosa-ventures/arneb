//! Memory budget framework for spillable operators.
//!
//! Vendor-port of DataFusion's `MemoryPool` API (Apache-2.0). The shape
//! is intentionally narrow:
//!
//!   - [`MemoryPool`] is a trait the global pool implements.
//!   - [`MemoryConsumer`] names an operator that wants memory.
//!   - [`MemoryReservation`] is the live handle: grow/shrink/free.
//!   - [`GreedyMemoryPool`] is the only implementation for now;
//!     first-come-first-served until the budget is exhausted.
//!
//! Spill is left to the operator: `try_grow` returns
//! `Err(ExecutionError::ResourceExhausted)` and the operator decides
//! whether to spill, fail, or compact. arneb's first spill consumer
//! will be `SemiJoinExec`; see the design note
//! `project_2026-05-21_arneb_spill_design.md`.
//!
//! Differences from DataFusion (intentional):
//!   - No `FairSpillPool`. Arneb tasks run at most one spillable
//!     operator at a time today (M×N join model), so per-consumer
//!     fairness is not needed yet.
//!   - No async ops. DF's pool ops are sync; we keep that.
//!   - `register`/`unregister` are no-ops on the pool — consumer
//!     identity is only used for error messages and (future) FairSpill
//!     accounting.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::error::ExecutionError;

/// Read-only memory profile snapshot emitted by an optional diagnostic
/// pool wrapper.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryProfileSnapshot {
    /// High-water mark of tracked pool reservations in bytes.
    pub pool_peak_bytes: u64,
    /// High-water mark of jemalloc `stats.resident` sampled by the wrapper.
    pub jemalloc_resident_peak_bytes: u64,
    /// High-water mark of jemalloc `stats.allocated` sampled by the wrapper.
    pub jemalloc_allocated_peak_bytes: u64,
    /// High-water mark of jemalloc `stats.active` sampled by the wrapper.
    pub jemalloc_active_peak_bytes: u64,
    /// High-water mark of jemalloc `stats.retained` sampled by the wrapper.
    pub jemalloc_retained_peak_bytes: u64,
    /// Top consumers by peak held reservation, as `(name, bytes)`.
    pub top_consumers: Vec<(String, u64)>,
}

/// A budget-managed memory pool that operators reserve from.
pub trait MemoryPool: fmt::Debug + Send + Sync + 'static {
    /// Notify the pool that a new consumer exists.
    fn register(&self, consumer: &MemoryConsumer);

    /// Notify the pool that a consumer is going away. Called when the
    /// last `MemoryReservation` for this consumer is dropped.
    fn unregister(&self, consumer: &MemoryConsumer);

    /// Try to grow the pool's reserved count by `additional` bytes on
    /// behalf of `consumer`. Returns
    /// `Err(ExecutionError::ResourceExhausted)` when the pool's limit
    /// would be exceeded; the pool's state is unchanged in that case.
    fn try_grow(&self, consumer: &MemoryConsumer, additional: usize) -> Result<(), ExecutionError>;

    /// Release `bytes` from the pool's reserved count. The caller is
    /// responsible for ensuring it has actually freed that much.
    fn shrink(&self, consumer: &MemoryConsumer, bytes: usize);

    /// Current total bytes reserved across all consumers.
    fn reserved(&self) -> usize;

    /// High-water mark of [`reserved`](Self::reserved) over this pool's
    /// lifetime. Default returns the current reservation (for pools that
    /// don't track a peak). The worker reports this vs RSS to measure the
    /// tracked-allocation fraction (exec-memory-accounting; the SF30 goal
    /// is to drive tracked_peak toward RSS so spill decisions are honest).
    fn reserved_peak(&self) -> usize {
        self.reserved()
    }

    /// Optional read-only diagnostic snapshot for gated memory profiling.
    fn memory_profile_snapshot(&self) -> Option<MemoryProfileSnapshot> {
        None
    }
}

/// Identifies an operator that wants memory.
///
/// Construct with `MemoryConsumer::new("OperatorName")`, then call
/// `register(pool)` to obtain a live `MemoryReservation`.
#[derive(Debug, Clone)]
pub struct MemoryConsumer {
    name: String,
    can_spill: bool,
}

impl MemoryConsumer {
    /// Construct a consumer identified by `name` (used in error
    /// messages and logs). `can_spill` defaults to false.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            can_spill: false,
        }
    }

    /// Marker for spillable operators. Currently advisory — the pool
    /// ignores it. Future FairSpillPool will use it to balance
    /// allocations across competing spillable consumers.
    pub fn with_can_spill(mut self, can_spill: bool) -> Self {
        self.can_spill = can_spill;
        self
    }

    /// The consumer's display name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether this consumer is marked spillable.
    pub fn can_spill(&self) -> bool {
        self.can_spill
    }

    /// Register this consumer with `pool` and obtain a fresh
    /// reservation. The reservation owns an `Arc<dyn MemoryPool>`, so
    /// the pool stays alive as long as any reservation does.
    pub fn register(self, pool: Arc<dyn MemoryPool>) -> MemoryReservation {
        pool.register(&self);
        MemoryReservation {
            consumer: self,
            size: 0,
            pool,
        }
    }
}

/// A live memory reservation held by an operator.
///
/// `try_grow`/`shrink` adjust the reserved size; `free` (or Drop)
/// returns all outstanding bytes.
#[derive(Debug)]
pub struct MemoryReservation {
    consumer: MemoryConsumer,
    size: usize,
    pool: Arc<dyn MemoryPool>,
}

impl MemoryReservation {
    /// Bytes currently reserved by this handle.
    pub fn size(&self) -> usize {
        self.size
    }

    /// The consumer identity this reservation belongs to.
    pub fn consumer(&self) -> &MemoryConsumer {
        &self.consumer
    }

    /// Try to grow by `additional` bytes. On failure the reservation's
    /// size is unchanged; the caller may then spill, free, or fail.
    pub fn try_grow(&mut self, additional: usize) -> Result<(), ExecutionError> {
        self.pool.try_grow(&self.consumer, additional)?;
        self.size += additional;
        Ok(())
    }

    /// Try to reach exactly `target` bytes. If `target` is larger,
    /// grows; if smaller, shrinks (cannot fail in the shrink case).
    pub fn try_resize(&mut self, target: usize) -> Result<(), ExecutionError> {
        if target > self.size {
            self.try_grow(target - self.size)
        } else {
            self.shrink(self.size - target);
            Ok(())
        }
    }

    /// Shrink by `bytes` bytes. Saturating: never below zero.
    pub fn shrink(&mut self, bytes: usize) {
        let bytes = bytes.min(self.size);
        if bytes > 0 {
            self.pool.shrink(&self.consumer, bytes);
            self.size -= bytes;
        }
    }

    /// Release all outstanding bytes.
    pub fn free(&mut self) {
        if self.size > 0 {
            self.pool.shrink(&self.consumer, self.size);
            self.size = 0;
        }
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.free();
        self.pool.unregister(&self.consumer);
    }
}

// ===========================================================================
// GreedyMemoryPool
// ===========================================================================

/// First-come-first-served pool with a fixed byte budget.
///
/// `try_grow` fails when the total reserved would exceed `limit`. No
/// consideration of which consumer asked, no fair-share scheduling.
/// Suitable when at most one spillable consumer is active per task,
/// which matches arneb's current execution model (one HashJoin/SemiJoin
/// build phase at a time on a worker α-task).
#[derive(Debug)]
pub struct GreedyMemoryPool {
    limit: usize,
    used: Mutex<usize>,
    /// High-water mark of `used` (exec-memory-accounting observability).
    peak: Mutex<usize>,
}

impl GreedyMemoryPool {
    /// New pool with a fixed byte `limit`.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            used: Mutex::new(0),
            peak: Mutex::new(0),
        }
    }

    /// The pool's hard limit in bytes.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn register(&self, _consumer: &MemoryConsumer) {}
    fn unregister(&self, _consumer: &MemoryConsumer) {}

    fn try_grow(&self, consumer: &MemoryConsumer, additional: usize) -> Result<(), ExecutionError> {
        let mut used = self.used.lock().expect("memory pool mutex poisoned");
        let new_total = used.saturating_add(additional);
        if new_total > self.limit {
            return Err(ExecutionError::ResourceExhausted(format!(
                "memory pool exhausted: consumer '{}' requested {} bytes; pool used {}/{} bytes",
                consumer.name(),
                additional,
                *used,
                self.limit
            )));
        }
        *used = new_total;
        let mut peak = self.peak.lock().expect("memory pool mutex poisoned");
        *peak = (*peak).max(new_total);
        Ok(())
    }

    fn shrink(&self, _consumer: &MemoryConsumer, bytes: usize) {
        let mut used = self.used.lock().expect("memory pool mutex poisoned");
        *used = used.saturating_sub(bytes);
    }

    fn reserved(&self) -> usize {
        *self.used.lock().expect("memory pool mutex poisoned")
    }

    fn reserved_peak(&self) -> usize {
        *self.peak.lock().expect("memory pool mutex poisoned")
    }
}

impl fmt::Display for GreedyMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GreedyMemoryPool({}/{} bytes used)",
            self.reserved(),
            self.limit
        )
    }
}

// ===========================================================================
// UnboundedMemoryPool
// ===========================================================================

/// No-budget pool used by single-node / non-spilling code paths and
/// most tests. `try_grow` never fails; `reserved` still tracks the
/// total so callers can observe high-water marks.
#[derive(Debug, Default)]
pub struct UnboundedMemoryPool {
    used: Mutex<usize>,
    /// High-water mark of `used` (exec-memory-accounting observability).
    peak: Mutex<usize>,
}

impl UnboundedMemoryPool {
    /// New pool with no budget cap.
    pub fn new() -> Self {
        Self::default()
    }
}

impl MemoryPool for UnboundedMemoryPool {
    fn register(&self, _consumer: &MemoryConsumer) {}
    fn unregister(&self, _consumer: &MemoryConsumer) {}

    fn try_grow(
        &self,
        _consumer: &MemoryConsumer,
        additional: usize,
    ) -> Result<(), ExecutionError> {
        let mut used = self.used.lock().expect("memory pool mutex poisoned");
        *used = used.saturating_add(additional);
        let mut peak = self.peak.lock().expect("memory pool mutex poisoned");
        *peak = (*peak).max(*used);
        Ok(())
    }

    fn shrink(&self, _consumer: &MemoryConsumer, bytes: usize) {
        let mut used = self.used.lock().expect("memory pool mutex poisoned");
        *used = used.saturating_sub(bytes);
    }

    fn reserved_peak(&self) -> usize {
        *self.peak.lock().expect("memory pool mutex poisoned")
    }

    fn reserved(&self) -> usize {
        *self.used.lock().expect("memory pool mutex poisoned")
    }
}

// ===========================================================================
// QueryMemoryPool
// ===========================================================================

/// Decorator that wraps an underlying [`MemoryPool`] with a per-instance
/// (typically per-query / per-task) byte cap.
///
/// Phase 4 (2026-05-21): Trino-style `query.max-memory-per-node` analog.
/// Per the spill research note, the canonical industry design is
/// *both* per-operator MemoryReservation (already in arneb, drives
/// cooperative spilling) AND a query-scoped tracker that closes the
/// "swiss-cheese" gap where untracked operators (Filter, Projection,
/// scan buffers, repartition queue, etc.) accumulate uncounted
/// allocations and OOM the worker before any single tracked operator
/// hits its own budget.
///
/// `try_grow` checks BOTH caps:
///   1. The query's own subtotal `≤ query_limit` (or this pool errors
///      with `"query memory limit exceeded"`).
///   2. The global pool's `try_grow` (cascades to the underlying
///      `GreedyMemoryPool` / `UnboundedMemoryPool`).
///
/// On global exhaustion the query bucket isn't grown (atomicity).
///
/// Construct one per query/task; drop releases all bytes from both
/// layers.
#[derive(Debug)]
pub struct QueryMemoryPool {
    global: Arc<dyn MemoryPool>,
    query_limit: usize,
    used: Mutex<usize>,
}

impl QueryMemoryPool {
    /// New per-query pool wrapping `global`. The query's own budget is
    /// `query_limit` bytes; the global pool may add its own cap on top.
    pub fn new(global: Arc<dyn MemoryPool>, query_limit: usize) -> Self {
        Self {
            global,
            query_limit,
            used: Mutex::new(0),
        }
    }

    /// The per-query byte cap.
    pub fn query_limit(&self) -> usize {
        self.query_limit
    }
}

impl MemoryPool for QueryMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.global.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.global.unregister(consumer);
    }

    fn try_grow(&self, consumer: &MemoryConsumer, additional: usize) -> Result<(), ExecutionError> {
        let mut used = self.used.lock().expect("memory pool mutex poisoned");
        let new_total = used.saturating_add(additional);
        if new_total > self.query_limit {
            return Err(ExecutionError::ResourceExhausted(format!(
                "query memory limit exceeded: consumer '{}' requested {} bytes; \
                 query used {}/{} bytes (per-query cap)",
                consumer.name(),
                additional,
                *used,
                self.query_limit
            )));
        }
        // Cascade to the global pool. If it fails, the query bucket
        // is unchanged (atomicity).
        self.global.try_grow(consumer, additional)?;
        *used = new_total;
        Ok(())
    }

    fn shrink(&self, consumer: &MemoryConsumer, bytes: usize) {
        // Shrink both layers in lockstep.
        let mut used = self.used.lock().expect("memory pool mutex poisoned");
        let to_shrink = bytes.min(*used);
        if to_shrink > 0 {
            *used -= to_shrink;
        }
        self.global.shrink(consumer, bytes);
    }

    fn reserved(&self) -> usize {
        *self.used.lock().expect("memory pool mutex poisoned")
    }

    /// Worker-level peak: delegate to the underlying global pool (the
    /// per-query wrapper sits in front of the shared worker pool, so the
    /// global's high-water is the worker's tracked peak).
    fn reserved_peak(&self) -> usize {
        self.global.reserved_peak()
    }
}

impl fmt::Display for QueryMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueryMemoryPool({}/{} bytes used; over {})",
            self.reserved(),
            self.query_limit,
            self.global.reserved()
        )
    }
}

// ===========================================================================
// TrackConsumersPool
// ===========================================================================

/// Decorator that records per-consumer reserved bytes so a `try_grow`
/// failure can name the largest consumers in its error. exec-memory-accounting
/// D4: pure diagnostics — no behavior change on the success path. Wraps any
/// inner pool (typically the global `GreedyMemoryPool`); on OOM it augments
/// the inner error with `top consumers: [name=bytes, ...]`, which is the
/// fastest way to see which operator is the SF30 memory hog.
#[derive(Debug)]
pub struct TrackConsumersPool {
    inner: Arc<dyn MemoryPool>,
    consumers: Mutex<HashMap<String, usize>>,
    top_n: usize,
}

impl TrackConsumersPool {
    /// Wrap `inner`, reporting the `top_n` largest consumers on OOM.
    pub fn new(inner: Arc<dyn MemoryPool>, top_n: usize) -> Self {
        Self {
            inner,
            consumers: Mutex::new(HashMap::new()),
            top_n,
        }
    }

    fn top_consumers_report(&self) -> String {
        let map = self.consumers.lock().expect("memory pool mutex poisoned");
        let mut entries: Vec<(&String, &usize)> = map.iter().collect();
        entries.sort_by(|a, b| b.1.cmp(a.1));
        entries
            .iter()
            .take(self.top_n)
            .map(|(name, bytes)| format!("{name}={bytes}B"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl MemoryPool for TrackConsumersPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn try_grow(&self, consumer: &MemoryConsumer, additional: usize) -> Result<(), ExecutionError> {
        match self.inner.try_grow(consumer, additional) {
            Ok(()) => {
                let mut map = self.consumers.lock().expect("memory pool mutex poisoned");
                *map.entry(consumer.name().to_string()).or_insert(0) += additional;
                Ok(())
            }
            Err(e) => Err(ExecutionError::ResourceExhausted(format!(
                "{e}; top consumers: [{}]",
                self.top_consumers_report()
            ))),
        }
    }

    fn shrink(&self, consumer: &MemoryConsumer, bytes: usize) {
        self.inner.shrink(consumer, bytes);
        let mut map = self.consumers.lock().expect("memory pool mutex poisoned");
        if let Some(v) = map.get_mut(consumer.name()) {
            *v = v.saturating_sub(bytes);
        }
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn reserved_peak(&self) -> usize {
        self.inner.reserved_peak()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    #[test]
    fn try_grow_within_budget_succeeds() {
        let pool = pool(1024);
        let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
        assert!(res.try_grow(500).is_ok());
        assert_eq!(res.size(), 500);
        assert_eq!(pool.reserved(), 500);
    }

    #[test]
    fn try_grow_beyond_budget_fails_without_changing_size() {
        let pool = pool(1024);
        let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
        res.try_grow(900).unwrap();
        let err = res.try_grow(200).unwrap_err();
        assert!(err.to_string().contains("memory pool exhausted"));
        assert_eq!(res.size(), 900, "size unchanged on failure");
        assert_eq!(pool.reserved(), 900);
    }

    #[test]
    fn shrink_reduces_size_and_pool_total() {
        let pool = pool(1024);
        let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
        res.try_grow(800).unwrap();
        res.shrink(300);
        assert_eq!(res.size(), 500);
        assert_eq!(pool.reserved(), 500);
    }

    #[test]
    fn shrink_saturates_at_zero() {
        let pool = pool(1024);
        let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
        res.try_grow(100).unwrap();
        res.shrink(1000);
        assert_eq!(res.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn try_resize_grows_to_target() {
        let pool = pool(1024);
        let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
        res.try_grow(100).unwrap();
        res.try_resize(500).unwrap();
        assert_eq!(res.size(), 500);
    }

    #[test]
    fn try_resize_shrinks_to_target() {
        let pool = pool(1024);
        let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
        res.try_grow(800).unwrap();
        res.try_resize(200).unwrap();
        assert_eq!(res.size(), 200);
        assert_eq!(pool.reserved(), 200);
    }

    #[test]
    fn drop_releases_outstanding_bytes() {
        let pool = pool(1024);
        {
            let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
            res.try_grow(500).unwrap();
            assert_eq!(pool.reserved(), 500);
        }
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn two_consumers_share_one_pool() {
        let pool = pool(1024);
        let mut a = MemoryConsumer::new("A").register(pool.clone());
        let mut b = MemoryConsumer::new("B").register(pool.clone());
        a.try_grow(400).unwrap();
        b.try_grow(500).unwrap();
        assert_eq!(pool.reserved(), 900);
        let err = b.try_grow(200).unwrap_err();
        assert!(err.to_string().contains("memory pool exhausted"));
        assert_eq!(pool.reserved(), 900);
    }

    #[test]
    fn unbounded_pool_never_fails() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
        let mut res = MemoryConsumer::new("Big").register(pool.clone());
        // 1 TB request — fine.
        res.try_grow(1024usize * 1024 * 1024 * 1024).unwrap();
        assert_eq!(pool.reserved(), 1024usize * 1024 * 1024 * 1024);
    }

    #[test]
    fn consumer_can_spill_flag_round_trips() {
        let c = MemoryConsumer::new("Op").with_can_spill(true);
        assert!(c.can_spill());
        assert_eq!(c.name(), "Op");
    }

    #[test]
    fn greedy_pool_tracks_peak_high_water() {
        let pool = pool(10_000);
        let mut res = MemoryConsumer::new("Op").register(pool.clone());
        res.try_grow(800).unwrap();
        res.try_grow(200).unwrap(); // used 1000, peak 1000
        res.shrink(600); // used 400, peak stays 1000
        assert_eq!(pool.reserved(), 400);
        assert_eq!(
            pool.reserved_peak(),
            1000,
            "peak is the high-water mark, not the current reservation"
        );
    }

    #[test]
    fn query_pool_peak_delegates_to_global_worker_peak() {
        let global: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000));
        let q: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(global.clone(), 5_000));
        let mut res = MemoryConsumer::new("Op").register(q.clone());
        res.try_grow(900).unwrap();
        res.shrink(900);
        // Per-query current is 0, but the worker-level peak is preserved.
        assert_eq!(q.reserved(), 0);
        assert_eq!(q.reserved_peak(), 900);
    }

    #[test]
    fn track_consumers_pool_names_top_on_oom() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1_000));
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(inner, 3));
        let mut big = MemoryConsumer::new("BigOp").register(pool.clone());
        let mut small = MemoryConsumer::new("SmallOp").register(pool.clone());
        big.try_grow(700).unwrap();
        small.try_grow(200).unwrap();
        // 900/1000 used; a further 200 fails and the error names the hog.
        let err = big.try_grow(200).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("top consumers"), "got: {msg}");
        assert!(
            msg.contains("BigOp=700B"),
            "biggest consumer named first: {msg}"
        );
        // Success path is unchanged: reserved totals match the inner pool.
        assert_eq!(pool.reserved(), 900);
    }

    #[test]
    fn track_consumers_pool_success_path_is_transparent() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000));
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(inner, 5));
        let mut res = MemoryConsumer::new("Op").register(pool.clone());
        res.try_grow(500).unwrap();
        res.shrink(200);
        assert_eq!(pool.reserved(), 300);
        assert_eq!(pool.reserved_peak(), 500);
    }

    #[test]
    fn free_zeros_the_reservation() {
        let pool = pool(1024);
        let mut res = MemoryConsumer::new("TestOp").register(pool.clone());
        res.try_grow(500).unwrap();
        res.free();
        assert_eq!(res.size(), 0);
        assert_eq!(pool.reserved(), 0);
        // Subsequent free is a no-op.
        res.free();
        assert_eq!(res.size(), 0);
    }

    // ---- QueryMemoryPool (Phase 4) ----

    fn query_pool(global_limit: usize, query_limit: usize) -> Arc<dyn MemoryPool> {
        let global: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(global_limit));
        Arc::new(QueryMemoryPool::new(global, query_limit))
    }

    #[test]
    fn query_pool_enforces_query_limit_before_global() {
        // global=10k, query=1k; the query cap should fire long before global.
        let pool = query_pool(10_000, 1_000);
        let mut res = MemoryConsumer::new("Op").register(pool.clone());
        res.try_grow(800).unwrap();
        let err = res.try_grow(300).unwrap_err();
        assert!(
            err.to_string().contains("query memory limit exceeded"),
            "expected per-query cap error, got: {err}"
        );
        // Both layers still see only the successful 800.
        assert_eq!(res.size(), 800);
        assert_eq!(pool.reserved(), 800);
    }

    #[test]
    fn query_pool_propagates_global_exhaustion() {
        // global=500, query=10k; the global cap fires first.
        let pool = query_pool(500, 10_000);
        let mut res = MemoryConsumer::new("Op").register(pool.clone());
        let err = res.try_grow(800).unwrap_err();
        assert!(
            err.to_string().contains("memory pool exhausted"),
            "expected global pool error, got: {err}"
        );
        assert_eq!(
            res.size(),
            0,
            "atomic: no bytes reserved on cascade failure"
        );
    }

    #[test]
    fn query_pool_shrink_releases_both_layers() {
        let pool = query_pool(10_000, 1_000);
        let mut res = MemoryConsumer::new("Op").register(pool.clone());
        res.try_grow(800).unwrap();
        res.shrink(300);
        assert_eq!(res.size(), 500);
        assert_eq!(pool.reserved(), 500, "query layer total");
    }

    #[test]
    fn query_pool_two_queries_have_independent_caps() {
        // Each query gets its own 1k cap; both share the 10k global.
        let global: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000));
        let q1: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(global.clone(), 1_000));
        let q2: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(global.clone(), 1_000));
        let mut r1 = MemoryConsumer::new("Q1Op").register(q1.clone());
        let mut r2 = MemoryConsumer::new("Q2Op").register(q2.clone());
        r1.try_grow(900).unwrap();
        r2.try_grow(900).unwrap();
        // Each query is near its own cap but the global at 1800/10000 has headroom.
        assert_eq!(global.reserved(), 1_800);
        // Q1 hits its own cap.
        assert!(r1.try_grow(200).is_err());
        // Q2 still has room of its own.
        assert!(r2.try_grow(80).is_ok());
    }
}
