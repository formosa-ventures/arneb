//! Worker-side TaskManager: receives, executes, and serves task output.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arneb_catalog::CatalogManager;
use arneb_common::inflight_budget::InflightBudget;
use arneb_common::{Domain, DynamicFilterId, QueryId, TaskId};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::MemoryPool;
use arneb_execution::DynamicFilterCollector;
use arneb_planner::LogicalPlan;
use arneb_rpc::{FlightState, OutputBuffer, TaskDescriptor};
use futures::StreamExt;

use crate::memory_probe::MemoryProbe;

/// Master switch for distributed dynamic-filter PUBLISH (`ARNEB_DISTRIBUTED_DF`,
/// default OFF). `ExecutionContext::dynamic_filtering_enabled` defaults false and
/// was never wired in distributed mode, so DF producers (HashJoinExec) never
/// published — every dynamic filter was dormant. Flipping this on activates the
/// publish path so probe-side fact scans get pruned. `=0` disables.
fn distributed_df_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_DISTRIBUTED_DF")
            .map(|v| v == "1")
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            distributed_df = enabled,
            "ARNEB_DISTRIBUTED_DF effective value (default off; =1 to enable distributed dynamic-filter publish)"
        );
        enabled
    })
}

/// Per-partition OutputBuffer channel capacity: batches buffered before
/// back-pressure/spill on the lineitem-class exchange output (task_manager
/// :593). Default 64 (history at that call site). The 2026-05-22 Q09 profile
/// pinned OutputBuffer + RepartitionExec channels + intermediate Arrow batches
/// as the dominant un-tracked per-worker memory; lowering this bounds in-flight
/// resident, trading latency headroom for memory on memory-bound queries (q05).
/// Cell-safe: pure buffering depth, never changes results. `ARNEB_OUTPUT_BUFFER_CAPACITY`.
fn output_buffer_capacity() -> usize {
    use std::sync::OnceLock;
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        let cap = std::env::var("ARNEB_OUTPUT_BUFFER_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(64);
        tracing::info!(
            target: "arneb::config",
            output_buffer_capacity = cap,
            "ARNEB_OUTPUT_BUFFER_CAPACITY effective value (default 64; lower trades latency for memory, cell-safe)"
        );
        cap
    })
}

/// Per-stage byte budget for batches buffered between operators. `0` disables
/// byte gating and preserves the existing count-cap-only behavior.
fn inflight_budget_bytes() -> u64 {
    use std::sync::OnceLock;
    static BYTES: OnceLock<u64> = OnceLock::new();
    *BYTES.get_or_init(|| {
        let bytes = std::env::var("ARNEB_INFLIGHT_BUDGET_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        tracing::info!(
            target: "arneb::config",
            inflight_budget_bytes = bytes,
            "ARNEB_INFLIGHT_BUDGET_BYTES effective value (default 0; 0 disables byte in-flight back-pressure)"
        );
        bytes
    })
}

fn emit_memory_profile(task_id: &str, memory_pool: &Arc<dyn MemoryPool>) {
    if !crate::memory_profile::mem_profile_enabled() {
        return;
    }

    let Some(snapshot) = memory_pool.memory_profile_snapshot() else {
        return;
    };
    let top_consumers = snapshot
        .top_consumers
        .iter()
        .map(|(name, bytes)| format!("{name}={bytes}B"))
        .collect::<Vec<_>>()
        .join(", ");
    let untracked_estimate_bytes = snapshot
        .jemalloc_resident_peak_bytes
        .saturating_sub(snapshot.pool_peak_bytes);

    tracing::info!(
        target: "arneb::memprofile",
        task_id = %task_id,
        pool_peak_bytes = snapshot.pool_peak_bytes,
        jemalloc_resident_peak_bytes = snapshot.jemalloc_resident_peak_bytes,
        jemalloc_allocated_peak_bytes = snapshot.jemalloc_allocated_peak_bytes,
        jemalloc_active_peak_bytes = snapshot.jemalloc_active_peak_bytes,
        jemalloc_retained_peak_bytes = snapshot.jemalloc_retained_peak_bytes,
        untracked_estimate_bytes,
        top_consumers = %top_consumers,
        "task memory profile"
    );
}

/// Task execution state.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Running,
    Finished,
    Failed(String),
}

/// Manages task execution on a worker node.
#[derive(Clone)]
pub struct TaskManager {
    flight_state: FlightState,
    catalog_manager: Arc<CatalogManager>,
    connector_registry: Arc<ConnectorRegistry>,
    /// A1.5 (2026-05-27): coordinator's Flight address (e.g.
    /// "http://coord:9090"). Workers use this to report build-side
    /// dynamic filter Domains via `arneb_rpc::report_dynamic_filters`.
    /// `None` on standalone-equivalent setups (single-node tests);
    /// the publisher then stays absent and producers skip the publish.
    coord_address: Option<String>,
    task_statuses: Arc<RwLock<HashMap<String, TaskStatus>>>,
    /// A1.3 (2026-05-27): per-task dynamic-filter collectors keyed by
    /// `(QueryId, TaskId)`. `handle_task` inserts an entry seeded
    /// from `descriptor.pending_dynamic_filters` so the FlightState
    /// `df_notify_callback` can route late arrivals into the right
    /// collector via `insert`. The 30s cleanup spawn drops the entry
    /// along with the OutputBuffer. A1.4 will wire `ScanExec` to read
    /// from this map; A1.3 only populates it.
    df_collectors: Arc<RwLock<HashMap<(QueryId, TaskId), DynamicFilterCollector>>>,
    /// Memory pool that spillable operators (currently SemiJoinExec
    /// build) reserve against. Phase 2c (2026-05-21): wired in from
    /// `main.rs` based on `[memory]` config + cgroup auto-detect, so
    /// the build phase will spill to disk before the kernel OOM-kills
    /// the worker process. Defaults to UnboundedMemoryPool when the
    /// caller doesn't override.
    memory_pool: Arc<dyn MemoryPool>,
    /// Phase 3b.6b (2026-05-21): optional jemalloc RSS probe. When
    /// configured with a threshold, `execute_task` checks the probe
    /// before kicking off task body and defers admission (returns a
    /// `ResourceExhausted`-shaped error after a safety timeout) until
    /// RSS drops back below the threshold. Catches the untracked Arrow
    /// allocations that the per-operator pool doesn't see. Unlike the
    /// removed `concurrency_limit` semaphore (Phase A, 2026-05-23),
    /// this gate does NOT hold a permit across an `.await` — it polls
    /// then proceeds, which is compatible with downstream stream
    /// back-pressure.
    memory_probe: Option<Arc<MemoryProbe>>,
}

impl TaskManager {
    /// Construct without an RSS-based admission probe. Used by tests
    /// and standalone-mode setup. Phase A (2026-05-23) removed the
    /// count-based concurrency semaphore — see `with_admission_gate`
    /// for the only remaining admission lever.
    pub fn new(
        flight_state: FlightState,
        catalog_manager: Arc<CatalogManager>,
        connector_registry: Arc<ConnectorRegistry>,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> Self {
        Self::with_admission_gate(
            flight_state,
            catalog_manager,
            connector_registry,
            memory_pool,
            None,
            None,
        )
    }

    /// Construct with an optional RSS-based admission probe
    /// (Phase 3b.6b). When `memory_probe` is set, `execute_task`
    /// defers admission whenever the probe reports current RSS above
    /// its configured threshold — catches the untracked Arrow
    /// allocations the per-operator pool misses.
    ///
    /// Phase A (2026-05-23): the count-based `concurrency_limit`
    /// semaphore was removed. It held a permit across the entire task
    /// body, which deadlocked downstream stream back-pressure
    /// producers (see streaming refactor diagnosis in commit history).
    /// Worker concurrency is now bounded by tokio worker threads +
    /// per-operator `MemoryPool` reservations + the optional RSS
    /// probe — none of which hold a permit across an `.await`.
    pub fn with_admission_gate(
        flight_state: FlightState,
        catalog_manager: Arc<CatalogManager>,
        connector_registry: Arc<ConnectorRegistry>,
        memory_pool: Arc<dyn MemoryPool>,
        memory_probe: Option<Arc<MemoryProbe>>,
        coord_address: Option<String>,
    ) -> Self {
        Self {
            flight_state,
            catalog_manager,
            connector_registry,
            coord_address,
            task_statuses: Arc::new(RwLock::new(HashMap::new())),
            df_collectors: Arc::new(RwLock::new(HashMap::new())),
            memory_pool,
            memory_probe,
        }
    }

    /// Returns the per-task `DynamicFilterCollector` for the given
    /// `(query_id, task_id)`, if one was registered by `handle_task`.
    /// Used by the FlightState `df_notify_callback` to route a
    /// coord-pushed Domain to the right task.
    pub fn df_collector(
        &self,
        query_id: QueryId,
        task_id: TaskId,
    ) -> Option<DynamicFilterCollector> {
        self.df_collectors
            .read()
            .unwrap()
            .get(&(query_id, task_id))
            .cloned()
    }

    /// Routes a Domain into the matching per-task collector. Returns
    /// `false` if no collector is registered for that `(query, task)`
    /// (the task already finished or never started). Used by main.rs
    /// to wire the FlightState `df_notify_callback`.
    pub async fn route_notify(
        &self,
        query_id: QueryId,
        task_id: TaskId,
        df_id: DynamicFilterId,
        domain: Domain,
    ) -> bool {
        let collector = self
            .df_collectors
            .read()
            .unwrap()
            .get(&(query_id, task_id))
            .cloned();
        match collector {
            Some(c) => {
                c.insert(df_id, domain).await;
                true
            }
            None => false,
        }
    }

    /// Handle an incoming task submission. Spawns execution in a background task.
    pub fn handle_task(&self, descriptor: TaskDescriptor) {
        // Buffer key includes the query_id so two queries with the same
        // local stage numbering (the fragmenter restarts at 0 per query)
        // never collide on the worker's `flight_state.buffers` map. The
        // old "0.0" reuse pattern caused intermittent "partition already
        // consumed" errors when coord's do_get hit the *previous* query's
        // drained buffer instead of the new one.
        let task_id_str = format!("{}.{}", descriptor.query_id, descriptor.task_id);
        let manager = self.clone();

        // Mark as running
        {
            let mut statuses = manager.task_statuses.write().unwrap();
            statuses.insert(task_id_str.clone(), TaskStatus::Running);
        }

        // A1.3 (2026-05-27): register a per-task `DynamicFilterCollector`
        // seeded with the DFs that already resolved on coord before
        // dispatch. Inert until A1.4 wires `ScanExec` to read it.
        let df_collector = DynamicFilterCollector::with_pending(
            descriptor.pending_dynamic_filters.iter().cloned(),
        );
        manager
            .df_collectors
            .write()
            .unwrap()
            .insert((descriptor.query_id, descriptor.task_id), df_collector);
        let df_collector_key = (descriptor.query_id, descriptor.task_id);

        tokio::spawn(async move {
            let task_id_for_cleanup = task_id_str.clone();
            match manager.execute_task(descriptor).await {
                Ok(()) => {
                    let mut statuses = manager.task_statuses.write().unwrap();
                    statuses.insert(task_id_str, TaskStatus::Finished);
                }
                Err(e) => {
                    tracing::error!(task_id = %task_id_for_cleanup, error = %e, "task failed");
                    let mut statuses = manager.task_statuses.write().unwrap();
                    statuses.insert(task_id_str, TaskStatus::Failed(e));
                }
            }
            emit_memory_profile(&task_id_for_cleanup, &manager.memory_pool);

            // Consume-aware cleanup: poll every 5 s until all receivers
            // have been taken by consumers (do_get), then drop the entry.
            // 600 s safety ceiling for tasks whose consumers never connect
            // (query cancelled, coordinator crashed, broadcast buffers).
            let flight_state = manager.flight_state.clone();
            let task_statuses = Arc::clone(&manager.task_statuses);
            let df_collectors = Arc::clone(&manager.df_collectors);
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(600);
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    let consumed = match flight_state.get_buffer(&task_id_for_cleanup) {
                        Some(arneb_rpc::BufferKind::Partitioned(buf)) => {
                            buf.lock().await.all_receivers_taken()
                        }
                        None => true,
                        _ => false,
                    };
                    if consumed {
                        break;
                    }
                }
                flight_state.remove_buffer(&task_id_for_cleanup);
                if let Ok(mut statuses) = task_statuses.write() {
                    statuses.remove(&task_id_for_cleanup);
                }
                if let Ok(mut collectors) = df_collectors.write() {
                    collectors.remove(&df_collector_key);
                }
            });
        });
    }

    /// Get the status of a task.
    pub fn task_status(&self, task_id: &str) -> Option<TaskStatus> {
        self.task_statuses.read().unwrap().get(task_id).cloned()
    }

    /// Map local `TaskStatus` to the RPC-layer `TaskStatusResponse`.
    /// Used by the `do_get` liveness check (D1 — replaces the hard
    /// 5-minute OutputBuffer-registration deadline).
    pub fn task_status_as_response(&self, task_id: &str) -> Option<arneb_rpc::TaskStatusResponse> {
        match self.task_status(task_id)? {
            TaskStatus::Running | TaskStatus::Finished => {
                Some(arneb_rpc::TaskStatusResponse::Running)
            }
            TaskStatus::Failed(reason) => Some(arneb_rpc::TaskStatusResponse::Failed(reason)),
        }
    }

    /// Execute a task: deserialize plan, run it, write output to buffer.
    async fn execute_task(&self, descriptor: TaskDescriptor) -> Result<(), String> {
        // Same query-scoped key as handle_task — see comment there.
        let task_id_str = format!("{}.{}", descriptor.query_id, descriptor.task_id);

        // Phase A (2026-05-23): the count-based admission semaphore was
        // removed. It held a permit across the entire task body, which
        // deadlocked downstream stream back-pressure producers (per-batch
        // probe yields → bounded OutputBuffer fills → producer blocks →
        // permit not released → consumer never admitted). Concurrency is
        // now bounded by tokio worker threads + per-operator MemoryPool +
        // the RSS-based gate below.

        // Phase 3b.6b: RSS-based admission gate. Wait for jemalloc's
        // resident-bytes reading to drop below the configured threshold
        // before kicking off the task body. Catches untracked Arrow
        // allocations (Filter / Project / RepartitionExec channel) that
        // overflow the container before the per-operator pool fires.
        // Unlike the removed concurrency semaphore, this is a polling
        // wait — it does not hold a permit across the task's `.await`s.
        if let Some(probe) = &self.memory_probe {
            let mut waited_ms = 0u64;
            while probe.over_threshold() {
                if waited_ms == 0 {
                    tracing::warn!(
                        task_id = %task_id_str,
                        rss_bytes = probe.resident_bytes(),
                        threshold_bytes = probe.threshold_bytes(),
                        "task admission deferred: worker RSS over threshold",
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                waited_ms += 100;
                // Safety cap: after 30 s of waiting, fail the task with
                // ResourceExhausted instead of admitting it into a
                // worker that's already in OOM-kill territory. Phase
                // M.2c+Z.1 (2026-05-22) bench showed: when 5+ Q09
                // fragments fan into one worker and RSS climbs to
                // ~2.8 GB, admitting a 6th task on the safety timeout
                // *guarantees* a kernel SIGKILL within ~20 s. Better
                // to surface a clean ResourceExhausted that the coord
                // can retry on the other worker than to lose the
                // entire worker process.
                if waited_ms > 30_000 {
                    let msg = format!(
                        "task {task_id_str} admission gate timed out after 30 s; \
                         worker RSS {} bytes exceeds threshold {} bytes — refusing \
                         admission to avoid kernel OOM-kill",
                        probe.resident_bytes(),
                        probe.threshold_bytes(),
                    );
                    tracing::warn!(target: "arneb::mem", "{msg}");
                    return Err(msg);
                }
            }
            if waited_ms > 0 {
                tracing::info!(
                    task_id = %task_id_str,
                    waited_ms,
                    rss_bytes = probe.resident_bytes(),
                    "task admission resumed",
                );
            }
        }

        tracing::info!(task_id = %task_id_str, "executing task");
        let inflight_budget = Arc::new(InflightBudget::new(inflight_budget_bytes()));

        // Deserialize the logical plan
        let plan: LogicalPlan = serde_json::from_str(&descriptor.plan_json)
            .map_err(|e| format!("failed to deserialize plan: {e}"))?;

        // Create execution context with the worker's memory pool
        // (Phase 2c: cgroup-derived budget) and register real data
        // sources via connectors. The pool is shared across all tasks
        // running on this worker; concurrent SemiJoinExec builds will
        // serialise on the pool's lock when the budget tightens.
        //
        // A1.4 (2026-05-27): also thread the per-task
        // `DynamicFilterCollector` (already registered by `handle_task`
        // and seeded from `descriptor.pending_dynamic_filters`). The
        // physical planner bakes a clone into every `ScanExec` so it
        // can await cross-fragment DFs at execute time. The feature
        // flag is left at its default (`false`) here; A1.6 will flip
        // the default once the SF10 Q09 gate passes. Until then this
        // is purely additive — ScanExec skips the wait when the flag
        // is off, so runtime behaviour is unchanged.
        let df_collector = self.df_collector(descriptor.query_id, descriptor.task_id);
        // A1.5 (2026-05-27): per-task publisher closed over coord
        // address + ids. Absent when the worker has no coord address
        // (single-node tests); HJ/SJ producers then skip the emit.
        let df_publisher: Option<arneb_execution::DynamicFilterPublisherRef> = self
            .coord_address
            .as_ref()
            .map(|addr| -> arneb_execution::DynamicFilterPublisherRef {
                Arc::new(crate::dynamic_filter::FlightDynamicFilterPublisher::new(
                    addr.clone(),
                    descriptor.query_id,
                    descriptor.task_id,
                ))
            });
        let mut exec_ctx = arneb_execution::ExecutionContext::new()
            .with_memory_pool(self.memory_pool.clone())
            .with_inflight_budget(Arc::clone(&inflight_budget))
            .with_dynamic_filter_collector(df_collector)
            .with_dynamic_filter_publisher(df_publisher)
            // Master switch for distributed dynamic-filter PUBLISH. Was never
            // wired (planner.rs default false) — so DF producers (HashJoinExec)
            // never published in distributed mode, leaving every DF dormant.
            // Gated ARNEB_DISTRIBUTED_DF (default off) until validated.
            .with_dynamic_filtering_enabled(distributed_df_enabled());
        register_task_data_sources(
            &plan,
            &self.catalog_manager,
            &self.connector_registry,
            &mut exec_ctx,
        )
        .await?;

        // W2 (2026-05-20): if the coord told us about upstream stages, hand
        // them to the physical planner so it can wire `ExchangeNode`
        // placeholders into `ExchangeExec` that pulls from the upstream
        // worker's `OutputBuffer`. Single-source fragments (leaf
        // TableScans) get an empty map and behave like before.
        // W3-Hash.6 fix (2026-05-20): aggregate by source_stage_id. The
        // coord emits one SourceExchange per upstream task (potentially
        // N per stage for α-model partitioned stages), so we group them
        // into Vec<(addr, task_id, partition_id)> per stage. The earlier
        // `.map` form overwrote duplicates and only kept the LAST entry
        // per stage — dropping N-1 sibling tasks' addresses.
        // M×N (2026-05-20, step 3b): the tuple's third field is
        // `sx.partition_id` — pre-resolved by coord per consumer task,
        // so this worker's planner can use it directly without knowing
        // upstream cardinality.
        let mut stage_results: HashMap<u32, Vec<(String, String, u32)>> = HashMap::new();
        for sx in &descriptor.source_exchanges {
            stage_results.entry(sx.source_stage_id).or_default().push((
                sx.flight_address.clone(),
                sx.source_task_id.clone(),
                sx.partition_id,
            ));
        }

        // Layer the consumer partition id on top of any stage_results — this
        // task is one of potentially N parallel instances of its stage, and
        // its plan's ExchangeExec instances should each fetch the matching
        // upstream partition slice.
        // Multi-worker scan: also install `scan_task_count` (M) so every
        // ScanExec strides 1/M of its DataSource partitions, with the
        // stride index = this task's `consumer_partition_id`. `1` (the
        // default) keeps the single-task whole-table scan.
        let plan_ctx = if stage_results.is_empty() {
            exec_ctx
                .with_consumer_partition_id(descriptor.task_id.partition_id)
                .with_scan_task_count(descriptor.scan_task_count)
        } else {
            exec_ctx
                .with_stage_results(stage_results)
                .with_consumer_partition_id(descriptor.task_id.partition_id)
                .with_scan_task_count(descriptor.scan_task_count)
        };

        let physical_plan = plan_ctx
            .create_physical_plan(&plan)
            .map_err(|e| format!("physical plan creation failed: {e}"))?;

        // W3-Hash.1 / W3-Hash.4 (2026-05-20): expose
        // `descriptor.output_partitions` output streams via the
        // OutputBuffer. Three cases:
        //
        // (a) `output_partitions == 1`: single-stream producer. If the
        // inner plan happens to be multi-partition (per-file row-range
        // scan splits), wrap with `CoalescePartitionsExec` so we drain
        // every input partition into the single output.
        //
        // (b) `output_partitions > 1` AND `output_hash_columns` is
        // non-empty: hash-partitioned producer. Wrap the plan with
        // `RepartitionExec(Hash(cols, N))` and drain all N output
        // partitions into the N-partition buffer. This is the
        // foundation for partitioned probe — the consumer stage's N
        // tasks each pull their own bucket.
        //
        // (c) `output_partitions > 1` AND `output_hash_columns` is
        // empty: caller didn't specify a hash key. Best-effort: leave
        // the plan alone and assume it already produces N partitions
        // (e.g. plan top is already a RepartitionExec). Mismatch will
        // surface as a `partition out of range` execute() error.
        let n_out = std::cmp::max(1, descriptor.output_partitions);
        let physical_plan: Arc<dyn arneb_execution::ExecutionPlan> =
            if n_out == 1 && physical_plan.output_partitioning().partition_count() > 1 {
                Arc::new(arneb_execution::CoalescePartitionsExec::new(physical_plan))
            } else if n_out > 1 && !descriptor.output_hash_columns.is_empty() {
                // Coalesce any incidental multi-partition input first so the
                // RepartitionExec sees a single stream to hash-route.
                let coalesced: Arc<dyn arneb_execution::ExecutionPlan> =
                    if physical_plan.output_partitioning().partition_count() > 1 {
                        Arc::new(arneb_execution::CoalescePartitionsExec::new(physical_plan))
                    } else {
                        physical_plan
                    };
                let schema = coalesced.schema();
                let hash_exprs: Vec<arneb_planner::PlanExpr> = descriptor
                    .output_hash_columns
                    .iter()
                    .map(|col_idx| {
                        let col_name = schema
                            .get(*col_idx as usize)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| format!("col{col_idx}"));
                        arneb_planner::PlanExpr::Column {
                            index: *col_idx as usize,
                            name: col_name,
                            span: None,
                        }
                    })
                    .collect();
                let partitioning = arneb_execution::Partitioning::Hash(hash_exprs, n_out);
                Arc::new(
                    arneb_execution::RepartitionExec::new(coalesced, partitioning)
                        .with_memory_pool(self.memory_pool.clone())
                        .with_inflight_budget(Arc::clone(&inflight_budget)),
                )
            } else {
                physical_plan
            };
        let output_schema_arrow =
            arneb_execution::column_info_to_arrow_schema(&physical_plan.schema());

        // A2.1.2 (2026-05-28): broadcast producer branch. When the coord
        // marks this fragment broadcast (`descriptor.broadcast == true`),
        // we route output through `BroadcastOutputBuffer` instead of the
        // partitioned `OutputBuffer`. Producer is a single pumper draining
        // partition 0 (the `n_out == 1 && partition_count() > 1` wrap
        // above already inserted a `CoalescePartitionsExec` if needed).
        // Consumers each `subscribe()` an independent `BroadcastStream`
        // via the `do_get` Broadcast arm — no per-consumer fan-out logic
        // is needed on this side. Inert until A2.2 teaches the fragmenter
        // to set this flag; until then the early-return branch is dead.
        if descriptor.broadcast {
            if n_out != 1 {
                return Err(format!(
                    "broadcast task expected output_partitions=1, got {n_out}"
                ));
            }
            let bbuf = Arc::new(arneb_rpc::BroadcastOutputBuffer::new(output_schema_arrow));
            let failure_flag = bbuf.failure_handle();
            self.flight_state
                .register_broadcast_buffer(task_id_str.clone(), Arc::clone(&bbuf));

            let plan_for_pump = physical_plan.clone();
            let task_id_for_log = task_id_str.clone();
            let bbuf_for_pump = Arc::clone(&bbuf);
            let pumper = tokio::spawn(async move {
                let record_failure = |err: String| {
                    if let Ok(mut guard) = failure_flag.lock() {
                        if guard.is_none() {
                            *guard = Some(err.clone());
                        }
                    }
                    err
                };

                let mut stream = match plan_for_pump.execute(0).await {
                    Ok(s) => s,
                    Err(e) => {
                        let msg = record_failure(format!("execute(0) failed: {e}"));
                        bbuf_for_pump.finish();
                        return Err(msg);
                    }
                };
                let mut rows: usize = 0;
                while let Some(batch_result) = stream.next().await {
                    let batch = match batch_result {
                        Ok(b) => b,
                        Err(e) => {
                            let msg = record_failure(format!("stream error during execution: {e}"));
                            bbuf_for_pump.finish();
                            return Err(msg);
                        }
                    };
                    rows += batch.num_rows();
                    bbuf_for_pump.write_batch(batch);
                }
                bbuf_for_pump.finish();
                Ok::<usize, String>(rows)
            });

            let rows_emitted = pumper
                .await
                .map_err(|e| format!("broadcast pumper panicked: {e}"))??;

            tracing::info!(
                task_id = %task_id_for_log,
                broadcast = true,
                rows = rows_emitted,
                "task completed"
            );

            return Ok(());
        }

        // OutputBuffer per-channel cap.
        // History: 1024 was the original. Phase 3b.5 (2026-05-21)
        // tried 32 to bound Q09 worker RSS — didn't help at the time
        // because the channel pool was not actually being filled to
        // capacity in the lineitem-class fragments.
        // 2026-05-22 Q09 profile (see project_2026-05-22_q09_profile)
        // identified OutputBuffer + RepartitionExec channels +
        // intermediate Arrow batches as the dominant ~3 GB of
        // un-tracked per-worker memory at b60b185. With 25 MB batches
        // and 1024 cap, a SINGLE pumper that emits faster than the
        // remote consumer reads can park 25 GB worth of batches before
        // back-pressure kicks in — bounded only by what the consumer
        // happens to pull. Drop cap to 64 to bound the in-flight
        // buffer to ~1.6 GB worst-case per partition (still generous;
        // typical lineitem-class fragments fit far smaller).
        let mut buffer = OutputBuffer::new(n_out, output_buffer_capacity(), output_schema_arrow)
            .with_memory_pool(self.memory_pool.clone())
            .with_inflight_budget(Arc::clone(&inflight_budget));
        let senders = buffer.take_senders();
        // B-fix-3 (2026-05-22): pumpers share this flag so a stream
        // error becomes a propagated Flight error to the coord,
        // instead of a silent EOF that produces partial results.
        let failure_flag = buffer.failure_handle();
        self.flight_state
            .register_partitioned_buffer(task_id_str.clone(), buffer);

        let output_schema_for_spill =
            arneb_execution::column_info_to_arrow_schema(&physical_plan.schema());
        // q21 SF30 fix: when our consumer must drain this output fully, a
        // mid-stream dropped receiver is a SILENT truncation (not a legitimate
        // LIMIT early-stop) — fail loud instead of returning Ok with missing
        // rows. Set by the coordinator from the consumer fragment's root.
        // `ARNEB_MUST_DRAIN=0` disables the guard (reverts to the old tolerate-Ok
        // behaviour): a diagnostic escape hatch to observe the underlying stall
        // without the early abort cascade (Layer-2 §1 measure-first), and a
        // safety valve if the guard is ever found to over-fire.
        let must_drain_disabled = std::env::var("ARNEB_MUST_DRAIN")
            .map(|v| v == "0")
            .unwrap_or(false);
        if must_drain_disabled {
            // Loud, once-per-process: this override silently re-enables the
            // q21-class truncation. Never set it for a correctness run (the
            // SF30 oracle refuses to run when it sees this).
            static WARN_ONCE: std::sync::Once = std::sync::Once::new();
            WARN_ONCE.call_once(|| {
                tracing::warn!(
                    "ARNEB_MUST_DRAIN=0 is set — the SF30 silent-truncation guard is \
                     DISABLED. must-drain exchanges will tolerate a mid-stream consumer \
                     drop and may SILENTLY return TRUNCATED results. Diagnostic escape \
                     hatch only — unset it for any correctness-sensitive run."
                );
            });
        }
        let must_drain = descriptor.must_drain && !must_drain_disabled;
        let mut pumpers = Vec::with_capacity(n_out);
        for (partition_idx, sender) in senders.into_iter().enumerate() {
            let plan_for_pump = physical_plan.clone();
            let task_id_for_log = task_id_str.clone();
            let failure_flag = Arc::clone(&failure_flag);
            let spill_schema = output_schema_for_spill.clone();
            pumpers.push(tokio::spawn(async move {
                let record_failure = |err: String| {
                    if let Ok(mut guard) = failure_flag.lock() {
                        if guard.is_none() {
                            *guard = Some(err.clone());
                        }
                    }
                    err
                };

                let mut stream = match plan_for_pump.execute(partition_idx).await {
                    Ok(s) => s,
                    Err(e) => {
                        return Err(record_failure(format!(
                            "execute({partition_idx}) failed: {e}"
                        )));
                    }
                };

                // D2 (2026-06-01): spillable producer. Use try_send
                // for the fast path; once the channel fills, start
                // spilling to Arrow IPC on disk. Once spilling starts,
                // ALL subsequent batches go to disk (preserves order).
                // After the execution stream ends, drain the spill
                // file back through the channel.
                let mut rows: usize = 0;
                let mut spill: Option<arneb_execution::spill::SpillWriter> = None;
                let mut consumer_gone = false;

                while let Some(batch_result) = stream.next().await {
                    let batch = match batch_result {
                        Ok(b) => b,
                        Err(e) => {
                            return Err(record_failure(format!(
                                "stream error during execution: {e}"
                            )));
                        }
                    };
                    rows += batch.num_rows();

                    if let Some(ref mut sw) = spill {
                        if let Err(e) = sw.write(&batch) {
                            return Err(record_failure(format!(
                                "exchange spill write failed: {e}"
                            )));
                        }
                        continue;
                    }

                    // exec-memory-accounting D2: spill the overflow on GLOBAL
                    // POOL PRESSURE (`PoolFull`), not only on the fixed channel
                    // cap (`ChannelFull`). With the default Unbounded pool
                    // (single-node) `PoolFull` never fires, so behaviour is
                    // unchanged; the distributed worker's cgroup-derived pool
                    // makes the exchange degrade to disk before it thrashes the
                    // host. The returned batch carries no live reservation in
                    // either spill case, so no pool bytes are held across the
                    // spill I/O (the D4 deadlock-safety invariant).
                    use arneb_rpc::TrackedSendOutcome;
                    let overflow = match sender.send_pooled(batch).await {
                        TrackedSendOutcome::Sent => None,
                        TrackedSendOutcome::PoolFull(returned)
                        | TrackedSendOutcome::ChannelFull(returned) => Some(returned),
                        TrackedSendOutcome::Closed(_) => {
                            if must_drain {
                                // q21 SF30 silent-truncation guard: a
                                // must-drain consumer (join / aggregate / sort)
                                // never legitimately stops reading mid-stream,
                                // so a dropped receiver here means an upstream
                                // stall/reset truncated this partition.
                                // Returning Ok would silently drop rows (q21
                                // returned ~62/100 wrong suppliers); fail loud.
                                return Err(record_failure(format!(
                                    "partition {partition_idx} consumer dropped receiver mid-stream \
                                     on a must-drain exchange — an upstream stall/reset truncated the \
                                     partition (q21 SF30 silent-truncation guard)"
                                )));
                            }
                            // Legitimate LIMIT early-stop or query teardown:
                            // the consumer can stop before EOF, so stop quietly.
                            tracing::warn!(
                                task_id = %task_id_for_log,
                                partition = partition_idx,
                                "consumer dropped receiver mid-stream — cancelling partition (LIMIT early-stop / teardown)"
                            );
                            consumer_gone = true;
                            break;
                        }
                    };
                    if let Some(returned) = overflow {
                        let mut sw = match arneb_execution::spill::SpillWriter::new(
                            spill_schema.clone(),
                            &format!("exchange_{task_id_for_log}_p{partition_idx}"),
                        ) {
                            Ok(w) => w,
                            Err(e) => {
                                return Err(record_failure(format!(
                                    "exchange spill create failed: {e}"
                                )));
                            }
                        };
                        if let Err(e) = sw.write(&returned) {
                            return Err(record_failure(format!(
                                "exchange spill write failed: {e}"
                            )));
                        }
                        spill = Some(sw);
                    }
                }

                // Drain spill file back through the channel.
                if !consumer_gone {
                    if let Some(sw) = spill {
                        match drain_spill_to_sender(sw, &sender, must_drain).await {
                            Ok((spill_batches, spill_bytes)) => {
                                tracing::info!(
                                    target: "arneb::profile",
                                    task_id = %task_id_for_log,
                                    partition = partition_idx,
                                    spill_batches,
                                    spill_bytes,
                                    "exchange overflow spilled to disk, drained"
                                );
                            }
                            Err(e) => return Err(record_failure(e)),
                        }
                    }
                }

                Ok::<usize, String>(rows)
            }));
        }

        let mut rows_emitted: usize = 0;
        for (idx, pumper) in pumpers.into_iter().enumerate() {
            let rows = pumper
                .await
                .map_err(|e| format!("partition {idx} pumper panicked: {e}"))??;
            rows_emitted += rows;
        }

        tracing::info!(
            task_id = %task_id_str,
            partitions = n_out,
            rows = rows_emitted,
            "task completed"
        );

        // exec-memory-accounting D1 observability: the worker's tracked
        // high-water (RepartitionExec channels + OutputBuffer staging +
        // join/aggregate build) vs the externally-measured RSS gives the
        // "tracked fraction". The SF30 goal is to drive tracked_peak_bytes
        // toward RSS so D2/D3 spill decisions act on an honest view.
        tracing::info!(
            target: "arneb::mem",
            task_id = %task_id_str,
            tracked_peak_bytes = self.memory_pool.reserved_peak(),
            tracked_now_bytes = self.memory_pool.reserved(),
            "worker tracked memory"
        );

        Ok(())
    }
}

/// Register data sources for a task's plan using actual connectors.
#[async_recursion::async_recursion]
async fn register_task_data_sources(
    plan: &LogicalPlan,
    catalog_manager: &CatalogManager,
    connector_registry: &ConnectorRegistry,
    ctx: &mut arneb_execution::ExecutionContext,
) -> Result<(), String> {
    match plan {
        LogicalPlan::TableScan {
            table,
            schema,
            properties,
            ..
        } => {
            let key = table.to_string();
            let connector_name = table
                .catalog
                .as_deref()
                .unwrap_or(catalog_manager.default_catalog());

            if let Some(factory) = connector_registry.get(connector_name) {
                if let Ok(ds) = factory.create_data_source(table, schema, properties).await {
                    ctx.register_data_source(key, ds);
                }
            }
            Ok(())
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        // A1 map-side fuse puts a PartialAggregate directly over the scan in
        // a SOURCE fragment, so we must recurse through it (and its Final
        // counterpart) to reach the TableScan and register its data source.
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Explain { input, .. } => {
            register_task_data_sources(input, catalog_manager, connector_registry, ctx).await
        }
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right, .. }
        | LogicalPlan::Except { left, right, .. } => {
            register_task_data_sources(left, catalog_manager, connector_registry, ctx).await?;
            register_task_data_sources(right, catalog_manager, connector_registry, ctx).await
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                register_task_data_sources(input, catalog_manager, connector_registry, ctx).await?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// exec-memory-accounting D2: drain a finished exchange spill file back through
/// the (tracked) sender in FIFO order — re-reserving best-effort on read-back
/// via `TrackedSender::send`. Extracted from the pumper so the spill-overflow
/// round-trip is unit-testable. Returns `(batches, bytes)`.
///
/// `must_drain` mirrors the main send-loop guard (q21 SF30 silent-truncation
/// fix): when our consumer must drain this output fully, a receiver that closes
/// *during the spill drain* (not just the main loop) is the SAME silent
/// truncation — fail loud. Without `must_drain` (a legitimate LIMIT early-stop
/// or query teardown) the drain stops cleanly. Layer-1 had this hole: the
/// guard lived only in the main loop, so a close during this phase slipped
/// through silently.
async fn drain_spill_to_sender(
    sw: arneb_execution::spill::SpillWriter,
    sender: &arneb_rpc::TrackedSender,
    must_drain: bool,
) -> Result<(usize, usize), String> {
    let spill_batches = sw.num_batches();
    let spill_bytes = sw.bytes_written();
    let sf = sw
        .finish()
        .map_err(|e| format!("exchange spill finish failed: {e}"))?;
    let reader = sf
        .open_reader()
        .map_err(|e| format!("exchange spill read failed: {e}"))?;
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("exchange spill read batch failed: {e}"))?;
        if sender.send(batch).await.is_err() {
            if must_drain {
                // q21 SF30 silent-truncation guard (spill-drain phase): a
                // must-drain consumer never legitimately stops mid-stream, so a
                // dropped receiver here means an upstream stall/reset truncated
                // this partition's spilled remainder — fail loud, don't return
                // Ok with the unsent tail silently lost.
                return Err(
                    "consumer dropped receiver during spill drain on a must-drain exchange — \
                     an upstream stall/reset truncated the partition (q21 SF30 silent-truncation \
                     guard)"
                        .to_string(),
                );
            }
            break; // consumer gone — stop cleanly (LIMIT early-stop / teardown)
        }
    }
    Ok((spill_batches, spill_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }
    fn batch(s: &Arc<Schema>, v: i32) -> RecordBatch {
        RecordBatch::try_new(s.clone(), vec![Arc::new(Int32Array::from(vec![v]))]).unwrap()
    }

    /// exec-memory-accounting D2 (3.4a): the spill-overflow drain delivers every
    /// spilled batch through the tracked sender in FIFO order — no loss, no
    /// duplication — which is the correctness property the SF30 q21/q09 e2e run
    /// exercises but does not isolate.
    #[tokio::test]
    async fn drain_spill_preserves_all_batches_in_fifo_order() {
        let s = schema();
        let mut sw =
            arneb_execution::spill::SpillWriter::new(s.clone(), "test_drain_fifo").unwrap();
        for v in 0..5 {
            sw.write(&batch(&s, v)).unwrap();
        }

        let mut buf = OutputBuffer::new(1, 64, s.clone());
        let mut rx = buf.take_receiver(0).unwrap();
        let sender = buf.take_senders().pop().unwrap();

        let (n, _bytes) = drain_spill_to_sender(sw, &sender, false).await.unwrap();
        assert_eq!(n, 5);
        drop(sender); // close the channel so the receiver ends after draining

        let mut got = Vec::new();
        while let Some(b) = rx.recv().await {
            got.push(
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
            );
        }
        assert_eq!(
            got,
            vec![0, 1, 2, 3, 4],
            "spill drained FIFO through the tracked sender — no loss/dup"
        );
    }

    /// q21 SF30 silent-truncation Layer-1 hole (2026-06-12): if the consumer
    /// closes its receiver DURING the spill-drain phase (not the main send
    /// loop, where the guard already lived), a `must_drain` producer must fail
    /// loud — not silently lose the unsent spilled tail.
    #[tokio::test]
    async fn drain_spill_must_drain_fails_loud_when_consumer_closes() {
        let s = schema();

        // must_drain=false: a closed consumer is a clean stop (LIMIT early-stop
        // / teardown).
        {
            let mut sw =
                arneb_execution::spill::SpillWriter::new(s.clone(), "test_drain_close_ok").unwrap();
            for v in 0..5 {
                sw.write(&batch(&s, v)).unwrap();
            }
            // OutputBuffer capacity 1; drop the receiver so the FIRST send past
            // the buffered slot finds the channel closed.
            let mut buf = OutputBuffer::new(1, 1, s.clone());
            let rx = buf.take_receiver(0).unwrap();
            let sender = buf.take_senders().pop().unwrap();
            drop(rx); // consumer gone
            let res = drain_spill_to_sender(sw, &sender, false).await;
            assert!(
                res.is_ok(),
                "must_drain=false tolerates a closed consumer mid-drain"
            );
        }

        // must_drain=true: the SAME close must surface as an error.
        {
            let mut sw =
                arneb_execution::spill::SpillWriter::new(s.clone(), "test_drain_close_err")
                    .unwrap();
            for v in 0..5 {
                sw.write(&batch(&s, v)).unwrap();
            }
            let mut buf = OutputBuffer::new(1, 1, s.clone());
            let rx = buf.take_receiver(0).unwrap();
            let sender = buf.take_senders().pop().unwrap();
            drop(rx); // consumer gone
            let res = drain_spill_to_sender(sw, &sender, true).await;
            let err = res.expect_err("must_drain=true must fail loud on a closed consumer");
            assert!(
                err.contains("silent-truncation guard"),
                "error should name the silent-truncation guard, got: {err}"
            );
        }
    }
}
