//! Physical planner: converts [`LogicalPlan`] trees into executable
//! [`ExecutionPlan`] operator trees.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arneb_common::error::ExecutionError;
use arneb_common::inflight_budget::InflightBudget;
use arneb_planner::{estimated_cardinality, CatalogStats, LogicalPlan, PlanExpr};

use crate::datasource::DataSource;
use crate::dynamic_filter_collector::DynamicFilterCollector;
use crate::dynamic_filter_publisher::DynamicFilterPublisherRef;
use crate::functions::{default_registry, FunctionRegistry};
use crate::hash_join::{extract_equi_join_keys, HashJoinExec};
use crate::memory_pool::{MemoryPool, UnboundedMemoryPool};
use crate::operator::{
    AggregateOutputColumn, ExecutionPlan, ExplainExec, FilterExec, HashAggregateExec, LimitExec,
    NestedLoopJoinExec, ProjectionExec, ScanExec, SortExec, TopKExec,
};
use crate::scan_context::ScanContext;

/// Default wait timeout for cross-fragment dynamic filters at the
/// scan side. Mirrors Trino's `dynamic_filtering_wait_timeout`
/// default of 10 s. A1.7 (2026-05-27) extended producer coverage to
/// every HJ build path (Grace + non-Grace single, per-partition
/// spill / no-spill, residual paths) + SemiJoin, so the timeout
/// should rarely fire on TPC-H workloads. A1.6.2 will wire a
/// `SET dynamic_filtering_wait_timeout` session param for per-query
/// tuning.
pub const DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Execution context holding registered data sources and function registry.
///
/// Data sources are registered by a key that matches the table reference
/// used in the logical plan. The key is the table's fully-qualified name
/// (as produced by `TableReference::to_string()`), or just the table name
/// for simple references.
#[derive(Debug)]
pub struct ExecutionContext {
    data_sources: HashMap<String, Arc<dyn DataSource>>,
    function_registry: Arc<FunctionRegistry>,
    /// Distributed execution: maps `stage_id.0` → list of `(remote_flight_addr,
    /// task_id_string, partition_id)` produced by `QueryCoordinator::execute`
    /// or constructed on a worker from `descriptor.source_exchanges`. When the
    /// physical planner converts a `LogicalPlan::ExchangeNode { stage_id, .. }`
    /// it looks the stage up here to instantiate an `ExchangeExec` that pulls
    /// from the right worker's `OutputBuffer`. The third tuple field is the
    /// partition_id pre-resolved per consumer (M×N step 3a/3b) — the planner
    /// uses it directly so it doesn't need to know upstream output cardinality.
    /// Empty in single-node mode.
    stage_results: HashMap<u32, Vec<(String, String, u32)>>,
    /// W3-Hash.2: which output partition of the upstream stage this task
    /// should fetch via its `ExchangeExec`. In partitioned-probe mode the
    /// coord launches N tasks per consuming stage with `partition_id`
    /// 0..N-1; each task's physical plan bakes that partition id into
    /// every `ExchangeExec` so it pulls its own slice. Defaults to 0
    /// (single-task / coalesced stage).
    consumer_partition_id: u32,
    /// Memory pool that spillable operators (currently SemiJoinExec
    /// build phase) reserve from. Defaults to [`UnboundedMemoryPool`]
    /// for single-node tests and the standalone server — no budget
    /// enforcement. Distributed worker tasks should call
    /// [`Self::with_memory_pool`] with a [`GreedyMemoryPool`] sized
    /// to a fraction of the container's cgroup limit so build-phase
    /// allocations fail fast (and, once spill lands in Phase 2b,
    /// trigger an on-disk grace hash join instead of a kernel OOM-kill).
    memory_pool: Arc<dyn MemoryPool>,
    /// Per-stage byte budget shared by repartition hand-offs in this plan.
    inflight_budget: Arc<InflightBudget>,
    /// A1.4 (2026-05-27): per-task dynamic-filter collector threaded
    /// into ScanExec. Set on worker tasks by `TaskManager::handle_task`
    /// from the per-task collector keyed by `(QueryId, TaskId)`. `None`
    /// on the coord and in unit tests — ScanExec then skips the
    /// cross-fragment wait regardless of `dynamic_filtering_enabled`.
    dynamic_filter_collector: Option<DynamicFilterCollector>,
    /// A1.4: session-level switch for the cross-fragment dynamic
    /// filtering feature. Default `false`; A1.6 flips it on once the
    /// SF10 Q09 gate passes. The flag gates the await in ScanExec —
    /// when off, the runtime behaves exactly as pre-A1.
    dynamic_filtering_enabled: bool,
    /// A1.4: timeout applied to each per-DF wait in ScanExec. The
    /// fallback on timeout is "no filter, scan everything", so this
    /// is purely a perf cap; correctness is preserved either way.
    dynamic_filtering_wait_timeout: Duration,
    /// A1.5 (2026-05-27): producer-side hook. HashJoinExec /
    /// SemiJoinExec call this once per `DynamicFilterProducer`
    /// annotation when their build phase finishes, shipping the
    /// partition's Domain to the coordinator's `DynamicFilterService`.
    /// `None` on coord-side / standalone / unit tests — operators
    /// then build no Domain and skip the publish.
    dynamic_filter_publisher: Option<DynamicFilterPublisherRef>,
    /// A2.2 (2026-05-28): broadcast-join size cap. When `Some(n)`, the
    /// `PlanFragmenter` marks a Join's build (right) child fragment as
    /// `PartitioningScheme::Broadcast` if its estimated output bytes
    /// fit within `n`. Mirrors Trino's `join_max_broadcast_table_size`
    /// session property (default 100 MB). `None` = broadcast disabled
    /// (the A2.2-landed default — A2.4 flips on for measurement).
    broadcast_max_build_bytes: Option<usize>,
    /// A2.2 (2026-05-28): per-query `CatalogStats` snapshot. Used by
    /// `PlanFragmenter` to estimate Join build-side bytes for the
    /// broadcast eligibility decision. Threaded through from
    /// `protocol::handler` via `plan_statement_with_context`. `None`
    /// on workers (only coord-side fragmenter consults it) and in
    /// unit tests.
    catalog_stats: Option<Arc<CatalogStats>>,
    /// Multi-worker scan parallelism `M`: the number of parallel tasks the
    /// coordinator scheduled for this scan SOURCE fragment. Each ScanExec
    /// built from this context reads only the strided 1/M of its
    /// DataSource partitions (stride index = `consumer_partition_id`).
    /// Defaults to `1` (single-task scan — reads every partition).
    scan_task_count: usize,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            data_sources: HashMap::new(),
            function_registry: Arc::new(default_registry()),
            stage_results: HashMap::new(),
            consumer_partition_id: 0,
            memory_pool: Arc::new(UnboundedMemoryPool::new()),
            inflight_budget: Arc::new(InflightBudget::new(0)),
            dynamic_filter_collector: None,
            // A1.6 (2026-05-27): default OFF. A1.5/A1.7 wire every
            // major HJ build path + SemiJoin's execute_inner pre-probe
            // emit, but several TPC-H SF1 queries (Q10/Q12/Q16/Q18/Q20)
            // still regress when the flag is default-ON — their
            // worker-side execution paths bypass the emit sites, so
            // the probe-side scan waits the full 10 s timeout. A1.6
            // is gated on the SF10 Q09 measurement; flipping the
            // default ON before that gate clears would be a SF1
            // latency regression with no observed win. Future work:
            // wire `SET dynamic_filtering_enabled = true` (session
            // param) so deployments can opt in per query; once every
            // producer path is covered + SF10 Q09 < 12 s gate clears,
            // flip the default back to true.
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT,
            dynamic_filter_publisher: None,
            // A2.2 (2026-05-28): default OFF. A2.4 flips on for the
            // SF10 Q09 measurement; if the gate clears, a follow-up
            // commit can switch the default to `Some(100 MiB)`.
            broadcast_max_build_bytes: None,
            catalog_stats: None,
            scan_task_count: 1,
        }
    }
}

impl ExecutionContext {
    /// Creates an execution context with the default function registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a reference to the function registry.
    pub fn function_registry(&self) -> &Arc<FunctionRegistry> {
        &self.function_registry
    }

    /// Registers a data source under the given key.
    pub fn register_data_source(&mut self, name: impl Into<String>, source: Arc<dyn DataSource>) {
        self.data_sources.insert(name.into(), source);
    }

    /// Build a transient clone of this context carrying distributed stage
    /// results, so the physical planner can wire `ExchangeNode` placeholders
    /// to concrete `ExchangeExec` operators that pull from remote workers.
    ///
    /// The `Arc<dyn DataSource>` clones are cheap; `data_sources` typically
    /// holds ≲ 20 entries even for TPC-H, so this is intended to be called
    /// once per distributed query on the coordinator.
    pub fn with_stage_results(
        &self,
        stage_results: HashMap<u32, Vec<(String, String, u32)>>,
    ) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results,
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// Build a transient clone with a specific `consumer_partition_id`. The
    /// worker that runs N parallel tasks of a partitioned stage calls this
    /// for each task so each task's `ExchangeExec` instances fetch their
    /// own partition slice of the upstream output.
    pub fn with_consumer_partition_id(&self, consumer_partition_id: u32) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// Multi-worker scan: install the scan parallelism `M` so every
    /// ScanExec built from this context reads only its strided 1/M of the
    /// DataSource partitions (stride index = `consumer_partition_id`). The
    /// worker sets this from `TaskDescriptor::scan_task_count`; `1` (the
    /// default) keeps the single-task whole-table scan.
    pub fn with_scan_task_count(&self, scan_task_count: usize) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count,
        }
    }

    /// Override the memory pool used by spillable operators built from
    /// this context. Distributed worker tasks call this to install a
    /// [`GreedyMemoryPool`](crate::memory_pool::GreedyMemoryPool) sized
    /// to a fraction of the container's memory budget.
    pub fn with_memory_pool(&self, memory_pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool,
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// Override the shared in-flight byte budget used by repartition
    /// operators planned inside this stage.
    pub fn with_inflight_budget(&self, inflight_budget: Arc<InflightBudget>) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget,
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// A1.4 (2026-05-27): install the per-task
    /// [`DynamicFilterCollector`] that scans will subscribe against.
    /// Workers call this from `TaskManager::handle_task` once per
    /// task; coord side and tests leave it `None` so ScanExec skips
    /// the cross-fragment wait.
    pub fn with_dynamic_filter_collector(
        &self,
        dynamic_filter_collector: Option<DynamicFilterCollector>,
    ) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector,
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// A1.4 (2026-05-27): toggle the cross-fragment dynamic filtering
    /// feature flag. Default off; A1.6 will flip the default after the
    /// SF10 Q09 gate passes. With the flag off, ScanExec does not
    /// wait on the collector regardless of whether it has annotations.
    pub fn with_dynamic_filtering_enabled(&self, enabled: bool) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// A1.4 (2026-05-27): override the per-DF wait timeout. Default
    /// is `DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT` (10 s). Soundness
    /// fallback on timeout is "no filter" so this only affects
    /// performance, not correctness.
    pub fn with_dynamic_filtering_wait_timeout(&self, timeout: Duration) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// A1.5 (2026-05-27): install the per-task
    /// [`DynamicFilterPublisher`]. Workers call this from
    /// `TaskManager::handle_task` once per task; coord side and unit
    /// tests leave it `None`, in which case HashJoinExec /
    /// SemiJoinExec skip the Domain build entirely.
    pub fn with_dynamic_filter_publisher(
        &self,
        publisher: Option<DynamicFilterPublisherRef>,
    ) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: publisher,
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// A2.2 (2026-05-28): set the broadcast-eligibility size cap. When
    /// `Some(n)`, the fragmenter marks Join builds smaller than `n`
    /// bytes for broadcast distribution. `None` disables the check —
    /// the fragmenter falls back to the W3-Hash α-model partitioning.
    pub fn with_broadcast_max_build_bytes(&self, bytes: Option<usize>) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: bytes,
            catalog_stats: self.catalog_stats.clone(),
            scan_task_count: self.scan_task_count,
        }
    }

    /// A2.2 (2026-05-28): attach a per-query `CatalogStats` snapshot.
    /// Threaded from `protocol::handler` after the planner runs the
    /// analyzer pipeline. Used by the fragmenter's broadcast decision.
    pub fn with_catalog_stats(&self, stats: Option<Arc<CatalogStats>>) -> Self {
        Self {
            data_sources: self.data_sources.clone(),
            function_registry: self.function_registry.clone(),
            stage_results: self.stage_results.clone(),
            consumer_partition_id: self.consumer_partition_id,
            memory_pool: self.memory_pool.clone(),
            inflight_budget: Arc::clone(&self.inflight_budget),
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            broadcast_max_build_bytes: self.broadcast_max_build_bytes,
            catalog_stats: stats,
            scan_task_count: self.scan_task_count,
        }
    }

    /// A2.2: read accessor for the broadcast-eligibility cap.
    pub fn broadcast_max_build_bytes(&self) -> Option<usize> {
        self.broadcast_max_build_bytes
    }

    /// A2.2: read accessor for the per-query stats snapshot.
    pub fn catalog_stats(&self) -> Option<&Arc<CatalogStats>> {
        self.catalog_stats.as_ref()
    }

    /// Access the per-task dynamic-filter collector, if installed.
    /// A1.4: ScanExec construction reads this to bake a clone into
    /// each leaf scan operator.
    pub fn dynamic_filter_collector(&self) -> Option<&DynamicFilterCollector> {
        self.dynamic_filter_collector.as_ref()
    }

    /// Access the cross-fragment dynamic filtering flag.
    pub fn dynamic_filtering_enabled(&self) -> bool {
        self.dynamic_filtering_enabled
    }

    /// Access the configured per-DF wait timeout.
    pub fn dynamic_filtering_wait_timeout(&self) -> Duration {
        self.dynamic_filtering_wait_timeout
    }

    /// A1.5: access the worker-side publisher used by HJ/SJ build
    /// phases. `None` on coord and standalone — those paths skip the
    /// publish.
    pub fn dynamic_filter_publisher(&self) -> Option<&DynamicFilterPublisherRef> {
        self.dynamic_filter_publisher.as_ref()
    }

    /// Internal helper: build a `ScanExec` from a `DataSource` and
    /// the per-scan context, populating the four A1.4 cross-fragment
    /// dynamic-filter fields from `self`. Centralises the boilerplate
    /// shared by the four `ScanExec` construction sites in `convert()`.
    fn build_scan_exec(
        &self,
        source: Arc<dyn DataSource>,
        table_name: String,
        scan_context: ScanContext,
        dynamic_filters_consumed: Vec<arneb_planner::DynamicFilterConsumer>,
    ) -> ScanExec {
        ScanExec {
            source,
            _table_name: table_name,
            scan_context,
            dynamic_filters: Default::default(),
            dynamic_filters_consumed,
            dynamic_filter_collector: self.dynamic_filter_collector.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            dynamic_filtering_wait_timeout: self.dynamic_filtering_wait_timeout,
            // Multi-worker scan: this task is `consumer_partition_id` of
            // `scan_task_count` (set by the worker from the TaskDescriptor);
            // the ScanExec reads only its strided 1/M of the partitions.
            scan_task_index: self.consumer_partition_id as usize,
            scan_task_count: self.scan_task_count,
        }
    }

    /// Access the active memory pool (e.g. so a spillable operator can
    /// register a [`MemoryConsumer`](crate::memory_pool::MemoryConsumer)).
    pub fn memory_pool(&self) -> &Arc<dyn MemoryPool> {
        &self.memory_pool
    }

    fn estimated_groups_for(&self, logical: &LogicalPlan) -> Option<usize> {
        self.catalog_stats.as_ref().and_then(|stats| {
            let estimate = estimated_cardinality(logical, stats);
            if estimate.is_finite() && estimate > 0.0 {
                Some(estimate.ceil() as usize)
            } else {
                None
            }
        })
    }

    fn all_scans_have_row_count(logical: &LogicalPlan, stats: &CatalogStats) -> bool {
        match logical {
            LogicalPlan::TableScan { table, .. } => {
                stats.get(table).and_then(|s| s.row_count).is_some()
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Projection { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. }
            | LogicalPlan::ScalarSubquery { subplan: input } => {
                Self::all_scans_have_row_count(input, stats)
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => {
                Self::all_scans_have_row_count(left, stats)
                    && Self::all_scans_have_row_count(right, stats)
            }
            LogicalPlan::UnionAll { inputs } => inputs
                .iter()
                .all(|input| Self::all_scans_have_row_count(input, stats)),
            LogicalPlan::ExchangeNode { .. } => false,
            LogicalPlan::OneRow
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateTableAsSelect { .. }
            | LogicalPlan::InsertInto { .. }
            | LogicalPlan::DeleteFrom { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. } => true,
        }
    }

    fn semi_mark_join_build_left(&self, left: &LogicalPlan, right: &LogicalPlan) -> bool {
        if !crate::semi_join::semi_mark_join_enabled() {
            return false;
        }
        let Some(stats) = self.catalog_stats.as_ref() else {
            return false;
        };
        if !Self::all_scans_have_row_count(left, stats)
            || !Self::all_scans_have_row_count(right, stats)
        {
            return false;
        }
        let left_rows = estimated_cardinality(left, stats);
        let right_rows = estimated_cardinality(right, stats);
        left_rows.is_finite()
            && right_rows.is_finite()
            && left_rows > 0.0
            && right_rows > 0.0
            && left_rows * 4.0 < right_rows
    }

    /// Pre-evaluate scalar subqueries in a PlanExpr, replacing them with Literal values.
    pub async fn resolve_scalar_subqueries(
        &self,
        expr: &PlanExpr,
    ) -> Result<PlanExpr, ExecutionError> {
        match expr {
            PlanExpr::ScalarSubquery { subplan, span } => {
                let exec = self.create_physical_plan(subplan)?;
                let stream = exec.execute(0).await?;
                let batches = arneb_common::stream::collect_stream(stream)
                    .await
                    .map_err(|e| {
                        ExecutionError::InvalidOperation(format!("scalar subquery failed: {e}"))
                    })?;
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if total_rows > 1 {
                    return Err(ExecutionError::InvalidOperation(
                        "scalar subquery must return at most one row".to_string(),
                    ));
                }
                if total_rows == 0 || batches.is_empty() {
                    return Ok(PlanExpr::Literal {
                        value: arneb_common::types::ScalarValue::Null,
                        span: *span,
                    });
                }
                let col = batches[0].column(0);
                if col.is_null(0) {
                    return Ok(PlanExpr::Literal {
                        value: arneb_common::types::ScalarValue::Null,
                        span: *span,
                    });
                }
                let val = arrow_to_scalar(col, 0);
                Ok(PlanExpr::Literal {
                    value: val,
                    span: *span,
                })
            }
            PlanExpr::BinaryOp {
                left,
                op,
                right,
                span,
            } => {
                let l = Box::pin(self.resolve_scalar_subqueries(left)).await?;
                let r = Box::pin(self.resolve_scalar_subqueries(right)).await?;
                Ok(PlanExpr::BinaryOp {
                    left: Box::new(l),
                    op: *op,
                    right: Box::new(r),
                    span: *span,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    /// Creates a physical execution plan from a logical plan.
    pub fn create_physical_plan(
        &self,
        logical: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        // Step CP2 (2026-05-16): logical pre-pass that propagates
        // "needed columns" top-down through Filter/Projection/
        // Aggregate/Sort/Limit/Join(Inner) into TableScans, wrapping
        // each pruned scan with a `Projection(needed cols)` and
        // rewriting all upstream column references through an
        // old→new index mapping. The existing Projection-over-
        // TableScan pushdown in `convert` then translates the
        // wrapped Projection into a `ScanContext.projection`, so
        // the Parquet reader skips unused columns at decode time.
        //
        // For multi-table inner joins (Q12, parts of Q05/Q10) this
        // extends the single-table CP gain (Q01 -67%) to join paths
        // — lineitem's 16 cols typically reduce to 3-5 actually
        // used. Non-INNER joins and unsupported variants bail out
        // (return original subtree + identity mapping).
        let needed_at_root: std::collections::BTreeSet<usize> =
            (0..logical.schema().len()).collect();
        let (pruned, _) = prune_for_columns(logical, &needed_at_root);
        self.convert(&pruned)
    }

    fn convert(&self, logical: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        // (`wrap_multipartition_scan` lives at module scope — see below.)
        match logical {
            LogicalPlan::TableScan {
                table,
                dynamic_filters_consumed,
                ..
            } => {
                let key = table.to_string();
                let source = self
                    .data_sources
                    .get(&key)
                    .or_else(|| self.data_sources.get(&table.table))
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(format!(
                            "data source not found for table '{key}'"
                        ))
                    })?;
                let scan: Arc<dyn ExecutionPlan> = Arc::new(self.build_scan_exec(
                    source.clone(),
                    key,
                    ScanContext::default(),
                    dynamic_filters_consumed.clone(),
                ));
                // Expose multi-partition output downstream; stateful
                // operators (Aggregate/Sort/Join/Limit/...) coalesce
                // their own inputs via `coalesce_if_multi`.
                Ok(scan)
            }

            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => {
                if let LogicalPlan::FinalAggregate {
                    input: agg_input,
                    group_by,
                    aggr_exprs,
                    schema: _,
                } = input.as_ref()
                {
                    if let Some(output_order) =
                        aggregate_projection_output_order(exprs, group_by.len(), aggr_exprs.len())
                    {
                        let estimated_groups = self.estimated_groups_for(input.as_ref());
                        let input_plan = self.convert(agg_input)?;
                        return Ok(Arc::new(HashAggregateExec {
                            input: input_plan,
                            group_by: group_by.clone(),
                            aggr_exprs: aggr_exprs.clone(),
                            output_schema: schema.clone(),
                            output_order: Some(output_order),
                            estimated_groups,
                            memory_pool: self.memory_pool.clone(),
                        }));
                    }
                }

                // Attempt projection pushdown: if input is a TableScan and all
                // exprs are simple column references, push projection into ScanContext.
                if let LogicalPlan::TableScan {
                    table,
                    dynamic_filters_consumed,
                    ..
                } = input.as_ref()
                {
                    let column_indices: Option<Vec<usize>> = exprs
                        .iter()
                        .map(|e| match e {
                            arneb_planner::PlanExpr::Column { index, .. } => Some(*index),
                            _ => None,
                        })
                        .collect();

                    if let Some(indices) = column_indices {
                        let key = table.to_string();
                        let source = self
                            .data_sources
                            .get(&key)
                            .or_else(|| self.data_sources.get(&table.table))
                            .ok_or_else(|| {
                                ExecutionError::InvalidOperation(format!(
                                    "data source not found for table '{key}'"
                                ))
                            })?;
                        let scan_ctx = ScanContext::default().with_projection(indices.clone());
                        let scan: Arc<dyn ExecutionPlan> = Arc::new(self.build_scan_exec(
                            source.clone(),
                            key,
                            scan_ctx,
                            dynamic_filters_consumed.clone(),
                        ));
                        // ProjectionExec is stateless and inherits the
                        // scan's multi-partition output.
                        // Rewrite exprs to use sequential indices since the scan
                        // output now contains only the projected columns in order.
                        let rewritten_exprs: Vec<_> = indices
                            .iter()
                            .enumerate()
                            .map(|(new_idx, _)| {
                                let orig = &exprs[new_idx];
                                match orig {
                                    arneb_planner::PlanExpr::Column { name, span, .. } => {
                                        arneb_planner::PlanExpr::Column {
                                            index: new_idx,
                                            name: name.clone(),
                                            span: *span,
                                        }
                                    }
                                    other => other.clone(),
                                }
                            })
                            .collect();
                        return Ok(Arc::new(ProjectionExec {
                            input: scan,
                            exprs: rewritten_exprs,
                            output_schema: schema.clone(),
                        }));
                    }
                }

                let input_plan = self.convert(input)?;
                Ok(Arc::new(ProjectionExec {
                    input: input_plan,
                    exprs: exprs.clone(),
                    output_schema: schema.clone(),
                }))
            }

            LogicalPlan::Filter { input, predicate } => {
                // When filtering a table scan, pass the filter to ScanContext
                // for potential pushdown into the connector. The FilterExec is
                // still kept for correctness — the connector may only partially
                // support the predicate.
                let input_plan = if let LogicalPlan::TableScan {
                    table,
                    dynamic_filters_consumed,
                    ..
                } = input.as_ref()
                {
                    let key = table.to_string();
                    let source = self
                        .data_sources
                        .get(&key)
                        .or_else(|| self.data_sources.get(&table.table))
                        .ok_or_else(|| {
                            ExecutionError::InvalidOperation(format!(
                                "data source not found for table '{key}'"
                            ))
                        })?;
                    let scan_ctx = ScanContext::default().with_filters(vec![predicate.clone()]);
                    let scan: Arc<dyn ExecutionPlan> = Arc::new(self.build_scan_exec(
                        source.clone(),
                        key,
                        scan_ctx,
                        dynamic_filters_consumed.clone(),
                    ));
                    // FilterExec inherits scan's multi-partition output;
                    // no coalesce here, downstream stateful ops decide.
                    scan
                } else {
                    self.convert(input)?
                };
                Ok(Arc::new(FilterExec {
                    input: input_plan,
                    predicate: predicate.clone(),
                }))
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                dynamic_filter_ids,
            } => {
                let left_plan = self.convert(left)?;
                let right_plan = self.convert(right)?;
                let left_col_count = left_plan.schema().len();

                // Try to use hash join for equi-join conditions. A residual
                // non-equi predicate (e.g. `AND o_comment NOT LIKE '%x%'` in
                // TPC-H Q13) is carried through and re-evaluated on each
                // candidate match so outer-join semantics stay correct.
                if let Some((key_pairs, residual)) =
                    extract_equi_join_keys(condition, left_col_count)
                {
                    let (left_keys, right_keys): (Vec<usize>, Vec<usize>) =
                        key_pairs.into_iter().unzip();
                    // Same-fragment dynamic-filter targets: for each probe key,
                    // the probe-subtree columns join-equal to it (its
                    // equivalence class). The build-side IN(...) filter is
                    // injected only at these indices, never by name — so a
                    // self-join twin that merely shares the key's name is not
                    // misrouted (TPC-H Q08), while a transitively-equal
                    // cross-table sibling still is (Q18). Indices are in the
                    // left (probe) child's output schema, which `build_join_inputs`
                    // preserves, so the runtime index descent aligns.
                    let df_targets: Vec<Vec<usize>> = left_keys
                        .iter()
                        .map(|&lk| arneb_planner::properties::equivalent_output_columns(left, lk))
                        .collect();
                    let (left_arg, right_arg) = build_join_inputs(
                        left_plan,
                        right_plan,
                        &left_keys,
                        &right_keys,
                        *join_type,
                        &self.inflight_budget,
                    );
                    return Ok(HashJoinExec {
                        left: left_arg,
                        right: right_arg,
                        join_type: *join_type,
                        left_keys,
                        right_keys,
                        residual,
                        build_state: Default::default(),
                        peak_build_bytes: Default::default(),
                        memory_pool: self.memory_pool.clone(),
                        // A1.5 (2026-05-27): pass through the planner
                        // annotation. With flag OFF and/or no publisher
                        // installed, the build phase just ignores it.
                        dynamic_filter_producers: dynamic_filter_ids.clone(),
                        dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
                        dynamic_filtering_enabled: self.dynamic_filtering_enabled,
                        df_targets,
                    }
                    .new_arc());
                }

                // Fall back to nested loop for non-equi joins.
                Ok(Arc::new(NestedLoopJoinExec {
                    left: coalesce_if_multi(left_plan),
                    right: coalesce_if_multi(right_plan),
                    join_type: *join_type,
                    condition: condition.clone(),
                }))
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                let estimated_groups = self.estimated_groups_for(logical);
                // Step CP (2026-05-16): combined column-projection
                // and predicate pushdown for `Aggregate over
                // [Filter] over TableScan` — the dominant shape for
                // pure-aggregate TPC-H queries (Q01, Q06). The
                // earlier Projection-over-TableScan pushdown didn't
                // fire on these because there is no intermediate
                // Projection — the Aggregate references TableScan
                // columns directly. Without pruning, Q01 reads 16
                // lineitem columns when it only needs 6; Q06 reads
                // 16 when it needs 4. Parquet decode is per-column,
                // so the saved I/O is large.
                if let Some(pruned) = self.try_aggregate_with_scan_pruning(
                    input.as_ref(),
                    group_by,
                    aggr_exprs,
                    schema,
                    estimated_groups,
                )? {
                    return Ok(pruned);
                }

                // Streaming fast path: when group_by contains a column
                // produced by an `AssignUniqueId` (i.e., monotone /
                // contiguous-runs in the input stream), use the
                // hash-free fold aggregate. Saves the 36-key row-hash
                // overhead in the F-Perf11c Q21 rewrite.
                if let Some(unique_idx) = find_unique_key_idx(input.as_ref(), group_by) {
                    let input_plan = coalesce_if_multi(self.convert(input)?);
                    return Ok(Arc::new(crate::operator::StreamingHashAggregateExec {
                        input: input_plan,
                        group_by: group_by.clone(),
                        aggr_exprs: aggr_exprs.clone(),
                        output_schema: schema.clone(),
                        unique_key_idx: unique_idx,
                        memory_pool: self.memory_pool.clone(),
                    }));
                }

                // HashAggregateExec handles multi-partition input natively:
                // each input partition is processed in parallel and the
                // partial maps merged via `Accumulator::merge`. No
                // coalesce wrap needed.
                let input_plan = self.convert(input)?;
                Ok(Arc::new(HashAggregateExec {
                    input: input_plan,
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    output_schema: schema.clone(),
                    output_order: None,
                    estimated_groups,
                    memory_pool: self.memory_pool.clone(),
                }))
            }

            LogicalPlan::Sort { input, order_by } => {
                let input_plan = coalesce_if_multi(self.convert(input)?);
                Ok(Arc::new(SortExec {
                    input: input_plan,
                    order_by: order_by.clone(),
                    memory_pool: self.memory_pool.clone(),
                }))
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                // Top-K rewrite: `LIMIT k` directly on top of a Sort
                // (no OFFSET, finite k) collapses into TopKExec to
                // skip the full O(n log n) sort. Saves big on TPC-H
                // Q02/Q03/Q10 where k is small and the sort input is
                // millions of rows.
                if let (Some(k), None) = (*limit, *offset) {
                    if let LogicalPlan::Sort {
                        input: sort_input,
                        order_by,
                    } = input.as_ref()
                    {
                        let inner = coalesce_if_multi(self.convert(sort_input)?);
                        return Ok(Arc::new(TopKExec {
                            input: inner,
                            order_by: order_by.clone(),
                            fetch: k,
                            memory_pool: self.memory_pool.clone(),
                        }));
                    }
                }
                let input_plan = coalesce_if_multi(self.convert(input)?);
                Ok(Arc::new(LimitExec {
                    input: input_plan,
                    limit: *limit,
                    offset: *offset,
                }))
            }

            LogicalPlan::Explain { input, analyze } => {
                let inner_physical = if *analyze {
                    Some(self.convert(input)?)
                } else {
                    None
                };
                Ok(Arc::new(ExplainExec {
                    plan: *input.clone(),
                    analyze_inner: inner_physical,
                }))
            }

            // PartialAggregate and FinalAggregate are treated as regular Aggregate
            // in single-node mode (no distribution).
            LogicalPlan::PartialAggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            }
            | LogicalPlan::FinalAggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                let estimated_groups = self.estimated_groups_for(logical);
                // Same multi-partition handling as `Aggregate` above.
                let input_plan = self.convert(input)?;
                Ok(Arc::new(HashAggregateExec {
                    input: input_plan,
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    output_schema: schema.clone(),
                    output_order: None,
                    estimated_groups,
                    memory_pool: self.memory_pool.clone(),
                }))
            }

            LogicalPlan::ExchangeNode { stage_id, schema } => {
                // Look up the worker tasks that produced this stage. The
                // coordinator populates `stage_results` in
                // `QueryCoordinator::execute` before calling
                // `create_physical_plan_with_stages` (which in turn builds
                // the transient context via `with_stage_results`).
                let stage_key = stage_id.0;
                let candidates = self.stage_results.get(&stage_key).ok_or_else(|| {
                    ExecutionError::InvalidOperation(format!(
                        "ExchangeNode references stage {stage_key} but no stage_results \
                         entry exists; the coordinator must dispatch the source fragment \
                         and populate stage_results before building the root physical plan"
                    ))
                })?;
                if candidates.is_empty() {
                    return Err(ExecutionError::InvalidOperation(format!(
                        "ExchangeNode stage {stage_key} resolved to empty result list"
                    )));
                }
                if candidates.len() == 1 {
                    // Single producer task — fetch the pre-resolved partition.
                    // Coord (M×N step 3a) set `sx.partition_id` per consumer
                    // task: 0 for single-partition upstreams, `consumer_k`
                    // for multi-partition upstreams (β: 1 task × N parts).
                    let (addr, task_id, partition_id) = candidates[0].clone();
                    Ok(Arc::new(crate::distributed::ExchangeExec::new(
                        addr,
                        task_id,
                        partition_id,
                        schema.clone(),
                    )))
                } else {
                    // M producer tasks — gather via UnionAll. Each sibling's
                    // partition_id is pre-resolved by coord (step 3a):
                    //   - upstream emits 1 partition/task (current α model
                    //     with empty hash cols): partition_id = 0 for all.
                    //   - upstream emits N partitions/task (M×N, post step 4):
                    //     partition_id = consumer_k for all M siblings,
                    //     making each consumer pull its own bucket from
                    //     every upstream task.
                    let exchanges: Vec<Arc<dyn ExecutionPlan>> = candidates
                        .iter()
                        .map(|(addr, task_id, partition_id)| {
                            Arc::new(crate::distributed::ExchangeExec::new(
                                addr.clone(),
                                task_id.clone(),
                                *partition_id,
                                schema.clone(),
                            )) as Arc<dyn ExecutionPlan>
                        })
                        .collect();
                    Ok(Arc::new(crate::set_ops::UnionAllExec::new(exchanges)))
                }
            }

            LogicalPlan::SemiJoin {
                left,
                right,
                left_key,
                right_key,
                residual,
                dynamic_filter_ids,
                ..
            } => {
                let left_exec = coalesce_if_multi(self.create_physical_plan(left)?);
                let right_exec = coalesce_if_multi(self.create_physical_plan(right)?);
                let build_left = self.semi_mark_join_build_left(left, right);
                // Same-fragment dynamic-filter targets for the probe key:
                // its equivalence class within the left subtree (index
                // descent, never by name — so a self-join twin sharing the
                // key's name is not misrouted).
                let df_targets = match left_key {
                    PlanExpr::Column { index, .. } => {
                        arneb_planner::properties::equivalent_output_columns(left, *index)
                    }
                    _ => Vec::new(),
                };
                Ok(Arc::new(
                    crate::semi_join::SemiJoinExec::new(
                        left_exec,
                        right_exec,
                        left_key.clone(),
                        right_key.clone(),
                        residual.clone(),
                        false,
                        self.memory_pool.clone(),
                    )
                    .with_build_left(build_left)
                    .with_dynamic_filters(
                        dynamic_filter_ids.clone(),
                        self.dynamic_filter_publisher.clone(),
                        self.dynamic_filtering_enabled,
                    )
                    .with_df_targets(df_targets),
                ))
            }

            LogicalPlan::AntiJoin {
                left,
                right,
                left_key,
                right_key,
                residual,
            } => {
                let left_exec = coalesce_if_multi(self.create_physical_plan(left)?);
                let right_exec = coalesce_if_multi(self.create_physical_plan(right)?);
                let build_left = self.semi_mark_join_build_left(left, right);
                Ok(Arc::new(
                    crate::semi_join::SemiJoinExec::new(
                        left_exec,
                        right_exec,
                        left_key.clone(),
                        right_key.clone(),
                        residual.clone(),
                        true,
                        self.memory_pool.clone(),
                    )
                    .with_build_left(build_left),
                ))
            }

            LogicalPlan::ScalarSubquery { subplan } => {
                let sub_exec = coalesce_if_multi(self.create_physical_plan(subplan)?);
                Ok(Arc::new(crate::scalar_subquery::ScalarSubqueryExec::new(
                    sub_exec,
                )))
            }

            LogicalPlan::UnionAll { inputs } => {
                let children: Vec<Arc<dyn ExecutionPlan>> = inputs
                    .iter()
                    .map(|p| self.create_physical_plan(p).map(coalesce_if_multi))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(crate::set_ops::UnionAllExec::new(children)))
            }

            LogicalPlan::Distinct { input } => {
                let child = coalesce_if_multi(self.create_physical_plan(input)?);
                Ok(Arc::new(crate::set_ops::DistinctExec::new(child)))
            }

            LogicalPlan::Intersect { left, right } => {
                let l = coalesce_if_multi(self.create_physical_plan(left)?);
                let r = coalesce_if_multi(self.create_physical_plan(right)?);
                Ok(Arc::new(crate::set_ops::IntersectExec::new(l, r)))
            }

            LogicalPlan::Except { left, right } => {
                let l = coalesce_if_multi(self.create_physical_plan(left)?);
                let r = coalesce_if_multi(self.create_physical_plan(right)?);
                Ok(Arc::new(crate::set_ops::ExceptExec::new(l, r)))
            }

            // DDL/DML plans are handled at the protocol/server level, not here
            LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateTableAsSelect { .. }
            | LogicalPlan::InsertInto { .. }
            | LogicalPlan::DeleteFrom { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. } => Err(ExecutionError::InvalidOperation(
                "DDL/DML plans are handled at the protocol level, not the execution engine"
                    .to_string(),
            )),

            LogicalPlan::Window { input, functions } => {
                let child = coalesce_if_multi(self.create_physical_plan(input)?);
                Ok(Arc::new(crate::window::WindowExec::new(
                    child,
                    functions.clone(),
                )))
            }

            LogicalPlan::AssignUniqueId { input, id_column } => {
                // Stateless per-row pass-through: keep multi-partition
                // input so downstream parallelism survives.
                let child = self.convert(input)?;
                Ok(Arc::new(crate::operator::AssignUniqueIdExec::new(
                    child,
                    id_column.clone(),
                )))
            }

            LogicalPlan::OneRow => Ok(Arc::new(crate::operator::OneRowExec)),
        }
    }

    /// Step CP column-pruning fast path. Returns `Some(physical_plan)`
    /// iff the shape is `Aggregate over [Filter over] TableScan` AND
    /// the union of columns referenced by group_by / aggr_exprs /
    /// predicate is a strict subset of the table's full schema.
    ///
    /// Effect: ScanContext gets a `projection` listing only the used
    /// indices, and (when present) the rewritten predicate is also
    /// pushed via `filters` for Parquet row-group / row-level
    /// pruning. The remaining group_by / aggr_exprs / FilterExec
    /// predicate are rewritten with the old→new column-index
    /// mapping. The Aggregate output schema is unchanged.
    fn try_aggregate_with_scan_pruning(
        &self,
        input: &LogicalPlan,
        group_by: &[PlanExpr],
        aggr_exprs: &[PlanExpr],
        out_schema: &[arneb_common::types::ColumnInfo],
        estimated_groups: Option<usize>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, ExecutionError> {
        use std::collections::BTreeSet;

        // Detect `Aggregate over Filter over TableScan` or
        // `Aggregate over TableScan`.
        let (filter_predicate, scan_node) = match input {
            LogicalPlan::Filter {
                input: f_in,
                predicate,
            } => (Some(predicate), f_in.as_ref()),
            other => (None, other),
        };
        let (table_ref, scan_schema, scan_consumed) = match scan_node {
            LogicalPlan::TableScan {
                table,
                schema,
                dynamic_filters_consumed,
                ..
            } => (table, schema, dynamic_filters_consumed),
            _ => return Ok(None),
        };

        // Collect used column indices from all the expressions that
        // reference scan output.
        let mut used: BTreeSet<usize> = BTreeSet::new();
        for e in group_by.iter().chain(aggr_exprs.iter()) {
            collect_column_indices_into(e, &mut used);
        }
        if let Some(p) = filter_predicate {
            collect_column_indices_into(p, &mut used);
        }

        // No pruning win when every column is used. Skip the empty-
        // set case too (COUNT(*) over a filter only — leaving the
        // default path is harmless and avoids reasoning about
        // empty-projection semantics in the connector).
        if used.is_empty() || used.len() >= scan_schema.len() {
            return Ok(None);
        }

        let indices: Vec<usize> = used.iter().copied().collect();
        let mapping: HashMap<usize, usize> = indices
            .iter()
            .enumerate()
            .map(|(new, &old)| (old, new))
            .collect();

        let key = table_ref.to_string();
        let source = self
            .data_sources
            .get(&key)
            .or_else(|| self.data_sources.get(&table_ref.table))
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(format!(
                    "data source not found for table '{key}' (column pruning)"
                ))
            })?;

        // Push only the projection. The Parquet pushdown layer
        // (`crates/connectors/src/parquet_pushdown.rs`) resolves the
        // filter against the SCAN's full (pre-projection) schema for
        // row-group min/max statistics, so our rewritten predicate
        // (which uses the new projected indices) would mismatch and
        // raise "Float64 <= Date32"-style type errors. The filter
        // therefore runs only at the FilterExec level above the
        // ScanExec — we still get the I/O win from reading fewer
        // columns, just not the row-group pruning.
        let scan_ctx = ScanContext::default().with_projection(indices.clone());
        let rewritten_predicate = filter_predicate.map(|p| rewrite_column_indices(p, &mapping));
        let scan: Arc<dyn ExecutionPlan> =
            Arc::new(self.build_scan_exec(source.clone(), key, scan_ctx, scan_consumed.clone()));

        let after_filter: Arc<dyn ExecutionPlan> = if let Some(rp) = rewritten_predicate {
            Arc::new(FilterExec {
                input: scan,
                predicate: rp,
            })
        } else {
            scan
        };

        let new_group_by: Vec<PlanExpr> = group_by
            .iter()
            .map(|e| rewrite_column_indices(e, &mapping))
            .collect();
        let new_aggr_exprs: Vec<PlanExpr> = aggr_exprs
            .iter()
            .map(|e| rewrite_column_indices(e, &mapping))
            .collect();

        Ok(Some(Arc::new(HashAggregateExec {
            input: after_filter,
            group_by: new_group_by,
            aggr_exprs: new_aggr_exprs,
            output_schema: out_schema.to_vec(),
            output_order: None,
            estimated_groups,
            memory_pool: self.memory_pool.clone(),
        })))
    }
}

/// Walks `expr` and inserts every `Column { index }` it finds into
/// `out`. Stops at `ScalarSubquery` boundaries — those reference
/// columns from a different scope.
fn collect_column_indices_into(expr: &PlanExpr, out: &mut std::collections::BTreeSet<usize>) {
    use PlanExpr as E;
    match expr {
        E::Column { index, .. } => {
            out.insert(*index);
        }
        E::Literal { .. } | E::Wildcard | E::Parameter { .. } => {}
        E::BinaryOp { left, right, .. } => {
            collect_column_indices_into(left, out);
            collect_column_indices_into(right, out);
        }
        E::UnaryOp { expr, .. }
        | E::IsNull { expr, .. }
        | E::IsNotNull { expr, .. }
        | E::Cast { expr, .. } => collect_column_indices_into(expr, out),
        E::Between {
            expr, low, high, ..
        } => {
            collect_column_indices_into(expr, out);
            collect_column_indices_into(low, out);
            collect_column_indices_into(high, out);
        }
        E::InList { expr, list, .. } => {
            collect_column_indices_into(expr, out);
            for e in list {
                collect_column_indices_into(e, out);
            }
        }
        E::Function { args, .. } => {
            for a in args {
                collect_column_indices_into(a, out);
            }
        }
        E::ScalarSubquery { .. } => {
            // Subquery columns belong to a different scope.
        }
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                collect_column_indices_into(o, out);
            }
            for (w, r) in when_clauses {
                collect_column_indices_into(w, out);
                collect_column_indices_into(r, out);
            }
            if let Some(e) = else_result {
                collect_column_indices_into(e, out);
            }
        }
    }
}

fn aggregate_projection_output_order(
    exprs: &[PlanExpr],
    group_count: usize,
    aggr_count: usize,
) -> Option<Vec<AggregateOutputColumn>> {
    exprs
        .iter()
        .map(|expr| match expr {
            PlanExpr::Column { index, .. } if *index < group_count => {
                Some(AggregateOutputColumn::Group(*index))
            }
            PlanExpr::Column { index, .. } if *index < group_count + aggr_count => {
                Some(AggregateOutputColumn::Aggregate(*index - group_count))
            }
            _ => None,
        })
        .collect()
}

/// Walks `expr` and returns a copy where every `Column { index }` has
/// its `index` remapped through `mapping`. Columns not in the mapping
/// keep their original index (defensive — the caller should ensure
/// every referenced column was collected first).
fn rewrite_column_indices(expr: &PlanExpr, mapping: &HashMap<usize, usize>) -> PlanExpr {
    use PlanExpr as E;
    match expr {
        E::Column { index, name, span } => E::Column {
            index: *mapping.get(index).unwrap_or(index),
            name: name.clone(),
            span: *span,
        },
        E::Literal { .. } | E::Wildcard | E::Parameter { .. } => expr.clone(),
        E::BinaryOp {
            left,
            op,
            right,
            span,
        } => E::BinaryOp {
            left: Box::new(rewrite_column_indices(left, mapping)),
            op: *op,
            right: Box::new(rewrite_column_indices(right, mapping)),
            span: *span,
        },
        E::UnaryOp { op, expr, span } => E::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_column_indices(expr, mapping)),
            span: *span,
        },
        E::Function {
            name,
            args,
            distinct,
            span,
        } => E::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_column_indices(a, mapping))
                .collect(),
            distinct: *distinct,
            span: *span,
        },
        E::IsNull { expr, span } => E::IsNull {
            expr: Box::new(rewrite_column_indices(expr, mapping)),
            span: *span,
        },
        E::IsNotNull { expr, span } => E::IsNotNull {
            expr: Box::new(rewrite_column_indices(expr, mapping)),
            span: *span,
        },
        E::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => E::Between {
            expr: Box::new(rewrite_column_indices(expr, mapping)),
            negated: *negated,
            low: Box::new(rewrite_column_indices(low, mapping)),
            high: Box::new(rewrite_column_indices(high, mapping)),
            span: *span,
        },
        E::InList {
            expr,
            list,
            negated,
            span,
        } => E::InList {
            expr: Box::new(rewrite_column_indices(expr, mapping)),
            list: list
                .iter()
                .map(|a| rewrite_column_indices(a, mapping))
                .collect(),
            negated: *negated,
            span: *span,
        },
        E::Cast {
            expr,
            data_type,
            span,
        } => E::Cast {
            expr: Box::new(rewrite_column_indices(expr, mapping)),
            data_type: data_type.clone(),
            span: *span,
        },
        E::ScalarSubquery { subplan, span } => E::ScalarSubquery {
            // Subquery is independent — columns inside reference its
            // own scope, not the outer one being remapped.
            subplan: subplan.clone(),
            span: *span,
        },
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => E::CaseExpr {
            operand: operand
                .as_ref()
                .map(|o| Box::new(rewrite_column_indices(o, mapping))),
            when_clauses: when_clauses
                .iter()
                .map(|(w, r)| {
                    (
                        rewrite_column_indices(w, mapping),
                        rewrite_column_indices(r, mapping),
                    )
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(rewrite_column_indices(e, mapping))),
            span: *span,
        },
    }
}

/// Logical pre-pass that propagates a "needed columns" set top-down
/// through the tree and rewrites column indices bottom-up. Returns
/// the pruned plan and a mapping from the original output indices to
/// the new (post-prune) output indices.
///
/// Supported variants:
/// - TableScan: wraps with `Projection(needed cols)` when prunable.
/// - Filter / Sort / Limit: pass-through schema; child needed =
///   self needed ∪ predicate / sort cols.
/// - Projection / Aggregate: schema-changing — child needed is what
///   the node's own expressions reference (parent needed becomes
///   irrelevant once the schema is rewritten). Output mapping is
///   identity since these nodes emit the same logical schema.
/// - Join (INNER only): splits needed into per-side, recurses each,
///   combines the per-side mappings into a joined mapping. Non-INNER
///   joins bail out to preserve unmatched-row semantics.
/// - Other variants: identity bail-out.
pub fn prune_for_columns(
    plan: &arneb_planner::LogicalPlan,
    needed: &std::collections::BTreeSet<usize>,
) -> (arneb_planner::LogicalPlan, HashMap<usize, usize>) {
    use arneb_common::types::ColumnInfo;
    use arneb_planner::{JoinCondition, LogicalPlan as L, PlanExpr};
    use arneb_sql_parser::ast;
    use std::collections::BTreeSet;

    let n_out = plan.schema().len();

    match plan {
        L::TableScan {
            table,
            schema,
            alias,
            properties,
            dynamic_filters_consumed,
        } => {
            // Skip pruning when the parent needs every column anyway,
            // OR when the parent needs zero columns. The zero case
            // (e.g. `SELECT COUNT(*) FROM t`) would otherwise produce
            // an empty-schema scan, which `RecordBatch::try_new`
            // rejects with "must either specify a row count or at
            // least one column".
            if needed.is_empty() || needed.len() >= n_out {
                return (plan.clone(), identity_map(n_out));
            }
            let indices: Vec<usize> = needed.iter().copied().collect();
            let proj_exprs: Vec<PlanExpr> = indices
                .iter()
                .map(|&i| PlanExpr::Column {
                    index: i,
                    name: schema[i].name.clone(),
                    span: None,
                })
                .collect();
            let projected_schema: Vec<ColumnInfo> =
                indices.iter().map(|&i| schema[i].clone()).collect();
            // A1.4 (2026-05-27): the wrapped TableScan keeps its
            // ORIGINAL schema (the projection lives in the wrapping
            // node below), so the `column_index` inside each
            // `DynamicFilterConsumer` still points at the right
            // column. Preserve the annotations so `convert()` can
            // hand them to `ScanExec` and the cross-fragment wait
            // actually fires when the feature flag is on.
            let scan = L::TableScan {
                table: table.clone(),
                schema: schema.clone(),
                alias: alias.clone(),
                properties: properties.clone(),
                dynamic_filters_consumed: dynamic_filters_consumed.clone(),
            };
            let projection = L::Projection {
                input: Box::new(scan),
                exprs: proj_exprs,
                schema: projected_schema,
            };
            let mapping: HashMap<usize, usize> = indices
                .iter()
                .enumerate()
                .map(|(new, &old)| (old, new))
                .collect();
            (projection, mapping)
        }
        L::Filter { input, predicate } => {
            let mut child_needed = needed.clone();
            collect_column_indices_into(predicate, &mut child_needed);
            let (new_input, child_mapping) = prune_for_columns(input, &child_needed);
            let new_predicate = rewrite_column_indices(predicate, &child_mapping);
            (
                L::Filter {
                    input: Box::new(new_input),
                    predicate: new_predicate,
                },
                child_mapping,
            )
        }
        L::Projection {
            input,
            exprs,
            schema,
        } => {
            // Keep only the projection outputs the parent actually
            // references (`needed`). Without this, a Projection that
            // forwards every input column acts as an opaque barrier:
            // parent's Aggregate may need 6 cols but the Projection
            // carries all 32 from below, defeating pruning into the
            // scans (Q18 saw 32-col Projection → 6-col Aggregate;
            // pre-aggregate data movement dominated runtime).
            //
            // Bail out (keep all exprs) when the parent needs every
            // col anyway or when it needs zero (e.g. `SELECT COUNT(*)`
            // at the root — dropping all exprs would produce a
            // zero-column plan that `RecordBatch::try_new` rejects).
            if needed.is_empty() || needed.len() >= exprs.len() {
                let mut child_needed = BTreeSet::new();
                for e in exprs {
                    collect_column_indices_into(e, &mut child_needed);
                }
                let (new_input, child_mapping) = prune_for_columns(input, &child_needed);
                let new_exprs: Vec<PlanExpr> = exprs
                    .iter()
                    .map(|e| rewrite_column_indices(e, &child_mapping))
                    .collect();
                return (
                    L::Projection {
                        input: Box::new(new_input),
                        exprs: new_exprs,
                        schema: schema.clone(),
                    },
                    identity_map(schema.len()),
                );
            }
            let kept_indices: Vec<usize> = needed.iter().copied().collect();
            let kept_exprs: Vec<PlanExpr> =
                kept_indices.iter().map(|&i| exprs[i].clone()).collect();
            let kept_schema: Vec<ColumnInfo> =
                kept_indices.iter().map(|&i| schema[i].clone()).collect();
            let mut child_needed = BTreeSet::new();
            for e in &kept_exprs {
                collect_column_indices_into(e, &mut child_needed);
            }
            let (new_input, child_mapping) = prune_for_columns(input, &child_needed);
            let new_exprs: Vec<PlanExpr> = kept_exprs
                .iter()
                .map(|e| rewrite_column_indices(e, &child_mapping))
                .collect();
            let mapping: HashMap<usize, usize> = kept_indices
                .iter()
                .enumerate()
                .map(|(new, &old)| (old, new))
                .collect();
            (
                L::Projection {
                    input: Box::new(new_input),
                    exprs: new_exprs,
                    schema: kept_schema,
                },
                mapping,
            )
        }
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        }
        | L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        }
        | L::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let mut child_needed = BTreeSet::new();
            for e in group_by.iter().chain(aggr_exprs.iter()) {
                collect_column_indices_into(e, &mut child_needed);
            }
            let (new_input, child_mapping) = prune_for_columns(input, &child_needed);
            let new_group_by: Vec<PlanExpr> = group_by
                .iter()
                .map(|e| rewrite_column_indices(e, &child_mapping))
                .collect();
            let new_aggr_exprs: Vec<PlanExpr> = aggr_exprs
                .iter()
                .map(|e| rewrite_column_indices(e, &child_mapping))
                .collect();
            let rebuilt = match plan {
                L::PartialAggregate { .. } => L::PartialAggregate {
                    input: Box::new(new_input),
                    group_by: new_group_by,
                    aggr_exprs: new_aggr_exprs,
                    schema: schema.clone(),
                },
                L::FinalAggregate { .. } => L::FinalAggregate {
                    input: Box::new(new_input),
                    group_by: new_group_by,
                    aggr_exprs: new_aggr_exprs,
                    schema: schema.clone(),
                },
                _ => L::Aggregate {
                    input: Box::new(new_input),
                    group_by: new_group_by,
                    aggr_exprs: new_aggr_exprs,
                    schema: schema.clone(),
                },
            };
            (rebuilt, identity_map(schema.len()))
        }
        L::Sort { input, order_by } => {
            let mut child_needed = needed.clone();
            for s in order_by {
                collect_column_indices_into(&s.expr, &mut child_needed);
            }
            let (new_input, child_mapping) = prune_for_columns(input, &child_needed);
            let new_order_by: Vec<arneb_planner::SortExpr> = order_by
                .iter()
                .map(|s| arneb_planner::SortExpr {
                    expr: rewrite_column_indices(&s.expr, &child_mapping),
                    asc: s.asc,
                    nulls_first: s.nulls_first,
                })
                .collect();
            (
                L::Sort {
                    input: Box::new(new_input),
                    order_by: new_order_by,
                },
                child_mapping,
            )
        }
        L::Limit {
            input,
            limit,
            offset,
        } => {
            let (new_input, child_mapping) = prune_for_columns(input, needed);
            (
                L::Limit {
                    input: Box::new(new_input),
                    limit: *limit,
                    offset: *offset,
                },
                child_mapping,
            )
        }
        L::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => {
            // INNER and LEFT joins are safe to column-prune: both
            // preserve schema structure identically (left || right),
            // pruning columns from a child is independent of row
            // preservation. RIGHT/FULL bail out — they'd require
            // swapping semantics that the planner doesn't model.
            if !matches!(join_type, ast::JoinType::Inner | ast::JoinType::Left) {
                return (plan.clone(), identity_map(n_out));
            }
            let left_n = left.schema().len();
            let _right_n = right.schema().len();

            // Collect cols used by the join condition. These MUST stay
            // alive in their respective sides regardless of `needed`.
            let mut cond_cols: BTreeSet<usize> = BTreeSet::new();
            if let JoinCondition::On(expr) = condition {
                collect_column_indices_into(expr, &mut cond_cols);
            }

            // Split `needed` and `cond_cols` by side (joined output
            // is `left || right`, so indices < left_n live on the
            // left, indices >= left_n live on the right shifted by
            // -left_n).
            let mut left_needed: BTreeSet<usize> =
                needed.iter().filter(|&&i| i < left_n).copied().collect();
            let mut right_needed: BTreeSet<usize> = needed
                .iter()
                .filter(|&&i| i >= left_n)
                .map(|&i| i - left_n)
                .collect();
            for &c in &cond_cols {
                if c < left_n {
                    left_needed.insert(c);
                } else {
                    right_needed.insert(c - left_n);
                }
            }

            let (new_left, left_mapping) = prune_for_columns(left, &left_needed);
            let (new_right, right_mapping) = prune_for_columns(right, &right_needed);

            let new_left_n = new_left.schema().len();
            let mut combined: HashMap<usize, usize> = HashMap::new();
            for (&old, &new) in &left_mapping {
                combined.insert(old, new);
            }
            for (&old, &new) in &right_mapping {
                combined.insert(old + left_n, new + new_left_n);
            }

            let new_condition = match condition {
                JoinCondition::On(expr) => {
                    JoinCondition::On(rewrite_column_indices(expr, &combined))
                }
                JoinCondition::None => JoinCondition::None,
            };

            // A1.5 (2026-05-27): preserve cross-fragment DF producers,
            // rewriting `build_index` (right schema) and `probe_index`
            // (left schema) through the per-side mappings. Producers
            // whose columns were entirely pruned away are dropped —
            // the matching consumer scan times out and falls back to
            // the static-filter path (sound).
            let new_dynamic_filter_ids: Vec<arneb_planner::DynamicFilterProducer> =
                dynamic_filter_ids
                    .iter()
                    .filter_map(|p| {
                        let new_build = right_mapping.get(&p.build_index)?;
                        let new_probe = left_mapping.get(&p.probe_index)?;
                        Some(arneb_planner::DynamicFilterProducer {
                            id: p.id,
                            build_index: *new_build,
                            probe_index: *new_probe,
                            column_name: p.column_name.clone(),
                        })
                    })
                    .collect();
            (
                L::Join {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    join_type: *join_type,
                    condition: new_condition,
                    dynamic_filter_ids: new_dynamic_filter_ids,
                },
                combined,
            )
        }
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        }
        | L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            // SemiJoin/AntiJoin output schema = left schema. Right side
            // is purely consumed by `right_key` (equi-match) and the
            // residual predicate's right half. Prune right down to those.
            // Without this, lineitem's 16 cols stay in the build side
            // (Q21: 1 GB build vs ~100 MB needed — kills container bench).
            let left_width = left.schema().len();

            // Parent's `needed` indices refer to SemiJoin output ==
            // left schema, so they map straight into left_needed.
            let mut left_needed: BTreeSet<usize> = needed.clone();
            let mut right_needed: BTreeSet<usize> = BTreeSet::new();

            collect_column_indices_into(left_key, &mut left_needed);
            collect_column_indices_into(right_key, &mut right_needed);

            if let Some(res) = residual {
                let mut res_cols: BTreeSet<usize> = BTreeSet::new();
                collect_column_indices_into(res, &mut res_cols);
                for &c in &res_cols {
                    if c < left_width {
                        left_needed.insert(c);
                    } else {
                        right_needed.insert(c - left_width);
                    }
                }
            }

            let (new_left, left_mapping) = prune_for_columns(left, &left_needed);
            let (new_right, right_mapping) = prune_for_columns(right, &right_needed);

            let new_left_width = new_left.schema().len();
            let new_left_key = rewrite_column_indices(left_key, &left_mapping);
            let new_right_key = rewrite_column_indices(right_key, &right_mapping);

            // Residual uses concat [left || right] indices; old right
            // col `j` lived at `left_width + j` and now lives at
            // `new_left_width + right_mapping[j]`.
            let new_residual = residual.as_ref().map(|res| {
                let mut combined: HashMap<usize, usize> = HashMap::new();
                for (&old, &new) in &left_mapping {
                    combined.insert(old, new);
                }
                for (&old, &new) in &right_mapping {
                    combined.insert(old + left_width, new + new_left_width);
                }
                rewrite_column_indices(res, &combined)
            });

            let new_plan = if matches!(plan, L::AntiJoin { .. }) {
                L::AntiJoin {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    left_key: new_left_key,
                    right_key: new_right_key,
                    residual: new_residual,
                }
            } else {
                // A1.5 (2026-05-27): preserve cross-fragment DF
                // producers on SemiJoin too, remapping `build_index`
                // (right) and `probe_index` (left). Producers whose
                // columns were pruned away drop out — soundness
                // fallback at the scan side handles the missing DF.
                let preserved_dfs: Vec<arneb_planner::DynamicFilterProducer> =
                    if let L::SemiJoin {
                        dynamic_filter_ids, ..
                    } = plan
                    {
                        dynamic_filter_ids
                            .iter()
                            .filter_map(|p| {
                                let new_build = right_mapping.get(&p.build_index)?;
                                let new_probe = left_mapping.get(&p.probe_index)?;
                                Some(arneb_planner::DynamicFilterProducer {
                                    id: p.id,
                                    build_index: *new_build,
                                    probe_index: *new_probe,
                                    column_name: p.column_name.clone(),
                                })
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                L::SemiJoin {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    left_key: new_left_key,
                    right_key: new_right_key,
                    residual: new_residual,
                    dynamic_filter_ids: preserved_dfs,
                }
            };

            // Output mapping = left_mapping since output schema = left.
            (new_plan, left_mapping)
        }
        L::ExchangeNode { stage_id, schema } => {
            // Cross-exchange column pruning (2026-05-26): the
            // ExchangeNode is a leaf to the parent fragment but the
            // parent often consumes only a small subset of the columns
            // the upstream stage produces. Q09's coord-side Aggregate
            // input arrives as a 49-column batch but only needs 5
            // columns (`n_name` for GROUP BY, `l_extendedprice` /
            // `l_discount` / `l_quantity` / `ps_supplycost` for the SUM
            // expression). Wrap the ExchangeNode in a Projection here
            // so the parent's hash table / sort buffer / etc. see only
            // the needed columns. Saves coord peak memory by ~80%.
            //
            // This does NOT save network transfer or worker-side
            // memory — the upstream stage is independently planned and
            // still emits the full schema. A follow-up pass that
            // pushes the Projection INTO the upstream fragment would
            // give worker-side wins too, but even this parent-side
            // pruning is a strict improvement.
            if needed.is_empty() || needed.len() >= n_out {
                return (plan.clone(), identity_map(n_out));
            }
            let indices: Vec<usize> = needed.iter().copied().collect();
            let proj_exprs: Vec<PlanExpr> = indices
                .iter()
                .map(|&i| PlanExpr::Column {
                    index: i,
                    name: schema[i].name.clone(),
                    span: None,
                })
                .collect();
            let projected_schema: Vec<ColumnInfo> =
                indices.iter().map(|&i| schema[i].clone()).collect();
            let exchange = L::ExchangeNode {
                stage_id: *stage_id,
                schema: schema.clone(),
            };
            let projection = L::Projection {
                input: Box::new(exchange),
                exprs: proj_exprs,
                schema: projected_schema,
            };
            let mapping: HashMap<usize, usize> = indices
                .iter()
                .enumerate()
                .map(|(new, &old)| (old, new))
                .collect();
            (projection, mapping)
        }
        // Unsupported variants bail out — the plan stays as-is.
        _ => (plan.clone(), identity_map(n_out)),
    }
}

fn identity_map(n: usize) -> HashMap<usize, usize> {
    (0..n).map(|i| (i, i)).collect()
}

/// If `group_by` contains a `Column` reference whose `name` matches an
/// `AssignUniqueId.id_column` somewhere in `input`'s subtree, return
/// that column's index in `group_by`. The match implies the column is
/// monotone non-decreasing within each input partition AND that
/// equal-key rows are contiguous, so `StreamingHashAggregateExec` is
/// sound.
///
/// Walk strategy: descend through plan nodes that preserve row order
/// and don't introduce hashing that could reshuffle the unique
/// column. `AssignUniqueId` is the terminating success case;
/// `Aggregate` / `HashJoinExec` / `Repartition(Hash)` / set-op-style
/// nodes break the property and return `None`.
fn find_unique_key_idx(
    input: &arneb_planner::LogicalPlan,
    group_by: &[arneb_planner::PlanExpr],
) -> Option<usize> {
    let id_col = find_assign_unique_id_column(input)?;
    group_by.iter().position(|e| {
        matches!(
            e,
            arneb_planner::PlanExpr::Column { name, .. } if name == &id_col
        )
    })
}

/// Walk `plan` looking for an `AssignUniqueId` whose `id_column` we
/// can match against a parent aggregate's group_by. Recurse only
/// through nodes that preserve "same-key rows are contiguous"
/// (Projection / Filter / Sort / Limit / Join / LEFT-side joins). On
/// anything that could break the contiguity (Aggregate, Distinct,
/// hash-repartition exchanges), bail out — returning `None` is safe;
/// the caller falls back to the regular `HashAggregateExec` path.
fn find_assign_unique_id_column(plan: &arneb_planner::LogicalPlan) -> Option<String> {
    use arneb_planner::LogicalPlan as L;
    match plan {
        L::AssignUniqueId { id_column, .. } => Some(id_column.clone()),
        L::Projection { input, .. }
        | L::Filter { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. } => find_assign_unique_id_column(input),
        // For Join: the unique-id column on the LEFT propagates
        // through INNER and LEFT joins as long as same-rowid runs are
        // contiguous in the output. arneb's streaming probe
        // (Step ST) preserves left-row order within each output
        // partition, so this holds for INNER and LEFT.
        L::Join {
            left,
            join_type:
                arneb_sql_parser::ast::JoinType::Inner | arneb_sql_parser::ast::JoinType::Left,
            ..
        } => find_assign_unique_id_column(left),
        L::Join { .. } => None,
        // Aggregate, Distinct, set ops, etc. break the run-contiguity
        // property — bail out.
        _ => None,
    }
}

/// Extract a scalar value from an Arrow array at a given row.
fn arrow_to_scalar(array: &arrow::array::ArrayRef, row: usize) -> arneb_common::types::ScalarValue {
    use arneb_common::types::ScalarValue;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::DataType as ArrowDT;

    if array.is_null(row) {
        return ScalarValue::Null;
    }
    match array.data_type() {
        ArrowDT::Int64 => ScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        ArrowDT::Float64 => ScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        ArrowDT::Utf8 => ScalarValue::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        _ => {
            // Fallback: convert to string
            let s = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            ScalarValue::Utf8(s)
        }
    }
}

/// Wrap `input` in `CoalescePartitionsExec` if it exposes more than one
/// output partition. Stateful operators (Aggregate, Sort, Join, Limit,
/// etc.) call this on each child so they see a single merged stream;
/// stateless operators (Projection, Filter) skip the wrap and inherit
/// their input's partitioning for downstream parallelism.
fn coalesce_if_multi(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if input.output_partitioning().partition_count() > 1 {
        Arc::new(crate::CoalescePartitionsExec::new(input))
    } else {
        input
    }
}

/// Target number of partitions for hash-repartitioned joins. Hardware-
/// derived; the value caps both sides' shuffle width so an N-way join
/// uses at most `target` cores per level. Mirrors Trino's
/// `driverInstanceCount`.
fn target_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

/// Wrap an `ExecutionPlan` in `RepartitionExec(Hash(...))` keyed by
/// column indices in its schema. Used to feed `HashJoinExec` two
/// co-partitioned inputs so each partition can independently build
/// and probe its own hash table (no shared OnceCell).
fn hash_repartition(
    input: Arc<dyn ExecutionPlan>,
    key_indices: &[usize],
    n: usize,
    inflight_budget: &Arc<InflightBudget>,
) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    let exprs: Vec<PlanExpr> = key_indices
        .iter()
        .map(|&i| PlanExpr::Column {
            index: i,
            name: schema[i].name.clone(),
            span: None,
        })
        .collect();
    Arc::new(
        crate::RepartitionExec::new(input, crate::partitioning::Partitioning::Hash(exprs, n))
            .with_inflight_budget(Arc::clone(inflight_budget)),
    )
}

/// Build the `(left, right)` execution-plan inputs to a `HashJoinExec`,
/// optionally inserting hash-repartition shuffles so each partition can
/// build+probe independently (Trino-style parallel hash join).
///
/// Behaviour:
/// - INNER joins with a 2+ partition input on either side get hashed
///   on both sides via `RepartitionExec(Hash([...keys], N))`. Per-
///   partition build kicks in inside `HashJoinExec`.
/// - INNER joins with both sides already 1-partition: nothing to gain
///   from a shuffle; fall through to single-shared-build path.
/// - LEFT joins keep the prior probe-side-parallel behaviour (multi-
///   partition left, single-partition right).
/// - RIGHT/FULL joins coalesce both sides (legacy execute_single path).
fn build_join_inputs(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_keys: &[usize],
    right_keys: &[usize],
    join_type: arneb_sql_parser::ast::JoinType,
    inflight_budget: &Arc<InflightBudget>,
) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    use arneb_sql_parser::ast::JoinType as JT;
    match join_type {
        JT::Inner => {
            // Hash-shuffle ONLY at the leaf level. The all-level variant
            // was tried 2026-05-15 and regressed deep multi-join queries
            // (Q05 +175%, Q07 +85%, Q08 +180%) because each upper-level
            // re-shuffle of the cumulative 6M-row intermediate dominated
            // any parallel-build benefit. Leaf-only keeps Q08 at 3.06×
            // vs Trino (was 5×+) without regressing other queries.
            let leaf_to_leaf = is_scan_like(&left) && is_scan_like(&right);
            let some_multi = left.output_partitioning().partition_count() > 1
                || right.output_partitioning().partition_count() > 1;
            if leaf_to_leaf && some_multi {
                let n = target_partitions().max(2);
                let left_hashed = hash_repartition(left, left_keys, n, inflight_budget);
                let right_hashed = hash_repartition(right, right_keys, n, inflight_budget);
                (left_hashed, right_hashed)
            } else {
                (left, coalesce_if_multi(right))
            }
        }
        JT::Left => (left, coalesce_if_multi(right)),
        _ => (coalesce_if_multi(left), coalesce_if_multi(right)),
    }
}

fn is_scan_like(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.display_name() == "ScanExec"
        || plan.display_name() == "FilterExec"
        || plan.display_name() == "ProjectionExec"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
    use arneb_planner::{JoinCondition, PlanExpr, WindowFunctionDef};
    use arneb_sql_parser::ast;
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn test_context() -> (ExecutionContext, Vec<ColumnInfo>) {
        let schema = vec![
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
        ];

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "alice", "bob", "carol", "dave", "eve",
                ])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .unwrap();

        let source = Arc::new(InMemoryDataSource::new(schema.clone(), vec![batch]));
        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("users", source);
        (ctx, schema)
    }

    #[tokio::test]
    async fn plan_table_scan() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::TableScan {
            table: TableReference::table("users"),
            schema,
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn cte_self_agg_window_plan_matches_scalar_subquery_boundary() {
        let (ctx, supplier_schema, revenue_schema) = revenue0_context();
        let scalar_plan = resolve_top_filter_scalar_subquery(
            &ctx,
            q15_scalar_revenue_filter_plan(&supplier_schema, &revenue_schema),
        )
        .await;
        let window_plan = q15_window_revenue_filter_plan(&supplier_schema, &revenue_schema);

        let scalar_rows = execute_supplier_revenue_rows(&ctx, &scalar_plan).await;
        let window_rows = execute_supplier_revenue_rows(&ctx, &window_plan).await;

        assert_eq!(scalar_rows, vec![(1, 100.0), (2, 99.995)]);
        assert_eq!(window_rows, scalar_rows);
    }

    fn revenue0_context() -> (ExecutionContext, Vec<ColumnInfo>, Vec<ColumnInfo>) {
        let supplier_schema = vec![
            ColumnInfo {
                name: "s_suppkey".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "s_name".into(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ];
        let revenue_schema = vec![
            ColumnInfo {
                name: "supplier_no".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "total_revenue".into(),
                data_type: DataType::Float64,
                nullable: false,
            },
        ];

        let supplier_arrow = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", ArrowDataType::Int64, false),
            Field::new("s_name", ArrowDataType::Utf8, false),
        ]));
        let supplier_batch = RecordBatch::try_new(
            supplier_arrow,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["top", "near", "low"])),
            ],
        )
        .unwrap();

        let revenue_arrow = Arc::new(Schema::new(vec![
            Field::new("supplier_no", ArrowDataType::Int64, false),
            Field::new("total_revenue", ArrowDataType::Float64, false),
        ]));
        let revenue_batch = RecordBatch::try_new(
            revenue_arrow,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![100.0, 99.995, 99.98])),
            ],
        )
        .unwrap();

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source(
            "supplier",
            Arc::new(InMemoryDataSource::new(
                supplier_schema.clone(),
                vec![supplier_batch],
            )),
        );
        ctx.register_data_source(
            "revenue0",
            Arc::new(InMemoryDataSource::new(
                revenue_schema.clone(),
                vec![revenue_batch],
            )),
        );

        (ctx, supplier_schema, revenue_schema)
    }

    fn supplier_scan(schema: &[ColumnInfo]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table("supplier"),
            schema: schema.to_vec(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn revenue_scan(schema: &[ColumnInfo]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table("revenue0"),
            schema: schema.to_vec(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn q15_join(supplier_schema: &[ColumnInfo], revenue_input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(supplier_scan(supplier_schema)),
            right: Box::new(revenue_input),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "s_suppkey".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "supplier_no".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn q15_scalar_revenue_filter_plan(
        supplier_schema: &[ColumnInfo],
        revenue_schema: &[ColumnInfo],
    ) -> LogicalPlan {
        let scalar_subquery = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Aggregate {
                input: Box::new(revenue_scan(revenue_schema)),
                group_by: Vec::new(),
                aggr_exprs: vec![PlanExpr::Function {
                    name: "MAX".into(),
                    args: vec![PlanExpr::Column {
                        index: 1,
                        name: "total_revenue".into(),
                        span: None,
                    }],
                    distinct: false,
                    span: None,
                }],
                schema: vec![ColumnInfo {
                    name: "MAX(total_revenue)".into(),
                    data_type: DataType::Float64,
                    nullable: true,
                }],
            }),
            exprs: vec![PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "MAX(total_revenue)".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Minus,
                right: Box::new(PlanExpr::Literal {
                    value: ScalarValue::Float64(0.01),
                    span: None,
                }),
                span: None,
            }],
            schema: vec![ColumnInfo {
                name: "MAX(total_revenue) - 0.01".into(),
                data_type: DataType::Float64,
                nullable: true,
            }],
        };

        project_supplier_revenue(LogicalPlan::Filter {
            input: Box::new(q15_join(supplier_schema, revenue_scan(revenue_schema))),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 3,
                    name: "total_revenue".into(),
                    span: None,
                }),
                op: ast::BinaryOp::GtEq,
                right: Box::new(PlanExpr::ScalarSubquery {
                    subplan: Box::new(scalar_subquery),
                    span: None,
                }),
                span: None,
            },
        })
    }

    fn q15_window_revenue_filter_plan(
        supplier_schema: &[ColumnInfo],
        revenue_schema: &[ColumnInfo],
    ) -> LogicalPlan {
        let windowed_revenue = LogicalPlan::Window {
            input: Box::new(revenue_scan(revenue_schema)),
            functions: vec![WindowFunctionDef {
                name: "MAX".into(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "total_revenue".into(),
                    span: None,
                }],
                partition_by: Vec::new(),
                order_by: Vec::new(),
                output_name: "__cte_max".into(),
            }],
        };

        project_supplier_revenue(LogicalPlan::Filter {
            input: Box::new(q15_join(supplier_schema, windowed_revenue)),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 3,
                    name: "total_revenue".into(),
                    span: None,
                }),
                op: ast::BinaryOp::GtEq,
                right: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(PlanExpr::Column {
                        index: 4,
                        name: "__cte_max".into(),
                        span: None,
                    }),
                    op: ast::BinaryOp::Minus,
                    right: Box::new(PlanExpr::Literal {
                        value: ScalarValue::Float64(0.01),
                        span: None,
                    }),
                    span: None,
                }),
                span: None,
            },
        })
    }

    fn project_supplier_revenue(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Projection {
            input: Box::new(input),
            exprs: vec![
                PlanExpr::Column {
                    index: 0,
                    name: "s_suppkey".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 3,
                    name: "total_revenue".into(),
                    span: None,
                },
            ],
            schema: vec![
                ColumnInfo {
                    name: "s_suppkey".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "total_revenue".into(),
                    data_type: DataType::Float64,
                    nullable: false,
                },
            ],
        }
    }

    async fn execute_supplier_revenue_rows(
        ctx: &ExecutionContext,
        plan: &LogicalPlan,
    ) -> Vec<(i64, f64)> {
        let exec = ctx.create_physical_plan(plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let suppkeys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let revenues = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((suppkeys.value(row), revenues.value(row)));
            }
        }
        rows.sort_by_key(|(suppkey, _)| *suppkey);
        rows
    }

    async fn resolve_top_filter_scalar_subquery(
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> LogicalPlan {
        let LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } = plan
        else {
            panic!("expected projection over filter");
        };
        let LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } = *input
        else {
            panic!("expected projection over filter");
        };
        let predicate = ctx.resolve_scalar_subqueries(&predicate).await.unwrap();
        LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Filter {
                input: filter_input,
                predicate,
            }),
            exprs,
            schema,
        }
    }

    #[tokio::test]
    async fn plan_filter() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                    span: None,
                }),
                op: ast::BinaryOp::LtEq,
                right: Box::new(PlanExpr::Literal {
                    value: ScalarValue::Int32(3),
                    span: None,
                }),
                span: None,
            },
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn plan_projection() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            exprs: vec![PlanExpr::Column {
                index: 1,
                name: "name".to_string(),
                span: None,
            }],
            schema: vec![ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            }],
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn test_computing_projection_over_final_aggregate() {
        let schema = vec![
            ColumnInfo {
                name: "a".to_string(),
                data_type: DataType::Float64,
                nullable: false,
            },
            ColumnInfo {
                name: "b".to_string(),
                data_type: DataType::Float64,
                nullable: false,
            },
        ];

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Float64, false),
            Field::new("b", ArrowDataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
                Arc::new(Float64Array::from(vec![50.0, 70.0])),
            ],
        )
        .unwrap();

        let source = Arc::new(InMemoryDataSource::new(schema.clone(), vec![batch]));
        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("metrics", source);

        let aggregate_schema = vec![
            ColumnInfo {
                name: "sum_a".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
            ColumnInfo {
                name: "sum_b".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ];

        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::FinalAggregate {
                input: Box::new(LogicalPlan::TableScan {
                    table: TableReference::table("metrics"),
                    schema,
                    alias: None,
                    properties: Default::default(),
                    dynamic_filters_consumed: Vec::new(),
                }),
                group_by: vec![],
                aggr_exprs: vec![
                    PlanExpr::Function {
                        name: "SUM".to_string(),
                        args: vec![PlanExpr::Column {
                            index: 0,
                            name: "a".to_string(),
                            span: None,
                        }],
                        distinct: false,
                        span: None,
                    },
                    PlanExpr::Function {
                        name: "SUM".to_string(),
                        args: vec![PlanExpr::Column {
                            index: 1,
                            name: "b".to_string(),
                            span: None,
                        }],
                        distinct: false,
                        span: None,
                    },
                ],
                schema: aggregate_schema,
            }),
            exprs: vec![PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(PlanExpr::Literal {
                        value: ScalarValue::Float64(100.0),
                        span: None,
                    }),
                    op: ast::BinaryOp::Multiply,
                    right: Box::new(PlanExpr::Column {
                        index: 0,
                        name: "sum_a".to_string(),
                        span: None,
                    }),
                    span: None,
                }),
                op: ast::BinaryOp::Divide,
                right: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "sum_b".to_string(),
                    span: None,
                }),
                span: None,
            }],
            schema: vec![ColumnInfo {
                name: "ratio".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            }],
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let projection = exec.as_any().downcast_ref::<ProjectionExec>().unwrap();
        projection
            .input
            .as_any()
            .downcast_ref::<HashAggregateExec>()
            .unwrap();

        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let ratio = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(ratio.value(0), 25.0);
    }

    #[tokio::test]
    async fn plan_limit_offset() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            limit: Some(2),
            offset: Some(1),
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 3);
    }

    #[tokio::test]
    async fn plan_sort() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            order_by: vec![arneb_planner::SortExpr {
                expr: PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                    span: None,
                },
                asc: false,
                nulls_first: false,
            }],
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 5);
        assert_eq!(ids.value(4), 1);
    }

    #[tokio::test]
    async fn plan_aggregate_count_sum() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
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
            schema: vec![
                ColumnInfo {
                    name: "count".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "sum_value".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 5);
        let sum = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum.value(0), 1500);
    }

    #[tokio::test]
    async fn plan_explain() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Explain {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            analyze: false,
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let text = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(text.value(0).contains("TableScan"));
    }

    #[test]
    fn plan_table_not_found() {
        let ctx = ExecutionContext::new();
        let plan = LogicalPlan::TableScan {
            table: TableReference::table("nonexistent"),
            schema: vec![],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        };
        let result = ctx.create_physical_plan(&plan);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn end_to_end_filter_project_limit() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Limit {
            limit: Some(2),
            offset: None,
            input: Box::new(LogicalPlan::Projection {
                exprs: vec![PlanExpr::Column {
                    index: 1,
                    name: "name".to_string(),
                    span: None,
                }],
                schema: vec![ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                }],
                input: Box::new(LogicalPlan::Filter {
                    predicate: PlanExpr::BinaryOp {
                        left: Box::new(PlanExpr::Column {
                            index: 0,
                            name: "id".to_string(),
                            span: None,
                        }),
                        op: ast::BinaryOp::Gt,
                        right: Box::new(PlanExpr::Literal {
                            value: ScalarValue::Int32(2),
                            span: None,
                        }),
                        span: None,
                    },
                    input: Box::new(LogicalPlan::TableScan {
                        table: TableReference::table("users"),
                        schema,
                        alias: None,
                        properties: Default::default(),
                        dynamic_filters_consumed: Vec::new(),
                    }),
                }),
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "carol");
        assert_eq!(names.value(1), "dave");
    }

    #[tokio::test]
    async fn end_to_end_having_filter_after_aggregate() {
        // Build data with duplicate names to test GROUP BY + HAVING
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "alice", "alice", "bob", "carol", "carol", "carol",
                ])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
            ],
        )
        .unwrap();
        let schema = vec![
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
        ];
        let source = Arc::new(InMemoryDataSource::new(schema.clone(), vec![batch]));
        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("t", source);

        // GROUP BY name, COUNT(*) → HAVING COUNT(*) > 1
        let agg_schema = vec![
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
            ColumnInfo {
                name: "cnt".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "cnt".to_string(),
                    span: None,
                }),
                op: ast::BinaryOp::Gt,
                right: Box::new(PlanExpr::Literal {
                    value: ScalarValue::Int64(1),
                    span: None,
                }),
                span: None,
            },
            input: Box::new(LogicalPlan::Aggregate {
                input: Box::new(LogicalPlan::TableScan {
                    table: TableReference::table("t"),
                    schema,
                    alias: None,
                    properties: Default::default(),
                    dynamic_filters_consumed: Vec::new(),
                }),
                group_by: vec![PlanExpr::Column {
                    index: 0,
                    name: "name".to_string(),
                    span: None,
                }],
                aggr_exprs: vec![PlanExpr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                    distinct: false,
                    span: None,
                }],
                schema: agg_schema,
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // alice (2) and carol (3) have count > 1; bob (1) filtered out
        assert_eq!(total_rows, 2);
    }

    fn join_equivalence_sources() -> (
        ExecutionContext,
        Vec<ColumnInfo>,
        Vec<ColumnInfo>,
        Vec<ColumnInfo>,
    ) {
        let left_info = vec![
            ColumnInfo {
                name: "l_id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "l_val".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ];
        let right_info = vec![
            ColumnInfo {
                name: "r_id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "r_val".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ];
        let left_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("l_id", ArrowDataType::Int32, false),
                Field::new("l_val", ArrowDataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let right_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("r_id", ArrowDataType::Int32, false),
                Field::new("r_val", ArrowDataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![2, 1, 3, 2])),
                Arc::new(Int32Array::from(vec![200, 100, 300, 201])),
            ],
        )
        .unwrap();
        let mut output_schema = left_info.clone();
        output_schema.extend(right_info.clone());

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source(
            "left_t",
            Arc::new(InMemoryDataSource::new(left_info.clone(), vec![left_batch])),
        );
        ctx.register_data_source(
            "right_t",
            Arc::new(InMemoryDataSource::new(
                right_info.clone(),
                vec![right_batch],
            )),
        );
        (ctx, left_info, right_info, output_schema)
    }

    fn table_scan(name: &str, schema: Vec<ColumnInfo>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(name),
            schema,
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn inner_join_condition(left_idx: usize, right_idx: usize) -> arneb_planner::JoinCondition {
        arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: left_idx,
                name: "left_key".into(),
                span: None,
            }),
            op: ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: right_idx,
                name: "right_key".into(),
                span: None,
            }),
            span: None,
        })
    }

    async fn collect_i32_rows(
        ctx: &ExecutionContext,
        plan: &LogicalPlan,
    ) -> (Vec<ColumnInfo>, Vec<[i32; 4]>) {
        let exec = ctx.create_physical_plan(plan).unwrap();
        let schema = exec.schema();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let c0 = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let c1 = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let c2 = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let c3 = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push([c0.value(row), c1.value(row), c2.value(row), c3.value(row)]);
            }
        }
        rows.sort();
        (schema, rows)
    }

    #[tokio::test]
    async fn swapped_inner_join_projection_matches_unswapped_rows_and_columns() {
        let (ctx, left_info, right_info, output_schema) = join_equivalence_sources();
        let unswapped = LogicalPlan::Join {
            left: Box::new(table_scan("left_t", left_info.clone())),
            right: Box::new(table_scan("right_t", right_info.clone())),
            join_type: ast::JoinType::Inner,
            condition: inner_join_condition(0, 2),
            dynamic_filter_ids: Vec::new(),
        };
        let swapped_join = LogicalPlan::Join {
            left: Box::new(table_scan("right_t", right_info)),
            right: Box::new(table_scan("left_t", left_info)),
            join_type: ast::JoinType::Inner,
            condition: inner_join_condition(2, 0),
            dynamic_filter_ids: Vec::new(),
        };
        let swapped_restored = LogicalPlan::Projection {
            input: Box::new(swapped_join),
            exprs: vec![
                PlanExpr::Column {
                    index: 2,
                    name: "l_id".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 3,
                    name: "l_val".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 0,
                    name: "r_id".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 1,
                    name: "r_val".into(),
                    span: None,
                },
            ],
            schema: output_schema.clone(),
        };

        let (unswapped_schema, unswapped_rows) = collect_i32_rows(&ctx, &unswapped).await;
        let (swapped_schema, swapped_rows) = collect_i32_rows(&ctx, &swapped_restored).await;

        assert_eq!(unswapped_schema, output_schema);
        assert_eq!(swapped_schema, output_schema);
        assert_eq!(swapped_rows, unswapped_rows);
        assert_eq!(
            swapped_rows,
            vec![[1, 10, 1, 100], [2, 20, 2, 200], [2, 20, 2, 201]]
        );
    }

    // ---------------------------------------------------------------
    // Semi-join / Anti-join / Scalar subquery tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn end_to_end_semi_join() {
        // Left: orders with customer_ids [1, 2, 3, 4]
        // Right: customers with ids [2, 4]
        // SemiJoin should return orders for customers 2 and 4
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", ArrowDataType::Int64, false),
            Field::new("customer_id", ArrowDataType::Int64, false),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();
        let left_info = vec![
            ColumnInfo {
                name: "order_id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "customer_id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];

        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let right_batch =
            RecordBatch::try_new(right_schema, vec![Arc::new(Int64Array::from(vec![2, 4]))])
                .unwrap();
        let right_info = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];

        let left_src = Arc::new(InMemoryDataSource::new(left_info.clone(), vec![left_batch]));
        let right_src = Arc::new(InMemoryDataSource::new(
            right_info.clone(),
            vec![right_batch],
        ));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("orders", left_src);
        ctx.register_data_source("customers", right_src);

        let plan = LogicalPlan::SemiJoin {
            left: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("orders"),
                schema: left_info,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            right: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("customers"),
                schema: right_info,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            left_key: PlanExpr::Column {
                index: 1,
                name: "customer_id".into(),
                span: None,
            },
            right_key: PlanExpr::Column {
                index: 0,
                name: "id".into(),
                span: None,
            },
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn end_to_end_anti_join() {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let left_batch = RecordBatch::try_new(
            left_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        let left_info = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];

        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let right_batch =
            RecordBatch::try_new(right_schema, vec![Arc::new(Int64Array::from(vec![2, 4]))])
                .unwrap();
        let right_info = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];

        let left_src = Arc::new(InMemoryDataSource::new(left_info.clone(), vec![left_batch]));
        let right_src = Arc::new(InMemoryDataSource::new(
            right_info.clone(),
            vec![right_batch],
        ));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("left_t", left_src);
        ctx.register_data_source("right_t", right_src);

        // AntiJoin: returns rows from left NOT in right
        let plan = LogicalPlan::AntiJoin {
            left: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("left_t"),
                schema: left_info,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            right: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("right_t"),
                schema: right_info,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
            left_key: PlanExpr::Column {
                index: 0,
                name: "id".into(),
                span: None,
            },
            right_key: PlanExpr::Column {
                index: 0,
                name: "id".into(),
                span: None,
            },
            residual: None,
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 1, 3, 5 NOT IN [2, 4]
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn end_to_end_scalar_subquery() {
        let schema_info = vec![ColumnInfo {
            name: "val".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowDataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();
        let src = Arc::new(InMemoryDataSource::new(schema_info.clone(), vec![batch]));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("t", src);

        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("t"),
                schema: schema_info,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 42);
    }

    #[tokio::test]
    async fn scalar_subquery_zero_rows_returns_null() {
        let schema_info = vec![ColumnInfo {
            name: "val".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let src = Arc::new(InMemoryDataSource::new(schema_info.clone(), vec![]));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("empty", src);

        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("empty"),
                schema: schema_info,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert!(batches[0].column(0).is_null(0));
    }

    #[tokio::test]
    async fn scalar_subquery_multi_row_errors() {
        let schema_info = vec![ColumnInfo {
            name: "val".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowDataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let src = Arc::new(InMemoryDataSource::new(schema_info.clone(), vec![batch]));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("multi", src);

        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("multi"),
                schema: schema_info,
                alias: None,
                properties: Default::default(),
                dynamic_filters_consumed: Vec::new(),
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let result = exec.execute(0).await;
        assert!(result.is_err());
    }

    #[test]
    fn end_to_end_scalar_function_via_evaluate() {
        use crate::expression;
        use crate::functions::default_registry;
        use arrow::array::StringArray;

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                Arc::new(Int64Array::from(vec![-10, 20, -30])),
            ],
        )
        .unwrap();

        let reg = default_registry();

        // Test UPPER(name)
        let upper_expr = PlanExpr::Function {
            name: "UPPER".to_string(),
            args: vec![PlanExpr::Column {
                index: 0,
                name: "name".to_string(),
                span: None,
            }],
            distinct: false,
            span: None,
        };
        let result = expression::evaluate(&upper_expr, &batch, Some(&reg)).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "ALICE");
        assert_eq!(arr.value(1), "BOB");
        assert_eq!(arr.value(2), "CAROL");

        // Test ABS(value)
        let abs_expr = PlanExpr::Function {
            name: "ABS".to_string(),
            args: vec![PlanExpr::Column {
                index: 1,
                name: "value".to_string(),
                span: None,
            }],
            distinct: false,
            span: None,
        };
        let result = expression::evaluate(&abs_expr, &batch, Some(&reg)).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 20);
        assert_eq!(arr.value(2), 30);
    }
}
