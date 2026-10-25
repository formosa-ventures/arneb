//! QueryCoordinator: orchestrates distributed multi-stage query execution.
//!
//! Lives in the server crate because it depends on both trino-rpc and trino-execution,
//! which can't depend on each other.

use std::sync::{Arc, OnceLock};

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::identifiers::{QueryId, StageId, TaskId};
use arneb_common::stream::collect_stream;
use arneb_common::DynamicFilterId;
use arneb_planner::LogicalPlan;
use arneb_protocol::DistributedExecutor;
use arneb_scheduler::{DynamicFilterService, DynamicFilterServiceRegistry, NodeRegistry};
use arrow::array::RecordBatch;
use async_trait::async_trait;

use arneb_execution::ExecutionContext;

use crate::fragment_pruning::prune_fragment_tree;

fn dfrpc_domain_variant(domain: &arneb_common::Domain) -> String {
    match domain {
        arneb_common::Domain::DistinctValues(values) => {
            format!("DistinctValues(len={})", values.len())
        }
        arneb_common::Domain::Range { .. } => "Range".to_string(),
        arneb_common::Domain::Bloom(_) => "Bloom".to_string(),
        arneb_common::Domain::All => "All".to_string(),
    }
}

/// Orchestrates distributed query execution across multiple workers.
pub struct QueryCoordinator {
    node_registry: NodeRegistry,
    /// A1.3 (2026-05-27): per-query dynamic-filter merge services live
    /// here. `execute()` registers an empty service at query start and
    /// drops it at end; A1.5 will start populating it from the plan's
    /// `dynamic_filter_ids` annotations so producers can actually
    /// report Domains and consumers can subscribe. Shared with the
    /// FlightState `df_report_callback` registered in `main.rs`.
    df_registry: DynamicFilterServiceRegistry,
    /// P4 stage 2: re-run settings, `None` when retry is off.
    retry: Option<QueryRetry>,
}

impl QueryCoordinator {
    pub fn new(node_registry: NodeRegistry, df_registry: DynamicFilterServiceRegistry) -> Self {
        Self {
            node_registry,
            df_registry,
            retry: None,
        }
    }

    /// Re-run read-only queries that fail because a worker was lost
    /// (`[cluster] retry_policy = "query"`); `None` keeps retry off.
    pub fn with_retry(mut self, retry: Option<QueryRetry>) -> Self {
        self.retry = retry;
        self
    }

    /// Returns the per-query dynamic-filter service registry shared
    /// with the FlightState `df_report_callback`. Tests use this to
    /// inject reports without spinning up a Flight server.
    pub fn df_registry(&self) -> &DynamicFilterServiceRegistry {
        &self.df_registry
    }

    /// Check if distributed execution is available (workers registered).
    pub fn has_workers(&self) -> bool {
        self.node_registry.alive_count() > 0
    }

    /// Execute a query distributedly: fragment → schedule → dispatch → collect.
    /// With retry on, a read-only query that fails because one of its
    /// workers was lost re-runs on the remaining workers. Results are
    /// collected in full before they are returned, so a re-run never sends
    /// a client the same rows twice.
    pub async fn execute(
        &self,
        plan: LogicalPlan,
        exec_ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let retry = self.retry.filter(|_| !writes_data(&plan));
        with_query_retry(retry, &self.node_registry, || {
            self.execute_once(&plan, exec_ctx)
        })
        .await
    }

    /// One run of [`Self::execute`]. A failure carries the workers the run
    /// used, so the caller can tell whether one of them was lost.
    async fn execute_once(
        &self,
        plan: &LogicalPlan,
        exec_ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, AttemptError> {
        // The protocol layer records the statement in the query tracker;
        // this id only keys the distributed tasks.
        let query_id = QueryId::new();

        // The DF service is built shortly after fragmentation, once we
        // know which DF ids exist + how many partitions each producer
        // stage runs with. RAII guard unregisters on return so per-
        // query state never leaks even on error paths.
        let _df_guard = DfRegistrationGuard {
            registry: self.df_registry.clone(),
            query_id,
        };

        // Fragment the plan.
        //
        // A2.2 (2026-05-28): thread the broadcast threshold + per-query
        // `CatalogStats` snapshot from the execution context into the
        // fragmenter so its Join arm can decide build-side broadcast
        // eligibility. Both default to `None`; broadcast detection is
        // inert until `ExecutionContext::with_broadcast_max_build_bytes`
        // is set (A2.4 measurement flips it on).
        // dist-adaptive-partition: feed the live worker count + the resolved
        // partition policy so the fragmenter sizes hash fan-out to the cluster
        // and the intermediate's estimated cardinality instead of a fixed 2.
        let worker_count = self.node_registry.alive_count();
        let (target_rows, max_partitions) = crate::config::resolve_hash_partition_policy();
        tracing::info!(
            worker_count,
            hash_partition_target_rows = target_rows,
            max_hash_partitions = max_partitions,
            "adaptive hash-partition policy resolved"
        );
        let mut fragmenter = arneb_planner::PlanFragmenter::new()
            .with_broadcast_threshold(exec_ctx.broadcast_max_build_bytes())
            .with_stats(exec_ctx.catalog_stats().cloned())
            .with_worker_count(worker_count)
            .with_partition_policy(target_rows, max_partitions);
        let mut root_fragment = fragmenter.fragment(plan.clone());

        // Cross-fragment column pruning (2026-05-26): for each
        // ExchangeNode in the parent plan, push a Projection into the
        // matching child fragment so the worker produces (and ships)
        // only the columns the parent actually references. Cuts Q09's
        // worker-side peak by 30-40% and inter-stage Flight bytes by
        // 80% (49 columns -> 5 at the coord boundary). See
        // `fragment_pruning.rs` for the algorithm; based on Trino's
        // `PruneExchangeColumns` rule.
        prune_fragment_tree(&mut root_fragment);

        // dist-mxn-nested-joins T2 (2026-06-05): env-gated fragment-tree dump.
        if fragments_traced() {
            eprintln!("[FRAGTRACE] === query {query_id} fragment tree (post-prune) ===");
            trace_fragment_tree(&root_fragment);
            eprintln!("[FRAGTRACE] === end fragment tree ===");
        }

        // Draining workers finish what they run but get no new query.
        let workers = self.node_registry.active_workers();
        if workers.is_empty() {
            return Err(AttemptError {
                error: ExecutionError::InvalidOperation(
                    "no workers available for distributed execution".to_string(),
                ),
                worker_ids: Vec::new(),
            });
        }
        // admission-control D4: from here on, any exit other than success
        // (an error, a panic, or this future being dropped by a cancel,
        // timeout or client disconnect) cancels the query on every worker.
        let mut cancel_guard = CancelGuard::new(
            query_id,
            workers.iter().map(|w| flight_address(&w.address)).collect(),
        );

        let num_fragments = count_fragments(&root_fragment);
        tracing::info!(
            query_id = %query_id,
            workers = workers.len(),
            fragments = num_fragments,
            "starting distributed execution"
        );

        // A1.5 (2026-05-27): collect every cross-fragment dynamic
        // filter id this query touches, with its producing stage id +
        // expected partition count. The DF service must be registered
        // BEFORE any worker task starts (otherwise early build-side
        // reports race ahead of registration and get dropped). The
        // walker uses the same stage-task-count formula as
        // `execute_child_on_worker` below.
        let producers = collect_df_producers(&root_fragment, workers.len());
        let consumers = collect_df_consumers(&root_fragment);
        let register_entries: Vec<(DynamicFilterId, StageId, u32)> = producers
            .iter()
            .map(|p| (p.df_id, p.stage_id, p.task_count))
            .collect();
        for (df_id, stage_id, task_count) in &register_entries {
            eprintln!(
                "[DFRPC] coord register_entry query_id={} df_id={} producer_stage_id={} expected_task_count={}",
                query_id, df_id, stage_id, task_count
            );
        }
        let mut svc = DynamicFilterService::new();
        svc.register_query(&register_entries);
        // A1.5: take per-DF receivers BEFORE registering the service in
        // the shared registry. Subscribing here avoids the race where
        // a worker report arrives, the service fires, and our spawned
        // pushers (registered below) miss the broadcast. Pre-firing
        // happens correctly on `subscribe()` if the DF resolves before
        // the receiver is awaited.
        let mut pre_receivers: Vec<(
            DynamicFilterId,
            tokio::sync::oneshot::Receiver<arneb_common::Domain>,
        )> = Vec::with_capacity(register_entries.len());
        for (df_id, _, _) in &register_entries {
            if let Ok(rx) = svc.subscribe(*df_id) {
                pre_receivers.push((*df_id, rx));
            }
        }
        self.df_registry.register(query_id, svc);

        // Execute stages bottom-up: dispatch child fragments to workers.
        // Each entry: (flight_addr, buffer_key, consumer_partition_id).
        // The third tuple element is the partition_id the future consumer
        // of this entry will pull from the upstream task — pre-resolved
        // here (per consumer task k) so the planner doesn't need to know
        // upstream's output cardinality. Coord-side stage_results is only
        // consumed by coord's own root plan (single-task, k=0), so coord
        // always pushes 0; workers receive per-consumer SourceExchange
        // lists with the correct partition_id set by `execute_child_on_worker`.
        let mut stage_results: std::collections::HashMap<u32, Vec<(String, String, u32)>> =
            std::collections::HashMap::new();

        // P4 stage 1: race the query against its workers' liveness, so a
        // hung or dead worker fails the query (and the CancelGuard cancels
        // its tasks everywhere) instead of leaving it waiting forever.
        let worker_ids: Vec<String> = workers.iter().map(|w| w.worker_id.clone()).collect();
        let run = async {
            // q21 SF30 fix: the root fragment is consumed by the coordinator's own
            // root plan. Its producers must_drain unless that root can early-stop
            // (a top-level LIMIT short-circuit); otherwise a mid-stream consumer
            // drop is a silent truncation and must fail loud.
            let root_must_drain = !root_fragment.root.may_stop_input_early();
            for child in &root_fragment.source_fragments {
                execute_child_on_worker(
                    child,
                    &workers,
                    &self.node_registry,
                    &mut stage_results,
                    &query_id,
                    root_must_drain,
                )
                .await?;
            }

            // A1.5 (2026-05-27): for each consumer DF, spawn an async
            // pusher that waits on the pre-subscribed receiver and pushes
            // the resolved Domain to every worker task of the consumer
            // stage via the `notify_dynamic_filter` Flight action. The
            // pushers detach so coord's root-plan execution proceeds in
            // parallel; soundness fallback at the scan side (timeout →
            // no filter) preserves correctness if a pusher fails.
            for (df_id, rx) in pre_receivers {
                // Resolve every consumer stage's tasks to (worker_addr, task_id).
                // A DF can have several consumer stages (a probe-side filter
                // shared by a semi join's and an anti join's build scans). The
                // dispatch loop pushed one entry per `consumer_k` in ascending
                // order, so `index == partition_id`. A producer-only DF (no
                // scan annotation reachable from this fragment tree) gets no
                // targets: the service still merges reports, nobody waits.
                let consumer_stages: std::collections::BTreeSet<u32> = consumers
                    .iter()
                    .filter(|c| c.df_id == df_id)
                    .map(|c| c.stage_id.0)
                    .collect();
                let targets: Vec<(String, TaskId)> = consumer_stages
                    .into_iter()
                    .filter_map(|stage| {
                        let consumer_stage_id = StageId(stage);
                        let entries = stage_results.get(&stage)?;
                        Some(entries.iter().enumerate().map(move |(k, (addr, _, _))| {
                            (
                                addr.clone(),
                                TaskId {
                                    stage_id: consumer_stage_id,
                                    partition_id: k as u32,
                                },
                            )
                        }))
                    })
                    .flatten()
                    .collect();
                if targets.is_empty() {
                    continue;
                }
                let query_id_for_push = query_id;
                tokio::spawn(async move {
                    let domain = match rx.await {
                        Ok(d) => d,
                        Err(_) => {
                            // Service dropped before resolution (query ended
                            // early). Consumer-side timeouts handle this.
                            eprintln!(
                                "[DFRPC] coord pusher receiver_closed query_id={} df_id={}",
                                query_id_for_push, df_id
                            );
                            return;
                        }
                    };
                    eprintln!(
                        "[DFRPC] coord pusher resolved query_id={} df_id={} domain={} targets={}",
                        query_id_for_push,
                        df_id,
                        dfrpc_domain_variant(&domain),
                        targets.len()
                    );
                    for (addr, task_id) in targets {
                        let req = arneb_rpc::NotifyDynamicFilterRequest {
                            query_id: query_id_for_push,
                            task_id,
                            df_id,
                            domain: domain.clone(),
                        };
                        eprintln!(
                            "[DFRPC] coord notify_send query_id={} task_id={} df_id={} addr={} domain={}",
                            query_id_for_push,
                            task_id,
                            df_id,
                            addr,
                            dfrpc_domain_variant(&domain)
                        );
                        if let Err(e) = arneb_rpc::notify_dynamic_filter(&addr, &req).await {
                            tracing::warn!(
                                query_id = %query_id_for_push,
                                task_id = %task_id,
                                df_id = %df_id,
                                error = %e,
                                "dynamic filter notify push failed"
                            );
                        } else {
                            eprintln!(
                                "[DFRPC] coord notify_ok query_id={} task_id={} df_id={}",
                                query_id_for_push, task_id, df_id
                            );
                        }
                    }
                });
            }

            // Build the root fragment's physical plan. `ExchangeNode` placeholders
            // resolve through `stage_results` to `ExchangeExec` operators that
            // pull from the worker `OutputBuffer`s registered above. The coord no
            // longer re-executes the entire original plan locally — that was the
            // 2026-05-20 placeholder we just removed.
            let coord_stage_results = expand_stage_results_for_single_consumer(
                &root_fragment.source_fragments,
                &stage_results,
            );
            let coord_ctx = exec_ctx.with_stage_results(coord_stage_results);
            let physical_plan = coord_ctx.create_physical_plan(&root_fragment.root)?;
            // Same coalesce wrap the pgwire handler applies in single-node mode:
            // `ExchangeExec` + `CoalescePartitionsExec` siblings produce N
            // output partitions; pgwire only drains partition 0. Without this
            // we'd silently lose N-1 partitions of the final result.
            let physical_plan: Arc<dyn arneb_execution::ExecutionPlan> =
                if physical_plan.output_partitioning().partition_count() > 1 {
                    Arc::new(arneb_execution::CoalescePartitionsExec::new(physical_plan))
                } else {
                    physical_plan
                };
            let stream = physical_plan.execute(0).await?;
            // A worker's memory-limit failure stays typed (pgwire 53200, Trino
            // EXCEEDED_LOCAL_MEMORY_LIMIT): the exchange restores it from the
            // status code and operators keep it when they re-raise it.
            let batches = collect_stream(stream)
                .await
                .map_err(|e| ExecutionError::upstream("root stage collection failed", e))?;
            Ok::<_, ExecutionError>(batches)
        };
        // `biased`: when the stream breaks because its worker died, both
        // branches can be ready in the same poll; report the lost worker.
        let batches = tokio::select! {
            biased;
            lost = watch_workers(&self.node_registry, &worker_ids, WORKER_WATCH_INTERVAL) => {
                return Err(AttemptError {
                    error: lost_worker_error(&lost),
                    worker_ids: worker_ids.clone(),
                });
            }
            result = run => result.map_err(|e| AttemptError {
                error: attribute_worker_failure(e, &workers, &self.node_registry),
                worker_ids: worker_ids.clone(),
            })?,
        };

        tracing::info!(
            query_id = %query_id,
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "distributed query complete"
        );

        cancel_guard.finish(
            stage_results
                .into_values()
                .flatten()
                .map(|(addr, _, _)| addr),
        );
        Ok(batches)
    }
}

/// P4 stage 2 (`retry_policy = "query"`, Trino's `retry_policy=QUERY`):
/// how a read-only query that failed because one of its workers was lost
/// is re-run on the remaining workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryRetry {
    /// Most re-runs of one query.
    pub max_retries: u32,
    /// Wait before the first re-run. It doubles for each later one, up to
    /// [`MAX_RETRY_DELAY`].
    pub initial_delay: std::time::Duration,
}

/// Default [`QueryRetry::initial_delay`].
pub const DEFAULT_RETRY_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_secs(1);
/// Longest wait between two runs of a query.
pub const MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(10);

impl QueryRetry {
    /// Up to `max_retries` re-runs with the default delays.
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            initial_delay: DEFAULT_RETRY_INITIAL_DELAY,
        }
    }

    /// The wait before re-run number `done + 1`.
    fn delay(&self, done: u32) -> std::time::Duration {
        self.initial_delay
            .saturating_mul(1 << done.min(16))
            .min(MAX_RETRY_DELAY)
    }
}

/// A failed run of a distributed query and the workers it used.
struct AttemptError {
    error: ExecutionError,
    worker_ids: Vec<String>,
}

/// Whether a plan writes data, so re-running it is not safe.
fn writes_data(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::CreateTableAsSelect { .. }
        | LogicalPlan::InsertInto { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. } => true,
        LogicalPlan::Explain { input, .. } => writes_data(input),
        _ => false,
    }
}

/// Run `attempt` until it succeeds, or fails for a reason other than a
/// lost worker, or `retry` allows no further re-run, or no worker is left.
/// The error returned is the last run's.
async fn with_query_retry<T, F, Fut>(
    retry: Option<QueryRetry>,
    registry: &NodeRegistry,
    mut attempt: F,
) -> Result<T, ExecutionError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, AttemptError>>,
{
    let mut done = 0;
    loop {
        let failed = match attempt().await {
            Ok(value) => {
                if done > 0 {
                    tracing::info!(target: "arneb::cluster", retries = done, "query succeeded after retry");
                }
                return Ok(value);
            }
            Err(failed) => failed,
        };
        let Some(retry) = retry.filter(|r| done < r.max_retries) else {
            return Err(failed.error);
        };
        // Retry only a run that one of its own workers failed; the lost
        // worker is out of `active_workers` until it heartbeats again.
        let Some(lost) = registry.lost_worker(&failed.worker_ids) else {
            return Err(failed.error);
        };
        if registry.active_workers().is_empty() {
            return Err(failed.error);
        }
        let delay = retry.delay(done);
        done += 1;
        arneb_common::metrics::query_retried();
        tracing::warn!(
            target: "arneb::cluster",
            retry = done,
            max_retries = retry.max_retries,
            worker_id = %lost.worker_id,
            delay_ms = delay.as_millis() as u64,
            error = %failed.error,
            "retrying query after worker loss"
        );
        tokio::time::sleep(delay).await;
    }
}

/// How often a running distributed query checks its workers' liveness.
const WORKER_WATCH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Resolves once one of `worker_ids` is lost (marked dead or past its
/// heartbeat deadline); pending forever otherwise.
async fn watch_workers(
    registry: &arneb_scheduler::NodeRegistry,
    worker_ids: &[String],
    interval: std::time::Duration,
) -> arneb_scheduler::LostWorker {
    loop {
        if let Some(lost) = registry.lost_worker(worker_ids) {
            return lost;
        }
        tokio::time::sleep(interval).await;
    }
}

fn lost_worker_error(lost: &arneb_scheduler::LostWorker) -> ExecutionError {
    tracing::warn!(
        target: "arneb::cluster",
        worker_id = %lost.worker_id,
        reason = %lost.reason,
        "failing query: worker lost"
    );
    ExecutionError::InvalidOperation(format!(
        "worker '{}' ({}) lost: {}",
        lost.worker_id, lost.address, lost.reason
    ))
}

/// Whether an RPC error means the peer could not be reached or the
/// connection broke, as opposed to the peer answering with an error.
fn is_transport_failure(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    [
        "failed to connect",
        "transport error",
        "connection refused",
        "connection reset",
        "broken pipe",
        "brokenpipe",
        "h2 protocol error",
        "error trying to connect",
        "dns error",
    ]
    .iter()
    .any(|needle| m.contains(needle))
}

/// A transport failure from one of the query's workers marks that worker
/// dead (until its next heartbeat) and names it in the error. Any other
/// error, including typed ones such as a memory limit, is returned as is.
fn attribute_worker_failure(
    err: ExecutionError,
    workers: &[arneb_scheduler::WorkerInfo],
    registry: &arneb_scheduler::NodeRegistry,
) -> ExecutionError {
    let message = err.to_string();
    if !is_transport_failure(&message) || message.contains("worker '") {
        return err;
    }
    // Errors nest outermost first ("stream from worker-1 failed: … stream
    // from worker-2: broken pipe"), so the worker named last is the one
    // whose connection broke.
    let Some((worker, address, _)) = workers
        .iter()
        .map(|w| {
            let address = flight_address(&w.address);
            let position = message.rfind(&address);
            (w, address, position)
        })
        .filter(|(_, _, position)| position.is_some())
        .max_by_key(|(_, _, position)| *position)
    else {
        return err;
    };
    registry.mark_dead(&worker.worker_id, "transport failure during query");
    ExecutionError::InvalidOperation(format!(
        "worker '{}' ({address}) failed: {message}",
        worker.worker_id
    ))
}

/// A worker's Flight address with its scheme.
pub(crate) fn flight_address(address: &str) -> String {
    if address.starts_with("http") {
        address.to_owned()
    } else {
        format!("http://{address}")
    }
}

/// Ends a distributed query on its workers (admission-control D4). From
/// the moment tasks may be dispatched, `Drop` sends `cancel_query` to every
/// worker, covering error returns, panics and the future being dropped.
/// Once [`CancelGuard::finish`] records the result, it sends `release_query`
/// to the workers that ran a task instead, so they drop the query's tasks
/// at once (worker-graceful-drain). Both go from a detached task so `Drop`
/// never blocks. Delivery is best-effort: a failure is logged, and the
/// worker's own buffer cleanup still frees the tasks later.
pub(crate) struct CancelGuard {
    query_id: QueryId,
    workers: Vec<String>,
    action: EndAction,
}

#[derive(Clone, Copy)]
enum EndAction {
    Cancel,
    Release,
}

impl CancelGuard {
    pub(crate) fn new(query_id: QueryId, workers: Vec<String>) -> Self {
        Self {
            query_id,
            workers,
            action: EndAction::Cancel,
        }
    }

    /// The query finished normally: release it on the workers at
    /// `dispatched` (deduplicated) instead of cancelling it everywhere.
    pub(crate) fn finish(&mut self, dispatched: impl IntoIterator<Item = String>) {
        let dispatched: std::collections::BTreeSet<String> = dispatched.into_iter().collect();
        self.workers = dispatched.into_iter().collect();
        self.action = EndAction::Release;
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if self.workers.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let query_id = self.query_id;
        let workers = std::mem::take(&mut self.workers);
        let action = self.action;
        if let EndAction::Cancel = action {
            tracing::info!(
                target: "arneb::cluster",
                query_id = %query_id,
                workers = workers.len(),
                "cancelling query on workers"
            );
        }
        // All workers at once: one unreachable worker (waiting out its
        // connect timeout) must not delay the others.
        runtime.spawn(async move {
            futures::future::join_all(workers.into_iter().map(|addr| async move {
                let (result, kind) = match action {
                    EndAction::Cancel => (
                        arneb_rpc::cancel_query(&addr, query_id).await,
                        "cancel_query",
                    ),
                    EndAction::Release => (
                        arneb_rpc::release_query(&addr, query_id).await,
                        "release_query",
                    ),
                };
                if let Err(e) = result {
                    tracing::warn!(
                        target: "arneb::cluster",
                        query_id = %query_id,
                        worker = %addr,
                        error = %e,
                        "{kind} delivery failed"
                    );
                }
            }))
            .await;
        });
    }
}

/// Dispatch a child fragment to a worker.
async fn execute_child_on_worker(
    fragment: &arneb_planner::PlanFragment,
    workers: &[arneb_scheduler::WorkerInfo],
    registry: &arneb_scheduler::NodeRegistry,
    stage_results: &mut std::collections::HashMap<u32, Vec<(String, String, u32)>>,
    query_id: &QueryId,
    // q21 SF30 fix: true when THIS fragment's own consumer must drain it fully
    // (so a mid-stream consumer drop = silent truncation → fail loud). Set on
    // this fragment's task descriptors; computed by the caller from the
    // consumer's root via `may_stop_input_early`.
    consumer_must_drain: bool,
) -> Result<(), ExecutionError> {
    // Recursively execute children first. A child's consumer is THIS fragment,
    // so its producers must_drain unless this fragment can legitimately
    // early-stop (a Limit-rooted consumer); every other operator drains fully.
    let child_must_drain = !fragment.root.may_stop_input_early();
    for child in &fragment.source_fragments {
        Box::pin(execute_child_on_worker(
            child,
            workers,
            registry,
            stage_results,
            query_id,
            child_must_drain,
        ))
        .await?;
    }

    let stage_id = fragment.id;

    let plan_json = serde_json::to_string(&fragment.root)
        .map_err(|e| ExecutionError::InvalidOperation(format!("plan serialization failed: {e}")))?;

    // W3-Hash.6 (2026-05-20): distinguish β producer from α consumer.
    // Trino uses α: N parallel tasks each emitting 1 output partition.
    // arneb's hash producer (a Source fragment with Hash output) acts
    // as β: 1 task with N output buckets (RepartitionExec inside the
    // task hashes rows into N buffers). The consumer of that — a
    // HashPartitioned join fragment — is α: N parallel tasks each
    // fetching its own bucket from the 1 producer task and emitting
    // its own slice of the join result. Mismatch between these two
    // (e.g. task_count == output_partitions == N → N² buffers) was the
    // 2026-05-20 first-attempt regression.
    //
    // M×N (2026-05-20, step 2): when a HashPartitioned fragment's own
    // output_partitioning carries non-empty hash columns, some downstream
    // stage demands partitioning on those keys — the α task itself must
    // also re-hash its slice into N output partitions (becoming an
    // α-producer too). Downstream task k pulls partition k from every
    // upstream α task → full M×N exchange. Inert until the fragmenter
    // gate (step 4) populates non-empty columns on a HashPartitioned
    // fragment's output_partitioning; today only Source fragments get
    // non-empty hash columns on their output.
    let output_partitions = match (&fragment.fragment_type, &fragment.output_partitioning) {
        (
            arneb_planner::FragmentType::Source | arneb_planner::FragmentType::Fixed,
            arneb_planner::PartitioningScheme::Hash {
                partition_count, ..
            },
        ) => *partition_count, // β producer
        (
            arneb_planner::FragmentType::HashPartitioned,
            arneb_planner::PartitioningScheme::Hash {
                partition_count,
                columns,
            },
        ) if !columns.is_empty() => *partition_count, // M×N α producer-AND-consumer
        _ => 1,
    };

    // Multi-worker scan (2026-06-10): a SOURCE fragment whose root is a
    // PartialAggregate is an A1-fused scan-direct aggregate. Run it as one
    // task per worker; each task strides 1/M of the scan's DataSource
    // partitions (ScanExec reads its stride from the worker's
    // `scan_task_count`) and emits O(groups) partials that the parent
    // FinalAggregate gathers and combines. Bare-scan SOURCE fragments can
    // use the same striding path when explicitly enabled.
    let scan_task_count = scan_task_count_for_fragment(
        &fragment.fragment_type,
        &fragment.root,
        workers.len(),
        data_parallel_scan_enabled(),
    );
    let task_count =
        effective_task_count_for_fragment(fragment, workers.len(), data_parallel_scan_enabled())
            as usize;

    // dist-mxn-nested-joins T2 (2026-06-05): log how each fragment is
    // classified so the trace shows which join fragments land on the
    // `(N,N)` vs `(N,1)` path. Gated by `ARNEB_TRACE_FRAGMENTS`.
    if fragments_traced() {
        eprintln!(
            "[FRAGTRACE] SCHEDULE stage={} type={} output={:?} -> task_count={task_count} output_partitions={output_partitions} root={}",
            fragment.id.0,
            fragment.fragment_type,
            fragment.output_partitioning,
            plan_node_label(&fragment.root),
        );
    }

    // W3-Hash.4 (2026-05-20): tell the worker which columns to hash
    // on when it has to fan out into multiple output partitions.
    // Derived from `fragment.output_partitioning`; empty when the
    // fragment is single-partition. Hoisted out of the per-task loop
    // since it depends on `fragment`, not on `consumer_k`.
    let output_hash_columns: Vec<u32> = match &fragment.output_partitioning {
        arneb_planner::PartitioningScheme::Hash { columns, .. } => {
            columns.iter().map(|c| *c as u32).collect()
        }
        _ => Vec::new(),
    };

    for consumer_k in 0..task_count as u32 {
        let task_id = TaskId {
            stage_id,
            partition_id: consumer_k,
        };
        let worker_idx = ((stage_id.0 as usize)
            .wrapping_mul(31)
            .wrapping_add(consumer_k as usize))
            % workers.len();
        let worker = &workers[worker_idx];

        // M×N (2026-05-20, step 3a): build this consumer's source_exchanges.
        // For each child upstream:
        //   - if the child's output_partitioning has non-empty hash columns,
        //     the child is a multi-partition producer (β: 1 task × N parts,
        //     or α-producer-and-consumer: M tasks × N parts each). Parallel
        //     consumers pull partition_id = consumer_k from every upstream
        //     task; single-task gather consumers pull every bucket.
        //   - else the child is a single-partition producer per task; this
        //     consumer task pulls partition_id = 0 from each.
        // Pre-M×N behaviour was hardcoded partition_id = 0 for all consumers,
        // which silently fragmented data across α-producer outputs.
        let source_exchanges: Vec<arneb_rpc::SourceExchange> = fragment
            .source_fragments
            .iter()
            .flat_map(|child| {
                let partition_ids =
                    exchange_partition_ids_for_consumer(child, consumer_k, task_count as u32);
                stage_results
                    .get(&child.id.0)
                    .into_iter()
                    .flat_map(move |entries| {
                        let partition_ids = partition_ids.clone();
                        entries
                            .iter()
                            .flat_map(move |(addr, task_id, _upstream_pid)| {
                                let partition_ids = partition_ids.clone();
                                partition_ids.into_iter().map(move |partition_id| {
                                    arneb_rpc::SourceExchange {
                                        source_stage_id: child.id.0,
                                        source_task_id: task_id.clone(),
                                        flight_address: addr.clone(),
                                        partition_id,
                                        // A2.2 (2026-05-28): propagate broadcast
                                        // flag from the child fragment's output
                                        // partitioning. Consumer side is purely
                                        // informational at this point — the
                                        // server's `do_get` dispatches on the
                                        // producer-side BufferKind. (A2.3 will
                                        // consume this flag to skip probe-side
                                        // RepartitionExec wrap.)
                                        broadcast: matches!(
                                            &child.output_partitioning,
                                            arneb_planner::PartitioningScheme::Broadcast
                                        ),
                                    }
                                })
                            })
                    })
            })
            .collect();

        let descriptor = arneb_rpc::TaskDescriptor {
            task_id,
            stage_id,
            query_id: *query_id,
            plan_json: plan_json.clone(),
            output_partitions,
            output_hash_columns: output_hash_columns.clone(),
            // A2.2 (2026-05-28): broadcast flag from this fragment's
            // output partitioning. When true, `task_manager` allocates
            // a `BroadcastOutputBuffer` instead of a partitioned one
            // and runs a single pumper per task. A SOURCE broadcast
            // fragment runs as 1 task (or M striped scan tasks); a
            // HashPartitioned broadcast fragment runs one task per
            // upstream hash bucket (per `task_count_for_fragment`), and
            // consumers union every task's broadcast output.
            broadcast: matches!(
                fragment.output_partitioning,
                arneb_planner::PartitioningScheme::Broadcast
            ),
            source_exchanges,
            // q21 SF30 fix: fail loud on a mid-stream consumer drop when the
            // consumer must drain this output fully (silent truncation guard).
            must_drain: consumer_must_drain,
            // A1.3 (2026-05-27): empty until A1.5 lets producers
            // populate the coord-side `DynamicFilterService` with
            // resolved Domains. Until then the worker collector
            // starts empty and the scan side has nothing to consume.
            pending_dynamic_filters: Vec::new(),
            // Multi-worker scan parallelism: M (one task per worker) for an
            // A1-fused scan SOURCE fragment, else 1. The worker strides the
            // scan by (task_id.partition_id, scan_task_count).
            scan_task_count,
        };

        let flight_addr = flight_address(&worker.address);
        tracing::info!(
            stage_id = %stage_id,
            partition = consumer_k,
            worker = %worker.worker_id,
            "submitting task"
        );

        if let Err(e) = arneb_rpc::submit_task(&flight_addr, &descriptor).await {
            if is_transport_failure(&e) {
                registry.mark_dead(&worker.worker_id, &e);
                return Err(ExecutionError::InvalidOperation(format!(
                    "worker '{}' ({}) unreachable: task submission failed: {e}",
                    worker.worker_id, flight_addr
                )));
            }
            return Err(ExecutionError::InvalidOperation(format!(
                "task submission failed: {e}"
            )));
        }

        let buffer_key = format!("{query_id}.{task_id}");
        // Keep this directory as one entry per scheduled task. The third tuple
        // field is only the default single-partition ticket; coord-side root
        // execution expands hash-output tasks into all bucket tickets before
        // physical planning. Dynamic-filter notification code also relies on
        // this map staying one-entry-per-task.
        stage_results
            .entry(stage_id.0)
            .or_default()
            .push((flight_addr, buffer_key, 0));
    }

    // No sleep here — `ExchangeClient::fetch_partition` retries on `NotFound`
    // until the worker registers its `OutputBuffer` (at the end of
    // `task_manager::execute_task`). That lets coord proceed immediately
    // and naturally back-pressures on actual task duration instead of a
    // hardcoded 500ms guess. The buffer-key format
    // `format!("{query_id}.{task_id}")` matches what the worker uses to
    // index `flight_state.buffers`, so do_get from a peer worker (W2.2
    // path) finds the right buffer even if stage numbering collides
    // across queries.

    Ok(())
}

fn hash_output_bucket_count(fragment: &arneb_planner::PlanFragment) -> Option<usize> {
    match &fragment.output_partitioning {
        arneb_planner::PartitioningScheme::Hash {
            columns,
            partition_count,
        } if !columns.is_empty() => Some(*partition_count),
        _ => None,
    }
}

fn exchange_partition_ids_for_consumer(
    producer: &arneb_planner::PlanFragment,
    consumer_k: u32,
    consumer_task_count: u32,
) -> Vec<u32> {
    match hash_output_bucket_count(producer) {
        Some(partition_count) if consumer_task_count <= 1 => (0..partition_count as u32).collect(),
        Some(_) => vec![consumer_k],
        None => vec![0],
    }
}

fn expand_stage_results_for_single_consumer(
    producers: &[arneb_planner::PlanFragment],
    stage_results: &std::collections::HashMap<u32, Vec<(String, String, u32)>>,
) -> std::collections::HashMap<u32, Vec<(String, String, u32)>> {
    let mut expanded = stage_results.clone();
    for producer in producers {
        let Some(partition_count) = hash_output_bucket_count(producer) else {
            continue;
        };
        let Some(entries) = stage_results.get(&producer.id.0) else {
            continue;
        };
        expanded.insert(
            producer.id.0,
            entries
                .iter()
                .flat_map(|(addr, task_id, _)| {
                    (0..partition_count as u32)
                        .map(move |partition_id| (addr.clone(), task_id.clone(), partition_id))
                })
                .collect(),
        );
    }
    expanded
}

/// A1.5 (2026-05-27): one producer entry from a fragment walk —
/// declares that the `stage_id`-th fragment (running `task_count`
/// parallel tasks) emits a Domain for `df_id` from its HJ/SJ build.
struct DfProducer {
    df_id: DynamicFilterId,
    stage_id: StageId,
    task_count: u32,
}

/// A1.5: one consumer entry — declares that the `stage_id`-th
/// fragment's TableScan awaits a Domain for `df_id`.
struct DfConsumer {
    df_id: DynamicFilterId,
    stage_id: StageId,
}

/// Walk a fragment tree (root + every child) and enumerate every
/// `dynamic_filter_ids` entry on a `Join` / `SemiJoin`. The producing
/// stage is the fragment containing the join.
fn collect_df_producers(
    root: &arneb_planner::PlanFragment,
    worker_count: usize,
) -> Vec<DfProducer> {
    let mut out = Vec::new();
    visit_fragments(root, &mut |f| {
        let tc = effective_task_count_for_fragment(f, worker_count, data_parallel_scan_enabled());
        let join_tc = join_df_task_count_for_fragment(f, tc);
        collect_producers_from_plan(&f.root, f.id, tc, join_tc, &mut out);
    });
    out
}

fn join_df_task_count_for_fragment(fragment: &arneb_planner::PlanFragment, task_count: u32) -> u32 {
    match &fragment.output_partitioning {
        arneb_planner::PartitioningScheme::Hash {
            partition_count, ..
        } => (*partition_count as u32).max(task_count),
        _ => task_count,
    }
}

/// Walk a fragment tree and enumerate every TableScan's
/// `dynamic_filters_consumed`. The consumer stage is the fragment
/// containing the scan.
fn collect_df_consumers(root: &arneb_planner::PlanFragment) -> Vec<DfConsumer> {
    let mut out = Vec::new();
    visit_fragments(root, &mut |f| {
        collect_consumers_from_plan(&f.root, f.id, &mut out);
    });
    out
}

fn visit_fragments<F: FnMut(&arneb_planner::PlanFragment)>(
    fragment: &arneb_planner::PlanFragment,
    visitor: &mut F,
) {
    visitor(fragment);
    for child in &fragment.source_fragments {
        visit_fragments(child, visitor);
    }
}

fn collect_producers_from_plan(
    plan: &LogicalPlan,
    stage_id: StageId,
    task_count: u32,
    join_task_count: u32,
    out: &mut Vec<DfProducer>,
) {
    use LogicalPlan as L;
    match plan {
        L::Join {
            dynamic_filter_ids,
            left,
            right,
            ..
        } => {
            for p in dynamic_filter_ids {
                out.push(DfProducer {
                    df_id: p.id,
                    stage_id,
                    task_count: join_task_count,
                });
            }
            collect_producers_from_plan(left, stage_id, task_count, join_task_count, out);
            collect_producers_from_plan(right, stage_id, task_count, join_task_count, out);
        }
        L::SemiJoin {
            dynamic_filter_ids,
            probe_side_filter_ids,
            left,
            right,
            ..
        } => {
            for p in dynamic_filter_ids {
                out.push(DfProducer {
                    df_id: p.id,
                    stage_id,
                    task_count,
                });
            }
            // Probe-side filters are published once per join task, after
            // that task drains its slice of the left input, so the
            // coordinator expects one report per hash partition.
            for p in probe_side_filter_ids {
                out.push(DfProducer {
                    df_id: p.id,
                    stage_id,
                    task_count: join_task_count,
                });
            }
            collect_producers_from_plan(left, stage_id, task_count, join_task_count, out);
            collect_producers_from_plan(right, stage_id, task_count, join_task_count, out);
        }
        L::AntiJoin {
            probe_side_filter_ids,
            left,
            right,
            ..
        } => {
            for p in probe_side_filter_ids {
                out.push(DfProducer {
                    df_id: p.id,
                    stage_id,
                    task_count: join_task_count,
                });
            }
            collect_producers_from_plan(left, stage_id, task_count, join_task_count, out);
            collect_producers_from_plan(right, stage_id, task_count, join_task_count, out);
        }
        L::Filter { input, .. }
        | L::Projection { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. }
        | L::Aggregate { input, .. }
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Distinct { input, .. }
        | L::Explain { input, .. }
        | L::Window { input, .. }
        | L::AssignUniqueId { input, .. } => {
            collect_producers_from_plan(input, stage_id, task_count, join_task_count, out);
        }
        L::Intersect { left, right, .. } | L::Except { left, right, .. } => {
            collect_producers_from_plan(left, stage_id, task_count, join_task_count, out);
            collect_producers_from_plan(right, stage_id, task_count, join_task_count, out);
        }
        L::UnionAll { inputs } => {
            for i in inputs {
                collect_producers_from_plan(i, stage_id, task_count, join_task_count, out);
            }
        }
        _ => {}
    }
}

fn collect_consumers_from_plan(plan: &LogicalPlan, stage_id: StageId, out: &mut Vec<DfConsumer>) {
    use LogicalPlan as L;
    match plan {
        L::TableScan {
            dynamic_filters_consumed,
            ..
        } => {
            for c in dynamic_filters_consumed {
                out.push(DfConsumer {
                    df_id: c.id,
                    stage_id,
                });
            }
        }
        L::Filter { input, .. }
        | L::Projection { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. }
        | L::Aggregate { input, .. }
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Distinct { input, .. }
        | L::Explain { input, .. }
        | L::Window { input, .. }
        | L::AssignUniqueId { input, .. } => {
            collect_consumers_from_plan(input, stage_id, out);
        }
        L::Join { left, right, .. }
        | L::SemiJoin { left, right, .. }
        | L::AntiJoin { left, right, .. }
        | L::Intersect { left, right, .. }
        | L::Except { left, right, .. } => {
            collect_consumers_from_plan(left, stage_id, out);
            collect_consumers_from_plan(right, stage_id, out);
        }
        L::UnionAll { inputs } => {
            for i in inputs {
                collect_consumers_from_plan(i, stage_id, out);
            }
        }
        _ => {}
    }
}

/// Base task-count derivation used before data-parallel scan overrides.
fn task_count_for_fragment(fragment: &arneb_planner::PlanFragment) -> u32 {
    match (&fragment.fragment_type, &fragment.output_partitioning) {
        (arneb_planner::FragmentType::Source, arneb_planner::PartitioningScheme::Hash { .. }) => 1, // β producer: 1 task, N output partitions
        (
            arneb_planner::FragmentType::HashPartitioned,
            arneb_planner::PartitioningScheme::Hash {
                partition_count, ..
            },
        ) => *partition_count as u32, // α consumer (or α producer-and-consumer)
        (
            arneb_planner::FragmentType::HashPartitioned,
            arneb_planner::PartitioningScheme::Broadcast,
        ) => {
            let Some(bucket_count) = fragment
                .source_fragments
                .first()
                .and_then(hash_output_bucket_count)
            else {
                return 1;
            };
            // Every source must expose the same buckets for task k to read only bucket k.
            if fragment
                .source_fragments
                .iter()
                .all(|source| hash_output_bucket_count(source) == Some(bucket_count))
            {
                bucket_count as u32
            } else {
                1
            }
        }
        _ => 1,
    }
}

/// Same effective task-count derivation used by `execute_child_on_worker`.
/// DF registration uses this so `expected_partitions` matches the number of
/// tasks that can publish reports for a producer stage.
fn effective_task_count_for_fragment(
    fragment: &arneb_planner::PlanFragment,
    worker_count: usize,
    data_parallel_scan_enabled: bool,
) -> u32 {
    let mut task_count = task_count_for_fragment(fragment);
    let scan_task_count = scan_task_count_for_fragment(
        &fragment.fragment_type,
        &fragment.root,
        worker_count,
        data_parallel_scan_enabled,
    );
    if scan_task_count > 1 {
        task_count = scan_task_count as u32;
    }
    task_count
}

/// RAII guard that drops a query's `DynamicFilterService` entry when
/// the executor returns, including the error paths inside `execute`.
/// A1.3 — keeps the registry from leaking per-query state.
struct DfRegistrationGuard {
    registry: DynamicFilterServiceRegistry,
    query_id: QueryId,
}

impl Drop for DfRegistrationGuard {
    fn drop(&mut self) {
        self.registry.unregister(&self.query_id);
    }
}

fn count_fragments(fragment: &arneb_planner::PlanFragment) -> usize {
    1 + fragment
        .source_fragments
        .iter()
        .map(count_fragments)
        .sum::<usize>()
}

fn scan_task_count_for_fragment(
    fragment_type: &arneb_planner::FragmentType,
    root: &LogicalPlan,
    worker_count: usize,
    data_parallel_scan_enabled: bool,
) -> usize {
    if matches!(fragment_type, arneb_planner::FragmentType::Source)
        && worker_count > 1
        && (matches!(root, LogicalPlan::PartialAggregate { .. }) || data_parallel_scan_enabled)
    {
        worker_count
    } else {
        1
    }
}

// ---------------------------------------------------------------------------
// dist-mxn-nested-joins T2 (2026-06-05): env-gated fragment-tree trace.
//
// Set `ARNEB_TRACE_FRAGMENTS=1` on the coordinator to print, per fragment:
// id, type, output_partitioning (hash columns + count), and a within-fragment
// plan outline (descending children, stopping at the ExchangeNode boundary).
//
// This is the decisive task-2.1 artifact: it shows whether an INTERMEDIATE
// join fragment carries NON-EMPTY hash columns (→ coordinator's `(N,N)`
// α-producer-and-consumer path) or stays EMPTY (→ `(N,1)` α-consumer-only,
// the suspected row-drop / double-consume mechanism). `eprintln!` (not
// `tracing`) so it is unconditionally visible in `docker logs` regardless of
// the subscriber config. Debug diagnostic only — remove before ship.
// ---------------------------------------------------------------------------
fn fragments_traced() -> bool {
    std::env::var("ARNEB_TRACE_FRAGMENTS")
        .map(|v| v != "0" && !v.is_empty())
        .unwrap_or(false)
}

/// Whether bare-scan SOURCE fragments run data-parallel (one task per worker,
/// each striding 1/M of the DataSource partitions) like the q01 fused
/// scan+partial-agg path already does. This is a strict execution improvement —
/// validated at SF30 as cell-identical to Trino (22/22) with lower per-worker
/// peak memory and faster fact-table scans (flips q19, improves the deep joins
/// on both axes) — so it is ON BY DEFAULT. `ARNEB_DATA_PARALLEL_SCAN=0` is a
/// disable escape-hatch (e.g. if a very large worker count makes the M×N
/// fan-out connection count a concern); any other value (or unset) keeps it on.
fn data_parallel_scan_enabled() -> bool {
    static DATA_PARALLEL_SCAN_ENABLED: OnceLock<bool> = OnceLock::new();

    *DATA_PARALLEL_SCAN_ENABLED.get_or_init(|| {
        let v = std::env::var("ARNEB_DATA_PARALLEL_SCAN")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(true);
        tracing::info!(
            target: "arneb::config",
            data_parallel_scan = v,
            "ARNEB_DATA_PARALLEL_SCAN effective value (default on; =0 to disable)"
        );
        v
    })
}

/// Compact label for a single logical-plan node (no recursion).
fn plan_node_label(plan: &LogicalPlan) -> String {
    let base = match plan {
        LogicalPlan::Join {
            join_type,
            condition,
            ..
        } => format!("Join({join_type:?}, {condition:?})"),
        LogicalPlan::SemiJoin {
            left_key,
            right_key,
            ..
        } => format!("SemiJoin({left_key:?} = {right_key:?})"),
        LogicalPlan::AntiJoin {
            left_key,
            right_key,
            ..
        } => format!("AntiJoin({left_key:?} = {right_key:?})"),
        // Diagnostic (2026-06-09, q08 column-pruning): show the scan's
        // full schema width and the Projection's emitted column names so
        // the FRAGTRACE dump reveals exactly which columns each build /
        // intermediate carries (e.g. is `orders` pruned to its 3 join
        // columns, or does it still ship o_comment/o_clerk?).
        LogicalPlan::TableScan { table, schema, .. } => {
            format!("TableScan({table}, full={})", schema.len())
        }
        LogicalPlan::ExchangeNode { stage_id, .. } => format!("ExchangeNode(stage={})", stage_id.0),
        LogicalPlan::Projection { schema, .. } => {
            let cols: Vec<&str> = schema.iter().map(|c| c.name.as_str()).collect();
            format!("Projection[{}]", cols.join(", "))
        }
        LogicalPlan::Filter { .. } => "Filter".to_string(),
        LogicalPlan::Aggregate { .. } => "Aggregate".to_string(),
        LogicalPlan::PartialAggregate { .. } => "PartialAggregate".to_string(),
        LogicalPlan::FinalAggregate { .. } => "FinalAggregate".to_string(),
        LogicalPlan::Sort { .. } => "Sort".to_string(),
        LogicalPlan::Limit { .. } => "Limit".to_string(),
        other => {
            // Fallback: first token of the Debug repr (variant name).
            let dbg = format!("{other:?}");
            dbg.split([' ', '{', '(']).next().unwrap_or("?").to_string()
        }
    };
    // Append the node's output width so build / probe / exchange column
    // counts are visible at a glance.
    format!("{base} «w={}»", plan.schema().len())
}

/// Children of a logical-plan node, stopping the descent at the
/// ExchangeNode fragment boundary (its child belongs to another fragment).
fn plan_node_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    use LogicalPlan as L;
    match plan {
        L::TableScan { .. } | L::ExchangeNode { .. } | L::OneRow => vec![],
        L::Projection { input, .. }
        | L::Filter { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. }
        | L::Distinct { input }
        | L::Aggregate { input, .. }
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Window { input, .. }
        | L::AssignUniqueId { input, .. } => vec![input.as_ref()],
        L::Join { left, right, .. }
        | L::SemiJoin { left, right, .. }
        | L::AntiJoin { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        L::UnionAll { inputs } => inputs.iter().collect(),
        _ => vec![],
    }
}

fn write_plan_outline(plan: &LogicalPlan, depth: usize, out: &mut String) {
    out.push_str(&format!(
        "\n[FRAGTRACE]      {}{}",
        "  ".repeat(depth),
        plan_node_label(plan)
    ));
    for child in plan_node_children(plan) {
        write_plan_outline(child, depth + 1, out);
    }
}

fn trace_fragment_tree(frag: &arneb_planner::PlanFragment) {
    use arneb_planner::PartitioningScheme;
    let part = match &frag.output_partitioning {
        PartitioningScheme::Hash {
            columns,
            partition_count,
        } => format!("Hash{{cols={columns:?}, n={partition_count}}}"),
        other => format!("{other:?}"),
    };
    let srcs: Vec<u32> = frag.source_fragments.iter().map(|c| c.id.0).collect();
    let mut outline = String::new();
    write_plan_outline(&frag.root, 0, &mut outline);
    eprintln!(
        "[FRAGTRACE] id={} type={} output={part} sources={srcs:?}{outline}",
        frag.id.0, frag.fragment_type,
    );
    for child in &frag.source_fragments {
        trace_fragment_tree(child);
    }
}

#[async_trait]
impl DistributedExecutor for QueryCoordinator {
    async fn execute(
        &self,
        plan: LogicalPlan,
        exec_ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, ArnebError> {
        self.execute(plan, exec_ctx)
            .await
            .map_err(ArnebError::Execution)
    }

    fn has_workers(&self) -> bool {
        self.has_workers()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: &str, address: &str) -> arneb_scheduler::WorkerInfo {
        arneb_scheduler::WorkerInfo {
            worker_id: id.to_string(),
            address: address.to_string(),
            last_heartbeat: std::time::Instant::now(),
            max_splits: 256,
            alive: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
            draining: false,
        }
    }

    #[tokio::test]
    async fn watch_resolves_when_a_query_worker_is_lost() {
        let registry = arneb_scheduler::NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "http://w1:9090".into(), 256);
        registry.mark_dead("w1", "test");
        let lost = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            watch_workers(
                &registry,
                &["w1".to_string()],
                std::time::Duration::from_millis(20),
            ),
        )
        .await
        .expect("a lost worker is detected");
        assert_eq!(lost.worker_id, "w1");
        let message = lost_worker_error(&lost).to_string();
        assert!(
            message.contains("worker 'w1' (http://w1:9090) lost"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn watch_stays_pending_while_workers_heartbeat() {
        let registry = arneb_scheduler::NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "http://w1:9090".into(), 256);
        let watched = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            watch_workers(
                &registry,
                &["w1".to_string()],
                std::time::Duration::from_millis(20),
            ),
        )
        .await;
        assert!(watched.is_err(), "no worker is lost");
    }

    #[test]
    fn transport_failures_are_told_apart_from_remote_errors() {
        assert!(is_transport_failure(
            "failed to connect to http://w2:9090: transport error"
        ));
        assert!(is_transport_failure(
            "h2 protocol error: error reading a body from connection: BrokenPipe"
        ));
        assert!(!is_transport_failure(
            "remote task q.1.0 on http://w2:9090 failed: query exceeded memory limit"
        ));
    }

    #[test]
    fn a_broken_stream_is_blamed_on_the_innermost_worker_and_marks_it_dead() {
        let registry = arneb_scheduler::NodeRegistry::new(30);
        registry.heartbeat("worker-1".into(), "http://w1:9090".into(), 256);
        registry.heartbeat("worker-2".into(), "http://w2:9090".into(), 256);
        let workers = [
            worker("worker-1", "http://w1:9090"),
            worker("worker-2", "http://w2:9090"),
        ];
        let err = ExecutionError::InvalidOperation(
            "Flight stream error from http://w1:9090: remote task failed: \
             Flight stream error from http://w2:9090: h2 protocol error: broken pipe"
                .to_string(),
        );
        let message = attribute_worker_failure(err, &workers, &registry).to_string();
        assert!(
            message.starts_with("invalid operation: worker 'worker-2' (http://w2:9090) failed"),
            "{message}"
        );
        let alive: Vec<_> = registry
            .alive_workers()
            .into_iter()
            .map(|w| w.worker_id)
            .collect();
        assert_eq!(alive, vec!["worker-1".to_string()]);
    }

    #[test]
    fn a_remote_error_is_not_blamed_on_a_worker() {
        let registry = arneb_scheduler::NodeRegistry::new(30);
        registry.heartbeat("worker-1".into(), "http://w1:9090".into(), 256);
        let workers = [worker("worker-1", "http://w1:9090")];
        let err = ExecutionError::InvalidOperation(
            "remote task q.1.0 on http://w1:9090 failed: division by zero".to_string(),
        );
        let message = attribute_worker_failure(err, &workers, &registry).to_string();
        assert!(!message.contains("worker 'worker-1'"), "{message}");
        assert_eq!(registry.alive_count(), 1);
    }

    fn quick_retry(max_retries: u32) -> Option<QueryRetry> {
        Some(QueryRetry {
            max_retries,
            initial_delay: std::time::Duration::from_millis(1),
        })
    }

    fn two_worker_registry() -> arneb_scheduler::NodeRegistry {
        let registry = arneb_scheduler::NodeRegistry::new(30);
        registry.heartbeat("worker-1".into(), "http://w1:9090".into(), 256);
        registry.heartbeat("worker-2".into(), "http://w2:9090".into(), 256);
        registry
    }

    fn failure(message: &str) -> AttemptError {
        AttemptError {
            error: ExecutionError::InvalidOperation(message.to_string()),
            worker_ids: vec!["worker-1".to_string(), "worker-2".to_string()],
        }
    }

    /// Runs `with_query_retry` over scripted attempts: each one either
    /// loses `worker-2` and fails, or fails without losing a worker, or
    /// succeeds. Returns the result and the number of runs.
    async fn run_script(
        retry: Option<QueryRetry>,
        registry: &arneb_scheduler::NodeRegistry,
        script: &[&str],
    ) -> (Result<u32, String>, usize) {
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let result = with_query_retry(retry, registry, || {
            let n = runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let step = script[n.min(script.len() - 1)];
            async move {
                match step {
                    "lose" => {
                        registry.mark_dead("worker-2", "transport failure during query");
                        Err(failure(
                            "worker 'worker-2' (http://w2:9090) failed: broken pipe",
                        ))
                    }
                    "error" => Err(failure("division by zero")),
                    _ => Ok(n as u32),
                }
            }
        })
        .await
        .map_err(|e| e.to_string());
        (result, runs.load(std::sync::atomic::Ordering::SeqCst))
    }

    #[tokio::test]
    async fn retry_off_returns_the_first_failure() {
        let registry = two_worker_registry();
        let (result, runs) = run_script(None, &registry, &["lose", "ok"]).await;
        assert!(result.unwrap_err().contains("worker 'worker-2'"));
        assert_eq!(runs, 1);
    }

    #[tokio::test]
    async fn a_lost_worker_reruns_the_query_on_the_rest() {
        let registry = two_worker_registry();
        let (result, runs) = run_script(quick_retry(4), &registry, &["lose", "ok"]).await;
        assert_eq!(result, Ok(1));
        assert_eq!(runs, 2);
        let active: Vec<_> = registry
            .active_workers()
            .into_iter()
            .map(|w| w.worker_id)
            .collect();
        assert_eq!(active, vec!["worker-1".to_string()]);
    }

    #[tokio::test]
    async fn a_failure_without_a_lost_worker_is_not_retried() {
        let registry = two_worker_registry();
        let (result, runs) = run_script(quick_retry(4), &registry, &["error", "ok"]).await;
        assert_eq!(result.unwrap_err(), "invalid operation: division by zero");
        assert_eq!(runs, 1);
    }

    #[tokio::test]
    async fn retries_stop_at_the_limit_with_the_last_error() {
        let registry = two_worker_registry();
        // Each run loses worker-2 again (it heartbeats back in between).
        let registry_ref = &registry;
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let result: Result<(), _> = with_query_retry(quick_retry(2), registry_ref, || {
            runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            registry_ref.heartbeat("worker-2".into(), "http://w2:9090".into(), 256);
            async move {
                registry_ref.mark_dead("worker-2", "transport failure during query");
                Err(failure(
                    "worker 'worker-2' (http://w2:9090) failed: broken pipe",
                ))
            }
        })
        .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("worker 'worker-2'"));
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn no_retry_when_no_worker_is_left() {
        let registry = arneb_scheduler::NodeRegistry::new(30);
        registry.heartbeat("worker-2".into(), "http://w2:9090".into(), 256);
        let (result, runs) = run_script(quick_retry(4), &registry, &["lose", "ok"]).await;
        assert!(result.is_err());
        assert_eq!(runs, 1);
    }

    #[test]
    fn writes_are_never_retried_and_delays_double_up_to_the_cap() {
        let insert = LogicalPlan::InsertInto {
            table: TableReference::table("t"),
            source: Box::new(scan()),
        };
        assert!(writes_data(&insert));
        assert!(writes_data(&LogicalPlan::Explain {
            input: Box::new(insert),
            analyze: true,
        }));
        assert!(!writes_data(&partial_aggregate()));

        let retry = QueryRetry::new(10);
        let secs: Vec<u64> = (0..6).map(|n| retry.delay(n).as_secs()).collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 10, 10]);
    }

    /// A Flight server whose `cancel_query` callback records query ids,
    /// and whose `release_query` callback records them as `released`.
    async fn recording_worker() -> (String, Arc<std::sync::Mutex<Vec<QueryId>>>) {
        let (addr, seen, _) = recording_worker_with_releases().await;
        (addr, seen)
    }

    async fn recording_worker_with_releases() -> (
        String,
        Arc<std::sync::Mutex<Vec<QueryId>>>,
        Arc<std::sync::Mutex<Vec<QueryId>>>,
    ) {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let released = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut state = arneb_rpc::FlightState::new();
        let s = seen.clone();
        state.set_cancel_query_callback(Arc::new(move |q| {
            s.lock().unwrap().push(q);
            0
        }));
        let r = released.clone();
        state.set_release_query_callback(Arc::new(move |q| {
            r.lock().unwrap().push(q);
            0
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        tokio::spawn(arneb_rpc::__serve_flight_for_test(listener, state));
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        (addr, seen, released)
    }

    /// admission-control D4: a guard dropped before `finish` (error, panic,
    /// or the query future dropped) cancels the query on every worker; a
    /// finished one (success) releases it on the workers that ran a task,
    /// once each (worker-graceful-drain).
    #[tokio::test]
    async fn cancel_guard_cancels_on_drop_and_releases_once_finished() {
        let (a, seen_a, released_a) = recording_worker_with_releases().await;
        let (b, seen_b, released_b) = recording_worker_with_releases().await;
        let q = QueryId::new();
        drop(CancelGuard::new(q, vec![a.clone(), b.clone()]));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(*seen_a.lock().unwrap(), vec![q]);
        assert_eq!(*seen_b.lock().unwrap(), vec![q]);
        assert!(released_a.lock().unwrap().is_empty());

        let done = QueryId::new();
        let mut ok = CancelGuard::new(done, vec![a.clone(), b]);
        ok.finish([a.clone(), a]);
        drop(ok);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(seen_a.lock().unwrap().len(), 1);
        assert_eq!(seen_b.lock().unwrap().len(), 1);
        assert_eq!(*released_a.lock().unwrap(), vec![done]);
        assert!(released_b.lock().unwrap().is_empty(), "b ran no task");
    }

    /// #145: workers are cancelled concurrently, so a worker that never
    /// answers does not hold up the cancel on the others.
    #[tokio::test]
    async fn cancel_guard_does_not_wait_on_a_hung_worker() {
        // Accepts connections and never speaks: a call to it hangs.
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hung = format!("http://127.0.0.1:{}", silent.local_addr().unwrap().port());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = silent.accept().await {
                held.push(sock);
            }
        });
        let (a, seen) = recording_worker().await;
        let q = QueryId::new();
        drop(CancelGuard::new(q, vec![hung, a]));
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(*seen.lock().unwrap(), vec![q]);
    }

    /// Dropping the future of a query whose tasks are dispatched (here: a
    /// future parked on a guard, like `execute` awaiting the root stage)
    /// cancels it on the workers.
    #[tokio::test]
    async fn dropping_the_query_future_cancels_on_workers() {
        let (a, seen) = recording_worker().await;
        let q = QueryId::new();
        let fut = async move {
            let _guard = CancelGuard::new(q, vec![a]);
            futures::future::pending::<()>().await;
        };
        let handle = tokio::spawn(fut);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        handle.abort();
        let _ = handle.await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(*seen.lock().unwrap(), vec![q]);
    }
    use arneb_common::types::{ColumnInfo, DataType, TableReference};
    use arneb_planner::{FragmentType, JoinCondition, PartitioningScheme, PlanExpr, PlanFragment};

    fn schema() -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            name: "c0".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }]
    }

    fn scan() -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table("t"),
            schema: schema(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn partial_aggregate() -> LogicalPlan {
        LogicalPlan::PartialAggregate {
            input: Box::new(scan()),
            group_by: Vec::new(),
            aggr_exprs: Vec::new(),
            schema: schema(),
        }
    }

    #[test]
    fn scan_task_count_gates_bare_scan_on_data_parallel_knob() {
        let root = scan();

        assert_eq!(
            scan_task_count_for_fragment(&FragmentType::Source, &root, 2, true),
            2
        );
        assert_eq!(
            scan_task_count_for_fragment(&FragmentType::Source, &root, 2, false),
            1
        );
    }

    #[test]
    fn scan_task_count_keeps_partial_aggregate_always_on() {
        let root = partial_aggregate();

        assert_eq!(
            scan_task_count_for_fragment(&FragmentType::Source, &root, 2, true),
            2
        );
        assert_eq!(
            scan_task_count_for_fragment(&FragmentType::Source, &root, 2, false),
            2
        );
    }

    #[test]
    fn collect_df_producers_uses_effective_scan_task_count() {
        let df_id = DynamicFilterId(123);
        let root = LogicalPlan::SemiJoin {
            left: Box::new(scan()),
            right: Box::new(scan()),
            left_key: PlanExpr::Column {
                index: 0,
                name: "c0".to_string(),
                span: None,
            },
            right_key: PlanExpr::Column {
                index: 0,
                name: "c0".to_string(),
                span: None,
            },
            residual: None,
            dynamic_filter_ids: vec![arneb_planner::DynamicFilterProducer {
                id: df_id,
                build_index: 0,
                probe_index: 0,
                column_name: "c0".to_string(),
            }],
            probe_side_filter_ids: Vec::new(),
        };
        let fragment = PlanFragment {
            id: StageId(42),
            fragment_type: FragmentType::Source,
            root,
            output_partitioning: PartitioningScheme::RoundRobin,
            source_fragments: Vec::new(),
        };

        let producers = collect_df_producers(&fragment, 2);

        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].df_id, df_id);
        assert_eq!(producers[0].stage_id, StageId(42));
        assert_eq!(producers[0].task_count, 2);
    }

    #[test]
    fn collect_df_producers_uses_join_probe_partition_count() {
        let df_id = DynamicFilterId(456);
        let root = LogicalPlan::Join {
            left: Box::new(scan()),
            right: Box::new(scan()),
            join_type: arneb_sql_parser::ast::JoinType::Inner,
            condition: JoinCondition::None,
            dynamic_filter_ids: vec![arneb_planner::DynamicFilterProducer {
                id: df_id,
                build_index: 0,
                probe_index: 0,
                column_name: "c0".to_string(),
            }],
        };
        let fragment = PlanFragment {
            id: StageId(43),
            fragment_type: FragmentType::Source,
            root,
            output_partitioning: PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 8,
            },
            source_fragments: Vec::new(),
        };

        let producers = collect_df_producers(&fragment, 2);

        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].df_id, df_id);
        assert_eq!(producers[0].stage_id, StageId(43));
        assert_eq!(producers[0].task_count, 8);
    }

    #[test]
    fn collect_df_producers_registers_probe_side_filters_per_join_partition() {
        let df_id = DynamicFilterId(789);
        let producer = arneb_planner::DynamicFilterProducer {
            id: df_id,
            build_index: 0,
            probe_index: 0,
            column_name: "c0".to_string(),
        };
        let key = PlanExpr::Column {
            index: 0,
            name: "c0".to_string(),
            span: None,
        };
        let roots = [
            LogicalPlan::SemiJoin {
                left: Box::new(scan()),
                right: Box::new(scan()),
                left_key: key.clone(),
                right_key: key.clone(),
                residual: None,
                dynamic_filter_ids: Vec::new(),
                probe_side_filter_ids: vec![producer.clone()],
            },
            LogicalPlan::AntiJoin {
                left: Box::new(scan()),
                right: Box::new(scan()),
                left_key: key.clone(),
                right_key: key,
                residual: None,
                probe_side_filter_ids: vec![producer],
            },
        ];
        for root in roots {
            let fragment = PlanFragment {
                id: StageId(44),
                fragment_type: FragmentType::HashPartitioned,
                root,
                output_partitioning: PartitioningScheme::Hash {
                    columns: Vec::new(),
                    partition_count: 4,
                },
                source_fragments: Vec::new(),
            };

            let producers = collect_df_producers(&fragment, 2);

            // Every join task publishes once after draining its left slice.
            assert_eq!(producers.len(), 1);
            assert_eq!(producers[0].df_id, df_id);
            assert_eq!(producers[0].stage_id, StageId(44));
            assert_eq!(producers[0].task_count, 4);
        }
    }

    #[test]
    fn single_consumer_gathers_every_hash_output_bucket() {
        let producer = PlanFragment {
            id: StageId(7),
            fragment_type: FragmentType::Fixed,
            root: partial_aggregate(),
            output_partitioning: PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 4,
            },
            source_fragments: Vec::new(),
        };
        let mut stage_results = std::collections::HashMap::new();
        stage_results.insert(
            7,
            vec![("http://worker:9090".to_string(), "q.7.0".to_string(), 0)],
        );

        let expanded = expand_stage_results_for_single_consumer(
            std::slice::from_ref(&producer),
            &stage_results,
        );
        let partition_ids: Vec<u32> = expanded[&7].iter().map(|(_, _, p)| *p).collect();

        assert_eq!(partition_ids, vec![0, 1, 2, 3]);

        let bucket_groups = [
            vec![1_i64, 5],
            vec![2_i64, 6],
            vec![3_i64, 7],
            vec![4_i64, 8],
        ];
        let gathered: std::collections::BTreeSet<i64> = partition_ids
            .iter()
            .flat_map(|p| bucket_groups[*p as usize].iter().copied())
            .collect();
        let single_gather: std::collections::BTreeSet<i64> =
            bucket_groups.iter().flatten().copied().collect();

        assert_eq!(gathered, single_gather);
    }

    #[test]
    fn hash_partitioned_consumer_reads_only_its_bucket() {
        let producer = PlanFragment {
            id: StageId(8),
            fragment_type: FragmentType::Source,
            root: partial_aggregate(),
            output_partitioning: PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 4,
            },
            source_fragments: Vec::new(),
        };

        assert_eq!(
            exchange_partition_ids_for_consumer(&producer, 2, 4),
            vec![2]
        );
    }

    // L2 fixtures: mirror q18's `stage=3 type=HASH_PARTITIONED
    // output=Broadcast sources=[2]` (Tier-2 FRAGTRACE, 2026-10-10) where the
    // source stage=2 is a PartialAggregate with `Hash { columns: [0],
    // partition_count: 2 }`.
    fn hash_source(id: u32, columns: Vec<usize>, partition_count: usize) -> PlanFragment {
        PlanFragment {
            id: StageId(id),
            fragment_type: FragmentType::Source,
            root: partial_aggregate(),
            output_partitioning: PartitioningScheme::Hash {
                columns,
                partition_count,
            },
            source_fragments: Vec::new(),
        }
    }

    fn broadcast_final_aggregate_over(sources: Vec<PlanFragment>) -> PlanFragment {
        let input = match sources.first() {
            Some(src) => LogicalPlan::ExchangeNode {
                stage_id: src.id,
                schema: schema(),
            },
            None => scan(),
        };
        PlanFragment {
            id: StageId(3),
            fragment_type: FragmentType::HashPartitioned,
            root: LogicalPlan::FinalAggregate {
                input: Box::new(input),
                group_by: vec![PlanExpr::Column {
                    index: 0,
                    name: "c0".to_string(),
                    span: None,
                }],
                aggr_exprs: Vec::new(),
                schema: schema(),
            },
            output_partitioning: PartitioningScheme::Broadcast,
            source_fragments: sources,
        }
    }

    #[test]
    fn hash_partitioned_broadcast_fragment_runs_one_task_per_source_bucket() {
        let source = hash_source(2, vec![0], 2);
        let fragment = broadcast_final_aggregate_over(vec![source]);

        // One task per upstream hash bucket, so each task finalises a
        // disjoint slice of the groups instead of one task gathering all.
        assert_eq!(task_count_for_fragment(&fragment), 2);
        assert_eq!(effective_task_count_for_fragment(&fragment, 2, false), 2);

        // Wiring that already holds and makes the split correct: task k pulls
        // only bucket k from every upstream task, and every downstream
        // consumer pulls the (single) broadcast partition of EVERY task.
        let source = &fragment.source_fragments[0];
        assert_eq!(exchange_partition_ids_for_consumer(source, 0, 2), vec![0]);
        assert_eq!(exchange_partition_ids_for_consumer(source, 1, 2), vec![1]);
        assert_eq!(
            exchange_partition_ids_for_consumer(&fragment, 0, 2),
            vec![0]
        );
        assert_eq!(
            exchange_partition_ids_for_consumer(&fragment, 1, 2),
            vec![0]
        );
    }

    #[test]
    fn hash_partitioned_broadcast_fragment_follows_wider_source_bucket_count() {
        let fragment = broadcast_final_aggregate_over(vec![hash_source(2, vec![0], 4)]);

        assert_eq!(task_count_for_fragment(&fragment), 4);
    }

    #[test]
    fn hash_partitioned_broadcast_fragment_stays_single_task_without_source_hash_columns() {
        // Failure path: a source with NO hash columns exposes a single
        // partition per task (`exchange_partition_ids_for_consumer` → [0]),
        // so N tasks would each re-read the whole input and the broadcast
        // union downstream would carry N copies of every row.
        let fragment = broadcast_final_aggregate_over(vec![hash_source(2, Vec::new(), 2)]);

        assert_eq!(task_count_for_fragment(&fragment), 1);
        assert_eq!(
            exchange_partition_ids_for_consumer(&fragment.source_fragments[0], 1, 2),
            vec![0]
        );
    }

    #[test]
    fn hash_partitioned_broadcast_fragment_stays_single_task_without_sources() {
        let fragment = broadcast_final_aggregate_over(Vec::new());

        assert_eq!(task_count_for_fragment(&fragment), 1);
    }

    #[test]
    fn hash_partitioned_broadcast_fragment_stays_single_task_on_mismatched_source_buckets() {
        // Failure path: two sources that disagree on bucket count have no
        // common task index → bucket mapping; fall back to one gather task.
        let fragment = broadcast_final_aggregate_over(vec![
            hash_source(2, vec![0], 2),
            hash_source(9, vec![0], 4),
        ]);

        assert_eq!(task_count_for_fragment(&fragment), 1);
    }

    #[test]
    fn source_broadcast_fragment_task_count_is_unchanged() {
        // A SOURCE broadcast fragment (q18 stage=4, customer scan) keeps its
        // existing derivation: base 1, scan striding supplies the M tasks.
        let fragment = PlanFragment {
            id: StageId(4),
            fragment_type: FragmentType::Source,
            root: scan(),
            output_partitioning: PartitioningScheme::Broadcast,
            source_fragments: Vec::new(),
        };

        assert_eq!(task_count_for_fragment(&fragment), 1);
        assert_eq!(effective_task_count_for_fragment(&fragment, 2, false), 1);
        assert_eq!(effective_task_count_for_fragment(&fragment, 2, true), 2);
    }
}
