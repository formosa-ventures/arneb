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
use arneb_scheduler::{
    DynamicFilterService, DynamicFilterServiceRegistry, NodeRegistry, QueryTracker,
};
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
    query_tracker: Arc<QueryTracker>,
    /// A1.3 (2026-05-27): per-query dynamic-filter merge services live
    /// here. `execute()` registers an empty service at query start and
    /// drops it at end; A1.5 will start populating it from the plan's
    /// `dynamic_filter_ids` annotations so producers can actually
    /// report Domains and consumers can subscribe. Shared with the
    /// FlightState `df_report_callback` registered in `main.rs`.
    df_registry: DynamicFilterServiceRegistry,
}

impl QueryCoordinator {
    pub fn new(
        node_registry: NodeRegistry,
        query_tracker: Arc<QueryTracker>,
        df_registry: DynamicFilterServiceRegistry,
    ) -> Self {
        Self {
            node_registry,
            query_tracker,
            df_registry,
        }
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
    pub async fn execute(
        &self,
        plan: LogicalPlan,
        exec_ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let query_id = QueryId::new();
        let _query = self.query_tracker.create_query(format!("{plan:?}"));

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

        let workers = self.node_registry.alive_workers();
        if workers.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "no workers available for distributed execution".to_string(),
            ));
        }

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

        // q21 SF30 fix: the root fragment is consumed by the coordinator's own
        // root plan. Its producers must_drain unless that root can early-stop
        // (a top-level LIMIT short-circuit); otherwise a mid-stream consumer
        // drop is a silent truncation and must fail loud.
        let root_must_drain = !root_fragment.root.may_stop_input_early();
        for child in &root_fragment.source_fragments {
            execute_child_on_worker(
                child,
                &workers,
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
            let Some(consumer) = consumers.iter().find(|c| c.df_id == df_id) else {
                // Producer-only DF (no probe-side annotation reachable
                // from this fragment tree). Drop the receiver — service
                // still merges reports but nobody waits on the result.
                continue;
            };
            let consumer_stage_id = consumer.stage_id;
            // Resolve the consumer-stage tasks to (worker_addr, task_id).
            // The dispatch loop pushed one entry per `consumer_k` in
            // ascending order, so `index == partition_id`.
            let Some(entries) = stage_results.get(&consumer_stage_id.0) else {
                continue;
            };
            let targets: Vec<(String, TaskId)> = entries
                .iter()
                .enumerate()
                .map(|(k, (addr, _, _))| {
                    (
                        addr.clone(),
                        TaskId {
                            stage_id: consumer_stage_id,
                            partition_id: k as u32,
                        },
                    )
                })
                .collect();
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
        let batches = collect_stream(stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("root stage collection failed: {e}"))
        })?;

        tracing::info!(
            query_id = %query_id,
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "distributed query complete"
        );

        Ok(batches)
    }
}

/// Dispatch a child fragment to a worker.
async fn execute_child_on_worker(
    fragment: &arneb_planner::PlanFragment,
    workers: &[arneb_scheduler::WorkerInfo],
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
    let (mut task_count, output_partitions) =
        match (&fragment.fragment_type, &fragment.output_partitioning) {
            (
                arneb_planner::FragmentType::Source | arneb_planner::FragmentType::Fixed,
                arneb_planner::PartitioningScheme::Hash {
                    partition_count, ..
                },
            ) => (1, *partition_count), // β producer
            (
                arneb_planner::FragmentType::HashPartitioned,
                arneb_planner::PartitioningScheme::Hash {
                    partition_count,
                    columns,
                },
            ) if !columns.is_empty() => (*partition_count, *partition_count), // M×N α producer-AND-consumer
            (
                arneb_planner::FragmentType::HashPartitioned,
                arneb_planner::PartitioningScheme::Hash {
                    partition_count, ..
                },
            ) => (*partition_count, 1), // α consumer only
            _ => (1, 1),
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
    if scan_task_count > 1 {
        task_count = scan_task_count;
    }

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
            // and runs a single pumper. Coord schedules broadcast
            // fragments as 1 task (per `task_count_for_fragment`).
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

        let flight_addr = if worker.address.starts_with("http") {
            worker.address.clone()
        } else {
            format!("http://{}", worker.address)
        };
        tracing::info!(
            stage_id = %stage_id,
            partition = consumer_k,
            worker = %worker.worker_id,
            "submitting task"
        );

        arneb_rpc::submit_task(&flight_addr, &descriptor)
            .await
            .map_err(|e| {
                ExecutionError::InvalidOperation(format!("task submission failed: {e}"))
            })?;

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
        L::AntiJoin { left, right, .. }
        | L::Intersect { left, right, .. }
        | L::Except { left, right, .. } => {
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
}
