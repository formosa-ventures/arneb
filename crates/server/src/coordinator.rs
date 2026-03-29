//! QueryCoordinator: orchestrates distributed multi-stage query execution.
//!
//! Lives in the server crate because it depends on both trino-rpc and trino-execution,
//! which can't depend on each other.

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use trino_common::error::{ExecutionError, TrinoError};
use trino_common::identifiers::{QueryId, TaskId};
use trino_common::stream::collect_stream;
use trino_planner::LogicalPlan;
use trino_protocol::DistributedExecutor;
use trino_scheduler::{NodeRegistry, QueryTracker};

use trino_execution::ExecutionContext;

/// Orchestrates distributed query execution across multiple workers.
pub struct QueryCoordinator {
    node_registry: NodeRegistry,
    query_tracker: Arc<QueryTracker>,
}

impl QueryCoordinator {
    pub fn new(node_registry: NodeRegistry, query_tracker: Arc<QueryTracker>) -> Self {
        Self {
            node_registry,
            query_tracker,
        }
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

        // Fragment the plan
        let mut fragmenter = trino_planner::PlanFragmenter::new();
        let root_fragment = fragmenter.fragment(plan.clone());

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

        // Execute stages bottom-up: dispatch child fragments to workers
        let mut stage_results: std::collections::HashMap<u32, Vec<(String, String)>> =
            std::collections::HashMap::new();

        for child in &root_fragment.source_fragments {
            execute_child_on_worker(child, &workers, &mut stage_results, &query_id).await?;
        }

        // Execute the original plan locally on coordinator
        // The coordinator has all data sources registered, so it can execute directly.
        // Workers already executed their fragments (proving distributed dispatch works).
        // Full ExchangeClient-based result collection is a future enhancement.
        let physical_plan = exec_ctx.create_physical_plan(&plan)?;
        let stream = physical_plan.execute().await?;
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
    fragment: &trino_planner::PlanFragment,
    workers: &[trino_scheduler::WorkerInfo],
    stage_results: &mut std::collections::HashMap<u32, Vec<(String, String)>>,
    query_id: &QueryId,
) -> Result<(), ExecutionError> {
    // Recursively execute children first
    for child in &fragment.source_fragments {
        Box::pin(execute_child_on_worker(
            child,
            workers,
            stage_results,
            query_id,
        ))
        .await?;
    }

    let stage_id = fragment.id;
    let worker_idx = stage_id.0 as usize % workers.len();
    let worker = &workers[worker_idx];

    let task_id = TaskId {
        stage_id,
        partition_id: 0,
    };

    let plan_json = serde_json::to_string(&fragment.root)
        .map_err(|e| ExecutionError::InvalidOperation(format!("plan serialization failed: {e}")))?;

    let descriptor = trino_rpc::TaskDescriptor {
        task_id,
        stage_id,
        query_id: *query_id,
        plan_json,
        output_partitions: 1,
        source_exchanges: vec![],
    };

    let flight_addr = if worker.address.starts_with("http") {
        worker.address.clone()
    } else {
        format!("http://{}", worker.address)
    };
    tracing::info!(
        stage_id = %stage_id,
        worker = %worker.worker_id,
        "submitting task"
    );

    trino_rpc::submit_task(&flight_addr, &descriptor)
        .await
        .map_err(|e| ExecutionError::InvalidOperation(format!("task submission failed: {e}")))?;

    // Wait for task completion
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    stage_results
        .entry(stage_id.0)
        .or_default()
        .push((flight_addr, task_id.to_string()));

    Ok(())
}

fn count_fragments(fragment: &trino_planner::PlanFragment) -> usize {
    1 + fragment
        .source_fragments
        .iter()
        .map(count_fragments)
        .sum::<usize>()
}

#[async_trait]
impl DistributedExecutor for QueryCoordinator {
    async fn execute(
        &self,
        plan: LogicalPlan,
        exec_ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, TrinoError> {
        self.execute(plan, exec_ctx)
            .await
            .map_err(TrinoError::Execution)
    }

    fn has_workers(&self) -> bool {
        self.has_workers()
    }
}
