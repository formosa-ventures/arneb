//! Worker-side TaskManager: receives, executes, and serves task output.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arneb_catalog::CatalogManager;
use arneb_common::stream::collect_stream;
use arneb_connectors::ConnectorRegistry;
use arneb_planner::LogicalPlan;
use arneb_rpc::{FlightState, OutputBuffer, TaskDescriptor};

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
    task_statuses: Arc<RwLock<HashMap<String, TaskStatus>>>,
}

impl TaskManager {
    pub fn new(
        flight_state: FlightState,
        catalog_manager: Arc<CatalogManager>,
        connector_registry: Arc<ConnectorRegistry>,
    ) -> Self {
        Self {
            flight_state,
            catalog_manager,
            connector_registry,
            task_statuses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Handle an incoming task submission. Spawns execution in a background task.
    pub fn handle_task(&self, descriptor: TaskDescriptor) {
        let task_id_str = descriptor.task_id.to_string();
        let manager = self.clone();

        // Mark as running
        {
            let mut statuses = manager.task_statuses.write().unwrap();
            statuses.insert(task_id_str.clone(), TaskStatus::Running);
        }

        tokio::spawn(async move {
            match manager.execute_task(descriptor).await {
                Ok(()) => {
                    let mut statuses = manager.task_statuses.write().unwrap();
                    statuses.insert(task_id_str, TaskStatus::Finished);
                }
                Err(e) => {
                    tracing::error!(task_id = %task_id_str, error = %e, "task failed");
                    let mut statuses = manager.task_statuses.write().unwrap();
                    statuses.insert(task_id_str, TaskStatus::Failed(e));
                }
            }
        });
    }

    /// Get the status of a task.
    pub fn task_status(&self, task_id: &str) -> Option<TaskStatus> {
        self.task_statuses.read().unwrap().get(task_id).cloned()
    }

    /// Execute a task: deserialize plan, run it, write output to buffer.
    async fn execute_task(&self, descriptor: TaskDescriptor) -> Result<(), String> {
        let task_id_str = descriptor.task_id.to_string();
        tracing::info!(task_id = %task_id_str, "executing task");

        // Deserialize the logical plan
        let plan: LogicalPlan = serde_json::from_str(&descriptor.plan_json)
            .map_err(|e| format!("failed to deserialize plan: {e}"))?;

        // Create execution context and register real data sources via connectors
        let mut exec_ctx = arneb_execution::ExecutionContext::new();
        register_task_data_sources(
            &plan,
            &self.catalog_manager,
            &self.connector_registry,
            &mut exec_ctx,
        )?;

        let physical_plan = exec_ctx
            .create_physical_plan(&plan)
            .map_err(|e| format!("physical plan creation failed: {e}"))?;

        // Execute
        let stream = physical_plan
            .execute()
            .await
            .map_err(|e| format!("execution failed: {e}"))?;
        let batches = collect_stream(stream)
            .await
            .map_err(|e| format!("stream collection failed: {e}"))?;

        // Write output to buffer and register with flight state
        let num_partitions = std::cmp::max(1, descriptor.output_partitions);
        let output_schema = if batches.is_empty() {
            Arc::new(arrow::datatypes::Schema::empty())
        } else {
            batches[0].schema()
        };
        let buffer = OutputBuffer::new(num_partitions, 1024, output_schema);

        for batch in &batches {
            buffer
                .write_batch(0, batch.clone())
                .await
                .map_err(|e| format!("buffer write failed: {e}"))?;
        }

        // Register buffer with flight state so coordinator can fetch it
        self.flight_state
            .register_buffer(task_id_str.clone(), buffer);

        tracing::info!(
            task_id = %task_id_str,
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "task completed"
        );

        Ok(())
    }
}

/// Register data sources for a task's plan using actual connectors.
fn register_task_data_sources(
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
                if let Ok(ds) = factory.create_data_source(table, schema, properties) {
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
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Explain { input, .. } => {
            register_task_data_sources(input, catalog_manager, connector_registry, ctx)
        }
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right, .. }
        | LogicalPlan::Except { left, right, .. } => {
            register_task_data_sources(left, catalog_manager, connector_registry, ctx)?;
            register_task_data_sources(right, catalog_manager, connector_registry, ctx)
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                register_task_data_sources(input, catalog_manager, connector_registry, ctx)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
