//! Task descriptor for distributed task submission.

use arneb_common::identifiers::{QueryId, StageId, TaskId};
use serde::{Deserialize, Serialize};

/// Describes a task to be executed on a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDescriptor {
    /// Unique task identifier.
    pub task_id: TaskId,
    /// Stage this task belongs to.
    pub stage_id: StageId,
    /// Parent query identifier.
    pub query_id: QueryId,
    /// Serialized LogicalPlan fragment (JSON).
    pub plan_json: String,
    /// Output partitioning scheme.
    pub output_partitions: usize,
    /// Source exchange addresses: (task_id_str, worker_flight_address).
    pub source_exchanges: Vec<SourceExchange>,
}

/// Describes a source exchange — where to fetch input data from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceExchange {
    /// The task ID that produced the output.
    pub source_task_id: String,
    /// The Flight RPC address of the worker holding the data.
    pub flight_address: String,
    /// The partition index to fetch.
    pub partition_id: u32,
}

impl TaskDescriptor {
    /// Encode to JSON bytes for Flight action payload.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("TaskDescriptor serialization should not fail")
    }

    /// Decode from JSON bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("TaskDescriptor decode error: {e}"))
    }
}
