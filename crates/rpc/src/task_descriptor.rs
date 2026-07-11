//! Task descriptor for distributed task submission.

use arneb_common::identifiers::{QueryId, StageId, TaskId};
use arneb_common::{Domain, DynamicFilterId};
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
    /// A1.3 (2026-05-27): cross-fragment dynamic filters that already
    /// resolved on the coordinator before this task was dispatched. The
    /// worker seeds its per-task `DynamicFilterCollector` with these
    /// at admission time. Late arrivals (resolution after dispatch)
    /// come via the separate `notify_dynamic_filter` Flight Action.
    /// Default empty; inert until A1.4 wires the scan-side consumer.
    #[serde(default)]
    pub pending_dynamic_filters: Vec<(DynamicFilterId, Domain)>,
    /// W3-Hash.4 (2026-05-20): column indices the worker should hash on
    /// when producing >1 output partitions. When `output_partitions > 1`
    /// and this is non-empty, the worker wraps its physical plan with
    /// `RepartitionExec(Hash(cols, output_partitions))` so each output
    /// partition holds rows that hash to that bucket. Empty when no
    /// hash redistribution is needed (single-partition producer).
    #[serde(default)]
    pub output_hash_columns: Vec<u32>,
    /// B.1 (2026-05-20): when `true`, this task's output is broadcast —
    /// every downstream consumer task fetches the FULL output set
    /// independently rather than slicing by partition. The worker
    /// allocates a `BroadcastOutputBuffer` instead of a partitioned
    /// `OutputBuffer`. Ports the Ballista `shuffle_reader.broadcast`
    /// flag pattern (Apache-2.0). Default `false` keeps prior semantics.
    #[serde(default)]
    pub broadcast: bool,
    /// Source exchange addresses: (task_id_str, worker_flight_address).
    pub source_exchanges: Vec<SourceExchange>,
    /// Multi-worker scan parallelism `M`: how many parallel tasks the
    /// coordinator scheduled for this scan SOURCE fragment. The worker
    /// threads it into `ExecutionContext::with_scan_task_count` so each
    /// scan task reads only its strided 1/M of the table's partitions
    /// (stride index = `task_id.partition_id`). Default `1` — a
    /// single-task whole-table scan, the pre-multi-worker behavior;
    /// `#[serde(default)]` keeps old descriptors (without the field)
    /// decoding to 1.
    #[serde(default = "default_scan_task_count")]
    pub scan_task_count: usize,
    /// q21 SF30 silent-truncation fix (2026-06-12): when `true`, this task's
    /// CONSUMER is a must-drain operator (join / aggregate / sort / …) that
    /// never legitimately stops reading mid-stream. If the consumer's receiver
    /// is dropped while this producer still has rows (an upstream stall/reset
    /// under SF30 load), returning `Ok` would SILENTLY truncate the partition →
    /// wrong results (q21 returned ~62/100 wrong suppliers, non-deterministic).
    /// With `must_drain` the producer fails loud (the query errors, like q18's
    /// broken pipe) instead. `false` (the default) for producers feeding a
    /// `Limit`-rooted consumer or the coordinator's gather, where an early
    /// consumer-drop is a legitimate `LIMIT` short-circuit. Computed by the
    /// coordinator via [`arneb_planner::LogicalPlan::may_stop_input_early`].
    #[serde(default)]
    pub must_drain: bool,
}

fn default_scan_task_count() -> usize {
    1
}

/// Describes a source exchange — where to fetch input data from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceExchange {
    /// The stage that produced the output (matches `ExchangeNode.stage_id.0`
    /// in the receiving worker's serialized plan; the worker uses this to
    /// build its `stage_results` map keyed by `stage_id.0`).
    pub source_stage_id: u32,
    /// The buffer key on the source worker (matches the format used by
    /// `task_manager::execute_task`: `"{query_id}.{task_id}"`).
    pub source_task_id: String,
    /// The Flight RPC address of the worker holding the data.
    pub flight_address: String,
    /// The partition index to fetch. Ignored when `broadcast = true` —
    /// the consumer pulls the full upstream output regardless of partition.
    pub partition_id: u32,
    /// B.1 (2026-05-20): mirror of [`TaskDescriptor::broadcast`] on the
    /// consumer side. When `true`, the worker's `ExchangeExec` should
    /// drain the upstream's broadcast output rather than fetching a
    /// single partition. Default `false`.
    #[serde(default)]
    pub broadcast: bool,
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
