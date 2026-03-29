## Context

trino-alt Phase 2 has introduced async streaming execution (Change 1), predicate/projection pushdown (Change 2), query optimization (Change 3), S3 connector (Change 4), plan fragmentation (Change 5), a query state machine (Change 6), and a Flight RPC layer (Change 7). The plan fragmenter splits a LogicalPlan into stages connected by exchange operators. The query state machine tracks query lifecycle (queued → planning → running → finishing → finished/failed). The Flight RPC layer provides inter-node communication for task submission and data exchange.

Currently, the server binary runs in a single process: it accepts pgwire connections, plans queries, executes them locally, and returns results. There is no concept of coordinator vs worker roles, no worker tracking, and no distributed task scheduling. The server crate has a `main.rs` with a linear startup sequence: parse CLI → load config → init tracing → wire catalogs/connectors → start pgwire server.

The `scheduler` crate (from Change 5) provides `PlanFragmenter` and fragment types. The `rpc` crate (from Change 7) provides Flight RPC client/server for task submission and data exchange.

## Goals / Non-Goals

**Goals:**

- Support three server roles: standalone (default, current behavior), coordinator, and worker
- Coordinator: accept pgwire connections, plan/fragment queries, schedule tasks to workers, collect and return results
- Worker: accept task assignments from coordinator, execute fragments, serve output buffers via Flight RPC, heartbeat to coordinator
- Standalone: run coordinator + worker in same process with in-process communication (no network overhead)
- NodeRegistry: track worker nodes with health monitoring via heartbeats
- NodeScheduler: assign stages to workers using round-robin strategy
- TaskManager: manage local task execution on worker nodes
- DistributedQueryRunner: orchestrate full distributed query lifecycle on coordinator

**Non-Goals:**

- No automatic worker discovery (workers must know coordinator address)
- No worker-to-worker direct communication (all data flows through coordinator for MVP)
- No dynamic rebalancing of running tasks
- No fault tolerance or task retry on worker failure
- No authentication or encryption for inter-node communication
- No resource-aware scheduling (just round-robin on active workers)
- No persistent cluster state (all state is in-memory, lost on restart)

## Decisions

### D1: Three server roles with `--role` flag

**Choice**: Add a `--role` CLI argument accepting `standalone` (default), `coordinator`, or `worker`. The startup sequence branches after config loading and tracing initialization based on the role.

```
main():
  1. Parse CLI (including --role)
  2. Load config
  3. Init tracing
  4. Match role:
     standalone → start_standalone()
     coordinator → start_coordinator()
     worker → start_worker()
```

**Rationale**: A single binary simplifies deployment — the same artifact runs in any role. The `--role` flag is explicit and easy to understand. Default `standalone` preserves backwards compatibility.

**Alternative**: Separate binaries (`trino-coordinator`, `trino-worker`). Rejected — increases build complexity and duplicates shared startup logic. A single binary with role selection is the standard approach (used by Trino, Spark, etc.).

### D2: Coordinator architecture

**Choice**: The coordinator runs:
- pgwire listener (existing `ProtocolServer`) for client SQL connections
- Flight RPC server for worker heartbeats and result collection
- `NodeRegistry` for tracking worker status
- `DistributedQueryRunner` that replaces direct local execution for distributed queries

When a SQL query arrives via pgwire:
1. Parse → Plan → Optimize → Fragment (using existing pipeline + PlanFragmenter)
2. Create QueryStateMachine (queued → planning → running)
3. DistributedQueryRunner assigns stages to workers via NodeScheduler
4. Submit tasks to workers via Flight RPC client
5. Monitor task completion
6. Collect final stage output via ExchangeClient (Flight RPC)
7. Stream results back to pgwire client
8. Transition state machine to finished

**Rationale**: This follows the standard coordinator pattern from Trino/Presto. The coordinator never executes data-processing tasks itself (except in standalone mode). Separating planning from execution enables independent scaling.

### D3: Worker architecture

**Choice**: The worker runs:
- Flight RPC server for receiving task assignments and serving output data
- `TaskManager` that manages local task execution
- Heartbeat loop sending periodic status to coordinator

When a task assignment arrives via Flight RPC:
1. TaskManager creates an ExecutionContext for the fragment
2. Executes the plan fragment using the local execution engine
3. Writes output to an OutputBuffer
4. Marks task as complete
5. Output is served to coordinator via Flight RPC when requested

**Rationale**: Workers are simple execution engines. They receive fragments, run them, and serve results. No planning or coordination logic. This keeps workers lightweight and horizontally scalable.

### D4: NodeRegistry with heartbeat-based health tracking

**Choice**: `NodeRegistry` is a thread-safe (`Arc<RwLock>`) data structure on the coordinator:

```rust
struct NodeRegistry {
    workers: HashMap<WorkerId, WorkerInfo>,
}

struct WorkerInfo {
    id: WorkerId,
    address: String,          // host:port for Flight RPC
    status: WorkerStatus,     // Active, Draining, Dead
    capacity: usize,          // max concurrent splits
    active_tasks: usize,      // current running tasks
    last_heartbeat: Instant,  // last heartbeat timestamp
}
```

Workers send heartbeats every 10 seconds. If no heartbeat is received for 30 seconds, the worker is marked `Dead`. A background task on the coordinator checks for stale workers every 15 seconds.

**Rationale**: Heartbeat-based health monitoring is the standard approach. The 10/30-second intervals balance responsiveness with network overhead. `RwLock` allows concurrent reads (for scheduling) with exclusive writes (for heartbeat updates).

**Alternative**: Push-based registration (workers register once, no heartbeats). Rejected — cannot detect worker failures without heartbeats.

### D5: NodeScheduler with round-robin assignment

**Choice**: `NodeScheduler` assigns stages to workers using round-robin across active (non-dead, non-draining) workers:

```rust
struct NodeScheduler {
    registry: Arc<NodeRegistry>,
    next_index: AtomicUsize,
}

fn schedule_stage(&self, stage: &PlanFragment) -> Result<WorkerId> {
    let active = self.registry.active_workers();
    if active.is_empty() { return Err(NoActiveWorkers) }
    let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % active.len();
    Ok(active[idx].id)
}
```

**Rationale**: Round-robin is the simplest scheduling strategy that distributes work evenly. It is sufficient for initial distributed execution. Resource-aware scheduling (considering CPU, memory, locality) is a future optimization.

**Alternative**: Random assignment. Rejected — round-robin provides more predictable distribution. Capacity-aware scheduling: deferred — requires per-worker resource tracking and more complex assignment logic.

### D6: TaskManager for local execution

**Choice**: `TaskManager` runs on each worker and manages task lifecycle:

```rust
struct TaskManager {
    tasks: HashMap<TaskId, TaskHandle>,
    catalog_manager: Arc<CatalogManager>,
    connector_registry: Arc<ConnectorRegistry>,
}

struct TaskHandle {
    id: TaskId,
    query_id: QueryId,
    stage_id: StageId,
    status: TaskStatus,        // Running, Completed, Failed
    output_buffer: Arc<OutputBuffer>,
}
```

When a task arrives: create ExecutionContext → build physical plan from fragment → execute → write batches to OutputBuffer → mark complete. The OutputBuffer holds `Vec<RecordBatch>` and is served via Flight RPC `do_get`.

**Rationale**: TaskManager provides a clean abstraction for managing concurrent task execution on a worker. The OutputBuffer decouples execution from data transfer — the worker can complete execution before the coordinator fetches results.

### D7: DistributedQueryRunner lifecycle

**Choice**: `DistributedQueryRunner` on the coordinator manages the full distributed query lifecycle:

1. Receive SQL from pgwire handler
2. Parse → Plan → Optimize (existing pipeline)
3. Fragment plan via PlanFragmenter → Vec<PlanFragment>
4. Create QueryStateMachine (transition: queued → planning → running)
5. For each stage (bottom-up, leaves first):
   a. NodeScheduler assigns stage to a worker
   b. Submit task to worker via Flight RPC
6. Monitor task completions
7. When root stage completes: fetch output via ExchangeClient (Flight RPC do_get)
8. Stream results back to pgwire client
9. Transition state machine to finished

**Rationale**: Bottom-up scheduling ensures leaf stages (table scans) execute before stages that depend on their output. The coordinator drives the lifecycle — workers are passive executors.

### D8: Standalone mode — in-process coordinator + worker

**Choice**: Standalone mode creates both coordinator and worker components in the same process. Instead of using Flight RPC for communication, it uses in-process channels (tokio mpsc) to submit tasks and retrieve results. The NodeScheduler always returns the local worker. No heartbeat loop is needed.

**Rationale**: Standalone mode must be backwards compatible with current single-node behavior. Using in-process channels avoids network overhead and serialization costs. The same DistributedQueryRunner code path is used, ensuring it is exercised even without a real cluster.

**Alternative**: Keep the current direct execution path for standalone mode. Rejected — this would mean the distributed code path is never tested in standalone mode, increasing the risk of bugs when deploying to a real cluster.

### D9: Cluster configuration schema

**Choice**: Add a `[cluster]` section to `ServerConfig`:

```toml
[cluster]
role = "standalone"            # standalone | coordinator | worker
coordinator_address = ""       # host:port of coordinator (required for workers)
discovery_port = 9090          # port for Flight RPC (coordinator and worker)
worker_id = ""                 # unique worker identifier (auto-generated if empty)
```

The `--role` CLI flag overrides `cluster.role`. The `TRINO_CLUSTER_ROLE`, `TRINO_COORDINATOR_ADDRESS`, `TRINO_DISCOVERY_PORT`, and `TRINO_WORKER_ID` env vars provide overrides.

**Rationale**: Embedding cluster config in the existing `ServerConfig` keeps a single config file. The `[cluster]` section groups related settings. Auto-generated worker_id (hostname + random suffix) simplifies single-worker setups.

**Alternative**: Separate cluster config file. Rejected — adds configuration management complexity without benefit.

## Risks / Trade-offs

**[Single-point-of-failure coordinator]** → The coordinator is a single point of failure. If it goes down, all queries fail and workers lose their registration. **Mitigation**: Acceptable for Phase 2 MVP. High-availability coordinator (leader election, state replication) is a future enhancement.

**[In-memory cluster state]** → NodeRegistry, task state, and query state are all in-memory. Coordinator restart loses all state. **Mitigation**: Workers re-register on heartbeat failure. Running queries fail on coordinator restart. Persistent state storage is deferred.

**[No task retry]** → If a worker dies mid-task, the task and its query fail. **Mitigation**: The query state machine transitions to failed. The client receives an error. Task retry with speculative execution is a future enhancement.

**[Round-robin ignores data locality]** → Scheduling does not consider where data resides. **Mitigation**: For file-based connectors (CSV/Parquet on local disk), all workers must have access to the same files (shared filesystem or object store). The S3 connector (Change 4) provides shared access by design.

**[Standalone mode exercises distributed path]** → Using the distributed code path for standalone mode adds complexity compared to direct execution. **Mitigation**: This ensures the distributed path is always tested. The in-process channel avoids real network overhead. If standalone performance regresses, a fast path can be added later.
