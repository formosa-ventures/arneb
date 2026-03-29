## Context

All distributed building blocks are implemented but disconnected. The protocol handler calls `execute_query()` which runs everything locally. PlanFragmenter can split plans into fragments with ExchangeNode boundaries. Flight RPC can transport RecordBatches between nodes. NodeRegistry tracks alive workers. OutputBuffer manages partitioned data. ExchangeClient can fetch remote partitions. None of these are wired together.

The coordinator needs an orchestration layer that ties these components into a functioning distributed query pipeline.

## Goals / Non-Goals

**Goals:**

- Coordinator dispatches plan fragments to workers and collects results
- Workers execute fragments independently and serve output via Flight RPC
- Multi-stage queries (JOIN, aggregate) work across multiple nodes
- Single-table queries with no workers fall back to local execution (backward compatible)
- Configurable `include-coordinator` to control whether coordinator also runs tasks
- State machine tracks query/stage/task progress

**Non-Goals:**

- Dynamic re-scheduling on worker failure (retry with a different worker)
- Speculative execution (running duplicate tasks for stragglers)
- Pipeline execution (overlapping stage execution) — stages execute sequentially
- Data locality awareness (all workers are equal)
- Persistent task state (in-memory only)

## Decisions

### D1: Query execution routing

**Choice**: In `execute_query()`, after planning and optimization, check if the NodeRegistry has alive workers. If yes and the plan has more than one fragment (from PlanFragmenter), route to `QueryCoordinator::execute()`. Otherwise, use the existing local path.

**Rationale**: Minimal change to the existing handler. Standalone mode and single-fragment queries are unaffected. The distributed path is opt-in based on cluster state.

### D2: Sequential stage execution (bottom-up)

**Choice**: Execute stages bottom-up — leaf stages (TableScan) first, then intermediate stages (Join/Aggregate), then root stage. Wait for all tasks in a stage to complete before starting the next stage.

**Rationale**: Simplest correct approach. Avoids deadlocks from circular dependencies. Pipeline execution (overlapping stages) is a future optimization.

```
Execution order for: SELECT * FROM a JOIN b ON a.id = b.id

Stage 0: Scan(a) → tasks on workers → OutputBuffer
Stage 1: Scan(b) → tasks on workers → OutputBuffer
  (wait for 0 & 1 to finish)
Stage 2: Join(Exchange(0), Exchange(1)) → on coordinator
  (collect results via ExchangeClient)
```

### D3: Task submission via Flight RPC action

**Choice**: Add a `submit_task` Flight action. Payload: JSON-serialized `TaskDescriptor { task_id, stage_id, query_id, fragment (serialized LogicalPlan), output_partitioning, sources (list of exchange addresses) }`. Worker responds with acknowledgment.

**Rationale**: Reuses existing Flight RPC infrastructure. No new transport protocol. JSON serialization for LogicalPlan is sufficient for MVP (binary serialization can be added later for performance).

**Alternative**: gRPC with custom protobuf. Rejected — adds a new dependency and protocol. Flight actions are extensible and already set up.

### D4: ExchangeExec wraps ExchangeClient

**Choice**: Create `ExchangeExec` physical operator that reads from a remote worker's OutputBuffer via `ExchangeClient::fetch_partition()`. The operator is created when `ExecutionContext` encounters `LogicalPlan::ExchangeNode` and a remote source address is provided.

**Rationale**: Fits naturally into the physical plan tree. Upstream operators consume `ExchangeExec` output as a normal `SendableRecordBatchStream`, unaware that data comes from a remote node.

### D5: include-coordinator config

**Choice**: Add `cluster.include_coordinator` (default: `true`). When true, the coordinator registers itself as a worker in the NodeRegistry and can receive task submissions. When false, the coordinator only coordinates — all execution happens on workers.

**Rationale**: Matches Trino's `node-scheduler.include-coordinator`. Default true for development (single machine, coordinator does work too). Set false in production (coordinator reserved for planning and coordination).

### D6: LogicalPlan serialization

**Choice**: Derive `serde::Serialize` and `serde::Deserialize` on `LogicalPlan`, `PlanExpr`, and related types. Serialize as JSON for task submission.

**Rationale**: Simple and debuggable. JSON size is larger than binary formats but acceptable for plan transmission (plans are small, data is large). The actual data flows through Flight `do_get`, not the task submission.

## Risks / Trade-offs

**[Sequential stage execution]** → Slower than pipelined execution for multi-stage queries. **Mitigation**: Correct and simple. Pipeline execution can be added as a future optimization without changing the task model.

**[JSON plan serialization]** → Larger payload than binary. **Mitigation**: Plans are typically <10KB even for complex queries. The overhead is negligible compared to data transfer.

**[No fault tolerance]** → Worker failure during execution fails the entire query. **Mitigation**: Acceptable for Phase 2. Retry and fault tolerance are Phase 3 concerns.

**[All data materialized between stages]** → OutputBuffer holds full stage output in memory. **Mitigation**: For SF=0.01-1 (10MB-1GB), this is fine. Disk spill would be needed for larger datasets.
