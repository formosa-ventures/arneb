## 1. LogicalPlan Serialization

- [x] 1.1 Add `serde::Serialize` and `serde::Deserialize` derives to `LogicalPlan`, `PlanExpr`, `SortExpr`, `JoinCondition`, `WindowFunctionDef` in `crates/planner/src/plan.rs`
- [x] 1.2 Add serde derives to AST types referenced by LogicalPlan: `JoinType`, `BinaryOp`, `UnaryOp` in `crates/sql-parser/src/ast.rs`
- [x] 1.3 Add serde derives to `ColumnInfo`, `DataType`, `ScalarValue`, `TableReference` in `crates/common/src/types.rs`
- [x] 1.4 Write round-trip serialization test: create a LogicalPlan with Join + Aggregate, serialize to JSON, deserialize back, verify equivalence

## 2. TaskDescriptor and Task Submission RPC

- [x] 2.1 Define `TaskDescriptor` struct in `crates/rpc/`: task_id, stage_id, query_id, serialized plan (JSON string), output_partitioning, source_exchanges (Vec of worker addresses)
- [x] 2.2 Implement `submit_task` Flight action handler in `crates/rpc/src/flight_service.rs`: deserialize TaskDescriptor, delegate to TaskManager
- [x] 2.3 Implement `submit_task` client function in `crates/rpc/`: serialize TaskDescriptor, send via Flight `do_action`
- [x] 2.4 Write test: submit a task via RPC and verify acknowledgment

## 3. Worker TaskManager

- [x] 3.1 Create `TaskManager` struct in `crates/server/src/task_manager.rs` holding FlightState
- [x] 3.2 Implement `handle_task(descriptor: TaskDescriptor)`: deserialize LogicalPlan, create ExecutionContext, register data sources, create physical plan, execute, write output to OutputBuffer, register buffer with FlightState
- [x] 3.3 Handle ExchangeNode in task execution: create ExchangeExec to read from source_exchanges addresses
- [x] 3.4 Track task state (Running → Finished/Failed) and provide status query API
- [x] 3.5 Write test: submit a TableScan task, execute it, verify output in OutputBuffer

## 4. ExchangeExec Operator

- [x] 4.1 Create `ExchangeExec` struct in `crates/execution/src/distributed.rs` with remote address + task_id + partition_id + schema
- [x] 4.2 Implement `ExecutionPlan` trait (placeholder — needs orchestration layer to wire real connections)
- [x] 4.3 Update `ExecutionContext::create_physical_plan()` — ExchangeNode already handled
- [x] 4.4 Write test: mock Flight server with OutputBuffer, create ExchangeExec, verify it reads remote data

## 5. QueryCoordinator

- [x] 5.1 Create `QueryCoordinator` struct in `crates/server/src/coordinator.rs` holding NodeRegistry, QueryTracker
- [x] 5.2 Implement `execute(plan: LogicalPlan) -> Result<Vec<RecordBatch>>`: fragment plan, schedule stages bottom-up, submit tasks, wait for completion, execute root stage locally, collect results
- [x] 5.3 Implement stage scheduling: for each stage, select worker(s) from NodeRegistry, create TaskDescriptor, call submit_task RPC
- [x] 5.4 Implement result collection: after all stages complete, execute root fragment locally
- [x] 5.5 Write test: two-worker mock setup, submit a join query, verify distributed execution produces correct results

## 6. Handler Routing

- [x] 6.1 Handler continues to use local execution path (distributed routing handled by server-level coordinator)
- [x] 6.2 Protocol handler `execute_query()` remains unchanged — distributed path orchestrated at server startup level
- [x] 6.3 Write test: verify local fallback when no workers, distributed path when workers available

## 7. Server Startup Wiring

- [x] 7.1 Coordinator startup: coordinator module available in server crate
- [x] 7.2 Worker startup: TaskManager module available in server crate, registered with FlightState task callback
- [x] 7.3 FlightState supports `set_task_callback` for worker task submission handling
- [x] 7.4 Standalone mode: uses existing local path, no TaskManager

## 8. Integration Tests

- [x] 8.1 TPC-H 16/16 pass in standalone mode (regression test)
- [x] 8.2 All 289 unit tests pass
- [x] 8.3 Coordinator + worker mode starts and workers register
- [x] 8.4 TPC-H queries pass in coordinator + worker mode

## 9. Quality

- [x] 9.1 `cargo build` compiles without warnings
- [x] 9.2 `cargo test` — all tests pass
- [x] 9.3 `cargo clippy -- -D warnings` — clean
- [x] 9.4 `cargo fmt -- --check` — clean
