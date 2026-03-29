## Why

The coordinator currently executes all queries locally — parsing, planning, and executing everything in a single process, identical to standalone mode. All distributed infrastructure exists (PlanFragmenter, Flight RPC, NodeRegistry, OutputBuffer, ExchangeClient) but is never invoked. Workers register heartbeats but receive no tasks. To achieve actual distributed query execution across multiple nodes, the coordinator must fragment queries, dispatch tasks to workers via RPC, and collect results.

## What Changes

- Add a `QueryCoordinator` that orchestrates multi-stage distributed query execution: fragment plan → schedule stages → submit tasks to workers → monitor progress → collect results
- Add a `submit_task` Flight RPC action for the coordinator to send plan fragments to workers
- Add a `TaskManager` on the worker side that receives task submissions, executes fragments locally, and writes output to OutputBuffer
- Add `ExchangeExec` operator that reads remote task output via ExchangeClient (Arrow Flight `do_get`)
- Wire the distributed path into `execute_query`: when workers are available and the query has multiple fragments, use QueryCoordinator instead of local execution
- Add `include-coordinator` config option (default: true for dev, false for production) controlling whether the coordinator also executes tasks as a worker
- Preserve standalone mode behavior — single-fragment queries and standalone role continue to execute locally

## Capabilities

### New Capabilities

- `query-coordinator`: Orchestrates multi-stage distributed query execution on the coordinator side. Fragments logical plan, selects workers via NodeScheduler, submits tasks, monitors state machine transitions, collects final results via ExchangeClient.
- `task-submission-rpc`: Flight RPC action (`submit_task`) for coordinator → worker task dispatch. Serializes PlanFragment + task metadata. Worker acknowledges and begins execution.
- `worker-task-manager`: Worker-side component that receives task submissions, creates ExecutionContext, executes plan fragments, writes output to OutputBuffer, and reports completion.
- `exchange-exec-operator`: Physical operator that wraps ExchangeClient to read remote task output. Implements ExecutionPlan trait, connects to a specific worker's Flight server to fetch partition data.

### Modified Capabilities

- `pg-connection`: The protocol handler's `execute_query` checks for available workers and routes multi-fragment queries through the distributed path.
- `server-startup`: Coordinator startup wires QueryCoordinator with NodeRegistry, QueryTracker, and FlightState. Worker startup initializes TaskManager and registers it with the Flight server.

## Impact

- **Crates**: `protocol` (handler routing), `execution` (ExchangeExec, QueryCoordinator), `scheduler` (task scheduling), `rpc` (submit_task action, TaskManager), `server` (startup wiring)
- **Dependencies**: No new external crates
- **Unlocks**: True multi-node distributed query execution — the primary goal of the distributed architecture
