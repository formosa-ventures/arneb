## Why

arneb has a complete single-node query pipeline with async streaming execution, plan fragmentation, a query state machine, and a Flight RPC layer. However, the server binary runs everything in a single process with no concept of distributed roles. To scale beyond a single machine, the server must support distinct coordinator and worker roles — the coordinator accepts client connections, plans and fragments queries, schedules tasks across workers, and collects results; workers accept task assignments, execute fragments locally, and serve output buffers. This change splits the server into coordinator and worker roles with a backwards-compatible standalone mode that preserves current single-node behavior.

## What Changes

- Server binary gains a `--role` CLI flag accepting `standalone` (default), `coordinator`, or `worker`
- Coordinator mode: starts pgwire listener + Flight SQL listener + NodeRegistry + DistributedQueryRunner
- Worker mode: starts Flight SQL listener + TaskManager + heartbeat loop to coordinator
- Standalone mode: runs coordinator + worker in the same process (backwards compatible with Phase 1)
- New `NodeRegistry` tracks known workers (id, address, status, capacity) via heartbeat protocol
- New `NodeScheduler` assigns stages to workers using round-robin strategy based on availability
- New `TaskManager` on worker side receives task assignments via gRPC, creates ExecutionContext, runs fragments, writes to OutputBuffer
- New `DistributedQueryRunner` on coordinator orchestrates full distributed query lifecycle: fragment, schedule, monitor state machine, collect final results via ExchangeClient, return to pgwire client
- `ServerConfig` gains a `[cluster]` section with `role`, `coordinator_address`, `discovery_port`, `worker_id`
- Startup flow branches by role after config loading and tracing initialization

## Capabilities

### New Capabilities

- `coordinator-role`: Coordinator accepts client connections via pgwire, plans/optimizes/fragments queries, schedules tasks to workers, collects results via ExchangeClient
- `worker-role`: Worker accepts task assignments from coordinator via Flight RPC, executes plan fragments locally, serves output buffers, sends heartbeats to coordinator
- `node-registry`: NodeRegistry tracks known workers with their id, address, status (active/draining/dead), capacity (max_splits), and last heartbeat timestamp. Handles worker registration and heartbeat updates.
- `node-scheduler`: NodeScheduler assigns stages to available workers. Initial strategy: round-robin across active workers respecting capacity limits.
- `task-manager`: TaskManager on the worker side receives TaskRequest messages, creates local ExecutionContext for each task, runs plan fragments, writes results to OutputBuffer for collection by coordinator.
- `distributed-query-runner`: DistributedQueryRunner on coordinator orchestrates the full distributed query lifecycle — fragments the plan, assigns stages to workers via NodeScheduler, monitors query state machine transitions, collects final results via ExchangeClient, returns results to the pgwire client.
- `cluster-config`: Configuration schema for cluster mode including role selection, coordinator address for worker discovery, discovery port, and worker identity.

### Modified Capabilities

- `server-startup`: Server gains `--role` flag (coordinator|worker|standalone). Startup flow branches by role after config loading and tracing initialization. Standalone mode preserves current single-node behavior.
- `server-config`: ServerConfig gains a `[cluster]` section with fields for role, coordinator_address, discovery_port, and worker_id.

## Impact

- **Crates modified**: `server` (role branching, CLI flag, startup orchestration), `common` (ServerConfig cluster section)
- **New modules in server crate**: `coordinator.rs`, `worker.rs`, `node_registry.rs`, `node_scheduler.rs`, `task_manager.rs`, `distributed_query_runner.rs`
- **Dependencies**: Depends on Changes 5 (plan-fragmentation), 6 (query-state-machine), 7 (flight-rpc-layer) for PlanFragment, QueryStateMachine, and Flight RPC types
- **Backwards compatible**: Standalone mode (default) preserves current behavior. No changes to existing single-node query path.
- **New dependencies**: None beyond what Changes 5-7 already introduce (tonic, arrow-flight)
