## 1. Cluster Configuration

- [x] 1.1 Define `ClusterConfig` struct with fields: `role: String` (default `"standalone"`), `coordinator_address: String` (default empty), `discovery_port: u16` (default `9090`), `worker_id: String` (default empty) — derive `Deserialize`, `Clone`, `Debug`
- [x] 1.2 Add `#[serde(default)] cluster: ClusterConfig` field to `ServerConfig` in `crates/common`
- [x] 1.3 Extend `ServerConfig::apply_env_overrides()` to handle `TRINO_CLUSTER_ROLE`, `TRINO_COORDINATOR_ADDRESS`, `TRINO_DISCOVERY_PORT`, `TRINO_WORKER_ID`
- [x] 1.4 Extend `ServerConfig::validate()` to validate cluster config: role must be one of `standalone`/`coordinator`/`worker`, workers require non-empty `coordinator_address`, `discovery_port` must be > 0
- [x] 1.5 Implement auto-generation of `worker_id` (hostname + 6-char random alphanumeric suffix) when role is `worker` and `worker_id` is empty
- [x] 1.6 Update `ServerConfig` `Display` impl to include cluster role, coordinator_address (for workers), and worker_id (for workers)

## 2. CLI Role Flag

- [x] 2.1 Add `--role` argument to `CliArgs` in `crates/server`: `Option<String>` accepting `standalone`, `coordinator`, `worker`
- [x] 2.2 Apply CLI `--role` override to `config.server.cluster.role` after config loading (CLI > env > file > defaults)
- [x] 2.3 Re-validate config after CLI overrides (existing pattern from Phase 1)

## 3. Server Startup Branching

- [x] 3.1 Refactor `main()` to extract common startup path: parse CLI → load config → init tracing
- [x] 3.2 Add role-based dispatch after common startup: match on `cluster.role` → call `start_standalone()`, `start_coordinator()`, or `start_worker()`
- [x] 3.3 Implement `start_standalone()` — coordinator + worker in same process, in-process channel for task submission and result collection, pgwire listener, preserves current single-node behavior
- [x] 3.4 Update startup banner to include `"role: {role}"` in log output

## 4. NodeRegistry

- [x] 4.1 Create `node_registry.rs` module in `crates/server` with `NodeRegistry` struct containing `workers: Arc<RwLock<HashMap<WorkerId, WorkerInfo>>>`
- [x] 4.2 Define `WorkerId` newtype (String), `WorkerStatus` enum (Active, Draining, Dead), `WorkerInfo` struct (id, address, status, capacity, active_tasks, last_heartbeat)
- [x] 4.3 Implement `NodeRegistry::register_or_update(&self, heartbeat: HeartbeatRequest)` — creates new entry or updates existing worker
- [x] 4.4 Implement `NodeRegistry::active_workers(&self) -> Vec<WorkerInfo>` — returns workers with status Active, sorted by worker_id
- [x] 4.5 Implement `NodeRegistry::remove_worker(&self, worker_id: &WorkerId)` — removes a worker entry
- [x] 4.6 Implement heartbeat staleness checker: background tokio task that runs every 15 seconds, marks workers Dead if `last_heartbeat` > 30 seconds ago
- [x] 4.7 Write tests: register new worker, update existing worker, stale worker marked dead, dead worker recovers on new heartbeat, active_workers filtering

## 5. NodeScheduler

- [x] 5.1 Create `node_scheduler.rs` module in `crates/server` with `NodeScheduler` struct containing `registry: Arc<NodeRegistry>`, `next_index: AtomicUsize`
- [x] 5.2 Implement `NodeScheduler::schedule_stage(&self, fragment: &PlanFragment) -> Result<WorkerId>` — round-robin assignment across active workers, returns `SchedulingError::NoActiveWorkers` if none available
- [x] 5.3 Implement `NodeScheduler::schedule_query(&self, fragments: &[PlanFragment]) -> Result<Vec<(PlanFragment, WorkerId)>>` — assigns each fragment to a worker
- [x] 5.4 Write tests: single worker gets all stages, round-robin distributes across multiple workers, no active workers returns error, concurrent scheduling is safe

## 6. TaskManager

- [x] 6.1 Create `task_manager.rs` module in `crates/server` with `TaskManager` struct containing `tasks: Arc<RwLock<HashMap<TaskId, TaskHandle>>>`, `catalog_manager`, `connector_registry`
- [x] 6.2 Define `TaskId` newtype, `TaskStatus` enum (Running, Completed, Failed), `TaskHandle` struct (id, query_id, stage_id, status, output_buffer, error)
- [x] 6.3 Define `OutputBuffer` struct containing `schema: Arc<Schema>` and `batches: Vec<RecordBatch>`
- [x] 6.4 Implement `TaskManager::submit_task(&self, request: TaskRequest) -> Result<TaskId>` — creates TaskHandle, spawns tokio task for execution, returns task_id. Duplicate task_id returns existing handle.
- [x] 6.5 Implement task execution logic: deserialize plan fragment → create ExecutionContext → build physical plan → execute → collect stream → write to OutputBuffer → update status to Completed or Failed
- [x] 6.6 Implement `TaskManager::get_task_status(&self, task_id: &TaskId) -> Result<TaskStatus>`
- [x] 6.7 Implement `TaskManager::get_task_output(&self, task_id: &TaskId) -> Result<(Arc<Schema>, Vec<RecordBatch>)>` — returns error if task not completed
- [x] 6.8 Implement `TaskManager::remove_task(&self, task_id: &TaskId)` — drops TaskHandle and OutputBuffer
- [x] 6.9 Write tests: submit and complete task, submit failing task, get status of running/completed/failed task, get output of completed task, remove task frees memory

## 7. DistributedQueryRunner

- [x] 7.1 Create `distributed_query_runner.rs` module in `crates/server` with `DistributedQueryRunner` struct containing `scheduler: Arc<NodeScheduler>`, `catalog_manager`, `connector_registry`, Flight RPC client factory
- [x] 7.2 Implement `run_query(&self, sql: &str) -> Result<SendableRecordBatchStream>` orchestrating: parse → plan → optimize → fragment → schedule → submit tasks → monitor → collect results
- [x] 7.3 Implement stage dependency ordering: topological sort of fragments, submit leaf stages first, wait for dependencies before submitting dependent stages
- [x] 7.4 Implement task submission to workers via Flight RPC client (from Change 7)
- [x] 7.5 Implement task monitoring: poll worker task status until all stages complete or any fails
- [x] 7.6 Implement result collection: fetch root stage output from assigned worker via ExchangeClient (Flight RPC `do_get`)
- [x] 7.7 Integrate QueryStateMachine (from Change 6): create for each query, transition through queued → planning → running → finishing → finished/failed
- [x] 7.8 Write tests: successful distributed query end-to-end (with mock workers), query fails when no workers, query fails when worker task fails

## 8. Worker Heartbeat Protocol

- [x] 8.1 Define `HeartbeatRequest` struct: worker_id, address, status, capacity, active_tasks
- [x] 8.2 Define `HeartbeatResponse` struct: acknowledged (bool)
- [x] 8.3 Implement heartbeat sender on worker side: sends HeartbeatRequest to coordinator every 10 seconds via Flight RPC, logs warning on failure, retries next interval
- [x] 8.4 Implement heartbeat receiver on coordinator side: calls `NodeRegistry::register_or_update()` on each heartbeat, returns HeartbeatResponse
- [x] 8.5 Write tests: worker registers via heartbeat, worker heartbeat updates last_heartbeat, coordinator handles unknown worker heartbeat

## 9. Coordinator Startup

- [x] 9.1 Implement `start_coordinator()`: create NodeRegistry → start heartbeat staleness checker → create NodeScheduler → create DistributedQueryRunner → start Flight RPC server (for heartbeats + result serving) → start pgwire server (with DistributedQueryRunner as query handler) → `tokio::select!` for shutdown
- [x] 9.2 Wire pgwire handler to use DistributedQueryRunner instead of direct local execution
- [x] 9.3 Log coordinator startup banner: role, pgwire address, Flight RPC address, number of catalogs

## 10. Worker Startup

- [x] 10.1 Implement `start_worker()`: auto-generate worker_id if empty → create TaskManager → start Flight RPC server (for task submission + output serving) → start heartbeat loop → `tokio::select!` for shutdown
- [x] 10.2 Wire Flight RPC task submission handler to call `TaskManager::submit_task()`
- [x] 10.3 Wire Flight RPC `do_get` handler to call `TaskManager::get_task_output()` and stream results
- [x] 10.4 Log worker startup banner: role, worker_id, Flight RPC address, coordinator address

## 11. Standalone Mode

- [x] 11.1 Implement `start_standalone()`: create in-process worker (TaskManager with in-process channel) → create coordinator components (NodeRegistry with local worker pre-registered, NodeScheduler, DistributedQueryRunner with in-process task submission) → start pgwire server → `tokio::select!` for shutdown
- [x] 11.2 Implement in-process task submission channel: tokio mpsc channel bypassing Flight RPC serialization
- [x] 11.3 Implement in-process result collection: direct access to TaskManager OutputBuffer without Flight RPC
- [x] 11.4 Verify backwards compatibility: existing Phase 1 queries produce identical results in standalone mode

## 12. Tests — Cluster Configuration

- [x] 12.1 Test `ClusterConfig` deserialization from TOML with all fields
- [x] 12.2 Test `ClusterConfig` defaults when `[cluster]` section is absent
- [x] 12.3 Test env var overrides for cluster settings (`TRINO_CLUSTER_ROLE`, `TRINO_COORDINATOR_ADDRESS`, etc.)
- [x] 12.4 Test validation: invalid role returns error, worker without coordinator_address returns error, discovery_port 0 returns error
- [x] 12.5 Test auto-generated worker_id format (contains hostname, has random suffix)
- [x] 12.6 Test `ServerConfig` Display includes cluster role

## 13. Tests — Integration

- [x] 13.1 Integration test: start standalone server, connect via pgwire, run a query, verify results match Phase 1 behavior
- [x] 13.2 Integration test: start coordinator + worker (separate tokio tasks, using real Flight RPC on localhost), run a query via pgwire on coordinator, verify results
- [x] 13.3 Integration test: start coordinator with no workers, submit query, verify error response indicating no active workers
- [x] 13.4 Integration test: start coordinator + 2 workers, submit multiple queries, verify round-robin distribution (each worker receives tasks)
- [x] 13.5 Integration test: worker heartbeat registers with coordinator, verify worker appears in NodeRegistry active_workers

## 14. Quality & Build Verification

- [x] 14.1 `cargo build` compiles without warnings
- [x] 14.2 `cargo test` — all tests pass (including existing Phase 1 tests)
- [x] 14.3 `cargo clippy -- -D warnings` — clean
- [x] 14.4 `cargo fmt -- --check` — clean
- [x] 14.5 `cargo run --bin trino-alt -- --help` shows `--role` argument in usage output
- [x] 14.6 `cargo run --bin trino-alt -- --role coordinator` starts in coordinator mode (smoke test)
- [x] 14.7 `cargo run --bin trino-alt -- --role worker --bind 127.0.0.1` prints error about missing coordinator_address
