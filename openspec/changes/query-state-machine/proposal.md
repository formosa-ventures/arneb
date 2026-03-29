## Why

A distributed query engine needs rigorous lifecycle management. Without state machines, there's no way to track query progress, handle failures gracefully, enforce concurrency limits, or cancel running queries. The query/stage/task state machines define the contract between coordinator and workers, ensuring consistent behavior under normal operation and failure scenarios.

## What Changes

- Create new `crates/scheduler/` crate (trino-scheduler)
- Implement QueryStateMachine: QUEUED → PLANNING → STARTING → RUNNING → FINISHING → FINISHED + FAILED/CANCELLED
- Implement StageStateMachine: PLANNED → SCHEDULING → RUNNING → FLUSHING → FINISHED + FAILED/CANCELLED
- Implement TaskStateMachine: PLANNED → RUNNING → FLUSHING → FINISHED + FAILED/CANCELLED
- Implement QueryTracker: manages active queries, concurrency enforcement, query listing/cancellation
- Implement ResourceGroup: basic admission control (max concurrent, max queued)
- Add QueryId (UUID-based) type to common crate

## Capabilities

### New Capabilities
- `query-state-machine`: Query lifecycle states and validated transitions
- `stage-state-machine`: Stage lifecycle states and transitions
- `task-state-machine`: Task lifecycle states and transitions
- `query-tracker`: Active query management, listing, cancellation
- `resource-groups`: Query admission control with concurrency limits

### Modified Capabilities
- `common-data-types`: Add QueryId type

## Impact

- **Crates**: common (QueryId), scheduler (new crate)
- **New crate**: crates/scheduler/ with Cargo.toml depending on common, uuid, tokio
- **Dependencies**: uuid 1 (new workspace dependency)
