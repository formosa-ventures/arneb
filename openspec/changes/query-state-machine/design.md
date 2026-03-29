## Context

Phase 2 introduces distributed query execution where a coordinator manages queries across multiple workers. Each query goes through a lifecycle that must be tracked, and failures at any level (task, stage, query) must propagate correctly.

## Goals / Non-Goals

**Goals:**
- Type-safe state machines with validated transitions
- Hierarchical ownership: Query → Stages → Tasks
- Thread-safe query tracking with concurrent access
- Resource groups for basic admission control
- Foundation for coordinator query management

**Non-Goals:**
- Persistent query history (in-memory only for now)
- Complex resource group hierarchies (single flat group)
- Query priority or fair scheduling — simple FIFO

## Decisions

1. **State as Rust enum**: Each state machine uses an enum for states. Transition functions return Result — invalid transitions return Err.

2. **QueryStateMachine owns state + metadata**: Contains QueryId, current state, timestamps for each transition, error info if failed, list of StageIds.

3. **QueryTracker is Arc<RwLock<HashMap<QueryId, QueryStateMachine>>>**: Allows concurrent read access for listing, exclusive write for state transitions. Provides list_queries(), get_query(), cancel_query(), create_query() methods.

4. **ResourceGroup as pre-admission gate**: Before a query enters QUEUED state, ResourceGroup checks if max_running is reached. If so, query waits in a VecDeque. When a query finishes, next queued query is admitted.

5. **Failure propagation**: Task failure → Stage fails → Query fails. Implemented as cascade: when TaskStateMachine transitions to FAILED, parent stage checks if all tasks failed and transitions accordingly.

6. **QueryId = uuid::Uuid**: Globally unique, sortable by time (v7 UUID).

## Risks / Trade-offs

- **In-memory only**: Query tracker loses state on restart. Acceptable for Phase 2.
- **Single resource group**: No per-user or per-source limits. Sufficient for initial implementation.
