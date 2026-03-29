## 1. Scheduler Crate Setup

- [x] 1.1 Create crates/scheduler/Cargo.toml with dependencies on common, uuid, tokio
- [x] 1.2 Add scheduler to workspace Cargo.toml members
- [x] 1.3 Create crates/scheduler/src/lib.rs with module declarations

## 2. QueryId Type (common crate)

- [x] 2.1 Add uuid dependency to workspace and common crate
- [x] 2.2 Define QueryId as newtype wrapper around Uuid with Display, Debug, Clone, Hash, Eq
- [x] 2.3 Implement QueryId::new() using Uuid::now_v7()

## 3. QueryStateMachine

- [x] 3.1 Define QueryState enum: Queued, Planning, Starting, Running, Finishing, Finished, Failed, Cancelled
- [x] 3.2 Implement QueryStateMachine with current state, transition timestamps, error info
- [x] 3.3 Implement transition validation (only allowed transitions succeed)
- [x] 3.4 Implement fail() and cancel() methods that work from any active state
- [x] 3.5 Write tests for all valid and invalid transitions

## 4. StageStateMachine

- [x] 4.1 Define StageState enum: Planned, Scheduling, Running, Flushing, Finished, Failed, Cancelled
- [x] 4.2 Implement StageStateMachine with transition validation
- [x] 4.3 Write tests for stage state transitions

## 5. TaskStateMachine

- [x] 5.1 Define TaskState enum: Planned, Running, Flushing, Finished, Failed, Cancelled
- [x] 5.2 Implement TaskStateMachine with transition validation
- [x] 5.3 Write tests for task state transitions

## 6. QueryTracker

- [x] 6.1 Implement QueryTracker with Arc<RwLock<HashMap<QueryId, QueryStateMachine>>>
- [x] 6.2 Implement create_query() → QueryId
- [x] 6.3 Implement list_queries() with optional state filter
- [x] 6.4 Implement get_query(QueryId) → query info
- [x] 6.5 Implement cancel_query(QueryId) → Result
- [x] 6.6 Write tests for concurrent access scenarios

## 7. ResourceGroup

- [x] 7.1 Define ResourceGroup with max_running and max_queued limits
- [x] 7.2 Implement try_acquire() → bool (check if query can run)
- [x] 7.3 Implement queue management (VecDeque for waiting queries)
- [x] 7.4 Implement release() when query finishes (admit next queued)
- [x] 7.5 Write tests for admission control scenarios

## 8. Integration

- [x] 8.1 Wire QueryTracker into server startup
- [x] 8.2 Assign QueryId to each incoming query
- [x] 8.3 Verify all existing tests pass with new dependency
