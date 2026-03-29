## ADDED Requirements

### Requirement: QueryTracker struct
The system SHALL implement a `QueryTracker` struct backed by `Arc<RwLock<HashMap<QueryId, QueryStateMachine>>>` for thread-safe concurrent access. It SHALL be `Clone`, `Send`, and `Sync`.

### Requirement: create_query
The system SHALL implement `create_query() -> QueryId` that generates a new `QueryId`, creates a `QueryStateMachine` in `Queued` state, inserts it into the tracker, and returns the `QueryId`.

#### Scenario: Creating a new query
- **WHEN** `create_query()` is called
- **THEN** a new `QueryId` is returned
- **AND** the tracker contains a query in `Queued` state with that ID

### Requirement: list_queries with optional state filter
The system SHALL implement `list_queries(filter: Option<QueryState>) -> Vec<QueryId>` that returns all tracked query IDs, optionally filtered by state.

#### Scenario: Listing all queries
- **WHEN** 3 queries exist (2 running, 1 finished) and `list_queries(None)` is called
- **THEN** all 3 QueryIds are returned

#### Scenario: Listing queries by state
- **WHEN** 3 queries exist (2 running, 1 finished) and `list_queries(Some(Running))` is called
- **THEN** only the 2 running QueryIds are returned

### Requirement: get_query
The system SHALL implement `get_query(id: QueryId) -> Option<QueryInfo>` returning the query's current state, timestamps, and error info. It SHALL return `None` for unknown IDs.

#### Scenario: Getting an existing query
- **WHEN** `get_query(id)` is called for a known query
- **THEN** the query's state and metadata are returned

#### Scenario: Getting an unknown query
- **WHEN** `get_query(id)` is called for an unknown ID
- **THEN** `None` is returned

### Requirement: cancel_query
The system SHALL implement `cancel_query(id: QueryId) -> Result<()>` that calls `cancel()` on the query's state machine. It SHALL return `Err` if the query is not found or already in a terminal state.

#### Scenario: Cancelling a running query
- **WHEN** `cancel_query(id)` is called for a running query
- **THEN** the query transitions to `Cancelled` and `Ok(())` is returned

#### Scenario: Cancelling an unknown query
- **WHEN** `cancel_query(id)` is called for an unknown ID
- **THEN** `Err` is returned

### Requirement: Concurrent access safety
The system SHALL allow multiple concurrent readers via `RwLock` read guards while state-mutating operations acquire exclusive write access.

#### Scenario: Concurrent reads during listing
- **WHEN** multiple threads call `list_queries()` simultaneously
- **THEN** all calls succeed without blocking each other
