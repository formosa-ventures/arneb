## ADDED Requirements

### Requirement: ResourceGroup struct
The system SHALL implement a `ResourceGroup` struct with configurable `max_running: usize` and `max_queued: usize` limits. It SHALL track currently running queries and a FIFO queue of waiting queries via `VecDeque<QueryId>`.

### Requirement: try_acquire
The system SHALL implement `try_acquire(query_id: QueryId) -> AcquireResult` where `AcquireResult` is one of `Acquired` (query can run immediately), `Queued` (query added to wait queue), or `Rejected` (queue is full).

#### Scenario: Acquiring when below capacity
- **WHEN** `max_running` is 3 and 1 query is currently running
- **AND** `try_acquire(query_id)` is called
- **THEN** `Acquired` is returned and running count increases to 2

#### Scenario: Queuing when at capacity
- **WHEN** `max_running` is 2 and 2 queries are running, `max_queued` is 5
- **AND** `try_acquire(query_id)` is called
- **THEN** `Queued` is returned and the query is added to the wait queue

#### Scenario: Rejecting when queue is full
- **WHEN** `max_running` is 1 and 1 query is running, `max_queued` is 2 and 2 queries are queued
- **AND** `try_acquire(query_id)` is called
- **THEN** `Rejected` is returned

### Requirement: release
The system SHALL implement `release(query_id: QueryId) -> Option<QueryId>` that decrements the running count and, if the wait queue is non-empty, dequeues the next query and returns its `QueryId` (indicating it should now run). If the queue is empty, returns `None`.

#### Scenario: Releasing admits next queued query
- **WHEN** 2 queries are running (at capacity) and 1 query is queued
- **AND** `release(finished_query_id)` is called
- **THEN** the queued `QueryId` is returned, running count remains at max_running

#### Scenario: Releasing with empty queue
- **WHEN** 1 query is running and the queue is empty
- **AND** `release(query_id)` is called
- **THEN** `None` is returned and running count decreases to 0

### Requirement: Queue ordering
The system SHALL maintain FIFO ordering — queries that were queued earlier SHALL be admitted before queries queued later.

#### Scenario: FIFO ordering
- **WHEN** queries A, B, C are queued in that order
- **AND** slots become available one at a time
- **THEN** they are admitted in order A, B, C
