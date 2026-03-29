## ADDED Requirements

### Requirement: Distributed query lifecycle
The system SHALL implement a `DistributedQueryRunner` on the coordinator that orchestrates the full lifecycle of a distributed query: parse SQL, plan, optimize, fragment, schedule stages to workers, submit tasks, monitor completion, collect results, and return to the client.

#### Scenario: End-to-end distributed query
- **WHEN** a client submits `SELECT count(*) FROM file.default.lineitem` and 2 workers are registered
- **THEN** the DistributedQueryRunner parses, plans, optimizes, and fragments the query
- **AND** assigns stages to workers via NodeScheduler
- **AND** submits tasks to workers via Flight RPC
- **AND** monitors task completions
- **AND** collects the final stage output via ExchangeClient
- **AND** returns the result rows to the pgwire client

### Requirement: Stage scheduling order
The system SHALL schedule stages in dependency order (bottom-up, leaves first). Leaf stages (table scans) SHALL be submitted before stages that depend on their output. The runner SHALL wait for all dependencies of a stage to complete before submitting it.

#### Scenario: Two-stage query (scan + aggregate)
- **WHEN** a query fragments into a leaf stage (scan) and a root stage (aggregate)
- **THEN** the leaf stage is submitted to a worker first
- **AND** the root stage is submitted only after the leaf stage completes

#### Scenario: Fan-out join
- **WHEN** a query fragments into two leaf stages (left scan, right scan) and a root stage (join)
- **THEN** both leaf stages are submitted concurrently
- **AND** the root stage is submitted only after both leaf stages complete

### Requirement: Query state machine integration
The system SHALL create a QueryStateMachine for each distributed query and transition it through the lifecycle states: queued → planning → running → finishing → finished (or failed at any point).

#### Scenario: Successful query state transitions
- **WHEN** a distributed query completes successfully
- **THEN** the state machine transitions through queued → planning → running → finishing → finished

#### Scenario: Failed query state transition
- **WHEN** a worker task fails during execution
- **THEN** the state machine transitions to failed with the error from the worker

### Requirement: Result collection
The system SHALL collect the output of the root (final) stage from the assigned worker via ExchangeClient (Flight RPC `do_get`). The collected RecordBatch stream SHALL be forwarded to the pgwire handler for encoding and transmission to the client.

#### Scenario: Collect and stream results
- **WHEN** the root stage completes on a worker
- **THEN** the coordinator fetches the output RecordBatches via Flight RPC
- **AND** streams them to the client as pgwire DataRow messages

### Requirement: Error handling
The system SHALL handle errors at any stage of the distributed query lifecycle. If a worker task fails, the query SHALL be marked as failed and the error SHALL be propagated to the client as a pgwire ErrorResponse.

#### Scenario: Worker unreachable during task submission
- **WHEN** the coordinator cannot connect to the assigned worker for task submission
- **THEN** the query fails with an error indicating the worker is unreachable
- **AND** the client receives a pgwire ErrorResponse

#### Scenario: Worker fails mid-execution
- **WHEN** a worker reports a task failure via status update
- **THEN** the query transitions to failed
- **AND** the client receives a pgwire ErrorResponse with the failure reason
