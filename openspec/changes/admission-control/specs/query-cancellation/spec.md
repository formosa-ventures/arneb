## ADDED Requirements

### Requirement: Cancel a query on every worker
Workers SHALL accept a Flight `do_action` of type `cancel_query` carrying a query id. On receipt the worker SHALL abort every running task of that query, remove the query's output buffers, release the query's memory pool, and acknowledge. The action SHALL be idempotent: an unknown or already-finished query id SHALL be acknowledged without error. The action SHALL be subject to the cluster RPC authentication of `cluster-rpc-auth`.

#### Scenario: Running query cancelled
- **WHEN** a worker runs three tasks of query Q and receives `cancel_query(Q)`
- **THEN** all three tasks stop, Q's buffers are removed, Q's reserved memory on that worker drops to zero, and the worker logs the number of tasks aborted on `arneb::cluster`

#### Scenario: Unknown query
- **WHEN** a worker receives `cancel_query` for a query it never ran
- **THEN** it acknowledges and nothing else changes

### Requirement: Coordinator cancels on every early exit
The coordinator SHALL send `cancel_query` to every worker that received a task of a distributed query whenever that query ends other than by successful completion. That covers a fragment error, a client cancel (Trino `DELETE` or kill), the Trino abandonment reaper, a statement timeout, a pgwire client disconnect, and the query's future being dropped for any other reason. Sending SHALL NOT block the exit path. A failed delivery SHALL be logged on `arneb::cluster`.

#### Scenario: Trino client cancels
- **WHEN** a Trino client sends `DELETE` on a running distributed query's `nextUri`
- **THEN** every worker that received a task of that query receives `cancel_query`, and within a few seconds none of the query's tasks are running

#### Scenario: Fragment failure cancels the rest
- **WHEN** one worker task of a query fails
- **THEN** the query's tasks on the other workers are cancelled instead of running to completion

#### Scenario: Successful query
- **WHEN** a distributed query completes successfully
- **THEN** no `cancel_query` is sent
