## ADDED Requirements

### Requirement: Running cap with a FIFO queue
Coordinator and standalone nodes SHALL admit at most `max_running_queries` plan-executing statements at once, across the pgwire and Trino listeners combined. A statement arriving over the cap SHALL wait in a FIFO queue of at most `max_queued_queries` entries and SHALL start when a running statement releases its slot, in arrival order. A queued statement SHALL hold no plan, memory reservation or worker task. A queued statement whose client goes away, or whose timeout expires while it waits, SHALL leave the queue without taking a slot. `max_running_queries = 0` SHALL mean unlimited.

#### Scenario: Over the cap
- **WHEN** `max_running_queries = 2` and four long queries are submitted at once
- **THEN** two run, two wait, and each waiting query starts as soon as a running one finishes, in submission order

#### Scenario: Session and metadata statements bypass the queue
- **WHEN** the running cap is reached and a client sends `SET search_path`, `SHOW statement_timeout`, or a `pg_catalog` / `information_schema` / `system.jdbc` metadata query
- **THEN** the statement is answered immediately without queuing

#### Scenario: Unlimited
- **WHEN** `max_running_queries = 0`
- **THEN** no statement ever queues

### Requirement: Queue-full rejection
When the queue already holds `max_queued_queries` statements, a new plan-executing statement SHALL fail immediately. pgwire clients SHALL receive SQLSTATE `53000` (`insufficient_resources`); Trino clients SHALL receive `QUERY_QUEUE_FULL` (code 131074, type `INSUFFICIENT_RESOURCES`). The message SHALL state the running and queued counts.

#### Scenario: Queue full
- **WHEN** `max_running_queries = 1`, `max_queued_queries = 1`, one query is running and one is queued, and a third is submitted
- **THEN** the third fails immediately with `53000` over pgwire, or `QUERY_QUEUE_FULL` over the Trino protocol

### Requirement: Queuing is observable
A queued statement SHALL appear with state `QUEUED` in the Web UI query list and in the Trino protocol's `stats.state` until it is admitted. Admission SHALL log on the `arneb::admission` target: an INFO line when a statement is queued (with its queue position) and when it is admitted (with how long it waited). `/api/v1/cluster` SHALL report `running_queries`, `queued_queries`, `max_running_queries` and `max_queued_queries`.

#### Scenario: Web UI shows queued queries
- **WHEN** 20 queries are submitted concurrently with `max_running_queries = 4`
- **THEN** the Web UI lists 16 queries as `QUEUED` at first, `/api/v1/cluster` reports `running_queries = 4` and `queued_queries = 16`, and the log shows each queued query's admission and wait time
