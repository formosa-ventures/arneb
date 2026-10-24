## ADDED Requirements

### Requirement: SIGTERM drains a worker
On SIGTERM or Ctrl-C, a worker SHALL enter the draining state and send a
heartbeat reporting it without waiting for the heartbeat interval. It SHALL
keep running its tasks and accepting task submissions. It SHALL exit once
it holds no query the coordinator has not released or cancelled, waiting at
least one heartbeat interval first, or once `drain_timeout_secs` has passed
(`[cluster] drain_timeout_secs`, `ARNEB_DRAIN_TIMEOUT_SECS`, default 120).
Before exiting it SHALL send a heartbeat reporting `stopped`. A second
SIGTERM or Ctrl-C during the drain SHALL exit at once.

#### Scenario: Rolling restart under load
- **WHEN** a worker gets SIGTERM while SF10 q09 runs on it
- **THEN** q09 succeeds, the worker logs `worker draining` then `worker drained`, and it exits after the query ends

#### Scenario: Idle worker
- **WHEN** an idle worker gets SIGTERM
- **THEN** it exits within two heartbeat intervals

#### Scenario: Drain timeout
- **WHEN** a draining worker still holds a query after `drain_timeout_secs`
- **THEN** it logs `drain timed out` with the number of queries left and exits

### Requirement: Draining workers get no new queries
The coordinator SHALL schedule new queries only on workers that are alive
and active. A draining worker SHALL still count as alive for the queries it
already runs, and the workers API SHALL list it with `state: "draining"`.
A `stopped` heartbeat SHALL remove the worker from the registry and log
`worker deregistered`. An `active` heartbeat from the same id SHALL make it
schedulable again.

#### Scenario: Query started during a drain
- **WHEN** worker-2 is draining and a new query starts
- **THEN** the query's tasks are dispatched to worker-1 only and it succeeds

#### Scenario: Running query is not failed
- **WHEN** a worker that runs a query's tasks starts draining
- **THEN** the coordinator does not report the worker lost for that query

### Requirement: The coordinator releases finished queries
After a distributed query has collected its result, the coordinator SHALL
send `release_query` to each of its workers. A worker SHALL then drop that
query's tasks, buffers and dynamic-filter state, as for a cancel, without
logging it as a cancellation. A release of an unknown query SHALL be
acknowledged and do nothing.

#### Scenario: Broadcast buffers freed on success
- **WHEN** a query with a broadcast exchange succeeds
- **THEN** each worker holds no task of that query once the release arrives, instead of after the 600 s cleanup ceiling
