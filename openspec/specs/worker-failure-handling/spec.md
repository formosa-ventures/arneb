# worker-failure-handling Specification

## Purpose
TBD - created by archiving change fault-tolerance-fail-clean. Update Purpose after archive.
## Requirements
### Requirement: Running queries fail when a worker is lost
While a distributed query runs, the coordinator SHALL watch every worker it
dispatched the query to. A worker SHALL count as lost when it has sent no
heartbeat for longer than `heartbeat_timeout_secs`, or when it was marked dead
after a transport failure. When one is lost, the coordinator SHALL fail the
query with an error that names the worker's id and address, and SHALL cancel
the query's tasks on every worker.

#### Scenario: Hung worker
- **WHEN** a worker stops responding (process paused, sockets open) during SF10 q09 with default settings
- **THEN** the query fails within 30 s with an error containing `worker 'worker-2'` and `lost`

#### Scenario: Killed worker
- **WHEN** a worker process is killed during SF10 q09
- **THEN** the query fails within 30 s with an error naming that worker, and the surviving worker logs `query cancelled on worker` for the query

### Requirement: Lost workers are not scheduled
The coordinator SHALL mark a worker dead immediately when a task submission
or Flight stream to it fails with a transport error. A worker past its heartbeat deadline
SHALL be treated as dead. Queries that start afterwards SHALL be scheduled
only on live workers that are not draining.

#### Scenario: Next query after a kill
- **WHEN** a worker was killed and a query failed because of it
- **THEN** the next query succeeds on the remaining workers without waiting for the heartbeat timeout

#### Scenario: Next query during a drain
- **WHEN** a worker is draining
- **THEN** the next query is scheduled on the other workers only

### Requirement: A returning worker rejoins
A heartbeat from a worker marked dead SHALL mark it alive again, log `worker
re-registered`, and make it eligible for new queries.

#### Scenario: Restart
- **WHEN** a killed worker is restarted
- **THEN** within two heartbeat intervals it is listed as alive and a new query dispatches tasks to it

### Requirement: Fault handling is covered by an automated Tier 2 test
`benchmarks/tpch/scripts/fault_injection.sh` SHALL run against the bench
cluster. It SHALL cover a killed worker and a paused worker during SF10 q09,
and assert each of the scenarios above. With `RETRY=1` it SHALL run the
coordinator with `retry_policy = query` and assert that q09 completes with
the rows of a run without a fault, and that the retry is logged. It SHALL
exit non-zero when any check fails.

#### Scenario: Script passes
- **WHEN** the script runs against a healthy SF10 bench cluster
- **THEN** it reports every check as passed and exits 0

#### Scenario: Script passes with retry
- **WHEN** `RETRY=1 fault_injection.sh` runs against a healthy SF10 bench cluster
- **THEN** it reports every check as passed and exits 0

### Requirement: Read-only queries are re-run after a worker loss
When `retry_policy` is `query`, the coordinator SHALL re-run a distributed
query whose run failed while one of that run's workers was lost (marked dead,
past its heartbeat deadline, or departed), on the workers still active. It
SHALL NOT re-run a plan that writes data, a run that failed while none of its
workers was lost, or a query that has already been re-run
`query_retry_attempts` times, and it SHALL return the last run's error then.
The first re-run SHALL wait 1 s and each later one twice as long, up to 10 s.
Each re-run SHALL be logged as `retrying query after worker loss` on
`arneb::cluster` with the lost worker's id, and counted in
`arneb_query_retries_total`. When `retry_policy` is `none`, a worker loss
SHALL fail the query as before.

#### Scenario: Killed worker with retry
- **WHEN** retry is on and a worker process is killed during SF10 q09
- **THEN** q09 returns the same rows as a run without a fault, and the coordinator logs a retry naming that worker

#### Scenario: Hung worker with retry
- **WHEN** retry is on and a worker is paused during SF10 q09
- **THEN** q09 completes on the remaining worker after the heartbeat deadline, with the same rows

#### Scenario: Other failures are not retried
- **WHEN** retry is on and a run fails with a SQL or memory-limit error while every worker is alive
- **THEN** the query fails once with that error

