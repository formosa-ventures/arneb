## ADDED Requirements

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
only on live workers.

#### Scenario: Next query after a kill
- **WHEN** a worker was killed and a query failed because of it
- **THEN** the next query succeeds on the remaining workers without waiting for the heartbeat timeout

### Requirement: A returning worker rejoins
A heartbeat from a worker marked dead SHALL mark it alive again, log `worker
re-registered`, and make it eligible for new queries.

#### Scenario: Restart
- **WHEN** a killed worker is restarted
- **THEN** within two heartbeat intervals it is listed as alive and a new query dispatches tasks to it

### Requirement: Fault handling is covered by an automated Tier 2 test
`benchmarks/tpch/scripts/fault_injection.sh` SHALL run against the bench
cluster. It SHALL cover a killed worker and a paused worker during SF10 q09,
and assert each of the scenarios above. It SHALL exit non-zero when any of
them fails.

#### Scenario: Script passes
- **WHEN** the script runs against a healthy SF10 bench cluster
- **THEN** it reports every check as passed and exits 0
