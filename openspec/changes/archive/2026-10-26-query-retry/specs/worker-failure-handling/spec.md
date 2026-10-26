## ADDED Requirements

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

## MODIFIED Requirements

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
