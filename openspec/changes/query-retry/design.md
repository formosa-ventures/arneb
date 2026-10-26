## Context

`QueryCoordinator::execute` picks the active workers, dispatches every child
fragment, and collects the root stage on the coordinator into a
`Vec<RecordBatch>`, which it returns. Stage 1 races that run against
`watch_workers` and turns transport errors into "worker '<id>' failed"
errors that also mark the worker dead. Dropping the run (any error) cancels
the query on every worker through `CancelGuard`.

## Goals / Non-Goals

**Goals:** a read-only query whose worker is lost completes on the remaining
workers when `retry_policy = "query"`, with the same rows, and nothing
changes when it is `none`.

**Non-Goals:** task-level retry or exchange spooling (Trino `TASK`), retrying
standalone or coordinator-local failures, coordinator failover, a session
property to change the policy per query.

## Decisions

### D1 — Retry the whole distributed run inside `execute`

The run is split into `execute_once`, and `execute` loops over it. Each run
gets a new `QueryId`, its own `CancelGuard` and dynamic-filter registration,
so the failed run's tasks are cancelled on the surviving workers before the
next one starts dispatching. Results reach the protocol layer only when
`execute` returns, so a re-run never sends a row twice. That is the property
Trino needs exchange spooling for when results stream; Arneb gets it from
collecting the root stage. Scalar subqueries run through the same executor,
so each is retried on its own.

*Alternative:* retry in the protocol layer, re-planning from SQL. It would
also cover planning, but the planner and connector metadata do not depend on
workers, and re-planning repeats catalog round trips for no gain.

### D2 — Retry only when one of the run's own workers was lost

After a failed run the coordinator asks `NodeRegistry::lost_worker` about the
workers that run used. Stage 1 already marks a worker dead on a transport
failure and reports a missed heartbeat deadline or a departed worker, so this
one check covers a crash, a broken connection, a hang and a drain that ended
mid-query. Any other failure (SQL error, memory limit, a worker answering
with an error) is returned at once: it would fail the same way again. A
timeout or cancel drops the future, so it never reaches the loop.

The lost worker is out of `active_workers` until its next heartbeat, so the
re-run is scheduled on the rest. With no active worker left, the last error
is returned.

### D3 — Writes are never retried

A plan whose root is `INSERT`, `CREATE TABLE [AS]`, `DELETE`, `DROP` or a
view statement (or `EXPLAIN` of one) is run once, since part of a write may
have happened. Writes do not run distributed today; the check keeps that
true if they start to.

### D4 — Bounded attempts and backoff

`query_retry_attempts` (default 4, Trino's default) caps re-runs. The wait
starts at 1 s and doubles up to 10 s. Trino starts at 10 s, but it waits for
its failure detector there; here the lost worker is already excluded when
the run fails, so a short first delay is enough to avoid a hot loop.
`statement_timeout` still bounds the statement as a whole.

## Risks / Trade-offs

- **A heavy query re-runs from the start.** A q09 that loses a worker near
  its end costs about twice its time. That is the price of query-level
  retry; task-level retry needs spooling and is out of scope.
- **A worker lost for an unrelated reason during a query that failed for
  another reason** triggers a re-run of a failure that will repeat. It is
  bounded by the attempt limit and only possible while a worker is actually
  being lost.
- **A run on fewer workers needs more memory per worker.** A query that only
  fit on N workers may hit the memory limit on N−1; it then fails with that
  error, which is not retried.
