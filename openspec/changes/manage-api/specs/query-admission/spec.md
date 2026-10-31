## MODIFIED Requirements

### Requirement: Running cap with a FIFO queue
Coordinator and standalone nodes SHALL admit at most `max_running_queries` plan-executing statements at once, across the pgwire and Trino listeners combined. A statement arriving over the cap SHALL wait in a FIFO queue of at most `max_queued_queries` entries and SHALL start when a running statement releases its slot, in arrival order. A queued statement SHALL hold no plan, memory reservation or worker task. A queued statement whose client goes away, or whose timeout expires while it waits, SHALL leave the queue without taking a slot. `max_running_queries = 0` SHALL mean unlimited. An admission controller SHALL exist whenever SQL is served, also when `max_running_queries = 0`, so the limits can change at runtime.

#### Scenario: Over the cap
- **WHEN** `max_running_queries = 2` and four long queries are submitted at once
- **THEN** two run, two wait, and each waiting query starts as soon as a running one finishes, in submission order

#### Scenario: Session and metadata statements bypass the queue
- **WHEN** the running cap is reached and a client sends `SET search_path`, `SHOW statement_timeout`, or a `pg_catalog` / `information_schema` / `system.jdbc` metadata query
- **THEN** the statement is answered immediately without queuing

#### Scenario: Unlimited
- **WHEN** `max_running_queries = 0`
- **THEN** no statement ever queues, and the admission gauges are still reported

## ADDED Requirements

### Requirement: Limits change at runtime
`max_running_queries` and `max_queued_queries` SHALL be changeable while the server runs. Lowering `max_running_queries` below the running count SHALL stop no running query: new statements SHALL queue, and a waiter SHALL be granted a slot only while the running count is below the new cap, in queue order. Raising it, or setting `0`, SHALL grant queued statements at once, up to the new cap. Lowering `max_queued_queries` SHALL keep the statements already queued, and SHALL refuse new arrivals while the queue is at or over the new limit.

#### Scenario: Lowered below running
- **WHEN** five queries run and the cap is set to 2
- **THEN** the five finish, new queries queue until fewer than 2 run, and queued order is kept

#### Scenario: Raised
- **WHEN** three queries are queued behind a cap of 1 and the cap is raised to 4
- **THEN** the queued queries start at once

#### Scenario: Shorter queue
- **WHEN** three queries are queued and `max_queued_queries` is set to 1
- **THEN** the three stay queued, and a new statement fails with the queue-full error
