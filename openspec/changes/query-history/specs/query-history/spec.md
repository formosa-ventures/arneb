## ADDED Requirements

### Requirement: Every engine statement is recorded
The query tracker SHALL record every statement that reaches query execution
through pgwire or the Trino protocol, with its source protocol, user, SQL
text, and created, started and ended times. Its state SHALL be `QUEUED` until
admitted and `RUNNING` while executing. It SHALL end `FINISHED` with the
returned row count, `FAILED` with the error message, or `CANCELLED` when the
statement was dropped before it ended. Metadata queries and session commands
answered by the protocol layer SHALL NOT be recorded. A distributed query
SHALL appear once.

#### Scenario: pgwire and Trino statements
- **WHEN** user `ann` runs one successful and one failing statement over pgwire, and user `bob` runs one over the Trino protocol
- **THEN** the tracker holds them as `pgwire`/`ann` `FINISHED` with its row count, `pgwire`/`ann` `FAILED` with the error, and `trino`/`bob` `FINISHED`

### Requirement: Bounded history
The tracker SHALL keep every active query and at most `ARNEB_QUERY_HISTORY_SIZE`
finished queries (default 100). When a query finishes beyond the limit, the
tracker SHALL evict the oldest finished query. The effective size SHALL be
logged once.

#### Scenario: Eviction
- **WHEN** `ARNEB_QUERY_HISTORY_SIZE=5` and nine statements finish
- **THEN** only the five most recent finished statements, plus any still running, remain

### Requirement: system.runtime.queries
Over both pgwire and the Trino protocol, `system.runtime.queries` SHALL return
one row per tracked query, with the columns `query_id`, `state`, `user`,
`source`, `query`, `queued_time_ms`, `elapsed_time_ms`, `rows`, `created`,
`started`, `end` and `error_message`. The server SHALL evaluate filters,
ordering and limits on it like on any table. The statement reading it SHALL
see itself as `RUNNING`.

#### Scenario: Filtered read
- **WHEN** a client runs `SELECT source, state FROM system.runtime.queries WHERE state <> 'RUNNING' ORDER BY state`
- **THEN** it receives the finished and failed queries in that order, without itself

### Requirement: Web UI query list fields
`GET /api/v1/queries` and `/api/v1/queries/{id}` SHALL include each query's
`source`, `user`, `created_ms`, `elapsed_ms` and `rows`, newest first.

#### Scenario: API listing
- **WHEN** the history holds pgwire and Trino queries
- **THEN** the API lists both with their `source`
