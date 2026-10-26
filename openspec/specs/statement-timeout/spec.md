# statement-timeout Specification

## Purpose
TBD - created by archiving change admission-control. Update Purpose after archive.
## Requirements
### Requirement: pgwire statement_timeout
The pgwire listener SHALL support `SET statement_timeout`, `SET statement_timeout TO DEFAULT`, `RESET statement_timeout` and `SHOW statement_timeout` per connection. Values SHALL follow PostgreSQL: an integer is milliseconds, a string may carry a unit (`ms`, `s`, `min`, `h`, `d`), and `0` disables the timeout. The timeout SHALL be measured from the time the statement is received, including time spent queued. When it expires, the statement SHALL be cancelled on every node (see `query-cancellation`) and the client SHALL receive `ERROR 57014` with the message `canceling statement due to statement timeout`. The connection SHALL remain usable.

#### Scenario: Timeout fires
- **WHEN** a client runs `SET statement_timeout = '500ms'` and then a query that takes several seconds
- **THEN** after about 500 ms the client receives `ERROR 57014`, the query's tasks stop on every worker, and the next statement on the same connection runs normally

#### Scenario: Show and reset
- **WHEN** a client runs `SET statement_timeout = 30000` and then `SHOW statement_timeout`
- **THEN** the result is `30s`, and after `RESET statement_timeout` it is the server default

#### Scenario: Invalid value
- **WHEN** a client runs `SET statement_timeout = 'soon'`
- **THEN** the statement fails with `22023` (`invalid_parameter_value`) and the previous value is kept

### Requirement: Trino query_max_execution_time
The Trino listener SHALL interpret the session properties `query_max_execution_time` and `query_max_run_time` (from `X-Trino-Session` or `SET SESSION`) as Trino durations (for example `30s`, `5m`, `1h`). `query_max_execution_time` SHALL be measured from the time the query is admitted; `query_max_run_time` SHALL be measured from submission. When either expires, the query SHALL be cancelled on every node and SHALL fail with `EXCEEDED_TIME_LIMIT` (code 131075, type `INSUFFICIENT_RESOURCES`). An unparseable duration SHALL fail the query with `INVALID_SESSION_PROPERTY`.

#### Scenario: Execution time limit
- **WHEN** a Trino client submits a long query with `X-Trino-Session: query_max_execution_time=1s`
- **THEN** the final `QueryResults` has `error.errorName = "EXCEEDED_TIME_LIMIT"`, and the query's tasks stop on every worker

#### Scenario: Queue time excluded
- **WHEN** a query with `query_max_execution_time=2s` waits 5 s in the admission queue and then runs for 1 s
- **THEN** it completes successfully

### Requirement: Server default timeout
`[admission] statement_timeout` (overridable by `ARNEB_STATEMENT_TIMEOUT`, same syntax as pgwire) SHALL apply to every session on both protocols that does not set its own timeout. `0` (the default) SHALL disable it.

#### Scenario: Default applies
- **WHEN** `ARNEB_STATEMENT_TIMEOUT=2s` and a client that never sets a timeout runs a query taking 10 s
- **THEN** the query is cancelled after about 2 s with `57014` over pgwire, or `EXCEEDED_TIME_LIMIT` over the Trino protocol

