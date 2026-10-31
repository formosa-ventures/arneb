## ADDED Requirements

### Requirement: Asynchronous statements
`POST /api/v2/statements` with `{"sql","catalog","schema","max_rows"}` SHALL submit a statement as the caller and answer `201` with a `Location` header and a status `{"id","state","queue_position","elapsed_ms","update_type","columns","total_rows","truncated","max_rows","first_page","error"}`. `state` SHALL be `queued`, `running`, `finished`, `failed` or `cancelled`. The id SHALL be the query-tracker id, so `/api/v2/queries/{id}` shows the same query. `submit` and `GET /statements/{id}` SHALL accept `wait_ms` (default 0, at most `max_wait_ms`) and SHALL NOT block longer; a statement that finishes inside the wait SHALL carry its first page inline. A statement that fails after submission SHALL be reported in a `200`/`201` status with `state: "failed"` and an `error`.

#### Scenario: TPC-H Q6
- **WHEN** alice submits Q6 and polls until it is terminal
- **THEN** it is `finished`, and its revenue equals the value psql returns over pgwire

#### Scenario: Session statements
- **WHEN** alice submits `USE datalake.tpch`
- **THEN** the response is `invalid_argument` with the hint `pass "catalog" and "schema" in the request body`

### Requirement: Cancel
`DELETE /api/v2/statements/{id}` SHALL cancel a queued or running statement and return its status; it SHALL be idempotent on a terminal statement. The query tracker SHALL record the query as cancelled.

#### Scenario: Cancel a long statement
- **WHEN** alice submits a long query, polls until it is `running`, and cancels it
- **THEN** the status is `cancelled`, and `/api/v2/queries/{id}` shows the query cancelled

### Requirement: Per-user statements
Only the submitting user SHALL poll, fetch or cancel a statement. Any other user SHALL get `404 not_found`, exactly as for an id that does not exist. A user SHALL hold at most 32 statements and the server 1024. At a cap, a submit SHALL evict the least-recently-accessed terminal statement (the user's own for the per-user cap), and SHALL answer `overloaded` only when every counted statement is still queued or running.

#### Scenario: Another user's statement
- **WHEN** bob polls alice's statement
- **THEN** the response is `404` with code `not_found`

### Requirement: Bounded results
A statement's query SHALL stop producing rows after `max_rows + 1` of them, and the statement SHALL keep at most `max_rows` rows. Its status SHALL report `truncated`, and `total_rows` as the row count when not truncated and `null` when truncated. `GET /statements/{id}/results?cursor=&max_bytes=` SHALL return `{"rows","next_cursor","truncated_cells"}`, at most `max_bytes` of encoded rows (at least one row), where `cursor` is the row offset and `next_cursor` is `null` after the last retained row. A cell longer than `max_cell_bytes` SHALL be cut on a character boundary, end with `…`, and be counted in `truncated_cells`; a string cell SHALL be measured and cut as its raw UTF-8 text and any other cell as its serialized JSON, and a cell that fits SHALL be returned unchanged and not counted. A request value above its ceiling (`max_rows`, `max_bytes`, `wait_ms`, `rows`) SHALL be refused with `invalid_argument`, never clamped.

#### Scenario: SELECT * with max_rows
- **WHEN** alice submits `SELECT * FROM lineitem` with `max_rows=100` and walks every page
- **THEN** `truncated` is true, `total_rows` is `null`, exactly 100 rows arrive across the pages, and the query produces no more than 101 rows

#### Scenario: Result that fits
- **WHEN** alice submits `SELECT * FROM nation` with `max_rows=100`
- **THEN** `truncated` is false and `total_rows` is 25

#### Scenario: Cell near the limit
- **WHEN** a result holds an 8191-byte string and a 9000-byte string with `max_cell_bytes` 8192
- **THEN** the first is returned unchanged, the second as its first 8192 bytes plus `…`, and `truncated_cells` is 1

### Requirement: Runtime limits
The limits SHALL be `[agent_api]` fields with `ARNEB_AGENT_*` environment overrides (`default_max_rows` 1000, `max_rows` 10000, `default_page_bytes` 262144, `max_page_bytes` 4194304, `max_cell_bytes` 8192, `max_wait_ms` 10000, `idle_timeout_secs` 600), with precedence env over file over default, and the effective values SHALL be logged once on `arneb::config` at startup.

#### Scenario: Env override
- **WHEN** `ARNEB_AGENT_MAX_ROWS=500` is set and the file says `max_rows = 2000`
- **THEN** the effective `max_rows` is 500

### Requirement: Idle reaper
After `idle_timeout_secs` with no request for a statement, the server SHALL drop a terminal statement and cancel a running one with the reason `abandoned`, releasing its admission slot. A statement SHALL NOT count as idle while a request waits on it, and the server SHALL refuse to start unless `idle_timeout_secs` is at least twice `max_wait_ms`, with an error naming both.

#### Scenario: Abandoned statement
- **WHEN** a running statement is not polled for `idle_timeout_secs`
- **THEN** it is cancelled as `abandoned` and its admission permit is released
