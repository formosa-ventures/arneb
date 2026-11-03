## ADDED Requirements

### Requirement: Asynchronous statements
`POST /api/v2/statements` with `{"sql","catalog","schema","max_rows"}` SHALL submit a statement as the caller and answer `201` with a `Location` header and a status `{"id","state","queue_position","elapsed_ms","update_type","columns","total_rows","truncated","truncated_by","max_rows","first_page","error"}`. `state` SHALL be `queued`, `running`, `finished`, `failed` or `cancelled`. `queue_position` SHALL be the statement's current admission-queue position (1 = next) at the time of the request, and `null` when it is not queued. The id SHALL be the query-tracker id, so `/api/v2/queries/{id}` shows the same query. `submit` and `GET /statements/{id}` SHALL accept `wait_ms` (default 0, at most `max_wait_ms`) and SHALL NOT block longer; a statement that finishes inside the wait SHALL carry its first page inline. A statement that fails after submission SHALL be reported in a `200`/`201` status with `state: "failed"` and an `error`.

#### Scenario: TPC-H Q6
- **WHEN** alice submits Q6 and polls until it is terminal
- **THEN** it is `finished`, and its revenue equals the value psql returns over pgwire

#### Scenario: Session statements
- **WHEN** alice submits `USE datalake.tpch`
- **THEN** the response is `invalid_argument` with the hint `pass "catalog" and "schema" in the request body`

### Requirement: Cancel
`DELETE /api/v2/statements/{id}` SHALL cancel a queued or running statement, keep it, and return its status; the query tracker SHALL record the query as cancelled. On a terminal statement it SHALL return the final status, omit `first_page`, remove the statement and release its retained bytes from the budget (once any in-flight page fetch of it finishes); the id SHALL answer `not_found` afterwards and the tracker state SHALL not change. A statement that ends on its own while the DELETE is cancelling it SHALL be treated as terminal: released, with its final status.

#### Scenario: Cancel a long statement
- **WHEN** alice submits a long query, polls until it is `running`, and cancels it
- **THEN** the status is `cancelled`, and `/api/v2/queries/{id}` shows the query cancelled

#### Scenario: Release a finished statement
- **WHEN** alice deletes a finished statement that retains rows
- **THEN** the reply is its `finished` status, the retained budget's used bytes drop by what it held, and a later poll answers `not_found`

#### Scenario: Delete racing completion
- **WHEN** alice deletes a statement she saw running, and it finishes before the cancel takes effect
- **THEN** the reply is its `finished` status and it is released exactly as a finished statement would be

### Requirement: Per-user statements
Only the submitting user SHALL poll, fetch or cancel a statement. Any other user SHALL get `404 not_found`, exactly as for an id that does not exist. A user SHALL hold at most 32 statements and the server 1024. At a cap, a submit SHALL evict the least-recently-accessed terminal statement (the user's own for the per-user cap), and SHALL answer `overloaded` only when every counted statement is still queued or running.

#### Scenario: Another user's statement
- **WHEN** bob polls alice's statement
- **THEN** the response is `404` with code `not_found`

### Requirement: Bounded results
A statement's query SHALL stop producing rows after `max_rows + 1` of them, and the statement SHALL keep at most `max_rows` rows. The statement SHALL also keep at most `max_retained_bytes` of Arrow memory (an allocation shared by several kept batches counted once), one user's statements together at most `max_user_retained_bytes`, and all statements together at most `max_total_retained_bytes`, released when a statement is deleted, evicted or reaped. Rows SHALL be retained as they are produced, on a standalone server and from a distributed root stage alike (a re-run after a lost worker SHALL start the collection over), with strings and binary values cut to what `max_cell_bytes` can show, and the bytes held while collecting SHALL never exceed these limits. A first row that does not fit as is SHALL be kept in its page form (oversized cells as their cut strings) when that fits, and otherwise no row SHALL be kept and `truncated_hint` SHALL say the first row alone was too large. When the user's share or the server budget is short, the server SHALL evict ended statements that retain rows only when the evictable ones (excluding any a request still holds, such as a page fetch or a poll) cover the shortfall, one at a time and only until it is covered, rechecking it before each eviction so a shortfall that went away meanwhile evicts nothing: only the user's own for the user's share, and for the server budget the user's own first, then others'; within each, fully read ones first, then least recently accessed. An evicted statement SHALL answer `not_found` with a message saying it was evicted and a hint. Rows past a limit that still refuses SHALL be dropped, still counted, and the statement SHALL still finish. Its status SHALL report `truncated`, `total_rows` as the produced row count when the query produced at most `max_rows` rows and `null` otherwise, and `truncated_by` as `max_rows`, `max_retained_bytes`, `user_budget` or `server_budget` (`null` when not truncated). `GET /statements/{id}/results?cursor=&max_bytes=` SHALL return `{"rows","next_cursor","truncated_cells"}`, at most `max_bytes` of encoded rows (at least one row), where `cursor` is the row offset and `next_cursor` is `null` after the last retained row. A cell longer than `max_cell_bytes` SHALL be cut on a character boundary, end with `…`, and be counted in `truncated_cells`; a string cell SHALL be measured and cut as its raw UTF-8 text and any other cell as its serialized JSON, and a cell that fits SHALL be returned unchanged and not counted. A request value above its ceiling (`max_rows`, `max_bytes`, `wait_ms`, `rows`) SHALL be refused with `invalid_argument`, never clamped.

#### Scenario: SELECT * with max_rows
- **WHEN** alice submits `SELECT * FROM lineitem` with `max_rows=100` and walks every page
- **THEN** `truncated` is true, `total_rows` is `null`, exactly 100 rows arrive across the pages, and the query produces no more than 101 rows

#### Scenario: Result that fits
- **WHEN** alice submits `SELECT * FROM nation` with `max_rows=100`
- **THEN** `truncated` is false and `total_rows` is 25

#### Scenario: Wide result over the byte limit
- **WHEN** a statement's 200 rows exceed `max_retained_bytes` before `max_rows`
- **THEN** it finishes with `truncated` true, `total_rows` 200 and `truncated_by` `max_retained_bytes`, and it never held more than `max_retained_bytes` while collecting

#### Scenario: One oversized value
- **WHEN** alice selects one row whose only cell is an 80 MB string, an 80 MB binary, or a list far over `max_retained_bytes`
- **THEN** the result has that one row with the cell cut to `max_cell_bytes`, exactly as a page of the whole value would show it, and `truncated_cells` is 1

#### Scenario: Eviction that cannot help
- **WHEN** the only evictable results cannot cover what a statement lacks
- **THEN** nothing is evicted and the statement finishes with `truncated_by` `user_budget` or `server_budget`

#### Scenario: One user fills their share
- **WHEN** alice keeps submitting wide statements and never releases them
- **THEN** each of hers evicts her own least recently used ended ones once her share is full, she never holds more than `max_user_retained_bytes`, and bob's statement still keeps its full result

#### Scenario: Server budget short
- **WHEN** retained results hold most of `max_total_retained_bytes` and a statement needs more
- **THEN** ended statements are evicted, fully read ones first and then the least recently accessed, until it fits; only when none is left does it finish with `truncated_by` `server_budget`

#### Scenario: Cell near the limit
- **WHEN** a result holds an 8191-byte string and a 9000-byte string with `max_cell_bytes` 8192
- **THEN** the first is returned unchanged, the second as its first 8192 bytes plus `…`, and `truncated_cells` is 1

### Requirement: Runtime limits
The limits SHALL be `[agent_api]` fields with `ARNEB_AGENT_*` environment overrides (`default_max_rows` 1000, `max_rows` 10000, `default_page_bytes` 262144, `max_page_bytes` 4194304, `max_cell_bytes` 8192, `max_retained_bytes` 67108864, `max_user_retained_bytes` 268435456, `max_total_retained_bytes` 1073741824, `max_wait_ms` 10000, `idle_timeout_secs` 600), with precedence env over file over default, and the effective values SHALL be logged once on `arneb::config` at startup.

#### Scenario: Env override
- **WHEN** `ARNEB_AGENT_MAX_ROWS=500` is set and the file says `max_rows = 2000`
- **THEN** the effective `max_rows` is 500

### Requirement: Idle reaper
After `idle_timeout_secs` with no request for a statement, the server SHALL drop a terminal statement and cancel a running one with the reason `abandoned`, releasing its admission slot. A statement SHALL NOT count as idle while a request waits on it, and the server SHALL refuse to start when `idle_timeout_secs` is shorter than `max_wait_ms`, with an error naming both.

#### Scenario: Abandoned statement
- **WHEN** a running statement is not polled for `idle_timeout_secs`
- **THEN** it is cancelled as `abandoned` and its admission permit is released
