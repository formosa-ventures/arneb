## ADDED Requirements

### Requirement: MCP endpoint and authentication
The coordinator (or standalone server) SHALL serve an MCP streamable-HTTP endpoint at `/mcp` on the Web port. Under `[auth] type = "password"` every request to `/mcp` and its subpaths SHALL carry a Bearer API token, a Bearer session token or Basic credentials, and a missing or invalid credential SHALL get HTTP `401` with `WWW-Authenticate: Bearer realm="arneb"` before any JSON-RPC handling. Under `type = "none"` the caller SHALL be the `X-Arneb-User` header, or `anonymous`. The server SHALL report `serverInfo.name = "arneb"` and its version on `initialize`. Workers SHALL NOT serve `/mcp`.

#### Scenario: Missing credential
- **WHEN** password auth is on and a client posts `initialize` to `/mcp` without credentials
- **THEN** the response is `401` with `WWW-Authenticate: Bearer realm="arneb"`

#### Scenario: Token identity
- **WHEN** alice calls `whoami` with her API token
- **THEN** the result has `user` `alice`, `via` `token` and `auth` `password`

#### Scenario: Revoked token
- **WHEN** a token is revoked and used again on `/mcp`
- **THEN** the response is `401`

#### Scenario: Claimed user without auth
- **WHEN** auth is `none` and `whoami` is called with `X-Arneb-User: bob`, then without the header
- **THEN** the users are `bob` and `anonymous`, with `via` `unauthenticated`

### Requirement: Stateless transport
The endpoint SHALL keep no MCP session: each `POST /mcp` SHALL be handled independently and answered with a JSON body, and no session id SHALL be issued. `GET /mcp` SHALL NOT open a stream and SHALL answer `405`.

#### Scenario: GET is not a stream
- **WHEN** an authenticated client sends `GET /mcp` with `Accept: text/event-stream`
- **THEN** the response is not `200` and carries no JSON-RPC result

### Requirement: Disabled endpoint
With `[mcp] enabled = false`, `/mcp` and its subpaths SHALL NOT be served and SHALL answer `404`.

#### Scenario: Disabled
- **WHEN** MCP is disabled and a client posts `initialize` to `/mcp`
- **THEN** the response is `404` and carries no JSON-RPC body

### Requirement: Host validation
`/mcp` SHALL accept a request only when its `Host` header matches `[mcp] allowed_hosts` (`host` or `host:port`). A `*` anywhere in the list SHALL allow any host. When `allowed_hosts` is unset, any host SHALL be allowed under password auth and only `localhost`, `127.0.0.1` and `::1` under `type = "none"`. `allowed_hosts = []` SHALL stop startup, and an empty `ARNEB_MCP_ALLOWED_HOSTS` SHALL count as unset.

#### Scenario: Loopback default without auth
- **WHEN** auth is `none`, `allowed_hosts` is unset, and a request has `Host: evil.example`
- **THEN** the tool does not run

#### Scenario: Explicit list under password auth
- **WHEN** `allowed_hosts = ["db.internal"]` and a request has `Host: arneb.example.com`
- **THEN** the tool does not run, and `Host: db.internal:6432` is accepted

#### Scenario: Star among other hosts
- **WHEN** `allowed_hosts = ["a.example", "*"]`
- **THEN** any host is accepted

### Requirement: Identity-scoped tools
Every tool SHALL run as the request's authenticated identity and SHALL call the same function, with the same validation, as the matching `/api/v2` route, so visibility, access rules, admission and limits are those of the route. The server SHALL expose exactly these tools: `cancel_query`, `create_token`, `describe_table`, `explain_query`, `fetch_results`, `get_cluster`, `get_query`, `get_query_status`, `list_catalogs`, `list_queries`, `list_schemas`, `list_tables`, `list_tokens`, `list_workers`, `revoke_token`, `sample_table`, `submit_query` and `whoami`.

#### Scenario: Same body as the route
- **WHEN** alice calls `list_catalogs`, `get_cluster` and `list_workers`
- **THEN** the results equal the bodies of `GET /api/v2/catalogs`, `/cluster` and `/cluster/workers` (the workers array wrapped in `{"workers": …}`)

#### Scenario: Another user's statement
- **WHEN** bob calls `get_query_status` with a statement id that alice submitted
- **THEN** the result is an error with code `not_found`

### Requirement: Discovery tools
`list_catalogs`, `list_schemas` and `list_tables` SHALL return the H2 listing body for the objects the caller may see, `describe_table` the columns and statistics, `sample_table` a finished statement with a first page (default 10 rows, at most 100), and `explain_query` the plan and scan estimates without running the query.

#### Scenario: Explain does not execute
- **WHEN** alice calls `explain_query` for a query over a table
- **THEN** the result has a plan and the scan, and no statement is created

### Requirement: Statement tools
`submit_query` SHALL start a statement and wait up to `wait_ms` (at most 10000) before answering with its status; `get_query_status` SHALL return the status of the caller's statement, optionally waiting; `fetch_results` SHALL return a page of rows from `cursor` with `next_cursor`; `cancel_query` SHALL cancel the caller's statement and be idempotent. A statement that fails after submit, including a syntax error, SHALL be a normal status with `state` `failed` and an `error` object.

#### Scenario: Submit, poll and fetch all rows
- **WHEN** alice submits a query over 50,000 rows with `max_rows` 10000, polls it to `finished`, and calls `fetch_results` repeatedly following `next_cursor`
- **THEN** exactly 10000 rows are read, the status reports `truncated`, and the last page has a null `next_cursor`

#### Scenario: Cancel a running statement
- **WHEN** alice cancels a running statement
- **THEN** its state becomes `cancelled`, and cancelling again keeps it so

#### Scenario: Failure after submit
- **WHEN** alice submits a statement with a syntax error
- **THEN** the call succeeds and the status has `state` `failed` and an `error`

### Requirement: Bounded results
Every tool result SHALL be bounded. A tool page SHALL default to `[mcp] default_page_bytes` bytes (32768) when `max_bytes` is omitted, and the inline first page of `submit_query`, `get_query_status` and `sample_table` SHALL follow it too. A request value above its ceiling (`limit`, `rows`, `max_rows`, `wait_ms`, `max_bytes`) SHALL be refused with `invalid_argument` and never clamped. Long cells SHALL be truncated at `max_cell_bytes`.

#### Scenario: Default page is the MCP budget
- **WHEN** a result of 50,000 ~100-byte rows is fetched without `max_bytes`
- **THEN** the page holds at most `default_page_bytes` of rows (and at least one row), and `next_cursor` is set

#### Scenario: Ceiling refused
- **WHEN** a tool is called with `rows` 101, `max_rows` 10001, `wait_ms` 10001 or `max_bytes` above `[agent_api] max_page_bytes`
- **THEN** the result is an error with code `invalid_argument`

### Requirement: Bounded query history
`list_queries` SHALL return the queries the caller may view, newest first, at most `limit` (default 50), with `truncated` true when more matched. `limit` of 0 or above 500 SHALL be `invalid_argument`. `state` SHALL be one of `queued`, `running`, `finished`, `failed` or `cancelled` in any case, and another value SHALL be `invalid_argument`. `user` and `min_elapsed_ms` SHALL filter. The `sql` of each query in `list_queries` and `get_query` SHALL be cut at `max_cell_bytes` on a character boundary with `sql_truncated` true when cut.

#### Scenario: Newest first and truncated
- **WHEN** more queries exist than `limit` allows
- **THEN** the newest `limit` are returned in descending creation order and `truncated` is true

#### Scenario: Slow-query view
- **WHEN** `list_queries` is called with `min_elapsed_ms`
- **THEN** only queries at least that slow are returned

#### Scenario: Long SQL
- **WHEN** a query's text is longer than `max_cell_bytes`
- **THEN** `sql` is cut and `sql_truncated` is true

#### Scenario: View rules apply
- **WHEN** the `queries` rules let alice view only her own queries
- **THEN** `list_queries` omits bob's, and `get_query` for bob's returns `not_found`

### Requirement: Operations tools
`get_cluster` SHALL return the `/api/v2/cluster` body and `list_workers` the alive workers as `{"workers": […]}`.

#### Scenario: Cluster
- **WHEN** alice calls `get_cluster`
- **THEN** the role and worker count are those of `GET /api/v2/cluster`

### Requirement: Token tools
`create_token`, `list_tokens` and `revoke_token` SHALL manage the caller's own API tokens exactly like the `/api/v2/tokens` routes. They SHALL answer `auth_disabled` under `[auth] type = "none"`. A token minted with another token SHALL NOT outlive it: its expiry SHALL be clamped and the result's `hint` SHALL say so. `list_tokens` SHALL NOT return secrets, and another user's token id SHALL be `not_found`.

#### Scenario: Round trip
- **WHEN** alice creates a token, lists tokens, and revokes it
- **THEN** the token is listed without its secret, works as a Bearer credential, and is refused with `401` after the revoke

#### Scenario: Clamp
- **WHEN** a 1-day token creates a 30-day token
- **THEN** the new token expires no later than its parent and the hint contains "clamped"

#### Scenario: No auth
- **WHEN** auth is `none` and `create_token` is called
- **THEN** the result is an error with code `auth_disabled`

### Requirement: Tool errors
A failed tool call SHALL be a tool result with `isError: true` whose `structuredContent` is the `/api/v2` error envelope `{"error": {"code","message","hint","location"?,"candidates"?}}` with the same codes. Arguments that fail schema validation SHALL also be an `isError` result naming the field. JSON-RPC errors SHALL be reserved for protocol faults.

#### Scenario: Bad column
- **WHEN** a query names a column that does not exist
- **THEN** the error has code `column_not_found`, a `location`, and `candidates` that include the nearest column

#### Scenario: Missing argument
- **WHEN** `describe_table` is called without `table`
- **THEN** the result has `isError` true and its text names `table`

### Requirement: Agent-facing copy
The server SHALL send `instructions` of at most 1500 characters that name the workflow (discover, describe or sample, explain, submit, poll, fetch, cancel), the result caps and how to use an error's `hint` and `candidates`. Every tool description SHALL be 40 to 600 characters and SHALL name the follow-up tool where the workflow has one, and every input property SHALL have a description. Tools SHALL carry the annotations: `readOnlyHint` on `whoami`, `list_*`, `get_*`, `describe_table`, `sample_table`, `explain_query` and `fetch_results`; `destructiveHint` and `idempotentHint` on `cancel_query` and `revoke_token`; `openWorldHint` false on all.

#### Scenario: Tool list contract
- **WHEN** a client calls `tools/list`
- **THEN** the names are exactly the eighteen tools, every description and property is documented, `submit_query` names `get_query_status` and `fetch_results`, and the annotations match

#### Scenario: Instructions
- **WHEN** a client calls `initialize`
- **THEN** `instructions` is at most 1500 characters and names the workflow tools in order

### Requirement: Configuration
`[mcp]` SHALL accept `enabled` (default true), `allowed_hosts` and `default_page_bytes` (default 32768), with `ARNEB_MCP_ENABLED`, `ARNEB_MCP_ALLOWED_HOSTS` (comma-separated) and `ARNEB_MCP_DEFAULT_PAGE_BYTES` overriding the file. An unknown key, a `default_page_bytes` of 0 or above `[agent_api] max_page_bytes`, an empty `allowed_hosts` list or an invalid boolean SHALL stop startup. The effective values SHALL be logged once on `arneb::config`.

#### Scenario: Defaults
- **WHEN** the config has no `[mcp]` section
- **THEN** MCP is enabled, `allowed_hosts` is unset and `default_page_bytes` is 32768

#### Scenario: Environment wins
- **WHEN** the file sets `default_page_bytes = 1000` and `ARNEB_MCP_DEFAULT_PAGE_BYTES=4096`
- **THEN** the effective value is 4096

#### Scenario: Page budget above the ceiling
- **WHEN** `default_page_bytes` exceeds `[agent_api] max_page_bytes`
- **THEN** startup fails with an error naming both
