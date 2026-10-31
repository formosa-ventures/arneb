# HTTP API

The Web port (pgwire port + 1000, for example `6432`) serves a JSON HTTP API
next to the Web UI. It is how a script or an agent operates a running server
without a browser or a shell: sign in, mint an API token, then list and
cancel queries and read cluster state.

- `/api/v2` is the stable, headless API. Every error uses one
  [error envelope](#errors).
- `/api/v1` is what the Web UI uses. Its paths and JSON shapes are unchanged;
  under password auth it now needs credentials too.

The same API token also works as the password for
[pgwire and the Trino listener](#tokens-as-passwords), so one credential
covers every protocol.

The same operations are also available as tools for AI agents over the
[Model Context Protocol](/guide/mcp), at `/mcp` on this port.

## Authentication

Authentication follows `[auth] type` (see
[Authentication](/guide/configuration#authentication)).

Under `type = "password"`, every route needs credentials except:

- `GET /api/v1/info`
- `GET /api/v2/health`
- `POST /api/v2/sessions`
- the static Web UI files (the page itself asks you to sign in)

`/metrics` also stays open unless `[web] metrics_auth = true` (or
`ARNEB_WEB_METRICS_AUTH=true`); see [Web UI](/guide/configuration#web-ui).

Send one of these on each request:

| Credential | Header | Notes |
|---|---|---|
| API token | `Authorization: Bearer arneb_…` | For scripts and agents. Revocable, optional expiry. |
| Session token | `Authorization: Bearer arneb_s_…` | For the Web UI. Lives 8 hours in the server's memory. |
| Password or API token | `Authorization: Basic <base64 user:password>` | The password may be an API token. |

A missing or invalid credential gets `401` with
`WWW-Authenticate: Bearer realm="arneb"`.

Under `type = "none"` there is nothing to prove. The caller is the user in
the `X-Arneb-User` header, or `anonymous`. The `queries` access rules are
not applied, and the session and token routes answer `auth_disabled`.

Use TLS in front of the Web port when it is reachable beyond the machine:
bearer tokens and Basic credentials are sent in the clear over plain HTTP.

### Sessions

A session is a short-lived login for browsers. Exchange a user and password
(not an API token) for a session token:

```bash
curl -s -X POST http://127.0.0.1:6432/api/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","password":"secret"}'
# {"token":"arneb_s_…","expires_ms":1767000000000}
```

Sessions are kept in memory, last 8 hours, and are lost when the coordinator
restarts. A user has at most 16 (a new one ends their oldest), and the
server holds at most 10,000. `DELETE /api/v2/sessions/current` with the
session token ends it. Session tokens never work on pgwire or Trino. A
token-shaped password at login is refused like a wrong password.

## API tokens

An API token is a credential for automation. It looks like
`arneb_<16 hex>_<43 base64url>`: the 16 hex characters are its id and the
rest is the secret.

```bash
# Mint a token with a session (or with Basic, or another token)
curl -s -X POST http://127.0.0.1:6432/api/v2/tokens \
  -H "Authorization: Bearer $SESSION" \
  -H 'Content-Type: application/json' \
  -d '{"name":"ci","expires_in_days":30}'
# {"token":"arneb_0123456789abcdef_…","id":"0123456789abcdef","name":"ci",
#  "created_ms":…,"expires_ms":…}
```

- The plaintext token is in this response only. The server stores a salted
  hash (the same SCRAM verifier format as a password), so it cannot show it
  again.
- `name` (at most 128 characters) and `expires_in_days` (an integer from 1
  to 3650) are optional. Without an expiry the token lives until revoked.
- A user can hold at most 100 live tokens. Revoke unused ones.
- A token minted with another token (or with Basic using a token) cannot
  outlive it: its expiry is clamped to the parent's, and the response says so
  in `hint`.
- Tokens belong to the caller. `GET /api/v2/tokens` lists only your own
  (id, name, created and expiry times, never the secret), and
  `DELETE /api/v2/tokens/{id}` revokes one of yours. Another user's id is
  `404`.
- Revocation is immediate on every protocol: pgwire, Trino and the Web API
  all refuse the token on the next request or connection. An already
  established pgwire connection is not closed: new connections and new
  requests are refused.
- Changing a user's `password_hash` in `[[auth.users]]`, or removing the
  user, revokes all of their tokens on the next start (see
  [State](/guide/configuration#state)).
- Tokens are stored in the [state file](/guide/configuration#state). With no
  `[state] path` they live in memory, the response carries
  `"persistent": false` and a hint, and they are lost when the coordinator
  restarts.

### Tokens as passwords

A token is accepted wherever the user's password is:

```bash
psql "host=127.0.0.1 port=5432 user=alice password=$TOKEN" -c 'select 1'
trino --server https://coordinator:8080 --user alice --password   # paste the token
curl -u "alice:$TOKEN" https://coordinator:8080/v1/info
```

On pgwire the token goes through the normal SCRAM exchange, so it never
crosses the network. On Trino the token is sent as a Basic password, so
password auth without `[tls]` is a startup error; disable the listener with
`--no-trino` if you only need pgwire (see
[Trino clients](/guide/trino-clients)).

## Errors

`/api/v2` errors share one body:

```json
{"error":{"code":"unauthenticated","message":"…","hint":"…"}}
```

`hint` says what to do next and is `null` when there is nothing to add.

| Code | HTTP | Meaning |
|---|---|---|
| `unauthenticated` | 401 | No valid credentials, or a wrong login. |
| `forbidden` | 403 | Authenticated, but the access rules deny it (killing a query). |
| `not_found` | 404 | No such route, query or token, or one you may not see. |
| `invalid_argument` | 400 (405 for a wrong method) | A malformed request. |
| `auth_disabled` | 409 | The route needs `[auth] type = "password"`. |
| `overloaded` | 503 | Too many pending password checks or sessions; retry (`Retry-After: 1`). |
| `internal` | 500 | An unexpected failure; the server log has the detail. |

The [querying](#querying) routes add the codes in
[Querying errors](#querying-errors).

`/api/v1` keeps its historical `400` and `404` bodies; its new `401` and
`403` responses use the envelope.

## Routes

All `/api/v2` routes need credentials, except `health` and `POST /sessions`.

| Method and path | Description |
|---|---|
| `GET /api/v2/health` | `{"status","version","auth"}`; `auth` is `none` or `password`. Open. |
| `POST /api/v2/sessions` | Body `{"user","password"}`. Returns `201 {"token","expires_ms"}`. Open. |
| `DELETE /api/v2/sessions/current` | Ends the session whose token made the request (`204`). |
| `GET /api/v2/whoami` | `{"user","via"}`; `via` is `token`, `session`, `basic` or `unauthenticated`. |
| `POST /api/v2/tokens` | Mint a token (see above). `201`. |
| `GET /api/v2/tokens` | `{"tokens":[{"id","name","created_ms","expires_ms"}]}` for the caller. |
| `DELETE /api/v2/tokens/{id}` | Revoke the caller's token (`204`). |
| `GET /api/v2/queries` | Queries the caller may view. Same parameters and body as `/api/v1/queries`. |
| `GET /api/v2/queries/{id}` | One query; `404` if missing or hidden. |
| `DELETE /api/v2/queries/{id}` | Cancel a query (`204`); `403` if the caller may view but not kill it. |
| `GET /api/v2/cluster` | Cluster overview. |
| `GET /api/v2/cluster/workers` | Workers and their state. |
| `GET /api/v2/info` | Server version and role. |

The [querying](#querying) routes (`catalogs`, `explain`, `statements`) are
listed in their own section.

The `queries`, `cluster` and `info` bodies are the `/api/v1` JSON; only the
errors differ. The `/api/v1` routes are `/api/v1/queries`,
`/api/v1/queries/{id}` (GET, DELETE), `/api/v1/cluster`,
`/api/v1/cluster/workers` and `/api/v1/info`.

`?state=` on the query list keeps the queries in exactly that state, one of
`queued`, `planning`, `starting`, `running`, `finishing`, `finished`,
`failed` or `cancelled` (any case). `running` does not include `planning`,
`starting` or `finishing`. An unknown state filters nothing.

Both versions apply the [`queries` access rules](/guide/access-control):
a query the caller may not `view` is hidden (`404`), one they may view but
not `kill` answers `403`, and a user may always kill their own.

## Querying

An agent or script can run SQL and explore the catalog over HTTP, with no
driver. The routes live on the coordinator's Web port (standalone or
coordinator; a worker answers `404`). They run as the authenticated user, with
exactly the visibility and privileges that user has over Trino or pgwire:
an object the user may not see answers like one that does not exist.

Statements are asynchronous. Submit one, optionally wait a moment for it,
then poll, fetch result pages, or cancel. The statement id is the query id,
so `GET /api/v2/queries/{id}` shows the same query with `source: "api"`.
Only the submitting user can poll, fetch or cancel a statement.

| Method and path | Description |
|---|---|
| `GET /api/v2/catalogs` | List catalogs. |
| `GET /api/v2/catalogs/{c}/schemas` | List schemas. |
| `GET /api/v2/catalogs/{c}/schemas/{s}/tables` | List tables. |
| `GET /api/v2/catalogs/{c}/schemas/{s}/tables/{t}?limit=` | Columns (with Trino types) and statistics. |
| `GET /api/v2/catalogs/{c}/schemas/{s}/tables/{t}/sample?rows=` | Sample rows, as a statement status (default 10, at most 100; never more than the server's `max_rows`). |
| `POST /api/v2/explain` | Plan a query without running it. |
| `POST /api/v2/statements` | Submit a statement. `201` with a `Location` header. |
| `GET /api/v2/statements/{id}?wait_ms=` | Poll a statement. |
| `GET /api/v2/statements/{id}/results?cursor=&max_bytes=` | Fetch a page of rows. |
| `DELETE /api/v2/statements/{id}` | Cancel a statement. Idempotent. |

The three listings take `?prefix=` (case-insensitive) and `?limit=`, and
return `{"items":[{"name"}],"total","truncated"}`. Path segments are used
literally (percent-decoded), with no case folding.

A table description is `{"catalog","schema","table","columns","truncated",
"statistics"}`. `columns` holds at most `?limit=` columns in table order
(default `default_max_rows`, ceiling `max_rows`, the same bounds as the
listings), and `truncated` is `true` when columns beyond the limit were
dropped.

### Submit and poll

```json
POST /api/v2/statements?wait_ms=2000
{"sql":"SELECT l_orderkey, l_quantity FROM lineitem","catalog":"datalake","schema":"tpch","max_rows":100}
```

`catalog`, `schema` and `max_rows` are optional. The API is stateless per
statement, so session statements (`USE`, `SET SESSION`, `PREPARE`, ...) are
refused with `invalid_argument`; pass `catalog` and `schema` in the body
instead. `SHOW` works.

The reply is a status:

```json
{"id":"…","state":"finished","queue_position":null,"elapsed_ms":41,
 "update_type":null,
 "columns":[{"name":"l_orderkey","type":"bigint"},{"name":"l_quantity","type":"bigint"}],
 "total_rows":2,"truncated":false,"truncated_by":null,"max_rows":100,
 "first_page":{"rows":[[1,17],[2,36]],"…":"…"},"error":null}
```

`state` is `queued`, `running`, `finished`, `failed` or `cancelled`.
`queue_position` is the statement's current place in the admission queue
(1 = next), read at each poll, and `null` unless it is queued. A
statement that finishes inside `wait_ms` carries its first page inline;
otherwise poll with `GET /statements/{id}?wait_ms=` (long-poll) until it is
terminal, then fetch pages with `GET /statements/{id}/results?cursor=N`,
where `cursor` is the row offset (default 0).

A statement that fails after submission is still `200`/`201`: the status has
`state: "failed"` and an `error` object. HTTP error statuses are for the
request itself.

DDL and DML (`CREATE`, `INSERT`, `DELETE`, ...) are not supported over this
API in this release: they fail with an error, as on the Trino path. Writes
will come with write-privilege enforcement in a later release.

### Explain

```bash
curl -s -X POST http://127.0.0.1:6432/api/v2/explain \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT count(*) FROM lineitem","catalog":"datalake","schema":"tpch"}'
# {"plan":"…","plan_truncated":false,
#  "scans":[{"table":"datalake.tpch.lineitem","row_count":6001215,"size_bytes":…}],
#  "estimated_scan_bytes":…}
```

Explain plans only: it is not admitted or tracked, and does not run
`ANALYZE`. Submit `EXPLAIN ANALYZE ...` as a statement for that. A size is
`null` when the connector does not know it. The plan text is cut at
`default_page_bytes` (on a character boundary) and `plan_truncated` says
so; `scans` keeps one entry per table scan.

### Limits and truncation

Every response that returns data is bounded by rows and by bytes. When
something is cut, the response says so (`truncated`) and gives the full
count when it is known. A statement keeps at most `max_rows` rows. A cell longer than `max_cell_bytes`
is cut on a character boundary and ends with `…`, and the page counts it in
`truncated_cells`. A string is measured as its UTF-8 text (JSON quoting and
escapes do not count); any other value is measured as its JSON. A page holds as many rows as fit in `max_bytes`, but always at
least one, so a single wide row can make a page larger than `max_bytes`
(each of its cells is still capped at `max_cell_bytes`).

`max_rows` also bounds execution. A query stops producing rows once it has
`max_rows + 1` of them (one extra, to tell whether there are more), so a
`SELECT *` over a large table with `max_rows=100` reads only what it needs
for 101 rows, and an `ORDER BY` keeps the top rows instead of sorting
everything. The cost is that a truncated result has no exact count:
`total_rows` is the row count when `truncated` is `false`, and `null` when it
is `true`. To count a large result, run `SELECT count(*)`. Aggregations,
joins and sorts still read all their input; only the rows they emit are
capped.

Retained rows are also bounded by memory: a statement keeps at most
`max_retained_bytes` of Arrow data, and all statements together at most
`max_total_retained_bytes`. Rows past either are dropped as they are kept;
the statement still finishes, with `truncated: true` and `total_rows: null`.
`truncated_by` says which limit cut the result, so an agent can react:

| `truncated_by` | Cause | What to do |
|---|---|---|
| `max_rows` | More than `max_rows` rows. | Add `LIMIT`, a filter or an aggregate, or raise `max_rows`. |
| `max_retained_bytes` | The kept rows reached the per-statement byte limit. | Select fewer or narrower columns. |
| `server_budget` | Retained results across the server hit the global budget. | Retry later; results free up as statements are evicted or reaped. |
| `null` | Nothing was cut. | |

A request value above its ceiling is refused with `invalid_argument` and a
hint naming the ceiling; it is never silently clamped. That includes
`wait_ms`, `max_rows`, `max_bytes`, `rows` and `limit`. The one server-side cap is the
sample: `rows` (or its default of 10) is lowered to the server's `max_rows`
when that is smaller, so a small configured ceiling shortens a sample instead
of failing it. A malformed JSON body or
query parameter is also `invalid_argument`.

| `[agent_api]` field | Env var | Default |
|---|---|---|
| `default_max_rows` | `ARNEB_AGENT_DEFAULT_MAX_ROWS` | 1000 |
| `max_rows` | `ARNEB_AGENT_MAX_ROWS` | 10000 |
| `default_page_bytes` | `ARNEB_AGENT_DEFAULT_PAGE_BYTES` | 262144 |
| `max_page_bytes` | `ARNEB_AGENT_MAX_PAGE_BYTES` | 4194304 |
| `max_cell_bytes` | `ARNEB_AGENT_MAX_CELL_BYTES` | 8192 |
| `max_retained_bytes` | `ARNEB_AGENT_MAX_RETAINED_BYTES` | 67108864 (64 MiB) |
| `max_total_retained_bytes` | `ARNEB_AGENT_MAX_TOTAL_RETAINED_BYTES` | 1073741824 (1 GiB) |
| `max_wait_ms` | `ARNEB_AGENT_MAX_WAIT_MS` | 10000 |
| `idle_timeout_secs` | `ARNEB_AGENT_IDLE_TIMEOUT_SECS` | 600 |

A user may hold 32 statements and the server 1024. At a cap, a new
submission evicts the least-recently-accessed statement that has ended
(finished, failed or cancelled; the user's own for the per-user cap), which
from then on answers `not_found`. So an unfetched finished result can be
lost once the user submits 32 newer statements. When every held statement
is still queued or running, the submission is refused with `overloaded`.
After `idle_timeout_secs` with no request for a statement, a finished one
is dropped and a running one is cancelled as abandoned. A statement with a
request long-polling it is never idle, and `idle_timeout_secs` must be at
least twice `max_wait_ms`.

### Querying errors

Same envelope as above, with two optional fields: `location` (`{"line","column"}`
in the SQL) and `candidates` (the nearest existing names).

| Code | HTTP | Meaning |
|---|---|---|
| `syntax_error` | 400 | The SQL does not parse. |
| `catalog_not_found` | 404 | No such catalog, or one you may not see. |
| `schema_not_found` | 404 | No such schema, or one you may not see. |
| `table_not_found` | 404 | No such table, or one you may not see. |
| `column_not_found` | 400 | No such column; `candidates` suggests names. |
| `function_not_found` | 400 | No such function. |
| `type_mismatch` | 400 | Operand types do not fit. |
| `not_supported` | 400 | The engine does not support the statement. |
| `query_failed` | 400 | Any other error while running the statement. |
| `exceeded_memory` | 400 | The statement exceeded its memory limit. |
| `exceeded_time` | 400 | The statement exceeded its time limit. |
| `cancelled` | 409 | The statement was cancelled. |

`forbidden`, `overloaded`, `invalid_argument`, `not_found` and `internal`
are reused. For a statement, these codes appear in the status `error`
object (HTTP 200/201); for explain, discovery and a bad request, they are the
HTTP error.

### Worked session

```bash
H='Authorization: Bearer arneb_…'
B=http://127.0.0.1:6432/api/v2

curl -s -H "$H" "$B/catalogs/datalake/schemas/tpch/tables?prefix=line"
curl -s -H "$H" "$B/catalogs/datalake/schemas/tpch/tables/lineitem"

# Submit, waiting up to 2 s for the first page
curl -si -H "$H" -H 'Content-Type: application/json' -X POST "$B/statements?wait_ms=2000" \
  -d '{"sql":"SELECT l_returnflag, count(*) FROM datalake.tpch.lineitem GROUP BY 1"}'
# HTTP/1.1 201 Created
# Location: /api/v2/statements/0b9f…
# {"id":"0b9f…","state":"running",…}

# Still running: long-poll, then fetch pages
curl -s -H "$H" "$B/statements/0b9f…?wait_ms=10000"
curl -s -H "$H" "$B/statements/0b9f…/results?cursor=0&max_bytes=65536"

# Changed your mind
curl -s -H "$H" -X DELETE "$B/statements/0b9f…"
```

## Security notes

- **Revocation does not cascade.** Revoking a token does not revoke tokens
  that were minted with it. Revoke those by id too.
- **Timing can hint at a token count.** A pgwire SCRAM exchange tries the
  password and each live token, so its timing can reveal roughly how many
  tokens a user has. It does not reveal any secret.
- **Token-shaped config passwords.** A password in `[[auth.users]]` that
  looks like a token (`arneb_` plus 16 hex, `_`, 43 base64url characters)
  works on pgwire but not as Trino Basic, which treats token-shaped
  passwords as tokens. Do not choose such a password.
- **No login lockout.** Repeated wrong passwords are not locked out.
  Concurrent PBKDF2 checks are bounded (a burst gets `overloaded`), which
  limits the cost, not the guessing. Use long passwords, or tokens, and
  rate-limit at your proxy.
- **Plaintext tokens** appear only in the issue response. The server never
  logs them.
- **The Web port is plain HTTP.** Terminate TLS in front of it when it
  leaves the host.
