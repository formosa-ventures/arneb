# MCP Server

The Web port (pgwire port + 1000, for example `6432`) serves a
[Model Context Protocol](https://modelcontextprotocol.io) endpoint at
`/mcp`. An MCP client such as Claude Code, Cursor or the MCP Inspector can
explore the catalog, plan and run SQL, and read results as tools, without a
driver and without a shell on the server.

Every tool is the MCP form of an [HTTP API](/guide/http-api) route. A tool
runs as the authenticated user, with the same visibility, access rules,
admission control and limits as the matching `/api/v2` route. The
coordinator and standalone nodes serve `/mcp`; a worker has no Web port.

## Connect

Mint an [API token](/guide/http-api#api-tokens), then register the server.
With Claude Code:

```bash
claude mcp add --transport http arneb https://<host>/mcp \
  --header "Authorization: Bearer $ARNEB_TOKEN"
```

Any client that speaks streamable HTTP and can send a header works the same
way: point it at `https://<host>/mcp` and set
`Authorization: Bearer arneb_…`. Basic credentials and Web UI session tokens
are accepted too.

- **The Web port is plain HTTP.** The bearer token crosses the network in
  the clear. For anything beyond the same machine, put a TLS-terminating
  proxy in front of the Web port and connect to its `https://` address.
- **OAuth-only clients are not supported yet.** Clients that insist on an
  OAuth flow, such as claude.ai custom connectors, have to wait for H6 of
  the headless agent interface. Use a client that accepts a bearer header.

Under `[auth] type = "none"` there is nothing to prove: the caller is the
`X-Arneb-User` header or `anonymous`, and `/mcp` accepts only loopback
`Host` values by default (see [Host allow-list](#host-allow-list)).

## Tools

Eighteen tools. The server also sends `instructions` on connect, which tell
the agent the intended order: discover, describe or sample, explain, submit,
poll, fetch, cancel.

| Tool | Arguments | Returns | Same as |
|---|---|---|---|
| `whoami` | none | `{user, via, version, role, auth}` | `GET /whoami`, `/info`, `/health` |
| `list_catalogs` | `prefix?`, `limit?` | names, `total`, `truncated` | `GET /catalogs` |
| `list_schemas` | `catalog`, `prefix?`, `limit?` | same | `GET /catalogs/{c}/schemas` |
| `list_tables` | `catalog`, `schema`, `prefix?`, `limit?` | same | `GET …/tables` |
| `describe_table` | `catalog`, `schema`, `table`, `limit?` | columns, `truncated`, row count, size | `GET …/tables/{t}` |
| `sample_table` | `catalog`, `schema`, `table`, `rows?` | a finished statement with a first page | `GET …/sample` |
| `explain_query` | `sql`, `catalog?`, `schema?` | plan text, `plan_truncated`, scan estimates | `POST /explain` |
| `submit_query` | `sql`, `catalog?`, `schema?`, `max_rows?`, `wait_ms?` | statement status | `POST /statements` |
| `get_query_status` | `id`, `wait_ms?` | statement status | `GET /statements/{id}` |
| `fetch_results` | `id`, `cursor?`, `max_bytes?` | a page of rows and `next_cursor` | `GET /statements/{id}/results` |
| `cancel_query` | `id` | statement status after cancelling | `DELETE /statements/{id}` |
| `list_queries` | `state?`, `user?`, `min_elapsed_ms?`, `limit?` | `{queries, truncated}` | `GET /queries`, bounded |
| `get_query` | `id` | one tracked query | `GET /queries/{id}` |
| `get_cluster` | none | role, workers, admission counters | `GET /cluster` |
| `list_workers` | none | `{workers: [...]}` | `GET /cluster/workers` |
| `list_tokens` | none | your live tokens, never the secret | `GET /tokens` |
| `create_token` | `name?`, `expires_in_days?` | the new token, shown once | `POST /tokens` |
| `revoke_token` | `id` | `{revoked: id}` | `DELETE /tokens/{id}` |

The result of a tool is its `structuredContent`: the same JSON body as the
matching route (an array is wrapped in an object). Every argument has a
description in the tool's input schema.

Annotations tell a client which calls are safe to auto-approve. All tools set
`openWorldHint = false`. `whoami`, the `list_*` and `get_*` tools,
`describe_table`, `sample_table`, `explain_query` and `fetch_results` are
read-only. `cancel_query` and `revoke_token` are destructive and idempotent.
`submit_query` and `create_token` carry no read-only hint.

### Typical session

1. `list_catalogs`, `list_schemas`, `list_tables` to find the table.
2. `describe_table` for columns, `sample_table` for a few real rows.
3. `explain_query` to check the SQL and see how much it would scan.
4. `submit_query` with `wait_ms` (up to 10000). A short query comes back
   finished with its first page.
5. If it is `queued` or `running`, `get_query_status` with `wait_ms` until it
   is `finished`, `failed` or `cancelled`.
6. A finished status carries `first_page`. Start `fetch_results` from
   `first_page.next_cursor` and repeat with each page's `next_cursor` until
   it is null (a null `first_page.next_cursor` means you already have every
   row).
7. `cancel_query` for a statement that is no longer needed.

A statement that fails after it was submitted, including a SQL syntax error,
is a normal result whose status has `state: "failed"` and an `error` object.
Only a bad request is an error result.

## Limits

Results are bounded the way the [HTTP API](/guide/http-api#limits-and-truncation)
bounds them, and a value above a ceiling is refused with `invalid_argument`,
never clamped. The ceilings come from `[agent_api]`.

| Argument | Default | Ceiling | Source |
|---|---|---|---|
| `limit` of `list_catalogs`, `list_schemas`, `list_tables` | 1000 | 10000 | `[agent_api] default_max_rows`, `max_rows` |
| `limit` of `describe_table` (columns) | 1000 | 10000 | `[agent_api] default_max_rows`, `max_rows` |
| plan text of `explain_query` | 32768 bytes, cut and flagged `plan_truncated` | none | `[mcp] default_page_bytes` |
| `rows` of `sample_table` | 10 | 100 | fixed |
| `max_rows` of `submit_query` | 1000 | 10000 | `[agent_api] default_max_rows`, `max_rows` |
| `wait_ms` | 0 | 10000 | ceiling is `[agent_api] max_wait_ms` |
| `max_bytes` of `fetch_results` | 32768 | 4194304 | default is `[mcp] default_page_bytes`, ceiling is `[agent_api] max_page_bytes` |
| `limit` of `list_queries` | 50 | 500 (0 is refused) | fixed |

The tool descriptions, their argument descriptions and the server
instructions quote the configured numbers: they are filled from
`[agent_api]` and `[mcp] default_page_bytes` when the server starts, so
what an agent reads is what the server enforces. The table shows the
defaults.

Differences from the HTTP routes:

- **Smaller pages.** An agent pays for every byte of a result in its
  context, so the default page is `[mcp] default_page_bytes` (32768 bytes)
  rather than the 256 KiB of `/api/v2`. It applies to `fetch_results` when
  `max_bytes` is omitted, and to the inline first page of `submit_query`,
  `get_query_status`, `sample_table` and `cancel_query`. Ask for more with `max_bytes`, up to
  the `[agent_api] max_page_bytes` ceiling. The budget counts the bytes of
  the rows only. The status fields, the `columns` list and the text copy of
  the JSON that the MCP library adds to every result come on top, so a full
  page roughly doubles in what the agent receives.
- **Plans fit the page budget.** `explain_query` cuts the plan at the same
  `[mcp] default_page_bytes` budget (`/api/v2/explain` keeps
  `[agent_api] default_page_bytes`). Like every tool result, the text block
  repeats the structured JSON, so clients that only pass text content to the
  model still see the plan.
- **Bounded history.** `list_queries` returns the newest queries first, at
  most `limit`, and sets `truncated` when more matched. Each query's `state`
  is lowercase (`queued`, `running`, `finished`, ...), unlike
  `/api/v2/queries`, which keeps `Finished`. The `state` filter is one of
  the eight states the list reports, `queued`, `planning`, `starting`,
  `running`, `finishing`, `finished`, `failed` or `cancelled` (any case), and
  keeps exactly that state: `running` does not include `planning`,
  `starting` or `finishing`. Another value is `invalid_argument`. `user` keeps one owner's queries and
  `min_elapsed_ms` keeps only slow ones. The `sql` of a query in this tool and
  in `get_query` is cut at `max_cell_bytes` and flagged `sql_truncated`.
- **Cancel is for your statements.** `cancel_query` takes a statement id from
  `submit_query` and works like `DELETE /api/v2/statements/{id}`. Killing
  another user's query is not an MCP tool.
- **Tokens need password auth.** `create_token`, `list_tokens` and
  `revoke_token` answer `auth_disabled` under `type = "none"`. A token that
  mints a token cannot outlive its parent: the expiry is clamped and the
  result's `hint` says so.

## Errors

A failed call is a tool result with `isError: true` whose `structuredContent`
is the `/api/v2` [error envelope](/guide/http-api#errors), with the same
codes:

```json
{"error":{"code":"column_not_found","message":"…","hint":"…",
          "location":{"line":1,"column":8},"candidates":["l_quantity"]}}
```

`hint` says what to do next, and `candidates` lists the nearest existing
names for a misspelled table or column, so an agent can fix the call and
retry.

JSON-RPC errors are only for protocol faults, such as an unknown method or a
malformed JSON-RPC message. Bad tool arguments (a missing required field, a
wrong type) also come back as a tool result with `isError: true`, with a
text message that names the field.

## Configuration

`[mcp]` is on by default. See the
[configuration guide](/guide/configuration#mcp) for the full table.

```toml
[mcp]
enabled            = true
allowed_hosts      = ["db.internal"]   # omit for the default
allowed_origins    = ["https://app.example:443"]   # omit to refuse any Origin
default_page_bytes = 32768
```

With `enabled = false` (or `ARNEB_MCP_ENABLED=false`) `/mcp` is not served
and answers `404`, under password auth too (it asks for no credentials). The effective values are logged once on `arneb::config`
at startup.

### Host allow-list

`/mcp` checks the request's `Host` header against `allowed_hosts` to stop
DNS-rebinding attacks from a browser page.

- Under password auth the default is any host: a cross-site page cannot
  attach the bearer token, so rebinding gains nothing.
- Under `type = "none"` the default is loopback only (`localhost`,
  `127.0.0.1`, `::1`). A remote client then needs `allowed_hosts` set.
- Entries are `host` or `host:port`. A `*` anywhere in the list means any
  host. An empty list is a startup error.

### Origin check

`/mcp` also checks the browser `Origin` header, as the MCP transport
requires. A request without `Origin` (a CLI or desktop MCP client) always
passes. By default every request that carries an `Origin` is refused with
`403`, so a web page cannot drive `/mcp` from a visitor's browser. To allow a
browser-based client, list its origins in `allowed_origins`: each entry has a
scheme, as in `https://app.example:443`, `http://localhost:*` (any port) or
`null`. An entry without a port currently matches any port; give the port
explicitly.

## Security

- **Tool results are kept out of debug logs.** The MCP library logs every
  tool result at `debug`, and a result can hold rows or a new API token from
  `create_token`, and it logs three `info` lines for every request. Arneb
  therefore caps the `rmcp` log target at `warn`, even under
  `RUST_LOG=debug`; its warnings (a rejected `Origin` or `Host`) still log.
  A directive on an rmcp module, such as `RUST_LOG=debug,rmcp::transport=warn`,
  keeps the cap for the rest of rmcp and still applies to that module. Only
  a directive whose target is exactly `rmcp` (for example
  `RUST_LOG=info,rmcp=debug`) lifts the cap. That is an explicit opt-in:
  at `debug` rmcp logs tool results, new API tokens included, as does
  naming its `rmcp::service` module at `debug`. Do either only on a
  machine whose logs are as private as the tokens.
- **Host and Origin checks** stop DNS rebinding and cross-site calls; see
  [Host allow-list](#host-allow-list) and [Origin check](#origin-check).
- **Plain HTTP.** Put TLS in front of the Web port for remote clients (see
  [Connect](#connect)).

## Stateless behavior

The endpoint keeps no MCP session. Every `POST /mcp` is independent and
answers with a JSON body, so a coordinator restart or a load balancer never
strands a client, and no session id is issued. There is no
server-sent stream: `GET /mcp` answers `405 Method Not Allowed`
(`Allow: POST`) whatever its `Accept` header says, which the streamable HTTP
transport permits. The server never pushes notifications.

Statements created through `submit_query` do live in coordinator memory,
exactly as with the HTTP API: they are lost on restart, and an idle one is
reaped after `[agent_api] idle_timeout_secs`.

A request with no credentials, or with a bad one, gets HTTP `401` with
`WWW-Authenticate: Bearer realm="arneb"` before any MCP handling.
