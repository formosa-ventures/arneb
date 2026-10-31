## Context

Stage H3 of the headless agent interface, built on H1 (`headless-api-auth`: identity per request, API tokens, error envelope) and H2 (`agent-query-api`: `AgentQueryService` and the `/api/v2` agent routes). The tools add no new engine path: each one calls the function its route calls.

## Goals / Non-Goals

**Goals**
- A stock MCP client connects with a URL and a bearer header and can explore, query and manage its own tokens.
- A tool has exactly the rights of the matching route: same identity, access rules, admission, bounds.
- Results fit an agent's context: small default pages, ceilings, truncation always reported.
- The copy an agent reads (instructions, descriptions, schemas) is part of the tested contract.

**Non-Goals**
- OAuth (H6) and any MCP feature beyond tools: resources, prompts, sampling, notifications.
- Server-sent streams and MCP sessions.
- Killing another user's query, and DDL/DML (H4).
- TLS on the Web port.

## Decisions

**D1. Stateless.** No MCP sessions (`NeverSessionManager`, `legacy_session_mode = false`, `json_response = true`): every POST is independent, so a coordinator restart or a load balancer never strands a client. `GET /mcp` answers `405` with `Allow: POST`, which streamable HTTP allows. Alternative: stateful sessions; rejected because they pin a client to one coordinator process for no benefit, as no tool pushes notifications.

**D2. Host validation.** `[mcp] allowed_hosts` is a list of `host` or `host:port` (`ARNEB_MCP_ALLOWED_HOSTS`, comma-separated). A `*` anywhere in the list means any host. `allowed_hosts = []` is a startup error, and an empty `ARNEB_MCP_ALLOWED_HOSTS` counts as unset. Unset means any host under password auth (a cross-site page cannot attach the bearer token, so DNS rebinding gains nothing) and loopback (`localhost`, `127.0.0.1`, `::1`) under `type = "none"`. The effective value is logged as `allowed_hosts=any` or the list. Blank entries are dropped from the file list as from the env value, and a list with no entry left falls back to the default at resolve time too (the second line behind validation).

**D2b. Origin validation.** The MCP transport requires servers to validate `Origin`. `[mcp] allowed_origins` (`ARNEB_MCP_ALLOWED_ORIGINS`, comma-separated) lists accepted origins as rmcp parses them: each entry has a scheme (`scheme://host[:port]`, `:*` for any port) or is `null`; others, and an empty list, stop startup. Unset means rmcp's `enforce_origin_validation` with an empty list: every request that carries an `Origin` is refused, and requests without one (CLI and desktop clients) pass. The service always enforces, so an empty list never disables the check. Logged as `allowed_origins=none` or the list.

**D2c. rmcp debug logging.** rmcp logs every tool result at debug, so `RUST_LOG=debug` would print rows and `create_token` secrets. The tracing filter appends `rmcp=warn` after the base filter (the same composition as `--profile`) unless `RUST_LOG` has a directive whose target is exactly `rmcp` (`rmcp=debug`, `rmcp[span]=debug`), which counts as the operator opting in. A directive on a module (`rmcp::transport=warn`) does not lift the cap: the cap keeps the rest of rmcp, `rmcp::service` with its tool-result logging included, at `warn`, while tracing still applies the module directive because the most specific directive wins. Naming `rmcp::service` at debug is itself an opt-in for that module, and the guide says so. `warn` rather than `info` because rmcp logs three info lines (session start, stream end, serve end) for every stateless request; its rejected-Origin and rejected-Host events are warnings and stay.

**D3. Enabled by default** on coordinator and standalone (`[mcp] enabled = true`, `ARNEB_MCP_ENABLED`). Disabled, or on a node without the agent service, `/mcp` and `/mcp/*` answer `404` (an explicit handler, so they never fall through to the Web UI fallback). `requires_auth` exempts an unserved `/mcp`, so under password auth it is still `404`, never a `401` challenge that would start a client's credential flow against an endpoint that does not exist. Workers have no Web port.

**D4. MCP page budget.** `[mcp] default_page_bytes` = 32768 (`ARNEB_MCP_DEFAULT_PAGE_BYTES`), required to be in `1..=[agent_api] max_page_bytes`. `fetch_results` uses it when `max_bytes` is omitted; `submit_query`, `get_query_status`, `sample_table` and `cancel_query` pass it to the service as the page budget (`page_bytes`) of the inline `first_page`, which is then the page `results(cursor 0, max_bytes = default_page_bytes)` would return. The page is built once, at that budget; the HTTP routes pass none and get `[agent_api] default_page_bytes`. `/api/v2` defaults are unchanged. Reason: a 256 KiB default page is about 60k tokens of an agent's context.

**D5. Tool names and shapes** (stable; `structuredContent` is the `/api/v2` JSON body, wrapped in an object where the route returns an array):

| Tool | Arguments | Result | Same as |
|---|---|---|---|
| `whoami` | none | `{user, via, version, role, auth}` | `/whoami` + `/info` + `/health.auth` |
| `list_catalogs` | `prefix?, limit?` | listing body | `GET /catalogs` |
| `list_schemas` | `catalog, prefix?, limit?` | listing body | `GET /catalogs/{c}/schemas` |
| `list_tables` | `catalog, schema, prefix?, limit?` | listing body | `GET …/tables` |
| `describe_table` | `catalog, schema, table, limit?` | describe body (`columns` at most `limit`, `truncated`) | `GET …/tables/{t}?limit=` |
| `sample_table` | `catalog, schema, table, rows?` | statement status | `GET …/sample` |
| `explain_query` | `sql, catalog?, schema?` | explain body (`plan` cut at `[mcp] default_page_bytes`, `plan_truncated`) | `POST /explain` |
| `submit_query` | `sql, catalog?, schema?, max_rows?, wait_ms?` | statement status | `POST /statements` |
| `get_query_status` | `id, wait_ms?` | statement status | `GET /statements/{id}` |
| `fetch_results` | `id, cursor?, max_bytes?` | result page | `GET /statements/{id}/results` |
| `cancel_query` | `id` | statement status after cancel | `DELETE /statements/{id}` |
| `list_queries` | `state?, user?, min_elapsed_ms?, limit?` | `{queries, truncated}` | `GET /queries`, bounded (D6) |
| `get_query` | `id` | query body | `GET /queries/{id}` |
| `get_cluster` | none | cluster body | `GET /cluster` |
| `list_workers` | none | `{workers: [...]}` | `GET /cluster/workers` |
| `list_tokens` | none | tokens body | `GET /tokens` |
| `create_token` | `name?, expires_in_days?` | issued body | `POST /tokens` |
| `revoke_token` | `id` | `{revoked: id}` | `DELETE /tokens/{id}` |

A statement that fails after submit, including a syntax error, is a normal status with `state: "failed"` and an `error` object (as on the HTTP route). Bad tool arguments (a missing field, a wrong type) are rejected by the MCP library as an `isError` text result naming the field, not as a JSON-RPC error.

**D6. Bounded query history.** `list_queries` returns newest first (`created_ms` descending), at most `limit` (default 50; `0` or above 500 is `invalid_argument`), after the `queries` view rules. `state` is one of the eight states the list reports, `queued|planning|starting|running|finishing|finished|failed|cancelled` (case-insensitive, exactly that state, so `running` excludes `planning`, `starting` and `finishing`; anything else is `invalid_argument` with all eight in the hint, unlike `/api/v1`, which ignores it). One parser serves both lists; `user` filters by owner; `min_elapsed_ms` keeps queries at least that slow (the slow-query view). `truncated` is true when more matched. Ties in `created_ms` are ordered by query id, so the order and the cut are stable. `state` in the result is lowercase, the spelling the filter and the statement API use. Each query's `sql`, here and in `get_query`, is cut at `max_cell_bytes` bytes on a character boundary with `sql_truncated: true` when cut. `/api/v2/queries` is unchanged.

**D7. `cancel_query` cancels an agent statement,** the caller's own, by statement id, like `DELETE /api/v2/statements/{id}`. Killing any query by tracker id under the `queries` kill rule is H4 and is not a tool here.

**D8. Annotations.** `read_only_hint = true` on `whoami`, all `list_*`, `describe_table`, `sample_table`, `explain_query`, all `get_*` and `fetch_results`. `destructive_hint = true` and `idempotent_hint = true` on `revoke_token` and `cancel_query`. `open_world_hint = false` on all. `submit_query` and `create_token` carry no read-only hint.

**D9. Server info and instructions.** `serverInfo.name = "arneb"`, `version = env!("CARGO_PKG_VERSION")`. `instructions` is copy of at most 1500 characters that tells the agent the workflow: discover, `describe_table`/`sample_table`, `explain_query`, `submit_query` with `wait_ms`, `get_query_status` until terminal, `fetch_results` from `first_page.next_cursor` while `next_cursor` is not null, `cancel_query` when a statement is no longer needed; results are capped by `max_rows` and `truncated` is reported; on an error, read `hint` and `candidates` and retry. Every description says what the tool returns, its limits, and the next tool to call, and every input property has a description (from the arguments struct's doc comments). The numbers in the instructions, the descriptions and the property descriptions (row, wait, page and listing limits) are `{name}` placeholders filled from `[agent_api]` and `[mcp] default_page_bytes` when the service is built, never hard-coded, so an agent that follows the text is never refused. The service builds the filled `ToolRouter` and instructions once and clones them into each stateless request instead of rebuilding the 18 tools per POST. A contract test pins all of this, and another checks that non-default limits appear in the copy and the defaults do not.

**D10. Token tools need password auth.** Under `type = "none"` they answer `auth_disabled`, like the routes. `create_token` applies the same expiry clamp as the route when the caller authenticated with a token; the presenting credential is read from the request headers carried in the request parts.

## Risks / Trade-offs

- [A client that needs OAuth cannot connect] -> a bearer header is required until H6; documented.
- [The Web port is plain HTTP] -> documented: terminate TLS in a proxy for remote use.
- [Statements live in coordinator memory, so an MCP client loses them on restart] -> same as the HTTP API; the agent resubmits.
- [A stateless server cannot push progress] -> tools return a status and the agent polls with `wait_ms`.
- [Some results are still not bounded in width] -> known gap, not fixed in H3: a statement status carries every result column in `columns`, and a page always holds at least one row, so one row wider than the page budget makes a page larger than `max_bytes` (each cell is still capped at `max_cell_bytes`). `describe_table` (`limit`) and `explain_query` (plan cut at `[mcp] default_page_bytes`) are bounded.
- [The MCP library's version drives the wire format] -> pinned in the workspace; the contract test and the router tests exercise real JSON-RPC.

## Migration Plan

Additive. `/mcp` is on by default; set `[mcp] enabled = false` (or `ARNEB_MCP_ENABLED=false`) to turn it off. Roll back by deploying the previous release.

## Open Questions

None for H3. OAuth is H6; writes and cross-user cancel are H4.
