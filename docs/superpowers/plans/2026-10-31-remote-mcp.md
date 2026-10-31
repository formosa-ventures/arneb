# Remote MCP (H3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The coordinator serves a Model Context Protocol endpoint at `/mcp` on the Web port that exposes the H1 identity/token API, the H2 query API and the observe layer as tools written for agents, under the same authentication and access rules as `/api/v2`.

**Architecture:** `rmcp` 3.8 (`StreamableHttpService`, stateless: `legacy_session_mode = false`, `json_response = true`, `NeverSessionManager`) is mounted with `nest_service("/mcp", …)` inside `web::build_router`, so the existing `auth::authenticate` middleware runs first and puts `Identity` in the request extensions; each tool reads it from `RequestContext.extensions → http::request::Parts → extensions`. Tools call the same functions the `/api/v2` handlers call (`AgentQueryService`, `api::queries_response`/`visible_query`/`cluster_response`/`workers_response`, token functions extracted from `v2.rs`), so MCP cannot do anything the API cannot. Errors become tool results with `isError: true` and the `/api/v2` error envelope as structured content.

**Tech Stack:** Rust, axum 0.8, rmcp 3.8.0 (`default-features = false`, features `server`, `macros`, `transport-streamable-http-server`), schemars 1.

**Spec:** `docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md` (stage H3; Goals 2–3, Anti-goals, Surfaces). Builds on H1 (`docs/superpowers/plans/2026-10-30-headless-api-auth.md`) and H2 (`docs/superpowers/plans/2026-10-31-agent-query-api.md`).

**Verified by a spike** (scratchpad, not committed): with the config above, `initialize`, `tools/list` and `tools/call` work as plain JSON POSTs without a session; an `axum::middleware::from_fn` layer outside `nest_service` reaches the tool through `ctx.extensions.get::<http::request::Parts>()`; `CallToolResult::structured(json)` yields `structuredContent` plus a text copy; `CallToolResult::structured_error(json)` yields `isError: true`; a bad argument yields an `isError` result naming the field; `#[tool(annotations(read_only_hint = true))]` appears in `tools/list`. Macros: `#[tool_router] impl X { #[tool(description = "…")] async fn f(&self, Parameters(args): Parameters<Args>, ctx: RequestContext<RoleServer>) -> Result<CallToolResult, ErrorData> }`, `#[tool_handler(router = self.tool_router)] impl ServerHandler for X { fn get_info(&self) -> ServerConfig }`. rmcp's default `allowed_hosts` is loopback only; `disable_allowed_hosts()` and `with_allowed_hosts(..)` change it.

## Global Constraints

- Every tool runs as the request's `Identity` and goes through the same function the matching `/api/v2` route uses. No tool reads the tracker, state store or catalogs in a way its route does not (Anti-goal "Unequal rights").
- Every tool result is bounded: listings take `limit` with a ceiling, pages take `max_bytes` with a ceiling, long text fields are cut at `max_cell_bytes` (8192 by default) and say so. A request value above its ceiling is refused with `invalid_argument`, never clamped (same rule as H2).
- `/mcp` needs credentials under `[auth] type = "password"` exactly like `/api/v2` (Bearer API token, Bearer session token, or Basic). A missing or bad credential is HTTP `401` with `WWW-Authenticate: Bearer realm="arneb"`, before any JSON-RPC handling. Under `type = "none"` the caller is `X-Arneb-User` or `anonymous`, as on the API.
- Tool errors are tool results with `isError: true` and `structuredContent = {"error": {"code","message","hint","location"?,"candidates"?}}`, the `/api/v2` envelope with the same codes. JSON-RPC errors are only for protocol faults (unknown method, malformed JSON-RPC).
- Runtime knobs: `[mcp]` fields with `ARNEB_MCP_*` overrides, precedence env > file > default, effective values logged once on `arneb::config` at startup (CLAUDE.md config rule).
- Library-style code in the server crate uses `thiserror`/`ApiError`, never `anyhow`, inside `web/`. `tracing` for logs. Doc comments on public items.
- Dependency: `rmcp = { version = "3.8.0", default-features = false, features = ["server", "macros", "transport-streamable-http-server"] }` and `schemars = "1"` in `[workspace.dependencies]`; `cargo deny check` stays clean.
- Do not touch the untracked `docker/tpch-bench/` and `openspec/changes/selective-dim-first-gating/`. Never `git stash`.
- Tier 1 for every task: `cargo fmt -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` with the raw `test result:` lines in the report.

## Decisions

- **D1. Stateless.** No MCP sessions (`NeverSessionManager`, `legacy_session_mode = false`, `json_response = true`): every POST is independent, so a coordinator restart or a load balancer never strands a client. `GET /mcp` (server-sent stream) answers `405`, which streamable HTTP allows.
- **D2. Host validation.** `[mcp] allowed_hosts` (list of `host` or `host:port`; `ARNEB_MCP_ALLOWED_HOSTS`, comma-separated, `*` = any). Default when unset: any host under password auth (a cross-site page cannot attach the bearer token, so DNS rebinding gains nothing), loopback (`localhost`, `127.0.0.1`, `::1`) under `type = "none"`. Logged as `allowed_hosts=any` or the list.
- **D3. Enabled by default** on coordinator and standalone (`[mcp] enabled = true`, `ARNEB_MCP_ENABLED`). Disabled → `/mcp` is not mounted (falls to the frontend fallback, `404` for POST). Workers have no Web port.
- **D4. MCP page budget.** `[mcp] default_page_bytes` = 32768 (`ARNEB_MCP_DEFAULT_PAGE_BYTES`), must be in `1..=agent_api.max_page_bytes`. `fetch_results` uses it when `max_bytes` is omitted; `submit_query` and `get_query_status` replace the inline `first_page` with `results(cursor 0, max_bytes = default_page_bytes)`. `/api/v2` defaults are unchanged. Reason: a 256 KiB default page is ~60k tokens of an agent's context.
- **D5. Tool names and shapes** (stable; the `structuredContent` is the `/api/v2` JSON body, wrapped in an object where the route returns an array):

| Tool | Arguments | Result | Same as |
|---|---|---|---|
| `whoami` | — | `{user, via, version, role, auth}` | `/whoami` + `/info` + `/health.auth` |
| `list_catalogs` | `prefix?, limit?` | listing body | `GET /catalogs` |
| `list_schemas` | `catalog, prefix?, limit?` | listing body | `GET /catalogs/{c}/schemas` |
| `list_tables` | `catalog, schema, prefix?, limit?` | listing body | `GET …/tables` |
| `describe_table` | `catalog, schema, table` | describe body | `GET …/tables/{t}` |
| `sample_table` | `catalog, schema, table, rows?` | statement status | `GET …/sample` |
| `explain_query` | `sql, catalog?, schema?` | explain body | `POST /explain` |
| `submit_query` | `sql, catalog?, schema?, max_rows?, wait_ms?` | statement status | `POST /statements` |
| `get_query_status` | `id, wait_ms?` | statement status | `GET /statements/{id}` |
| `fetch_results` | `id, cursor?, max_bytes?` | result page | `GET /statements/{id}/results` |
| `cancel_query` | `id` | statement status after cancel | `DELETE /statements/{id}` |
| `list_queries` | `state?, user?, min_elapsed_ms?, limit?` | `{queries, truncated}` | `GET /queries` (bounded, see D6) |
| `get_query` | `id` | query body | `GET /queries/{id}` |
| `get_cluster` | — | cluster body | `GET /cluster` |
| `list_workers` | — | `{workers: [...]}` | `GET /cluster/workers` |
| `list_tokens` | — | tokens body | `GET /tokens` |
| `create_token` | `name?, expires_in_days?` | issued body | `POST /tokens` |
| `revoke_token` | `id` | `{revoked: id}` | `DELETE /tokens/{id}` |

- **D6. Bounded query history.** `list_queries` returns newest first (`created_ms` descending), at most `limit` (default 50, at most 500), after the `queries` view rules; `state` is one of `queued|running|finished|failed|cancelled` (case-insensitive; anything else is `invalid_argument`, unlike `/api/v1` which ignores it); `user` filters by owner; `min_elapsed_ms` keeps queries at least that slow (the slow-query view). `truncated` is true when more matched. Each query's `sql` (here and in `get_query`) is cut at `max_cell_bytes` characters' worth of bytes on a char boundary, with `sql_truncated: true` when cut. `/api/v2/queries` is unchanged in this branch.
- **D7. `cancel_query` cancels an agent statement** (the caller's own, by statement id), like `DELETE /api/v2/statements/{id}`. Killing any query by tracker id with the `queries` kill rule is H4 and is not a tool here.
- **D8. Annotations.** `read_only_hint = true` on `whoami`, all `list_*`, `describe_table`, `sample_table`, `explain_query`, `get_*`, `fetch_results`. `destructive_hint = true, idempotent_hint = true` on `revoke_token` and `cancel_query`. `open_world_hint = false` on all. `submit_query` and `create_token` carry no read-only hint.
- **D9. Server info and instructions.** `serverInfo.name = "arneb"`, `version = env!("CARGO_PKG_VERSION")`. `instructions` (fixed copy, Task 4) tells the agent the workflow: discover → `describe_table`/`sample_table` → `explain_query` → `submit_query` (with `wait_ms`) → `get_query_status` until terminal → `fetch_results` while `next_cursor` is not null → `cancel_query` when no longer needed; results are capped by `max_rows` and `truncated` is reported; on an error, read `hint` and `candidates` and retry.
- **D10. Token tools need password auth.** Under `type = "none"` they return `auth_disabled`, like the routes. `create_token` applies the same expiry clamp as the route when the caller authenticated with a token (the presenting credential comes from the request headers in `Parts`).

## Review Focus

1. **A credential that is valid for `/api/v2` but presented to `/mcp`** (Bearer token, session token, Basic, a token as Basic password) must authenticate identically; a revoked token is `401` on the very next `/mcp` POST. (Task 1 tests.)
2. **Another user's statement id passed to `get_query_status`, `fetch_results` or `cancel_query`** must be `not_found`, the same as the API. (Task 2 test.)
3. **A tool argument above its ceiling** (`limit` 501, `max_bytes` above `max_page_bytes`, `wait_ms` above `max_wait_ms`, `rows` above the sample ceiling) is an `isError` result with `invalid_argument` and a hint naming the ceiling, not a clamp. (Tasks 2–3 tests.)
4. **A `Host` header not in the allow-list** under `type = "none"` is refused before any tool runs; any host is accepted under password auth with the default config. (Task 1 test.)
5. **A query history of thousands of entries with very long SQL** returns at most `limit` entries with each `sql` cut and flagged, and `truncated: true`. (Task 3 test.)

---

### Task 1: `/mcp` endpoint, config, authentication and `whoami`

**Files:**
- Modify: `Cargo.toml` (workspace deps), `crates/server/Cargo.toml`
- Modify: `crates/server/src/config.rs` (new `McpConfig`, `[mcp]`, `apply_env`, validation)
- Modify: `crates/server/src/main.rs` (effective-value log next to the `agent_api` one; pass config into `WebState`)
- Create: `crates/server/src/web/mcp/mod.rs` (service construction, `ArnebMcp` handler, `get_info`, identity extraction, result/error helpers)
- Create: `crates/server/src/web/mcp/tools.rs` (the `#[tool_router]` impl; `whoami` in this task)
- Modify: `crates/server/src/web/mod.rs` (mount; `WebState.mcp: Option<McpSettings>` or equivalent)
- Modify: `crates/server/src/web/auth.rs` (`requires_auth` covers `/mcp` and `/mcp/…`)

**Interfaces:**
- Produces: `pub struct McpConfig { pub enabled: bool, pub allowed_hosts: Option<Vec<String>>, pub default_page_bytes: usize }` (`#[serde(default, deny_unknown_fields)]`, defaults `true`, `None`, `32768`), `McpConfig::apply_env()`, `McpConfig::validate(&self, max_page_bytes: usize) -> Result<(), String>`.
- Produces: `pub(super) fn mcp_service(state: WebState) -> StreamableHttpService<ArnebMcp, NeverSessionManager>`; `ArnebMcp { state: WebState, tool_router: ToolRouter<Self> }`.
- Produces for Tasks 2–3: `fn identity(ctx: &RequestContext<RoleServer>) -> Result<Identity, ErrorData>` (missing `Identity` = internal error, never anonymous), `fn ok<T: Serialize>(body: &T) -> Result<CallToolResult, ErrorData>` (structured), `fn tool_error(e: ApiError) -> CallToolResult` (`structured_error` with `{"error": …}`, the same serialization as the HTTP envelope).

- [ ] **Step 1: Write the failing tests** (in `web/mcp/mod.rs` tests, driving `build_router(state)` with `tower::ServiceExt::oneshot`; a helper `rpc(router, auth: Option<&str>, host, body) -> (StatusCode, HeaderMap, serde_json::Value)` sends `POST /mcp` with `Content-Type: application/json`, `Accept: application/json, text/event-stream`, `MCP-Protocol-Version: 2025-06-18`):
  - `initialize_reports_arneb`: `initialize` → `result.serverInfo.name == "arneb"`, `version == env!("CARGO_PKG_VERSION")`, `capabilities.tools` present, `instructions` non-empty.
  - `mcp_requires_credentials_under_password_auth`: no `Authorization` → `401` with `WWW-Authenticate: Bearer realm="arneb"`; wrong Basic → `401`.
  - `whoami_runs_as_the_token_user`: Bearer API token of `alice` → `tools/call whoami` → `structuredContent.user == "alice"`, `via == "token"`, `auth == "password"`; Basic `alice:secret` → `via == "basic"`; session token → `via == "session"`.
  - `revoked_token_is_refused_on_next_call`: revoke via the state store, next POST → `401`.
  - `auth_none_uses_claimed_user`: `type = "none"`, `X-Arneb-User: bob` → `user == "bob"`, `via == "unauthenticated"`; no header → `anonymous`.
  - `host_allow_list`: auth none, default config, `Host: evil.example` → not `200` (rmcp's host rejection) and the tool did not run; `Host: 127.0.0.1:6432` → `200`. Password auth, default config, `Host: arneb.example.com` → `200`. Password auth with `allowed_hosts = ["db.internal"]` → `Host: arneb.example.com` refused.
  - `disabled_mcp_is_not_mounted`: `enabled = false` → `POST /mcp` is `404` or `405` and the body is not JSON-RPC.
  - Config tests in `config.rs`: `[mcp]` parses with defaults; `ARNEB_MCP_ENABLED=false`, `ARNEB_MCP_ALLOWED_HOSTS="a.example, b.example:8443"`, `ARNEB_MCP_ALLOWED_HOSTS="*"` (any), `ARNEB_MCP_DEFAULT_PAGE_BYTES=4096` override the file; `default_page_bytes = 0` and `> max_page_bytes` fail validation; unknown `[mcp]` key fails to parse.
- [ ] **Step 2: Run them, confirm they fail** (`cargo test -p arneb-server mcp`), paste the failing lines.
- [ ] **Step 3: Implement.** Config struct + env (+ effective log: `enabled`, `allowed_hosts=any|<list>`, `default_page_bytes`, target `arneb::config`, message `mcp effective settings ([mcp] / ARNEB_MCP_*)`); host resolution per D2 at service construction; mount per D3 inside `build_router` *before* the `authenticate` layer is applied (so the layer wraps it). `get_info` per D9 with a placeholder `instructions` string (final copy in Task 4).
- [ ] **Step 4: Run tests, fmt, clippy, workspace tests; `cargo deny check` clean** (paste its summary line).
- [ ] **Step 5: Commit** `feat(web): serve MCP at /mcp with the Web port's authentication`.

### Task 2: Query and discovery tools

**Files:**
- Modify: `crates/server/src/web/mcp/tools.rs`
- Modify: `crates/server/src/web/agent.rs` (move the shared bits the tools need — e.g. `wait(service, wait_ms)` validation and `From<AgentError> for ApiError` already exists — to `pub(super)` so tools reuse them; no behavior change)

**Interfaces:**
- Consumes: Task 1 helpers; `AgentQueryService` (`list_catalogs`, `list_schemas`, `list_tables`, `describe_table`, `sample`, `explain`, `submit`, `status`, `results`, `cancel` — exact signatures in `crates/protocol/src/agent/{service,discovery,explain}.rs`).
- Produces: tools `list_catalogs`, `list_schemas`, `list_tables`, `describe_table`, `sample_table`, `explain_query`, `submit_query`, `get_query_status`, `fetch_results`, `cancel_query` per D5, argument structs deriving `Deserialize + schemars::JsonSchema` with a `///` doc comment on every field (it becomes the field's schema description).

- [ ] **Step 1: Write the failing tests** (memory catalog with a 50,000-row table `big` and a small table, alice and bob):
  - `discovery_tools_match_the_api`: for each listing tool and `describe_table`, the `structuredContent` equals the body of the matching `/api/v2` GET for the same user (call both through the same router).
  - `submit_poll_fetch_walks_all_rows`: `submit_query {sql:"SELECT n FROM big", max_rows: 1000, wait_ms: 5000}` → state `finished`, `truncated: true`, `total_rows: null`; inline `first_page` is at most `default_page_bytes` (32768) of encoded rows (assert `next_cursor` is non-null and rows < 1000); walking `fetch_results` from `next_cursor` collects exactly 1000 rows.
  - `fetch_results_default_page_is_mcp_budget`: omitted `max_bytes` → page JSON of the rows ≤ 32768 bytes (plus one row of slack); `max_bytes` above `max_page_bytes` → `isError`, `invalid_argument`, hint names the ceiling.
  - `other_users_statement_is_not_found`: bob calls `get_query_status`, `fetch_results`, `cancel_query` on alice's id → each `isError`, `code == "not_found"`.
  - `bad_column_error_carries_location_and_candidates`: `submit_query {sql:"SELECT nn FROM big", wait_ms: 5000}` → the status has `state: "failed"` and `error.code == "column_not_found"`, `error.candidates` contains `"n"`, `error.location.line == 1` (a failure after submit is a status, not an `isError` result, exactly like the API's `200`); a syntax error at submit time that the API answers with an error status code is `isError` with `syntax_error`.
  - `cancel_query_cancels_a_running_statement`: a statement over a blocking test source (reuse the H2 service tests' pattern) → `cancel_query` → status `cancelled`.
  - `ceilings_are_refused`: `wait_ms` above `max_wait_ms`, `max_rows` above `max_rows`, `limit` above `max_rows` on `list_tables`, `rows` above the sample ceiling → each `isError` `invalid_argument`.
  - `explain_query_does_not_execute`: tracker count unchanged after `explain_query`; result has the plan text.
- [ ] **Step 2: Run, confirm failures.**
- [ ] **Step 3: Implement** each tool as: `identity(&ctx)?` → the same service call the route makes → `ok(&body)` or `Ok(tool_error(e.into()))`. `submit_query`/`get_query_status` re-page `first_page` per D4. Descriptions: one or two sentences each saying what it returns, its limits, and the next tool to call (final copy reviewed in Task 4).
- [ ] **Step 4: Tier 1 commands.**
- [ ] **Step 5: Commit** `feat(mcp): query and discovery tools over the agent query API`.

### Task 3: Observe and token tools

**Files:**
- Modify: `crates/server/src/web/mcp/tools.rs`
- Modify: `crates/server/src/web/v2.rs` (extract `issue_token`'s body into `pub(super) async fn issue_token_for(state: &WebState, who: &Identity, headers: &HeaderMap, req: IssueTokenRequest) -> Result<IssuedTokenResponse, ApiError>`, likewise `list_tokens_for` and `revoke_token_for`; the route handlers become thin wrappers; existing H1 tests must pass unchanged)
- Modify: `crates/server/src/web/api.rs` only if a helper must become `pub(super)`

**Interfaces:**
- Consumes: `api::queries_response`, `api::visible_query`, `api::cluster_response`, `api::workers_response`, `api::info_response`, the extracted token functions.
- Produces: tools `whoami` (completed with `version`, `role`, `auth`), `list_queries`, `get_query`, `get_cluster`, `list_workers`, `list_tokens`, `create_token`, `revoke_token`.

- [ ] **Step 1: Write the failing tests:**
  - `list_queries_is_bounded_newest_first`: 600 tracked queries (insert through the tracker), `list_queries {}` → 50 entries, `created_ms` non-increasing, `truncated: true`; `limit: 500` → 500; `limit: 501` → `invalid_argument`; `state: "bogus"` → `invalid_argument`; `state: "FINISHED"` filters.
  - `list_queries_follows_view_rules`: with a `queries` rule letting alice view only her own, alice's list has no bob query and `get_query` on bob's id is `not_found`.
  - `slow_queries_filter`: entries with elapsed 10 ms and 5000 ms, `min_elapsed_ms: 1000` → only the slow one.
  - `long_sql_is_cut_and_flagged`: a query whose SQL is 20,000 bytes of multi-byte text → `sql` ≤ `max_cell_bytes` bytes, valid UTF-8, `sql_truncated: true`; a short one has `sql_truncated: false`.
  - `cluster_and_workers_match_the_api`: `get_cluster` equals `GET /api/v2/cluster`; `list_workers.workers` equals `GET /api/v2/cluster/workers`.
  - `token_tools_round_trip`: `create_token {name:"ci", expires_in_days: 1}` → `token` starts with `arneb_`; `list_tokens` shows id `ci`; the new token authenticates `/mcp`; `revoke_token {id}` → `{revoked: id}`; that token is then `401`; another user's id → `not_found`; under auth none every token tool is `auth_disabled`.
  - `create_token_with_token_is_clamped`: caller authenticated with a token expiring in 1 day asks for 30 → `expires_ms` ≤ parent's, `hint` present (same as the route's test).
- [ ] **Step 2: Run, confirm failures.**
- [ ] **Step 3: Implement** per D5–D7, D10.
- [ ] **Step 4: Tier 1 commands.**
- [ ] **Step 5: Commit** `feat(mcp): observe and token tools`.

### Task 4: Agent-facing copy, tool-list contract and docs

**Files:**
- Modify: `crates/server/src/web/mcp/{mod.rs,tools.rs}` (final `instructions` and descriptions)
- Create: `docs/guide/mcp.md`; Modify: `docs/.vitepress/config.ts` (sidebar entry after HTTP API), `docs/guide/http-api.md` (one paragraph linking MCP), `docs/guide/configuration.md` (`[mcp]` section with the knob table), `README.md` only if it lists interfaces
- Create: `openspec/changes/remote-mcp/` (`proposal.md`, `design.md` with D1–D10, `specs/remote-mcp/spec.md` with SHALL requirements + scenarios for: endpoint/auth, stateless, host validation, each tool group, bounded results, error results, config; `tasks.md`); `openspec validate remote-mcp --strict` passes

- [ ] **Step 1: Write the failing contract test** `tool_list_contract`: `tools/list` names equal the D5 set exactly (sorted); every description is 40–600 characters and names the follow-up tool where D9's workflow has one (`submit_query` mentions `get_query_status` and `fetch_results`; `get_query_status` mentions `fetch_results`; `list_tables` mentions `describe_table`); every input-schema property has a `description`; annotations match D8.
- [ ] **Step 2: Run, confirm failure; write the copy until it passes.** `instructions` per D9, at most 1500 characters.
- [ ] **Step 3: Docs.** `mcp.md`: what it is; connect Claude Code (`claude mcp add --transport http arneb https://<host>/mcp --header "Authorization: Bearer $ARNEB_TOKEN"`) and any client with a bearer header; the Web port is plain HTTP, so remote clients go through a TLS-terminating proxy and OAuth-only clients wait for H6; the tool table (D5) with limits; error results; `[mcp]` settings; stateless behavior and `GET` 405; host allow-list.
- [ ] **Step 4: Tier 1 commands; `openspec validate remote-mcp --strict`; docs build if `docs/package.json` has a build script and `node_modules` is present (skip otherwise and say so).**
- [ ] **Step 5: Commit** `docs(mcp): agent-facing tool copy, MCP guide and openspec change`.

### Task 5: Tier 2 smoke over MCP

**Files:**
- Create: `scripts/mcp-smoke.sh` (bash 3.2 compatible, `curl` + `jq`; inputs `ARNEB_URL` (Web port base), `ARNEB_TOKEN` or `ARNEB_USER`/`ARNEB_PASSWORD`, `ARNEB_TOKEN2` or `ARNEB_USER2`/`ARNEB_PASSWORD2`, optional `ARNEB_CATALOG` (`datalake`), `ARNEB_SCHEMA` (`tpch`); prints `PASS <step>` / `FAIL <step>: reason` and exits 1 on the first failure; same style as `scripts/agent-api-smoke.sh`)

Steps the script checks, all through `POST $ARNEB_URL/mcp` JSON-RPC:
1. `initialize` → `serverInfo.name == "arneb"`; `tools/list` has the 18 D5 tools.
2. No credentials → HTTP 401.
3. `list_catalogs` contains `$ARNEB_CATALOG`; `list_tables` contains `lineitem`; `describe_table lineitem` has `l_quantity`; `sample_table nation rows=5` → 5 rows.
4. `explain_query` on TPC-H Q6 → plan text non-empty; `submit_query` Q6 with `wait_ms` and polling → finished, one row.
5. `submit_query "SELECT * FROM lineitem" max_rows=100` → `truncated: true`, `total_rows: null`, walking `fetch_results` yields 100 rows.
6. `submit_query "SELECT l_quantiy FROM lineitem"` → `column_not_found` with `l_quantity` in `candidates`.
7. A long statement (`ARNEB_LONG_SQL`, default the 3-way lineitem self-join from the agent smoke) → observed `running` → `cancel_query` → `cancelled`.
8. `list_queries {min_elapsed_ms: 0, limit: 5}` → ≤ 5 entries, newest first; `get_cluster` has 2 workers; `list_workers` has 2 entries.
9. Second user: `get_query_status` on user 1's id → `not_found`.

- [ ] **Step 1: Write the script; `bash -n` and `shellcheck` (if installed) clean.**
- [ ] **Step 2: Run it against the local distributed stack** (coordinator + 2 workers, password auth, hive `datalake`; reuse the config layout in the H2 Tier 2 record, which the controller provides), once with tokens and once with Basic. Do not stop, restart or reconfigure any Docker container; ports 5432 and 8080 are taken. Paste raw output.
- [ ] **Step 3: Commit** `test(mcp): Tier 2 smoke script over the MCP endpoint`.
