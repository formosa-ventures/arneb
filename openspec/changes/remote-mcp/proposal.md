## Why

Stage H3 of the headless agent interface (`docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`). H1 (`headless-api-auth`) gave a remote agent a revocable credential and H2 (`agent-query-api`) gave it a bounded query API over `/api/v2`, but an MCP client (Claude Code, Claude Desktop, other agent hosts) still needs a wrapper to use them. H3 serves MCP directly, so a stock client connects with a URL and a bearer header.

## What Changes

- **`/mcp` endpoint.** A stateless streamable-HTTP MCP server on the Web port of the coordinator or standalone node, behind the Web auth middleware (Bearer token, session or Basic under password auth; `X-Arneb-User` under `none`). `GET` answers `405`; disabled, `/mcp` answers `404`.
- **18 tools** over the `/api/v2` service: `whoami`, discovery (`list_catalogs`, `list_schemas`, `list_tables`, `describe_table`, `sample_table`, `explain_query`), statements (`submit_query`, `get_query_status`, `fetch_results`, `cancel_query`), operations (`list_queries`, `get_query`, `get_cluster`, `list_workers`) and tokens (`list_tokens`, `create_token`, `revoke_token`). Each runs as the request's identity and makes the same service call as its route.
- **Agent-sized results.** `[mcp] default_page_bytes` (32768) bounds pages and explain plans; the bounded, filterable `list_queries` is the slow-query view.
- **Agent-facing copy.** Server instructions and tool descriptions that state the workflow and quote the configured limits, pinned by a contract test.
- **Safety.** Host and Origin validation (`[mcp] allowed_hosts`, `allowed_origins`); rmcp's tool-result debug logging is capped at `warn` unless `RUST_LOG` targets `rmcp` exactly.
- **Config.** `[mcp] enabled`, `allowed_hosts`, `allowed_origins`, `default_page_bytes` with `ARNEB_MCP_*` overrides.

Nothing else changes: `/api/v1`, `/api/v2`, pgwire and the Trino protocol behave as before, except that the `?state=` filter of the query list now accepts all eight query states.

## Capabilities

### New Capabilities

- `remote-mcp`: the `/mcp` endpoint, its tools, bounds, copy, validation and configuration.

### Modified Capabilities

## Impact

- New module `crates/server/src/web/mcp` (`mod.rs`, `tools.rs`) on `rmcp` 3.8.0 (pinned in the workspace).
- `arneb_protocol::agent`: `submit`, `status`, `sample`, `cancel` and `explain` take an optional page/plan byte budget (the HTTP routes pass none).
- `crates/server/src/config.rs`: `[mcp]` section; `crates/server/src/main.rs`: the rmcp log cap.
- Docs: `docs/guide/mcp.md`, `docs/guide/configuration.md`, `docs/guide/http-api.md`; `scripts/mcp-smoke.sh` (Tier 2 smoke).
