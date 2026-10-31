## 1. Endpoint and configuration

- [x] 1.1 `[mcp]` config with `ARNEB_MCP_*` overrides, validation (`default_page_bytes` within `[agent_api] max_page_bytes`, empty `allowed_hosts` refused) and the effective-values log line
- [x] 1.2 Stateless streamable-HTTP `/mcp` on the Web port behind the Web auth middleware; `404` when disabled; `GET` answers `405`
- [x] 1.3 Host allow-list with the auth-dependent default and `*`

## 2. Tools

- [x] 2.1 `whoami` and the discovery tools (`list_catalogs`, `list_schemas`, `list_tables`, `describe_table`, `sample_table`, `explain_query`)
- [x] 2.2 Statement tools (`submit_query`, `get_query_status`, `fetch_results`, `cancel_query`) with the MCP page budget
- [x] 2.3 Operations tools (`list_queries` bounded and filtered, `get_query`, `get_cluster`, `list_workers`)
- [x] 2.4 Token tools (`list_tokens`, `create_token`, `revoke_token`) with the expiry clamp and `auth_disabled`
- [x] 2.5 Tool errors as `isError` results carrying the `/api/v2` envelope

## 3. Agent-facing copy

- [x] 3.1 Server `instructions` (at most 1500 characters) and tool descriptions that name the next tool
- [x] 3.2 `tool_list_contract` test: tool names, description length, follow-up tools, property descriptions, annotations

## 4. Docs and validation

- [x] 4.1 `docs/guide/mcp.md`, sidebar entry, `[mcp]` in the configuration guide, pointer in the HTTP API guide
- [x] 4.2 Tier 1: `cargo fmt`, `cargo clippy --workspace --all-targets` (pinned and 1.99.0), `cargo test --workspace`
- [ ] 4.3 Tier 2: a real MCP client against a coordinator plus workers with password auth (the acceptance smoke)
