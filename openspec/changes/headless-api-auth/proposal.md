## Why

The v0.2.0 headless agent interface (`docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`) needs a remote agent to authenticate with a revocable credential and call a stable HTTP API. Today none of that exists: the Web port has no login and shows every query, the only credential is a password in the config file, and nothing is durable except that file. Stage H1 builds the foundation the MCP server and the operation APIs sit on.

## What Changes

- **Coordinator state store** (`arneb-state`). One versioned JSON file at `[state] path` / `ARNEB_STATE_PATH`, written atomically with mode `0600`. Users are seeded from `[[auth.users]]` on first start; afterwards the state wins, a divergence logs a WARN, and `--reseed-state` applies the config. Without a path the state is in memory and a WARN says tokens are lost on restart. Workers never open it.
- **API tokens.** `arneb_<16 hex>_<43 base64url>`, shown once, stored as a SCRAM verifier, revocable, with an optional expiry and a cap of 100 live tokens per user. A token is accepted as the password on pgwire (SCRAM) and on Trino Basic; revocation is immediate on every protocol.
- **Authenticated Web port.** Under `[auth] type = "password"`, every route except `/api/v1/info`, `/api/v2/health`, `POST /api/v2/sessions` and the static UI needs a bearer token, a session token or Basic. `/metrics` stays open unless `[web] metrics_auth = true`.
- **`/api/v2`.** Health, sessions, `whoami`, token issue/list/revoke, and mirrors of the queries, cluster, workers and info endpoints, with a structured error envelope `{"error":{"code","message","hint"}}`.
- **Query rules on the Web API.** `/api/v1/queries*` and `/api/v2/queries*` apply the `queries` view and kill rules.
- **Web UI sign-in.** A login page that exchanges the password for a session token.
- **Helm.** `coordinator.state.persistence.*` mounts a volume for the state file.
- **BREAKING (password auth only).** `/api/v1/*` (except `/api/v1/info`) now needs credentials, and the Web UI asks you to sign in. `/api/v1` cancel and list now apply the `queries` rules. Under `type = "none"` nothing changes.

## Capabilities

### New Capabilities

- `state-store`: the durable coordinator state file, seeding from the config, and the in-memory fallback.
- `api-tokens`: token format, lifecycle, and acceptance on pgwire and Trino.
- `web-api-auth`: Web port authentication, sessions, the `/api/v2` routes, the error envelope and query-rule enforcement.

### Modified Capabilities

## Impact

- New crate `crates/state`. Changes in `crates/protocol` (token-aware `CredentialSource`, `is_token_shaped`, SCRAM and Trino Basic), `crates/server` (config, startup, `web/` auth middleware, sessions, `v2.rs`, UI login), `charts/arneb`.
- Docs: new `docs/guide/http-api.md`; updates to configuration, access control and Kubernetes guides.
- Tier 1 (unit and integration tests) covers the logic. Tier 2 (local coordinator plus two workers) covers the end-to-end token path across pgwire, Trino and the Web API, including a coordinator restart.
