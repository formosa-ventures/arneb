## Why

Stage H5 of the headless agent interface (`docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`). H4 gave operators and agents a privilege model, manage actions on queries, workers and admission, and an audit log. Everything else an operator changes still needs a config edit and a restart of every node: the access rules, the users and their tokens, the catalogs, and the slow-query threshold. H5 makes them runtime state, behind the same `/api/v2` service, MCP tools, dry runs and audit log, so a manager can change them without a restart and the change survives one.

## What Changes

- **The state store is the source of truth** for catalogs, access rules and the slow-query threshold, as H1 made it for users. The first start that finds no seeded copy copies the config file's `[[catalogs]]` and rules file into the state; later starts keep the state and log one WARN per diverging config entry, with a re-seed hint.
- **Access rules at runtime.** `GET|PUT|DELETE /api/v2/access-rules`, live on every protocol from the next statement, with a lockout guard.
- **Users at runtime.** `GET|POST /api/v2/users`, `PATCH|DELETE /api/v2/users/{name}` (a password change revokes the user's tokens, drops cached Basic credentials and ends Web sessions), and `GET|POST|DELETE /api/v2/users/{name}/tokens[/{id}]` for another user's tokens.
- **Catalogs at runtime.** `GET|POST /api/v2/catalog-configs`, `GET|PUT|DELETE /api/v2/catalog-configs/{name}` for `hive` and `postgresql` catalogs. A create or update is probed on the coordinator first; secrets are always redacted in answers and audit entries; a `PUT` of a redacted value keeps the stored secret.
- **Workers receive catalogs from the coordinator** over Flight: a push on every change, a catalog generation and coordinator epoch in every heartbeat reply (a mismatch pulls), and a pull-once retry when a task names an unknown catalog. Workers ignore their own `[[catalogs]]`.
- **Settings.** `GET|PATCH /api/v2/settings` changes `slow_query_threshold_ms` at runtime, persisted.
- **Re-seed.** `POST /api/v2/state/reseed` (and `--reseed-state` at startup) replaces catalogs, rules, settings and config users with the config file's.
- **MCP.** 18 new tools (43 in total), each calling its route's function.
- **Error code** `conflict` (HTTP 409).

## Capabilities

### New Capabilities

- `runtime-config`: seeding and divergence, re-seed, catalog definitions and their sync to workers, users and their tokens, settings, locking, MCP tools.

### Modified Capabilities

- `access-control`: the rules live in the state store and change at runtime, with a lockout guard.

## Impact

- New: `crates/server/src/catalogs.rs` (`LiveCatalogs`), `crates/server/src/web/{rules,users,catalogs,settings,reseed}.rs`; `crates/state/src/{catalogs,rules}.rs`; `crates/rpc/src/catalog_sync.rs`; `arneb_catalog::RulesHandle`.
- Changed: the Flight actions `sync_catalogs` / `get_catalogs` and the JSON heartbeat reply (`arneb-rpc`), `ConnectorRegistry` (interior mutability), `config.rs` (`CatalogConfig` is `Serialize`), `main.rs` and `task_manager.rs` (coordinator builds catalogs after the state opens; workers ignore `[[catalogs]]`, pull-once on a task miss), `web/mcp/tools.rs`, `web/error.rs`.
- Docs: `configuration` (upgrade notes), `access-control`, `http-api`, `mcp` guides; `scripts/runtime-config-smoke.sh` (Tier 2 smoke).
- Upgrade notes: `docs/guide/configuration.md#upgrading-to-v020-runtime-config`.
