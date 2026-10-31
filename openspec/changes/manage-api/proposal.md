## Why

Stage H4 of the headless agent interface (`docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`). H1 to H3 let a remote agent authenticate, query and read the cluster, but nothing let an operator or an agent act on it: cancelling another user's query did not stop it on pgwire or Trino, a worker could only leave through SIGTERM, admission limits were frozen at startup, writes were not checked against the `tables` rules, and there was no durable record of who did what. H4 adds a privilege model, the first manage actions, and an audit log, behind the same `/api/v2` service and MCP tools.

## What Changes

- **Privilege layers.** Query (unchanged), observe (cluster, workers, admission, audit views) and manage (actions that change state). Manage needs password auth and `[access_control] admin_users` or a `system_information` rule that allows `write`; under `[auth] type = "none"` it is refused with `manage_requires_auth`. `whoami` reports `privileges`.
- **Write privileges.** `INSERT`, `DELETE` and `OWNERSHIP` (create/drop table and view, CTAS) are checked against the `tables` rules and `read-only` catalogs on pgwire, Trino, the agent API and MCP: `42501`, `PERMISSION_DENIED`, `forbidden`.
- **Cancel any query.** `DELETE /api/v2/queries/{id}` cancels a query from any protocol and now really stops it (per-query cancellers). It answers `200` with the manage envelope.
- **Workers.** `GET /api/v2/workers[/{id}]` and `POST /api/v2/workers/{id}/cordon|drain|uncordon`, persisted and restored at startup; a new `scheduling` field.
- **Admission.** `GET|PUT /api/v2/admission` changes `max_running_queries` and `max_queued_queries` at runtime, persisted; a stored value wins over the config seed.
- **Dry runs and envelope.** Every manage route takes `?dry_run=`; one response envelope for dry and real runs.
- **Audit log.** `audit.jsonl` beside the state file, fsynced per entry, bounded by `[audit] max_entries` / `retention_days`, read through `GET /api/v2/audit`.
- **MCP.** Seven new tools (25 in total), a widened `cancel_query` and a bounded `list_workers`.
- **Config.** `[access_control] admin_users` (`ARNEB_ADMIN_USERS`), `[audit]` with `ARNEB_AUDIT_*`.

## Capabilities

### New Capabilities

- `manage-api`: privilege layers, manage envelope, dry runs, audit log, cancel, workers, admission limits, MCP manage tools.

### Modified Capabilities

- `access-control`: the `system_information` section, kill rules and write privileges.
- `query-admission`: limits change at runtime and a controller always exists.
- `worker-graceful-drain`: coordinator-side cordon and drain.

## Impact

- New: `crates/server/src/web/{privilege,manage,workers,admission,audit}.rs`; `crates/state/src/{audit,cordons,settings}.rs`.
- Changed: `arneb_catalog::access` (write privileges, `check_kill`), `arneb_scheduler` (tracker cancellers, registry cordons, admission limits), `arneb_protocol` (write checks, cancellers), `web/v2.rs`, `web/mcp/tools.rs`, `config.rs`, `main.rs`.
- Docs: `access-control`, `configuration`, `http-api`, `mcp`, `distributed`, `observability` guides; `scripts/manage-smoke.sh` (Tier 2 smoke).
- Upgrade notes: `docs/guide/configuration.md#upgrading-to-v020-manage-api`.
