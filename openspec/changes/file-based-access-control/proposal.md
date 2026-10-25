## Why

Arneb authenticates users but does not authorize them: any user who can log
in can read every catalog, including a federated PostgreSQL, and see every
other user's SQL in `system.runtime.queries`. That rules out pointing a
shared Arneb at sensitive data. This is gap G3 in the "Path to production"
plan (`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`), due in
v0.1.8 and required before v0.2.0. Scenario 1 is replacing an internal Trino
cluster, so the rules follow Trino's file-based access control: a team can
bring its existing rules file.

## What Changes

- **A rules file.** `[access_control] rules_file` (or
  `ARNEB_ACCESS_CONTROL_RULES_FILE`) names a JSON file in Trino's
  file-based access control format. Arneb reads the `catalogs`, `schemas`,
  `tables` and `queries` sections with Trino's semantics: regex patterns,
  the first matching rule wins, no matching rule denies, and a section that
  is absent allows everything it would govern. Without a rules file,
  nothing changes.
- **Unsupported rules stop startup.** Rules Arneb cannot enforce (row
  filters, column masks, `group` and `role` matching, and other sections
  such as `impersonation`) are an error naming the rule, never ignored, so
  a Trino file never silently grants more than it says.
- **Table reads are checked.** Every table a query reads must have
  `SELECT` (or `OWNERSHIP`) for the session user, and its catalog must not
  be `none`. A denied query fails before it runs with
  `Access Denied: Cannot select from table <catalog>.<schema>.<table>`
  (SQLSTATE `42501` on pgwire, `PERMISSION_DENIED` on the Trino protocol),
  whether or not the table exists.
- **Metadata shows only what the user can use.** Catalog, schema and table
  listings on both protocols (`SHOW ...`, `information_schema`,
  `pg_catalog`, `system.jdbc`) hide what the rules do not grant.
- **Queries of other users.** `system.runtime.queries` shows other users'
  queries only where a `queries` rule allows `view`; users always see their
  own.
- **Startup logs the policy,** and warns when rules are set while `[auth]`
  is `none`, since the user name is then whatever the client claims.

## Capabilities

### New Capabilities

- `access-control`: the rules file, its supported subset, matching
  semantics, table-read checks, metadata filtering and query visibility.

### Modified Capabilities

- `server-config`: the `[access_control]` section and its env override.

## Impact

- `crates/catalog`: rules model, parser and checks; `CatalogManager` user
  views (`with_access`), access-checked `resolve_table`, filtered listings;
  `CatalogError::AccessDenied`.
- `crates/protocol`: the session user reaches the catalog view on pgwire
  (`session_catalog`) and Trino (`Engine::run`); metadata handlers use the
  filtered listings; error mapping; `system.runtime.queries` filter.
- `crates/server`: config, loading and logging the rules.
- Docs: `docs/guide/access-control.md`, `configuration.md`.
- Coordinator only: workers execute plans the coordinator already checked.
