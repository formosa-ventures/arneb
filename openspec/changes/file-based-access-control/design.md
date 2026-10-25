## Context

The session user reaches only the query tracker today. pgwire stores it in
the connection metadata (verified by SCRAM under `[auth] type = "password"`,
taken from the startup message otherwise); the Trino protocol has it in
`ClientSession.user` (verified by Basic auth, impersonation refused). Both
plan through a `CatalogManager`: pgwire picks it per statement in
`session_catalog` (the root manager, or the `SET search_path` view), Trino
builds a `with_session_defaults` view per query.

Every table a query reads goes through `CatalogManager::resolve_table`
(planner `plan_table_factor`). Metadata listings (`pg_catalog`,
`information_schema` on pgwire; `SHOW` and the virtual
`information_schema` / `system.jdbc` on Trino) walk `catalog_names()` and
each provider's `schema_names()` / `table_names()` directly. DDL and DML
plan but cannot execute, and there are no stored views. Workers never
resolve tables; they run fragments the coordinator planned.

## Goals / Non-Goals

**Goals:** read authorization on catalogs, schemas and tables; listings
that match it; per-user visibility of other users' queries; a rules file a
Trino deployment can reuse.

**Non-Goals:** row filters and column masks, groups and roles (Arneb has no
group provider), write privileges (no DDL/DML executes yet), reloading the
file without a restart, the Web UI (it has no login; it binds to loopback
under password auth).

## Decisions

### Trino's format and semantics, a strict subset

The file is JSON with Trino's field names. Supported:

| Section | Fields |
|---|---|
| `catalogs` | `user`, `catalog`, `allow` (`all`, `read-only`, `none`; `true`/`false` as in Trino's legacy form) |
| `schemas` | `user`, `catalog`, `schema`, `owner` |
| `tables` | `user`, `catalog`, `schema`, `table`, `privileges` |
| `queries` | `user`, `queryOwner`, `allow` (`execute`, `view`, `kill`) |

Patterns are regexes matched against the whole name (Java `matches`, so
`^(?:p)$` in Rust) and default to `.*`. In each section the first rule
whose patterns all match decides; if none matches, access is denied. A
section left out allows everything it would govern, as in Trino. Any other
field (`group`, `role`, `columns`, `filter`, `filter_environment`) or
section (`impersonation`, `system_information`, ...) fails parsing with an
error naming it. Ignoring them would turn a Trino file into a weaker policy
without anyone noticing.

Rejected: a new Arneb-specific format (teams moving from Trino would
rewrite their rules), and "unknown means allow" (unsafe).

### Checks

For user `u` and table `c.s.t`:

- **Catalog access:** the first `catalogs` rule matching `u`, `c` gives
  `all`, `read-only` or `none`. Reading needs anything but `none`.
- **Table read:** catalog access, and the first `tables` rule matching
  `u`, `c`, `s`, `t` grants `SELECT` or `OWNERSHIP`.
- **Visibility,** as Trino computes it from the rules alone (no listing of
  tables to decide):
  - a catalog is visible with access other than `none`;
  - a schema is visible when the first matching `schemas` rule makes the
    user `owner`, or some `tables` rule for `u`, `c`, `s` grants any
    privilege (its table pattern is not consulted);
  - a table is visible when the table rule grants any privilege.
- **Execute:** with a `queries` section, running any statement needs
  `execute` from the first matching rule without `queryOwner` (Trino's
  rule; a `queryOwner` rule allowing `execute` is rejected at load).
  Without it, a Trino file that blocks a user from querying would not
  block them here.
- **Queries:** a user always sees their own queries. Another user's query
  is visible when the first `queries` rule matching the viewer and
  `queryOwner` allows `view`.

The `system` catalog (virtual: `system.jdbc`, `system.metadata`,
`system.runtime`) and every `information_schema` stay visible, as in
Trino; their rows are filtered instead.

The check runs before the table is looked up, so a denied name returns the
same error whether or not the table exists.

### Where it is enforced

`CatalogManager` gains an optional `Access { user, rules }`.
`with_access(user, rules)` returns a view that keeps the receiver's
defaults and qualification, so it composes with `with_session_defaults` and
the `SET search_path` view. On a view with access:

- `resolve_table` checks the read before resolving and returns
  `CatalogError::AccessDenied`;
- `visible_catalog_names()`, `visible_schema_names(catalog)` and
  `visible_table_names(catalog, schema)` filter; on the root manager they
  return everything. The metadata handlers on both protocols switch to them.

pgwire wraps `session_catalog(client)` with the connection's user; Trino
wraps the per-query view with `ClientSession.user`. `register_data_sources`
walks every scanned table again after planning, but needs no second check:
it only sees tables `resolve_table` produced.

`CatalogError::AccessDenied` maps to SQLSTATE `42501`
(`insufficient_privilege`) on pgwire and `PERMISSION_DENIED`
(`USER_ERROR`) on the Trino protocol, with Trino's message text.

### Config and startup

`[access_control] rules_file` with `ARNEB_ACCESS_CONTROL_RULES_FILE`
(env > file), read on the coordinator and standalone server. A file that
cannot be read or parsed stops startup. Startup logs on `arneb::config` the
file and the number of rules per section, and warns when `[auth]` is
`none`: then the user name is whatever the client sends, and the rules only
separate cooperating clients.

## Risks / Trade-offs

- [No reload] → Changing rules needs a coordinator restart. Trino's
  `security.refresh-period` can come later.
- [Regex cost per check] → Rules compile once at load; a check is a few
  regex matches per scanned table and per listed name, negligible next to
  planning.
- [pgwire with `[auth] type = "none"`] → The user is self-declared. The
  startup warning and the docs say so.
- [Web UI] → Lists every query to anyone who reaches it; unchanged, and
  documented next to the rules.
