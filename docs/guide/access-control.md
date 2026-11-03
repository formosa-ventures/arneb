# Access Control

Arneb can restrict which catalogs, schemas and tables each user reads, and
whose queries they see. The rules use the format of Trino's
[file-based access control](https://trino.io/docs/current/security/file-system-access-control.html),
so a team moving from Trino can bring its rules file. Arneb supports the
part of that format it can enforce, and refuses to start on anything else.

Rules only mean something when users are authenticated. Turn on
[password authentication](/guide/configuration#authentication) first: with
`[auth] type = "none"` the user name is whatever the client sends, and
startup logs a warning.

## Enabling

```toml
[access_control]
rules_file = "/etc/arneb/rules.json"   # or ARNEB_ACCESS_CONTROL_RULES_FILE
```

The coordinator (or standalone server) reads the file at startup. A file
that cannot be read or parsed stops startup with an error naming the file
and the problem. Changing the rules needs a restart. Without a rules file,
every user reads every table, as before.

At startup the coordinator logs the file and the number of rules in each
section on `arneb::config`:

```
INFO arneb::config: access control rules loaded rules_file=/etc/arneb/rules.json catalogs=1 schemas=1 tables=2 queries=2
```

## Example

```json
{
  "catalogs": [
    {"user": "admin", "catalog": ".*", "allow": "all"},
    {"catalog": "pg", "allow": "none"},
    {"allow": "read-only"}
  ],
  "schemas": [
    {"owner": false}
  ],
  "tables": [
    {"user": "admin", "privileges": ["SELECT", "OWNERSHIP"]},
    {"user": "analyst_.*", "catalog": "datalake", "schema": "sales", "privileges": ["SELECT"]},
    {"user": "alice", "table": "nation|region", "privileges": ["SELECT"]}
  ],
  "queries": [
    {"user": "admin", "allow": ["execute", "view", "kill"]},
    {"allow": ["execute"]}
  ]
}
```

Here `admin` reads everything and sees every query. Users named
`analyst_*` read every table in `datalake.sales`. `alice` reads `nation`
and `region` in any catalog except `pg`, which no one but `admin` sees.
Everyone can run queries and sees only their own.

## How rules match

- Each section is a list of rules. For a given check, the **first** rule
  whose patterns all match decides. If no rule matches, access is denied.
- Patterns (`user`, `catalog`, `schema`, `table`, `queryOwner`) are regular
  expressions matched against the whole name, so `sales` does not match
  `sales_eu`. A pattern left out matches everything (`.*`).
- Patterns are case-sensitive: `secret` does not match `SECRET`. Write
  them against the names as the source stores them (lowercase in Hive;
  in PostgreSQL, lowercase unless the table was created with a quoted
  mixed-case name such as `"Orders"`).
- Some connectors find tables by a name other than the stored one: Hive
  always (HMS trims and lowercases names), PostgreSQL when no name matches
  exactly (a unique case-insensitive match). A read is checked on the name
  as written **and** on the name the connector resolved it to, and is
  refused if either is denied. So a deny rule on `secret` also covers
  `SECRET`, `Secret` and `PUBLIC.secret`, and a schema rule covers the
  schema however it is written. When only the resolved name is denied,
  the error is the one a missing table gets, so it does not reveal that
  the table exists.
- The same holds for **allow** rules: the name as written must match one
  too. Arneb folds an unquoted name to lowercase only when the name as
  written finds nothing and its lowercase form does (memory and file
  catalogs). Hive and PostgreSQL find `NATION` as written, so it is not
  folded, and an allow rule on `nation` refuses `SELECT * FROM NATION`.
  Write queries with the stored (usually lowercase) names, or make allow
  patterns case-insensitive, e.g. `"table": "(?i)nation"`. See
  [Upgrading from v0.1.8](#upgrading-from-v0-1-8) if your allow rules use
  another case than the stored names.
- When a connector cannot be reached (HMS or the PostgreSQL server is
  down), reads and `SET search_path` fail with an error instead of
  reporting a missing schema or table, so `SET search_path` never falls
  back to another schema. Trino `SHOW TABLES` fails too when it cannot
  look the schema up. Listings (`SHOW TABLES` once the schema is known,
  `information_schema`, `pg_catalog`, `system.jdbc`) show nothing for what
  they cannot reach, and the server logs a warning. Iceberg tables whose
  metadata files cannot be read still read as missing (logged).
- A PostgreSQL view is checked by the view's own name. The tables it
  reads are governed only by the PostgreSQL role the catalog connects as.
- A section left out of the file allows everything it governs. With no
  `tables` section, every user has every privilege on every table; with no
  `schemas` section, every user owns every schema; and so on.

## Upgrading from v0.1.8

Before v0.1.9, rules were checked only on the names as written in the
query. From v0.1.9 they are also checked on the stored names the Hive or
PostgreSQL connector resolved, so **an allow rule must match the stored
name as well**. An allow rule written in another case than the stored
name now refuses the read, and the error is the one a missing table gets,
not `Access Denied`, so it can look as if the data went away.

For example, with a Hive catalog whose database is stored as `tpch`:

```json
{
  "tables": [
    {"user": "alice", "schema": "TPCH", "privileges": ["SELECT"]},
    {"privileges": []}
  ]
}
```

`SELECT * FROM TPCH.nation` worked on v0.1.8. On v0.1.9 it is refused,
because the stored schema `tpch` does not match `TPCH`. Fix the rule by
writing the stored name, `"schema": "tpch"`, or by making the pattern
case-insensitive, `"schema": "(?i)tpch"`. Check every `catalog`, `schema`
and `table` pattern in an allow rule against `SHOW SCHEMAS` / `SHOW TABLES`
(which list the stored names) before upgrading. Deny rules need no change:
they now also cover every other spelling of the stored name. Memory and
file catalogs are not affected.

## Supported rules

| Section | Fields | Effect |
|---|---|---|
| `catalogs` | `user`, `catalog`, `allow` | `allow` is `all`, `read-only` or `none` (`true` / `false` also work). `none` hides the catalog and blocks every read in it. |
| `schemas` | `user`, `catalog`, `schema`, `owner` | Owning a schema makes it visible in listings. |
| `tables` | `user`, `catalog`, `schema`, `table`, `privileges` | Reading a table needs `SELECT` or `OWNERSHIP`. Any privilege makes the table visible. |
| `queries` | `user`, `queryOwner`, `allow` | `execute` is needed to run any statement. `view` shows another user's queries in `system.runtime.queries`. A rule with `queryOwner` cannot allow `execute`. |

Privileges other than `SELECT` and `OWNERSHIP` (`INSERT`, `DELETE`,
`UPDATE`, `GRANT_SELECT`) are accepted but grant no read, since Arneb does
not execute writes yet.

These Trino features are **not** supported, and a rules file that uses them
is refused at startup instead of being enforced partially:

- `group` and `role` matching (Arneb has no group or role provider);
- row filters (`filter`, `filter_environment`) and column masks or
  restrictions (`columns`);
- other sections, such as `impersonation`, `system_information`,
  `functions` or `procedures`.

## What users see

- **Reading a table** they may not read fails before the query runs, with
  the same message whether or not the table exists:
  `Access Denied: Cannot select from table datalake.hr.salaries`. This is
  SQLSTATE `42501` on pgwire and `PERMISSION_DENIED` on the Trino protocol.
  Unqualified names are checked after the session's default catalog and
  schema are applied.
- **Running a statement** without `execute` (when the file has a `queries`
  section) fails with `Access Denied: Cannot execute query`.
- **Listings** on both protocols show only what the user can use: a catalog
  unless its access is `none`; a schema the user owns, or in which some
  table rule grants them a privilege; a table they hold a privilege on.
  This covers `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES`,
  `information_schema`, `system.jdbc` and the `pg_catalog` tables psql and
  BI tools read. The `system` catalog and `information_schema` are always
  listed; their rows are filtered.
- **`system.runtime.queries`** always shows the user's own queries, and
  another user's only where a `queries` rule allows `view`.

## Limits

- The Web API (Web UI port) applies the `queries` rules to the query list,
  query detail and cancel endpoints, under both `/api/v1` and `/api/v2`. A
  query the caller may not `view` is hidden (`404`); one they may view but
  not `kill` answers `403`. A user may always kill their own query. Under
  `[auth] type = "none"` the Web API has no user, so these rules are not
  applied there. See [HTTP API](/guide/http-api). Under password auth the Web
  API also requires credentials, and the Web UI binds to `127.0.0.1` unless
  `[web] bind_address` is set.
- Rules are checked on the coordinator. Workers run the plan fragments the
  coordinator has already checked, and the cluster secret keeps other
  callers off their RPC port (see
  [Securing the Cluster](/guide/distributed#securing-the-cluster)).
