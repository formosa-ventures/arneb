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
- A section left out of the file allows everything it governs. With no
  `tables` section, every user has every privilege on every table; with no
  `schemas` section, every user owns every schema; and so on.

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
