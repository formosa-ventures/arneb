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
| `tables` | `user`, `catalog`, `schema`, `table`, `privileges` | Reading a table needs `SELECT`; `INSERT` and `DELETE` need the same-named privilege; `CREATE TABLE`, CTAS, `DROP TABLE`, `CREATE VIEW` and `DROP VIEW` need `OWNERSHIP`. `OWNERSHIP` implies every privilege. Any privilege makes the table visible. |
| `queries` | `user`, `queryOwner`, `allow` | `execute` is needed to run any statement. `view` shows another user's queries in `system.runtime.queries`. A rule with `queryOwner` cannot allow `execute`. |
| `system_information` | `user`, `allow` | `allow` lists `read` and/or `write`. `read` opens the observe layer (cluster, workers, admission, audit views); `write` grants the manage layer. `write` does not imply `read` in the rules file, but a user who manages can always observe. `group` and `role` are refused. |

Writes are checked on the target table after planning, on every protocol
(pgwire, Trino, the agent API and MCP). `EXPLAIN` of a write is checked like
the write. A `read-only` catalog refuses every write
(`Access Denied: Cannot create table lake.sales.x (catalog lake is
read-only)`); a `none` catalog refuses everything. A refused write answers
`Access Denied: Cannot <action> <catalog>.<schema>.<name> (requires
<PRIVILEGE>)`, where `<action>` is `insert into table`, `delete from table`,
`create table`, `drop table`, `create view` or `drop view`: SQLSTATE `42501` on pgwire, `PERMISSION_DENIED` on Trino and
`forbidden` in an agent or MCP statement status. The sources of CTAS and
`INSERT ... SELECT` need `SELECT` as usual. `UPDATE` is accepted in rules
files but maps to nothing, because the parser has no `UPDATE`;
`GRANT_SELECT` grants no read.

Connectors do not execute DDL or DML yet, so a permitted write still fails
with the existing not-supported error.

These Trino features are **not** supported, and a rules file that uses them
is refused at startup instead of being enforced partially:

- `group` and `role` matching (Arneb has no group or role provider);
- row filters (`filter`, `filter_environment`) and column masks or
  restrictions (`columns`);
- other sections, such as `impersonation`, `functions` or `procedures`.

## Privilege layers

Two layers sit above the query rules:

- **Observe** covers the cluster, worker, admission and audit views.
  - Under `[auth] type = "none"` it is open.
  - Under password auth it is open unless the rules file has a
    `system_information` section. Then the user needs a rule that allows
    `read`, or the manage layer.
  - Query visibility stays on the `queries` `view` rules.
  - `/metrics` is not part of it: the admission gauges, worker ids and
    heartbeat ages served there ignore `system_information`. `/metrics` is
    unauthenticated unless [`[web] metrics_auth`](/guide/configuration#web-ui)
    is set, and then needs only valid credentials.
- **Manage** covers actions that change system state.
  - It needs password auth. Under `none`, every manage call is refused with
    `manage_requires_auth`.
  - The user must be in `[access_control] admin_users`, or match a
    `system_information` rule that allows `write`. Nothing grants it by
    default, even without a rules file.

```toml
[access_control]
admin_users = ["root"]   # ARNEB_ADMIN_USERS="root,ops"
```

`GET /api/v2/whoami` and the MCP `whoami` tool report what the caller holds
as `privileges: {"observe": bool, "manage": bool}`.

A refusal is `403 forbidden` and its `hint` names what would grant it:

- manage: `needs a system_information rule that allows write for user <u>,
  or <u> in [access_control] admin_users`;
- observe: `needs a system_information rule that allows read for user <u>`;
- cancelling another user's query: `needs a queries rule that allows kill
  for queryOwner <owner>, or the manage privilege`.

A query's owner may always cancel it. Cancelling another user's query needs
password auth, then a `queries` rule that allows `kill` for that owner (or no
`queries` section), or the manage layer.

> **Under `[auth] type = "none"`, do not expose the Web port.** A cancel
> really stops the query, any peer that reaches the Web port can stop any
> query through `/api/v1`, and `/api/v2` and MCP accept a claimed
> `X-Arneb-User` as the owner. Bind the Web UI to loopback
> (`[web] bind_address = "127.0.0.1"`) or use password auth.

`system_information` is a rules-file section, so
`ARNEB_ACCESS_CONTROL_RULES_FILE` still decides whether rules apply at all.

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

## Upgrading to v0.2.0

- Writes are now checked against `tables` rules on every protocol, so a
  rules file with a `tables` section that grants only `SELECT` refuses
  DDL and DML. Without a `tables` section everything is still allowed.
- A `read-only` catalog refuses writes.
- `system_information` is now an accepted section.
- `DELETE /api/v2/queries/{id}` now answers `200` with the
  [manage envelope](/guide/http-api#dry-runs-and-the-response-envelope)
  instead of `204`, and stops the query on its protocol.

The full list, with the state-file and admission changes, is in
[Upgrading to v0.2.0 (manage API)](/guide/configuration#upgrading-to-v020-manage-api).

## Limits

- The Web API (Web UI port) applies the `queries` rules to the query list,
  query detail and cancel endpoints, under both `/api/v1` and `/api/v2`. A
  query the caller may not `view` is hidden (`404`). Cancelling follows the
  kill rule: the owner always may; otherwise a `queries` rule that allows
  `kill` for the owner (or no `queries` section), or the manage layer; a
  query the caller may neither view nor kill is `404`, any other refusal
  `403`. Under `[auth] type = "none"` the Web API has no authenticated user:
  `/api/v1` keeps its open list, detail and cancel (any caller may cancel
  any query, and the cancel really stops it), while `/api/v2` and MCP refuse cancelling another user's
  query with `manage_requires_auth`. See
  [Cancel any query](/guide/http-api#cancel-any-query). Under password auth the Web
  API also requires credentials, and the Web UI binds to `127.0.0.1` unless
  `[web] bind_address` is set.
- Rules are checked on the coordinator. Workers run the plan fragments the
  coordinator has already checked, and the cluster secret keeps other
  callers off their RPC port (see
  [Securing the Cluster](/guide/distributed#securing-the-cluster)).
