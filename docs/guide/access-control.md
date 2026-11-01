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
and the problem. Without a rules file, every user reads every table, as
before.

Under `[auth] type = "password"` the file only seeds the rules: the first
start copies it into the [state store](/guide/configuration#state), and from
then on the stored rules are in force and can be
[changed at runtime](#changing-rules-at-runtime). Under `none` there is no
state store, the file is the rules, and changing them needs a restart.

At startup the coordinator logs the file and the number of rules in each
section on `arneb::config`:

```
INFO arneb::config: access control rules loaded rules_file=/etc/arneb/rules.json catalogs=1 schemas=1 tables=2 queries=2
```

The line also says where the rules came from (`source="state"` or
`"config"`). When the file's text differs from the rules currently stored in
the state, every start logs a WARN that the state store's rules are used,
ending `Preview a re-seed with POST /api/v2/state/reseed?dry_run=true`. That
includes rules changed at runtime: after a `PUT` or `DELETE` the WARN fires
at every start until a re-seed copies the file again (or the file is edited
to match).

## Changing rules at runtime

Under password auth a user with the [manage](#privilege-layers) privilege
reads, replaces and clears the rules through `GET`, `PUT` and `DELETE
/api/v2/access-rules` (or the MCP tools `get_access_rules`,
`set_access_rules` and `clear_access_rules`). See the
[HTTP API](/guide/http-api#access-rules).

- `PUT` takes the whole rules file as its JSON body, checked exactly like
  the file at startup; an unsupported section or field is
  `invalid_argument`. `DELETE` removes every rule, which allows everything.
- The change is stored before the response and applies to every statement
  planned after it, on pgwire, Trino, the agent API and MCP, including
  connections already open. A statement already running keeps the rules it
  was checked with.
- It survives a restart: rules changed at runtime win over an unchanged
  `rules_file` (with a WARN at every start) until a
  [re-seed](/guide/http-api#re-seed) copies the file again. If the rules
  file is also edited after a runtime change, startup refuses until you
  restart with `--reseed-state` or restore the file to its last seeded
  version. Rules never changed at runtime follow the file: an edited
  `rules_file` applies at the next start. Runtime rules copied into the file
  (compared as JSON) count as the file's again and follow its later edits.
- Every change is audited (`access_rules.set`, `access_rules.clear`), with a
  SHA-256 of the rules instead of their text.
- `GET` reports `from_config: true` while the rules are the seeded copy of
  `rules_file`, and `false` after a runtime change or when there are no
  rules.

**Lockout guard.** A rules change, or a re-seed, after which you would no
longer hold manage is refused with `409 conflict`, hint `keep a
system_information write rule for yourself, or ask a user in
[access_control] admin_users`. A dry run is refused the same way. Users in
`[access_control] admin_users` always hold manage, so the guard never fires
for them, and `admin_users` itself is startup-only: keep at least one
operator there so a bad rules change can always be undone.

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

- **Observe** covers the cluster, worker, admission and audit views. The
  audit view shows the whole log only to a manager; any other observer sees
  only the entries of their own actions.
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
password auth, then an explicit `queries` rule that allows `kill` for that
owner, or the manage layer. Without a rules file, or without a `queries`
section, only the owner or a manager may cancel a query. This differs from
Trino's file-based access control, where a missing section allows
everything: add `{"allow": ["execute", "view", "kill"]}` to `queries` to let
every user cancel every query.

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

- Under password auth the rules are kept in the state store and can change
  at runtime (see [Changing rules at runtime](#changing-rules-at-runtime)).
  While they are not changed at runtime they follow the rules file at every
  start; after a runtime change they win over an unchanged file, and an
  edited file then stops startup until `--reseed-state`.

The full lists are in
[Upgrading to v0.2.0 (manage API)](/guide/configuration#upgrading-to-v020-manage-api)
and [Upgrading to v0.2.0 (runtime config)](/guide/configuration#upgrading-to-v020-runtime-config).

## Limits

- The Web API (Web UI port) applies the `queries` rules to the query list,
  query detail and cancel endpoints, under both `/api/v1` and `/api/v2`. A
  query the caller may not `view` is hidden (`404`). Cancelling follows the
  kill rule: the owner always may; otherwise an explicit `queries` rule that
  allows `kill` for the owner, or the manage layer; a
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
