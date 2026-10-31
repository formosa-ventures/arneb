## ADDED Requirements

### Requirement: Privilege layers
The server SHALL separate three layers: query (the catalog, schema, table and `queries` rules), observe (cluster, worker, admission and audit views) and manage (actions that change system state). Under `[auth] type = "none"` observe SHALL be allowed and every manage call SHALL be refused with `403 manage_requires_auth`. Under password auth observe SHALL be allowed when there is no rules file, the rules file has no `system_information` section, the first matching `system_information` rule allows `read`, or the user has manage. A refusal SHALL be `403 forbidden` with a `hint` naming the setting that would grant it. `whoami` SHALL report `privileges: {"observe", "manage"}`.

#### Scenario: Observe gated by system_information
- **WHEN** the rules file has a `system_information` section that allows `read` only for `ops`, and alice calls `GET /api/v2/workers`
- **THEN** the response is `403 forbidden` with a hint naming a `system_information` rule that allows `read` for alice

#### Scenario: Manage under none
- **WHEN** `[auth] type = "none"` and a caller sends `POST /api/v2/workers/worker-1/cordon`
- **THEN** the response is `403 manage_requires_auth` and the worker stays schedulable

#### Scenario: Privileges are reported
- **WHEN** a user in `admin_users` and a user without manage call `whoami`
- **THEN** the first sees `manage: true` and the second `manage: false`

### Requirement: Admin users
The manage privilege under password auth SHALL be granted to a user listed in `[access_control] admin_users` (`ARNEB_ADMIN_USERS`, comma-separated) or matched by the first `system_information` rule that allows `write`. With no `system_information` section, only `admin_users` SHALL grant it, even without a rules file. `write` SHALL NOT imply `read` in the rules file, but a user with manage SHALL always observe.

#### Scenario: Listed admin
- **WHEN** `admin_users = ["root"]`, there is no rules file and root cordons a worker
- **THEN** the call succeeds

#### Scenario: No grant by default
- **WHEN** a user not in `admin_users` calls a manage route and no rule allows `write`
- **THEN** the response is `403 forbidden` with a hint naming `admin_users`

### Requirement: Manage is refused without authentication
Under `[auth] type = "none"` no manage action SHALL be available on `/api/v2` or MCP, and cancelling another user's query SHALL be refused there with `manage_requires_auth`. A query's owner SHALL always be able to cancel it. `/api/v1` `DELETE /queries/{id}` SHALL keep its open behavior under `none`.

#### Scenario: Cancel another user's query under none
- **WHEN** `[auth] type = "none"` and `bob` sends `DELETE /api/v2/queries/{id}` for alice's query
- **THEN** the response is `403 manage_requires_auth` and the query keeps running

#### Scenario: v1 cancel under none
- **WHEN** `[auth] type = "none"` and any caller sends `DELETE /api/v1/queries/{id}`
- **THEN** the query is cancelled and stopped, and the response is `204`

### Requirement: Write privileges on every protocol
A write SHALL be authorized after planning against the `tables` rules and the catalog access of its target: `INSERT` needs `INSERT`, `DELETE` needs `DELETE`, and `CREATE TABLE`, CTAS, `DROP TABLE`, `CREATE VIEW` and `DROP VIEW` need `OWNERSHIP`. `OWNERSHIP` SHALL imply every privilege, a `read-only` catalog SHALL refuse every write, and `EXPLAIN` of a write SHALL be checked like the write. A refusal SHALL be SQLSTATE `42501` on pgwire, `PERMISSION_DENIED` on the Trino protocol, and error code `forbidden` with `state: failed` in an agent or MCP statement. A rules file without a `tables` section SHALL allow every write.

#### Scenario: SELECT-only user
- **WHEN** user2 holds only `SELECT` and runs `CREATE TABLE c.s.t AS SELECT 1 AS x` over pgwire and through `POST /api/v2/statements`
- **THEN** pgwire answers SQLSTATE `42501` and the statement ends `failed` with `error.code` `forbidden`

#### Scenario: Read-only catalog
- **WHEN** a `catalogs` rule gives `read-only` on `lake` and a user runs `INSERT INTO lake.s.t VALUES (1)`
- **THEN** the statement fails with `Access Denied` naming `catalog lake is read-only`

#### Scenario: Permitted writer
- **WHEN** a user holds `OWNERSHIP` on the target and runs a write
- **THEN** the privilege check passes and the statement is handled by the connector as before

### Requirement: Dry run
Every manage route SHALL take `dry_run` (`true` or `false`, default `false`; any other value `invalid_argument`) and every manage MCP tool SHALL take `dry_run`. A dry run SHALL apply the same authorization and validation as the real call, change nothing, return the response envelope with `dry_run: true` and no `result`, and not be audited.

#### Scenario: Dry run changes nothing
- **WHEN** an admin sends `POST /api/v2/workers/worker-1/cordon?dry_run=true`
- **THEN** the response has `dry_run: true`, and `GET /api/v2/workers/worker-1` still reports `scheduling` `schedulable`

#### Scenario: Refused dry run
- **WHEN** a user without manage sends the same request
- **THEN** the response is the same `403 forbidden` as without `dry_run`, and nothing is audited

### Requirement: Manage response envelope
A manage call SHALL answer `200` with `{dry_run, action, target, summary, changed, warning, result}`. `changed` SHALL be `false` for a no-op, which is not an error. `warning` SHALL be set when the action leaves no schedulable worker, and when the state store is in memory so the change is lost on restart. Actions SHALL be named `query.cancel`, `worker.cordon`, `worker.drain`, `worker.uncordon`, `admission.set`, `token.issue` and `token.revoke`.

#### Scenario: No-op
- **WHEN** an admin cordons a worker that is already cordoned
- **THEN** the response is `200` with `changed: false`

#### Scenario: In-memory warning
- **WHEN** the server runs under password auth with no `[state] path` and an admin sets an admission limit
- **THEN** the `warning` says the change is lost on restart

### Requirement: Durable audit log
Under password auth every non-dry manage run that passed authorization, every cancellation of another user's query and every token issue and revoke SHALL be appended as one JSON line to `audit.jsonl` in the directory of `[state] path` (mode `0600`) and synced to disk before the response returns. An entry SHALL hold `seq`, `time_ms`, `user`, `via`, `action`, `target`, `parameters` and `outcome`, and name a token by its 16-hex id only. `seq` SHALL increase across restarts. Refused calls SHALL NOT be audited. `GET /api/v2/audit?limit=&cursor=&user=&action=` SHALL list entries newest first and need observe.

#### Scenario: Cordon is recorded
- **WHEN** an admin cordons worker-1
- **THEN** `GET /api/v2/audit` lists a `worker.cordon` entry for the admin with target `worker-1` and outcome `ok`

#### Scenario: Refusal is not recorded
- **WHEN** user2 is refused a cordon
- **THEN** the audit log holds no entry by user2

#### Scenario: Sequence across restart
- **WHEN** the coordinator restarts after entries 1 to 9
- **THEN** the next entry has `seq` 10 or more

### Requirement: Audit log retention and failure
`[audit] max_entries` (default 10000, `1..=1000000`) and `retention_days` (default 90, `0` for no age limit) SHALL bound the log. Compaction SHALL rewrite the file atomically, keep the newest entry, and run at startup when it would drop something and whenever the file holds `max(max_entries / 10, 100)` more lines than `max_entries`. A line that cannot be parsed SHALL be skipped with a WARN naming its line number. If the log is not writable and cannot be reopened, an audited action SHALL be refused with `503 audit_unavailable` before anything is applied. If an append fails after the action was applied, the action's real result SHALL be returned, the full entry SHALL be logged at ERROR on `arneb::audit`, and later audited actions SHALL be refused until the log is writable. At most one entry per action in flight when the log fails SHALL be missing from the file, and each SHALL be in the ERROR log.

#### Scenario: Compaction keeps the newest
- **WHEN** every entry is older than `retention_days`
- **THEN** reads hide them, and the file keeps the newest entry so `seq` continues

#### Scenario: Torn line
- **WHEN** the last line of `audit.jsonl` is cut short by a crash
- **THEN** startup succeeds, logs a WARN naming the line, and compaction drops it

#### Scenario: Unwritable log
- **WHEN** the log cannot be written and an admin cordons a worker
- **THEN** the response is `503 audit_unavailable` and the worker stays schedulable

### Requirement: Cancel any query
`DELETE /api/v2/queries/{id}` SHALL cancel a query from any protocol and stop it there: pgwire answers `57014 canceling statement due to user request`, a Trino client `USER_CANCELED` `Query was killed by <user>`, and an agent statement turns `cancelled`. It SHALL answer `200` with the envelope and `result` `{query_id, owner, state_before, state}`. The owner SHALL always be allowed; otherwise the kill rule SHALL decide in this order: refused under `none`; a `queries` rule that allows `kill` for the owner, or no `queries` section; the manage privilege; otherwise `forbidden`. A caller whose rules grant `kill` but not `view` SHALL be able to cancel by id; a caller with neither SHALL get `not_found`. Cancelling a query that already ended SHALL answer `200` with `changed: false`. A kill that lands between a query's creation and its registration SHALL still stop it.

#### Scenario: Admin kills a pgwire query
- **WHEN** an admin cancels user2's running pgwire query
- **THEN** the response has `changed: true`, user2's client receives `57014`, and an audit entry `query.cancel` names the admin

#### Scenario: Ended query
- **WHEN** an admin cancels a query that finished a moment ago
- **THEN** the response is `200` with `changed: false` and no second state transition

#### Scenario: Unprivileged caller
- **WHEN** user2 cancels alice's query, `queries` rules allow `view` but not `kill`, and user2 has no manage
- **THEN** the response is `403 forbidden` with a hint naming the `kill` rule

### Requirement: Cordon, drain and uncordon workers
`POST /api/v2/workers/{id}/cordon`, `drain` and `uncordon` SHALL need manage. A cordoned worker SHALL get no new distributed query while its running queries continue. A drain SHALL be a cordon that also reports `scheduling` `draining` while distributed queries of the coordinator still use the worker and `drained` once none does. `scheduling` SHALL be a field separate from the worker-reported lifecycle `state`. Cordon and drain of an id the coordinator never saw SHALL be `not_found`. Cordoning the last schedulable worker SHALL succeed with a `warning`, and new distributed queries SHALL then fail naming the cordon count and the uncordon route. Cordons SHALL be persisted before they are applied and restored at startup before workers can rejoin, so a worker that restarts under the same id stays cordoned. `GET /api/v2/workers` SHALL be bounded (`limit` default 100, at most 1000, `truncated`) and list stored cordons of unregistered ids with `registered: false`.

#### Scenario: New queries avoid a cordoned worker
- **WHEN** worker-1 is cordoned and a distributed query starts
- **THEN** it runs on worker-2 only, and worker-1 reports `running_queries` 0

#### Scenario: Cordon survives a restart
- **WHEN** worker-1 is cordoned and the coordinator restarts
- **THEN** worker-1 is `cordoned` as soon as it re-registers

#### Scenario: Last worker
- **WHEN** an admin cordons the only schedulable worker
- **THEN** the response carries a `warning`, and the next distributed query fails with an error naming the uncordon route

#### Scenario: Limit above the ceiling
- **WHEN** a caller sends `GET /api/v2/workers?limit=1001`
- **THEN** the response is `400 invalid_argument`

### Requirement: Admission limits at runtime
`GET /api/v2/admission` SHALL need observe and report `{max_running_queries, max_queued_queries, running_queries, queued_queries, source}`. `PUT /api/v2/admission` SHALL need manage, take `{"max_running_queries"?, "max_queued_queries"?}` with at least one non-negative integer field and no unknown field, apply at once, and persist the limits in the state store. `source` SHALL be `config` until a limit is set at runtime, then `state`.

#### Scenario: Change at runtime
- **WHEN** an admin sends `PUT {"max_running_queries": 1}` and then starts two long queries
- **THEN** the second is reported `queued`, and `GET /api/v2/admission` reports `source` `state`

#### Scenario: Bad body
- **WHEN** the body is `{}`, `{"max_running_queries": -1}` or `{"bogus": 1}`
- **THEN** the response is `400 invalid_argument`

### Requirement: Stored admission limits take precedence
At startup a stored admission limit SHALL win over `[admission]` and its `ARNEB_*` variable. For each knob whose config value differs, startup SHALL log one WARN on `arneb::config` naming the knob and both values, and the effective-settings line SHALL report `admission_source` `config` or `state`.

#### Scenario: Pinned across a restart
- **WHEN** an admin sets `max_running_queries` to 4 while the config says 8, and the coordinator restarts
- **THEN** the limit is 4, `source` is `state`, and startup logged a WARN naming `max_running_queries`

### Requirement: MCP manage tools
The MCP server SHALL expose 25 tools in total. The manage tools `cordon_worker`, `drain_worker`, `uncordon_worker` and `set_admission_limits` SHALL take `dry_run`; `list_workers`, `get_worker`, `get_admission` and `list_audit_log` SHALL be read-only; `cancel_query` SHALL cancel an own agent statement as before, and any other id or any dry run through the same function as `DELETE /api/v2/queries/{id}`. Each tool SHALL make the same service call as its route, with the same rights, and report errors as `isError` results carrying the `/api/v2` envelope.

#### Scenario: Tool list
- **WHEN** a client calls `tools/list`
- **THEN** the result has 25 tools including `get_worker`, `cordon_worker` and `list_audit_log`

#### Scenario: Unprivileged tool call
- **WHEN** a user without manage calls `cordon_worker`
- **THEN** the result is `isError` with code `forbidden`

### Requirement: Bounded listings
Every listing added by this change (`workers`, `audit`) SHALL take `limit` with a ceiling, report `truncated`, and refuse a value above the ceiling with `400 invalid_argument` rather than clamp it. The audit listing's default SHALL be 50 and its ceiling 500.

#### Scenario: Audit limit above the ceiling
- **WHEN** a caller sends `GET /api/v2/audit?limit=501`
- **THEN** the response is `400 invalid_argument`

#### Scenario: Truncated page
- **WHEN** the log holds more entries than `limit`
- **THEN** the response has `truncated: true` and a `next_cursor`
