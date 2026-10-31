## Context

Stage H4 of the headless agent interface, built on H1 (`headless-api-auth`: identity per request, API tokens, error envelope), H2 (`agent-query-api`: `AgentQueryService`) and H3 (`remote-mcp`: tools over the same service calls). Every manage action is one `*_for` function that a `/api/v2` route and an MCP tool both call, so the two have exactly the same rights.

## Goals / Non-Goals

**Goals**
- An operator or agent can cancel any query, cordon, drain and uncordon workers, and change admission limits, without a restart.
- Writes are checked against the `tables` rules on every protocol.
- Every state-changing action is attributable: a durable, bounded audit log.
- A call can be previewed (`dry_run`) and is refused identically when it is not allowed.
- Changes survive a coordinator restart.

**Non-Goals**
- Catalog, user and rule edits, and re-seeding (H5); OAuth (H6).
- Making `statement_timeout` a runtime knob.
- Wider unauthenticated access: under `[auth] type = "none"` no manage action is available.

## Decisions

**D1. Privilege layers** (`web/privilege.rs`, `Authorizer`).
- *Query layer.* Unchanged: the catalog, schema and table rules, plus D3.
- *Observe layer* (cluster, workers, admission and audit views). Under `none` it is allowed. Under password it is allowed when there is no rules file, the rules file has no `system_information` section, the first `system_information` rule matching the user allows `read`, or the user has manage. Query visibility stays on the `queries` `view` rules. `/metrics` stays outside the observe layer (unauthenticated by default, credentials only with `metrics_auth`), so its admission gauges, worker ids and heartbeat ages ignore `system_information`; the docs say so.
- *Manage layer.* Under `none`, every manage call is refused with `manage_requires_auth`. Under password it needs the user in `admin_users`, or the first matching `system_information` rule allowing `write`. With no `system_information` section, only `admin_users` grants it (deny unless granted).
- *Rule shape.* `{"user": "<regex>", "allow": ["read", "write"]}`; `group` and `role` are refused like in the other sections.
- *Write does not imply read* inside `AccessRules` (Trino semantics). The `Authorizer` treats manage as implying observe, so a manager can always list what they manage.

**D2. Kill rule** (`Authorizer::check_kill`), in this order:
1. The caller owns the query: allowed (`KillGrant::Owner`), in any auth mode, including under `none` where the owner is the claimed `X-Arneb-User`.
2. Otherwise, under `none`: `manage_requires_auth`.
3. Otherwise, a rules file whose `queries` section allows `kill` for this owner, or no rules file or no `queries` section: allowed (`KillGrant::Rule`). This keeps the "a missing section allows everything" convention, and matches Trino file-based access control.
4. Otherwise, manage: allowed (`KillGrant::Manage`).
5. Otherwise: `forbidden`.

Lookup and visibility: a query the caller may not `view` is `not_found`, unless the caller has manage or the caller's rules grant `kill` on its owner. A caller who holds `kill` but not `view` may cancel the query by id (the id is the capability) without it ever being listed to them; with neither, `not_found`. `/api/v2` and MCP follow this rule everywhere. `/api/v1` `DELETE /queries/{id}` follows it under password auth; under `[auth] type = "none"` it keeps today's open behavior (any caller may cancel any query), so the default Web UI does not lose cancel for pgwire and Trino queries. This does widen what an unauthenticated caller can do: before this change that cancel only changed the recorded state (D7), and now it really stops the query. Likewise, under `none` `/api/v2` and MCP accept a claimed `X-Arneb-User` as the owner (step 1), so a caller who names the owner really stops that query. `none` is a trust-everyone mode (any peer that reaches a SQL port can already run SQL), so the docs and upgrade notes say plainly: do not expose the Web port under `none`; bind it to loopback or use password auth. Reversible: v1 under `none` can follow D2 instead (one branch, one test, one note).

**D3. Write privileges** (Trino names), checked after planning:

| Statement | Privilege on the target table |
|---|---|
| `INSERT` | `INSERT` |
| `DELETE` | `DELETE` |
| `CREATE TABLE`, CTAS, `DROP TABLE`, `CREATE VIEW`, `DROP VIEW` | `OWNERSHIP` |

- The `SELECT` sources of CTAS and `INSERT ... SELECT` are already checked by planning.
- `OWNERSHIP` implies every privilege. A `read-only` catalog denies every write; a `none` catalog denies everything. `EXPLAIN` of a write is checked like the write.
- The parser has no `UPDATE`: the privilege is accepted in rules files and maps to nothing.
- Message: `Access Denied: Cannot <verb> table <catalog>.<schema>.<table> (requires <PRIVILEGE>)` with verb `insert into`, `delete from`, `create`, `drop`; for views `Access Denied: Cannot create view|drop view <c>.<s>.<v> (requires OWNERSHIP)` (no word "table", as in Trino). A read-only catalog says `(catalog <c> is read-only)`.
- It is `CatalogError::AccessDenied`: SQLSTATE `42501` on pgwire, `PERMISSION_DENIED` on Trino, error code `forbidden` in an `/api/v2` or MCP statement status (`state: failed`, HTTP `200`).
- A missing `tables` section allows everything, as before.

**D4. Dry run.** Every manage route takes `?dry_run=true|false` (default `false`; anything else is `invalid_argument`); every manage MCP tool takes `dry_run: bool`. A dry run runs the same authorization and validation as the real call, so a refused real call is refused identically. It returns the D5 envelope with `dry_run: true` and no `result`, and changes nothing. Dry runs are not audited: they change nothing, and auditing them would let any manager fill the bounded log and cost an fsync per probe. They log at `DEBUG` on `arneb::audit`.

**D5. Manage response envelope** (HTTP `200` on dry and real runs):

```json
{"dry_run": false, "action": "worker.cordon", "target": "worker-1",
 "summary": "cordons worker worker-1: it gets no new queries (1 schedulable worker left)",
 "changed": true, "warning": null, "result": { }}
```

`changed: false` marks a no-op (cordoning a cordoned worker, cancelling an ended query); a no-op is not an error. `warning` is set when the action leaves no schedulable worker, and when the state store is in memory so the change is lost on restart (`ManageAction::persisted()` decides the second). Actions: `query.cancel`, `worker.cordon`, `worker.drain`, `worker.uncordon`, `admission.set`, `token.issue`, `token.revoke`. A no-op's audit entry (for an audited kind) carries `parameters.effect` such as `already_ended`; a default `unchanged_effect()` of `no_change` applies to the others.

**D6. Durable audit log** (`arneb_state::AuditLog`).
- *Location.* `audit.jsonl` in the directory of `[state] path`, mode `0600`. With an in-memory state store the log is in memory too, with a startup WARN. Under `none` it is an empty in-memory log with no WARN, because nothing is audited there.
- *Entry:* `{"seq", "time_ms", "user", "via": "token|session|basic", "action", "target", "parameters", "outcome": "ok" | {"error": "<code>"}}`. `seq` keeps increasing across restarts (max loaded + 1). A token is named by its 16-hex id only; the revoke target never holds a plaintext token.
- *Durability.* One line per entry, appended with `O_APPEND`, then `sync_data`, before the response returns.
- *Audited:* every non-dry manage run that passed authorization (ok or error outcome); cancelling another user's query (any `KillGrant` other than `Owner`); token issue and revoke. Refused calls are not audited; they log at `INFO` on `arneb::audit`.
- *Order and failure.* (1) Check the log is writable; if not and reopening fails, refuse with `audit_unavailable` (`503`, `Retry-After: 5`) before anything is applied. (2) Apply the action. (3) Append the entry. If the append fails the action cannot be undone: log the full entry at `ERROR` on `arneb::audit`, mark the log unwritable, return the action's real result.
- *Guarantee.* At most one missing entry per action in flight when the log fails, and each is logged in full at ERROR. No lock serializes audited calls: putting PBKDF2 token issuance behind one global mutex would queue every caller, and nothing is silent either way. Every later action is refused until the log is writable again.
- *Retention* (`[audit]`, runtime knobs): `max_entries` default 10000, range `1..=1_000_000`; `retention_days` default 90, `0` = no age limit. Compaction rewrites the file atomically (`.tmp`, fsync, rename, directory fsync), keeping entries younger than `retention_days` and at most the newest `max_entries`, and always keeps the newest entry (reads still hide it past retention), so `seq` never restarts below a used value. It runs at startup only when it would drop something, and whenever the file holds `max(max_entries / 10, 100)` more lines than `max_entries`.
- *Torn lines.* At load, an unparsable line is skipped with a WARN naming its line number; startup never fails on it; the next compaction drops it.
- *Endpoint:* `GET /api/v2/audit?limit=&cursor=&user=&action=`; observe; newest first; `limit` default 50, at most 500; `cursor` is the `seq` to continue below. Body `{entries, next_cursor, truncated, persistent, writable}`.

**D7. Cancel any query.**
- *Canceller.* `QueryTracker` keeps an optional canceller per query; each protocol registers one at submit. pgwire wraps the statement future in `futures::future::Abortable` (the handler runs the statement inline, not as a task) and answers `ERROR 57014 canceling statement due to user request`. Trino calls `cancel_query` with `USER_CANCELED` `Query was killed by <user>`. Agent statements call `cancel_statement` with `cancelled` `Statement was cancelled by <user>`.
- *Why.* `QueryTracker::cancel_query` only changed the recorded state, so before this change `DELETE /api/v1|v2/queries/{id}` did not stop a pgwire or Trino query.
- *Lifetime.* A query that turns terminal drops its canceller. Registering a canceller on an entry that is already `Cancelled` calls it (outside the lock) instead of dropping it, so a kill that lands between create and register still stops the query; other terminal states drop it.
- *Route.* `DELETE /api/v2/queries/{id}` is reused: `200` with the D5 envelope instead of `204`; `result` is `{query_id, owner, state_before, state}`.
- *Ended queries.* Cancelling a query that already ended, or ends between the check and the kill, is `200` with `changed: false`. `/api/v1` keeps `204` / `403` / `404` (an ended query is `204` in every mode) and has no dry run; under password auth it uses `cancel_query_for`, so a cancel of another user's query is audited and can answer `503 audit_unavailable`.

**D8. Workers.**
- *Cordon* is coordinator-side scheduler state: a cordoned worker is left out of `NodeRegistry::active_workers` and of the tracked selection (`schedule_workers`).
- *Drain* is cordon plus watching the coordinator's in-flight count for that worker. Each distributed query holds an `InFlightGuard` over its workers; selection and increment happen under the same lock as cordon changes, so no query slips onto a worker after it is cordoned. Drain completion is coordinator-side: a worker may still hold a task for a few hundred ms after the coordinator's query ends, because its release is asynchronous. `drained` means "no coordinator query uses it", which is what a planned shutdown needs. The SIGTERM path (#178) is unchanged.
- *`scheduling`:* `schedulable`, `cordoned`, `draining` (drain requested, in-flight > 0), `drained` (in-flight = 0). The lifecycle `state` (`active|draining`, reported by the worker) is a separate, unchanged field.
- *Persistence.* Cordons are persisted in the state store before they are applied and restored into the registry at startup, before the Flight server accepts heartbeats; a worker id that re-registers stays cordoned. Cordon changes are serialized by one coordinator-wide `tokio::sync::Mutex` on `WebState`, taken in `worker_action_for` and held across the whole `run_manage` (preview, store write, registry change, audit append), as for admission changes; so the audit order matches the apply order.
- *Unregistered ids.* Cordon or drain of an id the registry never saw is `not_found` with a hint naming `GET /api/v2/workers`. Uncordon works on any id with a stored cordon. A stored cordon whose worker is not registered is listed with `registered: false`.
- *Last schedulable worker.* Allowed, with the D5 `warning`. New distributed queries then fail with "no workers available for distributed execution", which gains `(<n> cordoned; uncordon one with POST /api/v2/workers/{id}/uncordon)`. Fan-out is sized to the scheduled workers.
- *Routes:* `GET /api/v2/workers?limit=` (observe; sorted by id; default 100, at most 1000; `{workers, truncated}`), `GET /api/v2/workers/{id}`, `POST /api/v2/workers/{id}/cordon|drain|uncordon[?dry_run=]` (manage). `/api/v1|v2/cluster/workers` stay arrays of alive workers, gain `scheduling` and need observe. `GET /api/v2/cluster/workers` is kept beside the new listing for compatibility.

**D9. Admission limits.**
- *Shared state.* `max_running_queries` and `max_queued_queries` move from immutable fields into the controller's mutex state.
- *Always a controller.* One is built whenever SQL is served, also for `max_running_queries = 0`, so the limits can change at runtime. Cost: one uncontended mutex per statement. Visible effect: the admission gauges now appear at cap 0.
- *Lowering* below the running count never stops a running query: new queries queue until fewer than the new cap run (release grants a waiter only while `running < max_running`). *Raising*, or setting `0`, grants queued waiters at once up to the new cap. *A shorter queue* keeps the queued queries; new arrivals are refused while the queue is at or over the new limit.
- *Persistence* in the state store (`settings.admission`). Stored values win over the config seed: each knob whose config value differs logs one WARN on `arneb::config` naming the knob and both values; the effective-settings line gains `admission_source = config|state`.
- *Validation:* non-negative integers (at most 2^63 - 1; `null` refused). The `PUT` body is `{"max_running_queries"?, "max_queued_queries"?}`, unknown fields refused, at least one required.
- *Routes:* `GET /api/v2/admission` (observe; `{max_running_queries, max_queued_queries, running_queries, queued_queries, source}`, `source` is `config` or `state`); `PUT /api/v2/admission[?dry_run=]` (manage; `result` is the same view after the change).
- *Out of scope.* `statement_timeout` stays startup-only: it is handed by value to three protocol builders, and the "admission limits" are the two counters.

**D10. MCP tools.** Seven new tools, 25 in total; `cancel_query` is widened (an `id` that is the caller's own agent statement keeps the H3 behavior and result unless `dry_run` is set; any other id, or any dry run, goes through `cancel_query_for` and returns the D5 envelope); `list_workers` becomes the bounded listing.

| Tool | Arguments | Same as | Annotations |
|---|---|---|---|
| `list_workers` | `limit?` | `GET /workers` | read-only |
| `get_worker` | `id` | `GET /workers/{id}` | read-only |
| `cordon_worker` | `id, dry_run?` | `POST /workers/{id}/cordon` | destructive, idempotent |
| `drain_worker` | `id, dry_run?` | `POST /workers/{id}/drain` | destructive, idempotent |
| `uncordon_worker` | `id, dry_run?` | `POST /workers/{id}/uncordon` | idempotent, not destructive |
| `get_admission` | none | `GET /admission` | read-only |
| `set_admission_limits` | `max_running_queries?, max_queued_queries?, dry_run?` | `PUT /admission` | destructive, idempotent |
| `list_audit_log` | `limit?, cursor?, user?, action?` | `GET /audit` | read-only |
| `cancel_query` | `id, dry_run?` | `DELETE /statements/{id}` or `DELETE /queries/{id}` | destructive, idempotent |

All tools are `open_world_hint = false`. Limits in the copy are filled from placeholders, never hard-coded.

**D11. `whoami`** (route and tool) gains `privileges: {"observe": bool, "manage": bool}`.

**D12. Error codes.** New: `manage_requires_auth` (`403`) and `audit_unavailable` (`503`, `Retry-After: 5`). `forbidden` carries a hint naming what would grant it: manage needs "a system_information rule that allows write for user <u>, or <u> in [access_control] admin_users"; observe needs "a system_information rule that allows read for user <u>"; kill needs "a queries rule that allows kill for queryOwner <owner>, or the manage privilege". `manage_requires_auth` reads "manage actions need an authenticated user; this server runs with [auth] type = \"none\"", hint "set [auth] type = \"password\" and add your user to [access_control] admin_users".

## Notes (where the code shaped the decisions)

- **DDL/DML do not execute in any protocol today.** `ExecutionContext::create_physical_plan` refuses every DDL/DML plan and nothing calls `DDLProvider`. The write check sits right after planning in `execute_query_capped`: a denied user gets `42501` / `PERMISSION_DENIED` / `forbidden`, a permitted user still gets today's not-supported error, and when writes start executing the check is already in front of them.
- **No `UPDATE` statement exists** in the parser or planner (D3).
- **State store only under password.** `main.rs` opens it only under `[auth] type = "password"`. Manage is refused under `none`, so nothing needs persisting there. Under password with no `[state] path`, cordons, limits and the audit log live in memory, with the startup WARN and a per-response `warning`.
- **Downgrade.** `StateData` gains `cordons` and `settings` with `#[serde(default)]`, and the version stays 1: an H1-H3 binary ignores unknown fields when reading, but its next write drops them. A downgrade therefore loses cordons and runtime limits, not users or tokens.
- **Stored limits pin values.** A runtime limit stays in force across restarts and config edits; clearing it means removing `settings.admission` from the state file while the server is stopped.
- **Listings stay bounded.** Every new listing takes `limit` with a ceiling and reports `truncated`; a value above the ceiling is `invalid_argument`, never clamped.

## Risks / Trade-offs

- A manage call applies before its audit entry is written, so a failing disk can leave one action unrecorded in the file (it is in the ERROR log), after which manage is closed until the log is writable. Chosen over refusing after the fact, because a cancelled query cannot resume.
- `drained` is coordinator-side and can precede a worker's own task teardown by a moment.
- The always-built admission controller adds one mutex acquisition per statement.
