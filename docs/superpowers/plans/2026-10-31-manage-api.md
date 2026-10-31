# Manage API (H4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A remote agent with the manage privilege can cancel any query, cordon / drain / uncordon a worker and change the admission limits at runtime, each with a dry run and a durable audit entry. Write privileges (DDL/DML) are enforced from the rules on every SQL protocol.

**Architecture:**
- **One privilege module** in the server crate, `web/privilege.rs` (`Authorizer`), decides observe, manage and kill for `/api/v1`, `/api/v2` and MCP. It reads the shared `AccessRules`, which gain a Trino `system_information` section and public table privileges, plus a new `[access_control] admin_users` list.
- **SQL write privileges** are checked once, in `arneb_protocol::handler::execute_query_capped`, right after planning. pgwire, the Trino protocol, `/api/v2` statements and MCP all run SQL through that function, so all four enforce the same check.
- **Manage actions** go through one runner, `web/manage.rs` (`run_manage`). It performs the dry run, applies the action on a blocking thread, then appends to the audit log. Durable state (cordons, admission limits) lives in the existing `arneb_state::StateStore`. The audit log is a new append-only JSON-lines file next to the state file (`arneb_state::AuditLog`).
- **Runtime levers** sit in the scheduler crate:
  - `QueryTracker` gains a per-query canceller that each protocol registers, so a kill really stops the query.
  - `NodeRegistry` gains cordon state and per-worker in-flight query counts.
  - `AdmissionController` gains `set_limits`.

**Tech Stack:** Rust, axum 0.8, rmcp 3.8 (already a dependency), serde/serde_json, tokio, thiserror, tracing. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`. This plan implements stage H4: Goals 4–5, the Anti-goals, Design decisions → Runtime state / Credentials / Authorization / Surfaces, the Stages row H4, Acceptance steps 4–6 and 10, and the negative checks. It builds on:
- H1: `docs/superpowers/plans/2026-10-30-headless-api-auth.md`
- H2: `docs/superpowers/plans/2026-10-31-agent-query-api.md`
- H3: `docs/superpowers/plans/2026-10-31-remote-mcp.md`

H5 (runtime edits to catalogs, users and rules, and re-seeding) and H6 (OAuth) are out of scope.

## Global Constraints

- **Errors and logs.** Library code uses `thiserror`; `web/` uses `ApiError` and never `anyhow`. Use `tracing` for every log line. The audit log's own target is `arneb::audit`; config lines go on `arneb::config`.
- **Doc comments** on every public item. Internal functions don't need them.
- **Runtime knobs.**
  - Each knob has a TOML field and an `ARNEB_*` env override, with precedence env > file > default.
  - The effective value is logged once at startup on `arneb::config`.
  - Never read a third-party allocator or runtime env var.
- **Secrets.** Never log or print a full API token, a session token or a password, in code, test output or the smoke script. Audit entries name a token by its 16-hex id only.
- **Test seeding.** Tests seed every user of a state store in ONE `seed_users` call, because seeding is a full sync: a second call deletes the users of the first.
- **Equal rights** (Anti-goal). Every MCP tool calls the same `*_for` function as its `/api/v2` route. No tool or route reads the tracker, registry, controller or state store in a way the other does not.
- **Bounded results** (Anti-goal). Every new listing takes `limit` with a ceiling and reports `truncated`. A value above the ceiling is refused with `invalid_argument`, never clamped (same rule as H2/H3).
- **Clippy.** CI runs Rust 1.99. Run `cargo +1.99.0 clippy --workspace --all-targets -- -D warnings` before every commit. Two lints bite on this branch:
  - `result_large_err`: `ApiError` is large. Box it or return `Result<_, Box<ApiError>>` only where clippy asks, and match the existing H1–H3 code.
  - The deprecated `fetch_update`: use `fetch_max`/`compare_exchange` loops instead.
- **Git.** Never `git stash`. Don't touch other worktrees. Commit at the end of every task.
- **Housekeeping.** Run `./scripts/target-housekeeping.sh` at the start of the session; run `--sweep` at 🟡.
- **Validation tiers.**
  - **Tier 1, every task:** `cargo fmt -- --check`, the clippy command above, and `cargo test --workspace`. Paste the raw `test result:` lines.
  - **Tier 2, Task 8:** a local coordinator plus 2 workers under password auth, driven by `scripts/manage-smoke.sh`.
  - No Tier 3: H4 has no performance claim.

## Decisions

These are my calls on top of the nine decisions the controller gave. Task 8 copies them into `openspec/changes/manage-api/design.md`.

- **D1. Privilege layers** (`web/privilege.rs`, `Authorizer`).
  - **Query layer.** Unchanged: the catalog, schema and table rules, plus D3.
  - **Observe layer** (cluster, workers, admission and audit views):
    - Under `[auth] type = "none"` it is allowed.
    - Under password it is allowed when any of these holds: there is no rules file; the rules file has no `system_information` section; the first `system_information` rule matching the user allows `read`; or the user has manage.
    - Query visibility stays on the `queries` `view` rules, as today.
  - **Manage layer:**
    - Under `none`, every manage call is refused with `manage_requires_auth`.
    - Under password, manage needs the user in `admin_users`, or the first matching `system_information` rule allowing `write`. With no `system_information` section, only `admin_users` grants it (deny unless granted).
  - **Rule shape.** A `system_information` rule is `{"user": "<regex>", "allow": ["read", "write"]}`. `group` and `role` are refused like in the other sections.
  - **Write does not imply read** inside `AccessRules` (Trino semantics). The `Authorizer` treats manage as implying observe, so a manager can always list what they manage.
- **D2. Kill rule** (`Authorizer::check_kill`), checked in this order:
  1. The caller owns the query: allowed (`KillGrant::Owner`), in any auth mode, including under `none` where the owner is the claimed `X-Arneb-User`.
  2. Otherwise, under `none`: `manage_requires_auth`.
  3. Otherwise, a rules file whose `queries` section allows `kill` for this owner, or no rules file or no `queries` section: allowed (`KillGrant::Rule`). This keeps today's "a missing section allows everything" convention.
  4. Otherwise, manage: allowed (`KillGrant::Manage`).
  5. Otherwise: `forbidden`.

  Lookup and visibility:
  - A query the caller may not `view` is `not_found`, unless the caller has manage.
  - `/api/v1` `DELETE /queries/{id}` uses the same rule. This is an upgrade note: under `none`, the Web UI can no longer cancel another user's query.
- **D3. Write privileges** (Trino names), checked after planning. Mapping:

  | Statement | Privilege on the target table |
  |---|---|
  | `INSERT` | `INSERT` |
  | `DELETE` | `DELETE` |
  | `CREATE TABLE` | `OWNERSHIP` |
  | CTAS (`CREATE TABLE … AS SELECT`) | `OWNERSHIP` |
  | `DROP TABLE` | `OWNERSHIP` |
  | `CREATE VIEW` | `OWNERSHIP` |
  | `DROP VIEW` | `OWNERSHIP` |

  - **Sources.** The `SELECT` sources of CTAS and `INSERT … SELECT` are already checked by planning (`resolve_table`).
  - **Implied privileges.** `OWNERSHIP` implies every privilege, as it already does for `SELECT`.
  - **Catalogs.** A `read-only` catalog denies every write; a `none` catalog denies everything.
  - **EXPLAIN.** `EXPLAIN` of a write is checked like the write, because analysis is where Trino checks access.
  - **UPDATE.** The parser has no `UPDATE`, so `UPDATE` stays a parse error. The privilege is accepted in rules files and maps to nothing.
  - **Message:** `Access Denied: Cannot <verb> table <catalog>.<schema>.<table> (requires <PRIVILEGE>)`, where `<verb>` is one of `insert into`, `delete from`, `create`, `drop`, `create view`, `drop view`. A read-only catalog uses `Access Denied: Cannot <verb> table <c>.<s>.<t> (catalog <c> is read-only)`.
  - **Error forms.** This is `CatalogError::AccessDenied`, which already maps to:
    - SQLSTATE `42501` on pgwire;
    - `PERMISSION_DENIED` on Trino;
    - error code `forbidden` in an `/api/v2` or MCP statement status (`state: failed`; an HTTP `200`, per H2).
  - **Default.** A missing `tables` section allows everything, as before.
- **D4. Dry run.**
  - Every manage route takes `?dry_run=true|false`, defaulting to `false`; any other value is `invalid_argument`. Every manage MCP tool takes `dry_run: bool`.
  - A dry run runs the same authorization and validation as the real call: a refused real call is refused identically as a dry run.
  - It returns the D5 envelope with `dry_run: true` and no `result`, and changes nothing.
  - **Dry runs are not audited.** They change nothing; the anti-goal is about actions; auditing them would let any manager fill the bounded log and cost an fsync per probe. They log at `DEBUG` on `arneb::audit` instead.
- **D5. Manage response envelope** (HTTP `200` on both dry and real runs):

  ```json
  {"dry_run": false, "action": "worker.cordon", "target": "worker-1",
   "summary": "cordons worker worker-1: it gets no new queries (1 schedulable worker left)",
   "changed": true, "warning": null, "result": { … }}
  ```

  - `changed: false` marks a no-op, such as cordoning a cordoned worker or cancelling an ended query. A no-op is not an error.
  - `warning` is set in two cases:
    - the action leaves no schedulable worker;
    - the state store is in memory, so the change is lost on restart.
  - Action names: `query.cancel`, `worker.cordon`, `worker.drain`, `worker.uncordon`, `admission.set`, `token.issue`, `token.revoke`.
- **D6. Durable audit log** (`arneb_state::AuditLog`).
  - **Location.** `audit.jsonl` in the directory of the state file (`[state] path`), mode `0600`. When the state store is in memory, the log is in memory too, with a startup WARN. Under `none` it is an in-memory empty log, with no WARN, because nothing is audited there.
  - **Entry:** `{"seq": u64, "time_ms": u64, "user", "via": "token|session|basic", "action", "target", "parameters": {…}, "outcome": "ok" | {"error": "<code>"}}`. `seq` keeps increasing across restarts (max loaded + 1).
  - **Durability.** Each entry is one line, appended with `O_APPEND`, then `sync_data`, before the response returns.
  - **What is audited:**
    - every non-dry manage run that passed authorization (ok or error outcome);
    - cancelling another user's query (any `KillGrant` other than `Owner`);
    - token issue and revoke.

    Refused calls are not audited; they are logged at `INFO` on `arneb::audit`.
  - **Order and failure:**
    1. Check that the log is writable. If it is not, and reopening fails, refuse with `audit_unavailable` (`503`) before anything is applied.
    2. Apply the action.
    3. Append the entry. If the append fails, the action has already happened and cannot be undone (a cancelled query cannot resume). Log the full entry at `ERROR` on `arneb::audit`, mark the log unwritable, and return the action's real result.

    So at most one action per failure episode is missing from the file, that action is in the error log, and every later action is refused until the log is writable again.
  - **Retention** (`[audit]`, both runtime knobs):
    - `max_entries`: default 10000, range `1..=1_000_000`.
    - `retention_days`: default 90; `0` means no age limit.

    Compaction rewrites the file atomically (`.tmp`, fsync, rename, then directory fsync), keeping entries younger than `retention_days` and at most the newest `max_entries`. It runs at startup and whenever the file holds `max(max_entries / 10, 100)` more lines than `max_entries`. Reads hide entries past `retention_days`.
  - **Torn or corrupt lines.** At load, an unparsable line is skipped with a WARN naming its line number; startup never fails on it. The next compaction drops it.
  - **Endpoint:** `GET /api/v2/audit?limit=&cursor=&user=&action=`. It needs observe; results are newest first; `limit` defaults to 50 and is at most 500; `cursor` is the `seq` to continue below. The body is `{entries, next_cursor, truncated, persistent, writable}`.
- **D7. Cancel any query.**
  - **Canceller.** `QueryTracker` keeps an optional canceller per query. Each protocol registers one at submit:
    - **pgwire** wraps the statement future in `futures::future::Abortable`. An aborted statement answers `ERROR 57014 canceling statement due to user request`.
    - **Trino** calls the existing `cancel_query(state, query, …)` with `USER_CANCELED` `Query was killed by <user>`.
    - **Agent statements** call `cancel_statement` with `cancelled` `Statement was cancelled by <user>`.
  - **Lifetime.** A query that turns terminal drops its canceller.
  - **Route.** `DELETE /api/v2/queries/{id}` is reused (no new route). It now answers `200` with the D5 envelope instead of `204`. `result` is `{query_id, owner, state_before, state}`.
  - **Ended queries.** Cancelling a query that already ended, or that ends between the check and the kill, is `200` with `changed: false` and a summary like `query q… owned by bob already finished; nothing to cancel`.
  - `/api/v1` keeps `204` / `403` / `404` and has no dry run.
- **D8. Workers.**
  - **Cordon** is coordinator-side scheduler state: a cordoned worker is left out of `NodeRegistry::active_workers` and of the new tracked selection (`schedule_workers`).
  - **Drain** is cordon plus watching the coordinator's in-flight count for that worker. Each distributed query holds an `InFlightGuard` over the workers it was given. Selection and increment happen under the same lock as cordon changes, so no query slips onto a worker after it is cordoned.
  - **`scheduling` values:**
    - `schedulable`
    - `cordoned`
    - `draining`: drain requested, in-flight > 0
    - `drained`: drain requested, in-flight = 0
  - **Lifecycle `state` is separate.** The existing field `state` (`active|draining`, reported by the worker for the SIGTERM drain-then-exit path of #178) is unchanged; `scheduling` is a new field next to it.
  - **Persistence.** Cordons are persisted in the state store (`cordons`) before they are applied, and restored into the registry at startup, before the Flight server accepts heartbeats. A worker id that re-registers stays cordoned.
  - **Unregistered ids.**
    - Cordon or drain of an id the registry has never seen is `not_found`, with a hint naming `GET /api/v2/workers`.
    - Uncordon works on any id with a stored cordon, including one that never came back.
    - A stored cordon whose worker is not registered is listed with `registered: false`.
  - **Last schedulable worker.** Cordoning or draining it is allowed, with the D5 `warning`. New distributed queries then fail with the existing "no workers available for distributed execution" error, which gains `(<n> cordoned; uncordon one with POST /api/v2/workers/{id}/uncordon)`.
  - **Routes:**
    - `GET /api/v2/workers?limit=`: observe; sorted by id; `limit` defaults to 100, at most 1000; returns `{workers, truncated}`.
    - `GET /api/v2/workers/{id}`: observe.
    - `POST /api/v2/workers/{id}/cordon|drain|uncordon[?dry_run=]`: manage.
  - `/api/v1|v2/cluster/workers` stay arrays of alive workers, gain `scheduling`, and need observe.
- **D9. Admission limits.**
  - **Shared state.** `max_running_queries` and `max_queued_queries` move from immutable fields into the controller's mutex state.
  - **Always a controller.** A controller is now built whenever SQL is served, also for `max_running_queries = 0` (unlimited), so the limits can change at runtime. The cost is one uncontended mutex per statement where there used to be none.
  - **Lowering** below the running count never stops a running query: new queries queue until fewer than the new cap run. Release grants a waiter only while `running < max_running`.
  - **Raising,** or setting `0` (unlimited), grants queued waiters at once, up to the new cap.
  - **A shorter queue** keeps the queries already queued; new arrivals are refused while the queue is at or over the new limit.
  - **Persistence.** The limits are persisted in the state store (`settings.admission`).
  - **Startup precedence:**
    1. A stored value wins over the config seed. Each knob whose config value differs logs one WARN on `arneb::config` naming the knob, e.g. `[admission] max_running_queries / ARNEB_MAX_RUNNING_QUERIES = 8 is overridden by the runtime value 4 in the state store`.
    2. The existing admission effective-settings line gains `admission_source = config|state`.
  - **Validation** is the config's: non-negative integers. The `PUT` body is `{"max_running_queries"?, "max_queued_queries"?}` with unknown fields refused, and at least one field required.
  - **Routes:**
    - `GET /api/v2/admission`: observe. Returns `{max_running_queries, max_queued_queries, running_queries, queued_queries, source}`.
    - `PUT /api/v2/admission[?dry_run=]`: manage. `result` is the same view after the change.
  - **Out of scope.** `statement_timeout` stays startup-only (see Notes).
- **D10. MCP tools.**
  - **Seven new tools**, for 25 in total.
  - **One changed tool.** `cancel_query` is widened: an `id` that is the caller's own agent statement keeps the H3 behavior and result, unless `dry_run` is set. Any other id, or any dry run, goes through `cancel_query_for` and returns the D5 envelope.
  - **One reshaped tool.** `list_workers` becomes the bounded listing.

  | Tool | Arguments | Same as | Annotations |
  |---|---|---|---|
  | `list_workers` | `limit?` | `GET /workers` | read-only |
  | `get_worker` | `id` | `GET /workers/{id}` | read-only |
  | `cordon_worker` | `id, dry_run?` | `POST /workers/{id}/cordon` | destructive, idempotent |
  | `drain_worker` | `id, dry_run?` | `POST /workers/{id}/drain` | destructive, idempotent |
  | `uncordon_worker` | `id, dry_run?` | `POST /workers/{id}/uncordon` | idempotent, not destructive |
  | `get_admission` | — | `GET /admission` | read-only |
  | `set_admission_limits` | `max_running_queries?, max_queued_queries?, dry_run?` | `PUT /admission` | destructive, idempotent |
  | `list_audit_log` | `limit?, cursor?, user?, action?` | `GET /audit` | read-only |
  | `cancel_query` | `id, dry_run?` | `DELETE /statements/{id}` or `DELETE /queries/{id}` | destructive, idempotent |

  All tools are `open_world_hint = false`. The copy is filled from placeholders, never hard-coded: `{worker_limit}`, `{max_worker_limit}`, `{audit_limit}`, `{max_audit_limit}`.
- **D11. `whoami`** (route and tool) gains `privileges: {"observe": bool, "manage": bool}`, so an agent learns what it may do without trial and error.
- **D12. Error codes** (`ErrorCode`, `snake_case`):
  - **New:** `manage_requires_auth` (`403`) and `audit_unavailable` (`503`, `Retry-After: 5`).
  - **`forbidden`** (existing) carries the hint naming what would grant it:
    - manage: `needs a system_information rule that allows write for user <u>, or <u> in [access_control] admin_users`
    - observe: `needs a system_information rule that allows read for user <u>`
    - kill: `cancelling a query of <owner> needs a queries rule that allows kill for queryOwner <owner>, or the manage privilege`
  - **`manage_requires_auth`:**
    - message: `manage actions need an authenticated user; this server runs with [auth] type = "none"`
    - hint: `set [auth] type = "password" and add your user to [access_control] admin_users`

## Review Focus

1. **Cordoning or draining the last schedulable worker.** The call succeeds with a `warning`. The next distributed query fails with the "no workers available" error naming the cordon count and the uncordon route; uncordon makes it succeed again. (Task 5 test `cordoning_the_last_worker_warns_and_queries_say_why`.)
2. **Cancelling a query that already ended, or ends during the call.** The answer is `200` with `changed: false`: no error and no second state transition. A non-owner's cancel is still audited with `parameters.effect = "already_ended"`. (Task 4 test `cancelling_an_ended_query_is_a_noop`.)
3. **A dry run the caller may not run.** It gets the same `forbidden` / `manage_requires_auth` envelope as the real call, nothing changes, and nothing is audited. This holds for cancel, cordon and admission. (Task 4 `forbidden_dry_run_is_refused_like_the_real_call`; Tasks 5 and 6 repeat it per route.)
4. **The audit append fails.** The action stays applied, the response is the real result, the entry is logged at ERROR, and the next manage call is `503 audit_unavailable` until the log is writable again. (Task 3 `failed_append_closes_manage_until_writable`.)
5. **Admission lowered below the running count.** Running queries finish. No new query is admitted until running < the new cap, queued order is kept, and raising the cap again grants the queue at once. (Task 6 `lowering_below_running_queues_new_queries`.)

---

### Task 1: Privilege layers, `admin_users`, the observe gate and `whoami.privileges`

**Files:**
- Modify: `crates/catalog/src/access.rs`:
  - add the `system_information` section to `SECTIONS`, `RulesFile` and `AccessRules`;
  - add `QueryAccess`-style `SystemAccess { Read, Write }`;
  - make `TablePrivilege` public, renaming the private `Privilege`;
  - `section_sizes` returns 5 entries.
- Modify: `crates/catalog/src/lib.rs` (re-export `TablePrivilege`).
- Modify: `crates/server/src/config.rs` (`AccessControlConfig.admin_users`, `ARNEB_ADMIN_USERS`).
- Modify: `crates/server/src/main.rs`:
  - the rules log destructures 5 sizes and adds `system_information=`;
  - a new effective line `manage privileges effective settings ([access_control] admin_users / ARNEB_ADMIN_USERS)` with `admin_users=<list>|none` and `auth=none|password`;
  - a WARN when `admin_users` is set under `none` (it has no effect);
  - a WARN naming the `admin_users` entries that are not users in the state store under password;
  - builds `Authorizer`.
- Create: `crates/server/src/web/privilege.rs`.
- Modify: `crates/server/src/web/mod.rs` (`WebState.authorizer`).
- Modify: `crates/server/src/web/error.rs` (`ManageRequiresAuth`).
- Modify: the six `WebState { … }` test constructors in `crates/server/src/web/{auth,v2,metrics,agent}.rs` and `web/mcp/{mod,tools}.rs` (add `authorizer`).
- Modify: `crates/server/src/web/api.rs` and `crates/server/src/web/v2.rs`:
  - `cluster` and `cluster/workers` on v1 and v2 call `authorizer.check(&who, Layer::Observe)`; the v1 refusal is the `403` envelope;
  - `whoami` gains `privileges`.
- Modify: `crates/server/src/web/mcp/tools.rs` (`whoami` privileges; `get_cluster`/`list_workers` observe-gated).
- Modify: `docs/guide/access-control.md`:
  - a "Privilege layers" section (D1, D2, D12 hints);
  - `system_information` added to the supported-rules table and removed from the unsupported list;
  - `admin_users`.

  Modify: `docs/guide/configuration.md` (Access Control: `admin_users` and the env var).

**Interfaces:**
- Produces in `arneb_catalog`:
  - `pub enum TablePrivilege { Select, Insert, Delete, Update, Ownership }`, with `Display` as `SELECT` / `INSERT` / `DELETE` / `UPDATE` / `OWNERSHIP`. `GRANT_SELECT` still parses and maps to no privilege.
  - `AccessRules::has_table_privilege(&self, user: &str, catalog: &str, schema: &str, table: &str, p: TablePrivilege) -> bool`
  - `AccessRules::catalog_access(..)`, unchanged
  - `AccessRules::system_information_read(&self, user: &str) -> bool` (section absent → `true`)
  - `AccessRules::system_information_write(&self, user: &str) -> bool` (section absent → `false`)
  - `AccessRules::section_sizes(&self) -> [Option<usize>; 5]`
- Produces in config: `AccessControlConfig { rules_file, admin_users: Vec<String> }`. An empty `ARNEB_ADMIN_USERS` counts as unset; the comma list is trimmed with blanks dropped.
- Produces in `web::privilege`:
  - `#[derive(Clone)] pub struct Authorizer`
  - `Authorizer::new(rules: Option<Arc<AccessRules>>, admin_users: Vec<String>, password_auth: bool) -> Self`
  - `pub enum Layer { Observe, Manage }`
  - `Authorizer::allows(&self, who: &Identity, layer: Layer) -> bool`
  - `Authorizer::check(&self, who: &Identity, layer: Layer) -> Result<Grant, ApiError>`
  - `Authorizer::check_kill(&self, who: &Identity, owner: Option<&str>) -> Result<KillGrant, ApiError>`
  - `pub struct Grant(())`: a proof value constructible only inside `privilege.rs`
  - `pub enum KillGrant { Owner, Rule, Manage }`, with `KillGrant::grant(self) -> Grant` and `KillGrant::is_owner(&self) -> bool`
- Produces in `WebState`: `pub authorizer: privilege::Authorizer`.

- [ ] **Step 1: Write the failing tests.**
  - `access.rs`:
    - `system_information_read_and_write`: `{"system_information":[{"user":"ops","allow":["read","write"]},{"user":"viewer","allow":["read"]}]}`.
      - `ops` reads and writes.
      - `viewer` reads, does not write.
      - `carol` neither.
      - `rules("{}")` reads for anyone, writes for no one.
      - `section_sizes()[4] == Some(2)`.
    - `system_information_refuses_group_and_unknown_allow`: `"group"` → `Unsupported`; `"allow":["kill"]` → `Invalid`.
    - `table_privileges_and_ownership`: `{"catalogs":[{"catalog":"ro","allow":"read-only"},{"allow":"all"}],"tables":[{"user":"w","privileges":["SELECT","INSERT"]},{"user":"o","privileges":["OWNERSHIP"]}]}`.
      - `w` has `Insert`, not `Delete` or `Ownership`.
      - `o` has all five.
      - `o` has no `Insert` on catalog `ro`, but has `Select` there.
      - No `tables` section: everything granted.
  - `privilege.rs` (pure unit tests with `Identity` values):
    - `observe_defaults_open_and_follows_the_section`: no rules → allowed; a section without the user → `forbidden` with the D12 observe hint; an admin user is allowed without a read rule; under `none` → allowed.
    - `manage_is_denied_unless_granted`: no rules and no admins → `forbidden` with the D12 manage hint; `admin_users = ["root"]` → `root` allowed; a `write` rule → allowed; under `none`, even for an `admin_users` name → `manage_requires_auth` with the D12 message and hint.
    - `kill_rule_order`: the D2 order, cases 1–5. The owner under `none` is allowed; a non-owner under `none` is `manage_requires_auth`; `{"queries":[{"user":"ops","queryOwner":"bob","allow":["kill"]},{"allow":["execute"]}]}` → ops/bob is `Rule`, ops/carol is `forbidden`, and an admin on carol is `Manage`.
  - `config.rs`:
    - `admin_users_parse_and_env_override`: the file list, `ARNEB_ADMIN_USERS=" a, ,b "` → `["a","b"]`, an empty env keeps the file value, an unknown key is refused.
  - `v2.rs`:
    - `whoami_reports_privileges`: alice `{observe:true, manage:false}`; `root` in `admin_users` `{true,true}`; under `none`, `manage:false`.
    - `cluster_needs_observe_when_the_section_exists`: rules with `system_information` granting only `ops` read → alice `GET /api/v2/cluster` is `403 forbidden`, `GET /api/v1/cluster` is `403`, ops is `200`.
  - `mcp/tools.rs`: `whoami_reports_privileges_over_mcp` (same values as the route).
- [ ] **Step 2: Run, confirm the failures** with `cargo test -p arneb-catalog access` and `cargo test -p arneb-server privilege whoami cluster_needs admin_users`. Paste the failing lines.
- [ ] **Step 3: Implement** per D1, D2, D11, D12. `check_kill` takes the owner as `Option<&str>`; `None` (a query with no user) is treated as owner `""`, as `may_kill` does today. Keep `api::may_view` for query visibility. Leave `api::may_kill` in place: its callers still use it until Task 4, which replaces them with `check_kill` and then deletes it.
- [ ] **Step 4: Tier 1 commands.**
- [ ] **Step 5: Commit** `feat(auth): observe and manage privilege layers with admin_users`.

### Task 2: Write privileges on every SQL protocol

**Files:**
- Modify: `crates/catalog/src/lib.rs` (`CatalogManager::check_table_privilege`).
- Modify: `crates/protocol/src/handler.rs` (`check_write_privileges`, called in `execute_query_capped` between Step 2 planning and Step 2.5 optimize).
- Test: `crates/protocol/tests/access_control.rs` (pgwire and Trino); `crates/protocol/src/agent/service.rs` tests (agent API); `crates/server/src/web/mcp/tools.rs` tests (MCP).
- Modify: `docs/guide/access-control.md`:
  - the `tables` row lists the D3 mapping;
  - replace "Privileges other than SELECT and OWNERSHIP … accepted but grant no read, since Arneb does not execute writes yet" with the D3 rules;
  - add a note that connectors do not execute DDL/DML yet, so a permitted write still fails with the existing not-supported error;
  - a new section `## Upgrading to v0.2.0` with three items:
    - writes are now checked against `tables` rules on every protocol, so a rules file with a `tables` section that grants only `SELECT` refuses DDL/DML;
    - a `read-only` catalog refuses writes;
    - `system_information` is now accepted.

**Interfaces:**
- Consumes: `TablePrivilege` and `AccessRules::has_table_privilege` from Task 1.
- Produces:
  - `CatalogManager::check_table_privilege(&self, reference: &TableReference, privilege: TablePrivilege, verb: &str) -> Result<(), CatalogError>`. It resolves catalog and schema from the manager's defaults exactly like `resolve_table`. It always passes on a manager without `with_access`. The message follows D3.
  - `pub(crate) fn check_write_privileges(plan: &LogicalPlan, catalogs: &CatalogManager) -> Result<(), ArnebError>`. It recurses through `Explain`, and checks D3's target per variant.

- [ ] **Step 1: Write the failing tests.**
  - In `access_control.rs`, extend `RULES`' `tables` with `{"user":"writer","catalog":"memory","privileges":["SELECT","INSERT","DELETE"]}` and `{"user":"owner","catalog":"memory","privileges":["OWNERSHIP"]}`. Make the first catalog rule `{"catalog":"memory","allow":"all"}`, keeping `lake` read-only for others.
  - `pgwire_refuses_writes_without_the_privilege`, using `pg_error`:
    - alice `CREATE TABLE memory.default.t2 (n BIGINT)` → `("42501", msg)` with `msg` containing `Cannot create table memory.default.t2 (requires OWNERSHIP)`.
    - alice `INSERT INTO memory.default.open_t SELECT n FROM memory.default.open_t` → `42501` with `requires INSERT`.
    - alice `DELETE FROM memory.default.open_t` → `requires DELETE`.
    - alice `DROP VIEW memory.default.v` → `requires OWNERSHIP`.
    - alice `EXPLAIN INSERT INTO …` → `42501`.
    - writer `INSERT …` → an error that is **not** `42501` (today's not-supported error).
    - owner `CREATE TABLE …` → not `42501`.
    - writer `CREATE TABLE …` → `42501`.
    - owner `CREATE TABLE lake.sales.x (n BIGINT)` → `42501` with `catalog lake is read-only`.
    - Unqualified `CREATE TABLE t3 (n BIGINT)` as alice names `memory.default.t3`.
  - `trino_refuses_writes_without_the_privilege`: the same alice CTAS through `/v1/statement` → `errorName == "PERMISSION_DENIED"`, and the message names the table and `OWNERSHIP`.
  - `service.rs`:
    - `write_without_privilege_fails_forbidden`: with rules granting alice `SELECT` only, `CREATE TABLE memory.default.t AS SELECT 1 AS x` → `state == Failed` with `error.code == AgentErrorCode::Forbidden` and `message` containing `requires OWNERSHIP`.
    - `ddl_statement_fails_with_error_body`, the existing test, still passes (no rules: not forbidden).
  - `mcp/tools.rs`: `submit_query_write_is_forbidden`: a fixture with the same rules; `submit_query` CTAS as alice → status `failed`, `error.code == "forbidden"`.
- [ ] **Step 2: Run, confirm the failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Tier 1 commands.** Also run `cargo test -p arneb-protocol --test access_control --test pg_catalog` and paste the result lines.
- [ ] **Step 5: Commit** `feat(auth): enforce table write privileges on every protocol`.

### Task 3: Durable audit log, `[audit]` config and `GET /api/v2/audit`

**Files:**
- Create: `crates/state/src/audit.rs`.
- Modify: `crates/state/src/lib.rs` (exports).
- Modify: `crates/state/Cargo.toml` (feature `test-hooks = []`).
- Modify: `crates/state/src/file.rs`: extract the tmp + fsync + rename + directory-fsync logic of `persist` into `pub(crate) fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), StateError>`, reused by compaction; no behavior change.
- Modify: `crates/server/Cargo.toml` (dev-dependency `arneb-state` with `features = ["test-hooks"]`).
- Modify: `crates/server/src/config.rs` (`AuditConfig`, `[audit]`, env).
- Modify: `crates/server/src/main.rs`:
  - open the log;
  - the effective line `audit effective settings ([audit] / ARNEB_AUDIT_*)` with `path=<…>|in-memory`, `max_entries`, `retention_days` and `loaded`;
  - the in-memory WARN;
  - extend the existing `no [state] path` WARN to: `API tokens, worker cordons, admission limits and the audit log are lost on restart`.
- Create: `crates/server/src/web/audit.rs` (route + the `audited` helper).
- Modify: `crates/server/src/web/{mod.rs,error.rs,v2.rs}`:
  - `WebState.audit`;
  - `AuditUnavailable`;
  - route merge;
  - `issue_token_for` and `revoke_token_for` audited.
- Modify: `docs/guide/configuration.md` (an `## Audit Log` section with the knob table; State section mention).
- Modify: `docs/guide/http-api.md` (an `### Audit log` subsection under a new `## Managing` heading, with the entry shape).

**Interfaces:**
- Produces in `arneb_state`:
  - `pub struct AuditSettings { pub max_entries: usize, pub retention_days: u32 }`
  - `pub struct AuditRecord { pub user: String, pub via: String, pub action: String, pub target: String, pub parameters: serde_json::Value, pub outcome: AuditOutcome }`
  - `pub enum AuditOutcome { Ok, Error(String) }`, serialized as `"ok"` or `{"error": code}`
  - `pub struct AuditEntry { pub seq: u64, pub time_ms: u64, #[serde(flatten)] pub record: AuditRecord }`
  - `pub struct AuditQuery { pub limit: usize, pub before_seq: Option<u64>, pub user: Option<String>, pub action: Option<String> }`
  - `pub struct AuditPage { pub entries: Vec<AuditEntry>, pub next_cursor: Option<u64>, pub truncated: bool }`
  - `pub struct AuditLog`, with:
    - `AuditLog::open(path: Option<&Path>, settings: AuditSettings) -> Result<AuditLog, StateError>`
    - `AuditLog::sibling_of(state_path: &Path) -> PathBuf` (the directory's `audit.jsonl`)
    - `append(&self, record: AuditRecord) -> Result<AuditEntry, StateError>`: blocking; write + `sync_data`; on failure marks the log unwritable
    - `query(&self, q: &AuditQuery) -> AuditPage`
    - `ensure_writable(&self) -> Result<(), StateError>`: no-op when writable; otherwise reopens the file
    - `is_writable(&self) -> bool`
    - `is_persistent(&self) -> bool`
    - `len(&self) -> usize`
    - `#[cfg(any(test, feature = "test-hooks"))] pub fn set_fail_appends(&self, fail: bool)`
  - Entries are kept in memory (`VecDeque`, at most `max_entries`) for reads; the file is the durable copy.
- Produces in config: `pub struct AuditConfig { pub max_entries: usize, pub retention_days: u32 }`, with `#[serde(default, deny_unknown_fields)]`, defaults `10000` / `90`, env vars `ARNEB_AUDIT_MAX_ENTRIES` and `ARNEB_AUDIT_RETENTION_DAYS`, and `validate`: `max_entries` in `1..=1_000_000`.
- Produces in `web::audit`:
  - `pub(super) struct AuditSubject { pub target: String, pub parameters: serde_json::Value }`
  - `pub(super) fn audited<T>(state: &WebState, who: &Identity, action: &'static str, run: impl FnOnce() -> Result<T, ApiError>, subject: impl FnOnce(Result<&T, &ApiError>) -> AuditSubject) -> Result<T, ApiError>`. It is blocking and must be called from `spawn_blocking`, and implements D6's order and failure rules.
  - `pub(super) fn audit_page_for(state: &WebState, who: &Identity, params: AuditParams) -> Result<AuditResponse, ApiError>`, where `AuditParams { limit: Option<usize>, cursor: Option<u64>, user: Option<String>, action: Option<String> }`.
  - `pub(super) const DEFAULT_AUDIT_LIMIT: usize = 50` and `MAX_AUDIT_LIMIT: usize = 500`.
  - `pub(super) fn routes() -> Router<WebState>`.
- Produces in `WebState`: `pub audit: Arc<arneb_state::AuditLog>`.

- [ ] **Step 1: Write the failing tests.**
  - `audit.rs` (tempdir):
    - `append_survives_reopen_and_seq_continues`: append 3, reopen → 3 entries, newest first; the next append has `seq == 4`; file mode `0600` (unix).
    - `torn_last_line_is_skipped`: write two valid lines plus `{"seq":3,` → open succeeds with 2 entries; the next compaction removes the torn line.
    - `retention_and_max_entries_compact_atomically`: `max_entries = 100`; append 250 → the file has at most 100 + 100 lines; reopen → exactly the newest 100. Entries with `time_ms` older than `retention_days = 1` are hidden from `query` and dropped at compaction. No `.tmp` is left behind.
    - `query_filters_and_pages`: 120 entries over 2 users and 2 actions; `limit 50` → 50 entries with `truncated: true` and `next_cursor == Some(seq of the 50th)`. Continuing with `before_seq` walks all of them exactly once. The `user` and `action` filters are exact matches.
    - `failed_append_marks_unwritable`: `set_fail_appends(true)` → `append` is `Err`, and `is_writable()` is false. `set_fail_appends(false)` → `ensure_writable()` is `Ok` and `is_writable()` is true.
    - `in_memory_log_is_not_persistent`.
  - `config.rs`:
    - `audit_config_defaults_env_and_validation`: the defaults; both env vars override; `max_entries = 0` is refused, and so is `1_000_001`; an unknown key is refused.
  - `web/audit.rs` (router tests, password auth, one `seed_users` call with alice, bob and root; `admin_users = ["root"]`):
    - `token_issue_and_revoke_are_audited`: alice issues and revokes a token → `GET /api/v2/audit` shows `token.revoke` then `token.issue`, `user == "alice"`, `via == "basic"`, `target` == the 16-hex id. The serialized entries contain no `arneb_` token text (assert `!body.contains(&issued_token)`).
    - `audit_listing_is_bounded_and_needs_observe`: `limit=501` → `invalid_argument` naming 500. With a `system_information` section granting only root read, alice gets `403`.
    - `failed_append_closes_manage_until_writable` (Review Focus 4):
      1. `set_fail_appends(true)`.
      2. alice issues a token → `201`, and the token works.
      3. The next issue → `503 audit_unavailable`, and no token is created (`GET /tokens` count unchanged).
      4. `set_fail_appends(false)`.
      5. The next issue → `201`, audited.
- [ ] **Step 2: Run, confirm the failures.**
- [ ] **Step 3: Implement** per D6.
  - In `issue_token_for`, move the `store.issue_token` call into the closure given to `audited`, inside the existing `spawn_blocking`.
  - In `revoke_token_for`, do the same for `store.revoke_token`.
  - Parameters: `{"name", "expires_ms"}` for issue, `{}` for revoke.
  - Token actions are not manage actions, so they need no manage grant; under `none` they stay `auth_disabled` before any audit.
- [ ] **Step 4: Tier 1 commands.**
- [ ] **Step 5: Commit** `feat(state): durable audit log with retention and GET /api/v2/audit`.

### Task 4: Manage runner, dry run and cancelling any query

**Files:**
- Modify: `crates/scheduler/src/tracker.rs` (canceller and `kill_query`), `crates/scheduler/src/lib.rs` (exports).
- Modify: `crates/protocol/src/handler.rs` (`run_statement`: `Abortable` and canceller registration; the aborted result maps to `57014`).
- Modify: `crates/protocol/src/trino/mod.rs` (register a canceller after `query.abort` is stored).
- Modify: `crates/protocol/src/agent/service.rs` (register a canceller in `submit` after the abort handle is stored).
- Create: `crates/server/src/web/manage.rs`.
- Modify: `crates/server/src/web/v2.rs` (`DELETE /queries/{id}` → `cancel_query_for`; the existing `cancel_query` test expectations go from `204` to `200` envelope) and `crates/server/src/web/api.rs` (v1 `DELETE` → `cancel_query_for(.., dry_run=false)`, mapped back to `204`, or `403`/`404`).
- Modify: `docs/guide/http-api.md` (`## Managing` → `### Dry runs and the response envelope` (D4/D5), `### Cancel any query` (D2/D7)); `docs/guide/access-control.md` (Limits: the kill paragraph now cites D2 and the `none` change).

**Interfaces:**
- Consumes: `Authorizer::check_kill`, `KillGrant`, `Grant` (Task 1); `audited`, `AuditSubject` (Task 3).
- Produces in `arneb_scheduler`:
  - `pub type Canceller = Box<dyn FnOnce(&str) + Send + Sync>`. The argument is the killer's user name.
  - `QueryTracker::set_canceller(&self, id: &QueryId, canceller: Canceller)`: dropped without being called if the query is already terminal.
  - `QueryTracker::kill_query(&self, id: &QueryId, by: &str) -> Result<KillOutcome, ExecutionError>`. Under the write lock it takes the canceller and records the prior state. It drops the lock, calls the canceller, then calls `cancel_query` (ignoring an already-terminal error).
  - `pub enum KillOutcome { Killed { was: QueryState }, AlreadyEnded(QueryState) }`.
  - `update()` drops an entry's canceller when it turns terminal.
- Produces in `web::manage`:
  - `#[derive(Deserialize, Default)] pub(super) struct DryRun { #[serde(default)] pub dry_run: bool }`
  - `pub(super) struct Preview { pub summary: String, pub changed: bool, pub warning: Option<String> }`
  - `pub(super) struct Applied<T> { pub preview: Preview, pub result: T }`
  - `#[derive(Serialize)] pub(super) struct ManageOutcome<T> { dry_run, action, target, summary, changed, warning, result: Option<T> }` (D5; `result` is skipped when `None`)
  - `pub(super) trait ManageAction: Send + 'static` with:
    - `type Output: Serialize + Send + 'static;`
    - `fn action(&self) -> &'static str;`
    - `fn target(&self) -> String;`
    - `fn parameters(&self) -> serde_json::Value;`
    - `fn audited(&self) -> bool { true }`
    - `fn preview(&self, state: &WebState) -> Result<Preview, ApiError>;`
    - `fn apply(self, state: &WebState) -> Result<Applied<Self::Output>, ApiError>;` (blocking)
  - `pub(super) async fn run_manage<A: ManageAction>(state: &WebState, who: &Identity, grant: Grant, dry_run: bool, action: A) -> Result<ManageOutcome<A::Output>, ApiError>`. A dry run returns from `preview`. A real run calls `apply` inside `spawn_blocking`, wrapped in `audited` when `action.audited()`. The audit `parameters` are `action.parameters()` merged with `{"effect": "applied"|"no_change"|"already_ended"}`.
- Produces in `web::v2`:
  - `pub(super) async fn cancel_query_for(state: &WebState, who: &Identity, id: &str, dry_run: bool) -> Result<ManageOutcome<KillResult>, ApiError>`
  - `#[derive(Serialize)] pub(super) struct KillResult { query_id: String, owner: Option<String>, state_before: String, state: String }`, with lowercase state names as in `api::QUERY_STATES`.
  - The action is `query.cancel`; it is audited unless `KillGrant::Owner`.

- [ ] **Step 1: Write the failing tests.**
  - `tracker.rs`:
    - `kill_calls_the_canceller_once_and_marks_cancelled`: the canceller records `by`; `kill_query(id, "root")` → `Killed { was: Running }`, the canceller got `"root"`, the state is `Cancelled`. A second kill → `AlreadyEnded(Cancelled)`, and the canceller is not called again.
    - `terminal_query_drops_its_canceller`: `finish_query` then `kill_query` → `AlreadyEnded(Finished)`, and the canceller was dropped uncalled (assert through a drop guard).
    - `canceller_may_reenter_the_tracker`: a canceller that calls `tracker.cancel_query(id)` itself does not deadlock.
  - `protocol/tests/access_control.rs` (real execution over a slow source; reuse the H2 blocking-source pattern):
    - `tracker_kill_stops_a_pgwire_statement`: a pgwire statement is running → `tracker.kill_query(id, "root")` → the client receives SQLSTATE `57014` within 2 s.
    - `tracker_kill_stops_a_trino_query`: the next poll reports `FAILED` with `errorName == "USER_CANCELED"` and a message containing `killed by root`.
  - `service.rs`:
    - `tracker_kill_cancels_an_agent_statement`: `status(..)` → `Cancelled`, the error message contains `cancelled by root`, and the abort happened (the existing `dropped` flag pattern).
  - `v2.rs` (password auth, one `seed_users`: alice, bob, root; `admin_users = ["root"]`; rules `{"queries":[{"user":"ops","queryOwner":"bob","allow":["kill"]},{"allow":["execute","view"]}]}`, with ops seeded too):
    - `owner_cancels_without_audit`: bob cancels his running query → `200`, `changed: true`, `result.state == "cancelled"`, and the audit is empty.
    - `kill_rule_and_manage_cancel_others_and_are_audited`:
      - ops cancels bob's query → `200`;
      - root cancels alice's query → `200`;
      - alice cancels bob's → `403 forbidden` with the D12 kill hint.
      - The audit has 2 `query.cancel` entries (ops and root) with `target` = the query ids and outcome `ok`.
    - `dry_run_cancels_nothing`: root `?dry_run=true` on alice's running query → `dry_run: true`, a `summary` matching `^cancels 1 query \(.+\) owned by alice`, no `result`. The query is still running and the audit is unchanged.
    - `forbidden_dry_run_is_refused_like_the_real_call` (Review Focus 3): alice `?dry_run=true` on bob's query → the same status and body as without `dry_run`. Under `none`, a non-owner dry run → `manage_requires_auth`.
    - `cancelling_an_ended_query_is_a_noop` (Review Focus 2): root on a finished query → `200`, `changed: false`, a summary containing `already finished`; the audit entry has `parameters.effect == "already_ended"`. A malformed id → `invalid_argument`; an unknown UUID → `not_found`.
    - `bad_dry_run_value_is_invalid_argument`: `?dry_run=maybe` → `400 invalid_argument`.
    - `v1_cancel_uses_the_same_rule`:
      - password auth: alice on bob's query → `403`; root → `204`, audited.
      - under `none`: another user's query → `403` with `manage_requires_auth`; one's own → `204`.
  - The `auth.rs` test `hidden_query_is_not_found_and_unkillable_query_is_forbidden` keeps passing, with updated status expectations where the route now answers `200`.
- [ ] **Step 2: Run, confirm the failures.**
- [ ] **Step 3: Implement** per D2, D4, D5, D7.
  - The `Abortable` wraps the `with_timeout(...)` future in `run_statement`.
  - Register the canceller right after `TrackedStatement::start`. Its closure holds only the `futures::future::AbortHandle`.
  - The Trino and agent cancellers hold a `Weak` of the query or statement, so the tracker never keeps them alive.
- [ ] **Step 4: Tier 1 commands.**
- [ ] **Step 5: Commit** `feat(manage): cancel any query with dry run and audit`.

### Task 5: Cordon, drain and uncordon, persisted

**Files:**
- Modify: `crates/scheduler/src/node_registry.rs`:
  - a `scheduling: Arc<Mutex<Scheduling>>` holding the cordons and the in-flight counts;
  - `active_workers` filters out cordoned workers.
- Modify: `crates/scheduler/src/lib.rs` (exports).
- Modify: `crates/server/src/coordinator.rs`:
  - `execute_once` uses `schedule_workers()` instead of `active_workers()` and holds the guard to the end of the attempt;
  - the "no workers available" message gains the cordon count and the uncordon hint (D8).
- Modify: `crates/state/src/file.rs` (`StateData.cordons`, `#[serde(default)]`), and create `crates/state/src/cordons.rs`.
- Modify: `crates/server/src/main.rs`: after the state store opens, restore the cordons into `node_registry` and log `restored worker cordons count=<n>` on `arneb::config`.
- Create: `crates/server/src/web/workers.rs`.
- Modify: `crates/server/src/web/{v2.rs,api.rs}` (merge the routes; `WorkerResponse.scheduling`).
- Modify: `docs/guide/http-api.md` (`### Workers`: the states and routes from D8) and `docs/guide/distributed.md` (one paragraph: cordon/drain from the API versus SIGTERM drain-then-exit, which is unchanged).

**Interfaces:**
- Produces in `arneb_scheduler`:
  - `pub enum CordonMode { Cordon, Drain }`
  - `pub enum SchedulingState { Schedulable, Cordoned, Draining, Drained }`, with `as_str()` → `schedulable|cordoned|draining|drained`
  - `NodeRegistry::set_cordon(&self, worker_id: &str, mode: Option<CordonMode>)`
  - `NodeRegistry::restore_cordons(&self, cordons: impl IntoIterator<Item = (String, CordonMode)>)`
  - `NodeRegistry::cordon_of(&self, worker_id: &str) -> Option<CordonMode>`
  - `NodeRegistry::cordoned_ids(&self) -> Vec<String>`
  - `NodeRegistry::scheduling_state(&self, worker_id: &str) -> SchedulingState`
  - `NodeRegistry::in_flight(&self, worker_id: &str) -> usize`
  - `NodeRegistry::is_known(&self, worker_id: &str) -> bool`: registered at any liveness
  - `NodeRegistry::schedule_workers(&self) -> (Vec<WorkerInfo>, InFlightGuard)`
  - `pub struct InFlightGuard`: decrements its workers' counts on drop
- Produces in `arneb_state`:
  - `pub enum CordonKind { Cordon, Drain }`
  - `pub struct CordonRecord { pub worker_id: String, pub kind: CordonKind, pub since_ms: u64, pub by: String }`
  - `StateStore::cordons(&self) -> Vec<CordonRecord>`
  - `StateStore::set_cordon(&self, record: CordonRecord) -> Result<(), StateError>` (upsert by `worker_id`)
  - `StateStore::clear_cordon(&self, worker_id: &str) -> Result<bool, StateError>`
- Produces in `web::workers`:
  - `pub(super) enum WorkerOp { Cordon, Drain, Uncordon }`
  - `#[derive(Serialize)] pub(super) struct WorkerView { worker_id, address, registered: bool, alive: bool, version, state: &'static str /* lifecycle */, scheduling: &'static str, running_queries: usize, cordoned_since_ms: Option<u64>, cordoned_by: Option<String>, last_heartbeat_secs_ago: Option<u64> }`
  - `pub(super) fn list_workers_for(state: &WebState, who: &Identity, limit: Option<usize>) -> Result<WorkerList, ApiError>`, where `WorkerList { workers: Vec<WorkerView>, truncated: bool }`
  - `pub(super) fn get_worker_for(state: &WebState, who: &Identity, id: &str) -> Result<WorkerView, ApiError>`
  - `pub(super) async fn worker_action_for(state: &WebState, who: &Identity, id: &str, op: WorkerOp, dry_run: bool) -> Result<ManageOutcome<WorkerView>, ApiError>`
  - `pub(super) const DEFAULT_WORKER_LIMIT: usize = 100` and `MAX_WORKER_LIMIT: usize = 1000`
  - `pub(super) fn routes() -> Router<WebState>`
  - `apply` persists first (`set_cordon` / `clear_cordon`), then updates the registry. A persist failure is an `internal` error and changes nothing.

- [ ] **Step 1: Write the failing tests.**
  - `node_registry.rs`:
    - `cordoned_worker_is_alive_but_not_scheduled`: `set_cordon("w1", Some(Cordon))` → `active_workers()` has no w1, `alive_workers()` still has it; `scheduling_state == Cordoned`.
    - `drain_goes_draining_then_drained`: `schedule_workers()` → guard over w1 and w2; `set_cordon("w1", Some(Drain))` → `Draining`; drop the guard → `Drained` with `in_flight("w1") == 0`.
    - `no_query_is_scheduled_onto_a_worker_cordoned_meanwhile`: 8 threads loop `schedule_workers()` while another thread cordons w1. After the cordon returns, every later selection excludes w1, and w1's count only falls.
    - `restored_cordon_applies_when_the_worker_registers`: `restore_cordons([("w9", Cordon)])`, then `heartbeat("w9", …)` → w9 is not in `active_workers()`.
  - `state` (`cordons.rs`): `cordons_survive_reopen_and_clear`: upsert two cordons, upsert one again with `Drain`, clear the other, reopen → one record with `kind == Drain`. A file without `cordons` (H1 format) loads with none.
  - `coordinator.rs`: `cordoned_workers_get_no_tasks` (the `recording_worker` pattern): two recording workers with w1 cordoned → a query dispatches only to w2. With both cordoned → the error contains `2 cordoned` and `/uncordon`.
  - `workers.rs` (router; password auth; one `seed_users`; `admin_users = ["root"]`; a registry with w1 and w2 heartbeating):
    - `manager_cordons_and_uncordons`: root `POST /workers/w1/cordon` → `changed: true`, `result.scheduling == "cordoned"`. `GET /workers/w1` agrees; the state store has the record; the audit has `worker.cordon` by root. Cordon again → `changed: false`. Uncordon → `schedulable`, audited.
    - `drain_reports_draining_then_drained`: hold `schedule_workers()`, drain w1 → `scheduling == "draining"` and `running_queries == 1` (returned at once); drop → `GET` shows `drained`.
    - `non_manager_is_refused_including_dry_run`: alice cordon, and alice cordon with `?dry_run=true` → the same `403 forbidden` with the manage hint; nothing changes. Under `none` → `manage_requires_auth`.
    - `dry_run_changes_nothing`: root `?dry_run=true` drain → `dry_run: true`, a summary starting `drains worker w1`; w1 still schedulable; the state store and audit are unchanged.
    - `unknown_worker_is_not_found`: cordon `nope` → `not_found` with a hint containing `GET /api/v2/workers`.
    - `cordoning_the_last_worker_warns_and_queries_say_why` (Review Focus 1): cordon w1 then w2 → the second response has a `warning` containing `no schedulable worker`.
    - `stale_cordon_is_listed_and_can_be_cleared`: a stored cordon for `gone` that never registers → `GET /workers` lists it with `registered: false, alive: false, scheduling: "cordoned"`; uncordon `gone` → `changed: true`, the record is removed; uncordon again → `not_found`.
    - `cordons_survive_a_restart`: a persistent store in a tempdir; cordon w1, then build a new registry and a new `WebState` from the reopened store (the same restore function main.rs calls) → w1 is cordoned before any heartbeat, and after its heartbeat it still is.
    - `worker_listing_is_bounded`: 1001 known ids → `limit=1001` → `invalid_argument` naming 1000; the default lists 100 with `truncated: true`.
    - `in_memory_store_warns_on_cordon`: an in-memory store → `warning` containing `lost on restart`.
    - `cluster_workers_report_scheduling`: `/api/v2/cluster/workers` entries carry `scheduling`, and the existing fields are unchanged.
- [ ] **Step 2: Run, confirm the failures.**
- [ ] **Step 3: Implement** per D8.
  - Summary copy:
    - `cordons worker <id>: it gets no new queries (<n> schedulable workers left)`
    - `drains worker <id>: no new queries; <k> running queries finish on it`
    - `uncordons worker <id>: it gets new queries again`
  - The no-op summaries say `already cordoned` / `already draining` / `not cordoned`.
- [ ] **Step 4: Tier 1 commands.**
- [ ] **Step 5: Commit** `feat(manage): cordon, drain and uncordon workers, persisted across restarts`.

### Task 6: Runtime admission limits, persisted

**Files:**
- Modify: `crates/scheduler/src/admission.rs` (limits under the mutex, `set_limits`, `release` honors the cap).
- Modify: `crates/state/src/file.rs` (`StateData.settings`, `#[serde(default)]`), and create `crates/state/src/settings.rs`.
- Modify: `crates/server/src/config.rs` (`resolve_admission_limits`).
- Modify: `crates/server/src/main.rs`:
  - always build the controller when SQL is served;
  - apply the resolved limits;
  - the per-knob divergence WARN;
  - `admission_source` on the existing effective line.
- Create: `crates/server/src/web/admission.rs`.
- Modify: `crates/server/src/web/v2.rs` (merge).
- Modify: `docs/guide/configuration.md` (Admission Control: runtime changes, precedence and the WARN text), `docs/guide/http-api.md` (`### Admission limits`).

**Interfaces:**
- Produces in `arneb_scheduler`: `AdmissionController::set_limits(&self, max_running: usize, max_queued: usize) -> AdmissionSnapshot` (D9 semantics). `snapshot()` reads the limits under the lock.
- Produces in `arneb_state`:
  - `#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)] pub struct AdmissionLimits { pub max_running_queries: usize, pub max_queued_queries: usize }`
  - `StateStore::admission_limits(&self) -> Option<AdmissionLimits>`
  - `StateStore::set_admission_limits(&self, limits: AdmissionLimits) -> Result<(), StateError>`
- Produces in config:
  - `pub fn resolve_admission_limits(config: &AdmissionConfig, stored: Option<AdmissionLimits>) -> ResolvedAdmission`, with `ResolvedAdmission { limits: AdmissionLimits, source: &'static str /* "config" | "state" */, diverging: Vec<(&'static str /* knob */, usize /* config */, usize /* stored */)> }`. It is pure; main.rs logs from it.
- Produces in `web::admission`:
  - `#[derive(Serialize)] pub(super) struct AdmissionView { max_running_queries, max_queued_queries, running_queries, queued_queries, source: &'static str }`
  - `#[derive(Deserialize, Default)] #[serde(deny_unknown_fields)] pub(super) struct AdmissionUpdate { pub max_running_queries: Option<serde_json::Value>, pub max_queued_queries: Option<serde_json::Value> }`. The values are kept untyped so that `-1`, `1.5` or `"x"` get the range hint, as `expires_in_days` does.
  - `pub(super) fn get_admission_for(state: &WebState, who: &Identity) -> Result<AdmissionView, ApiError>`
  - `pub(super) async fn set_admission_for(state: &WebState, who: &Identity, req: AdmissionUpdate, dry_run: bool) -> Result<ManageOutcome<AdmissionView>, ApiError>`
  - `pub(super) fn routes() -> Router<WebState>`
  - `WebState` keeps `admission: Option<AdmissionController>`, which is `Some` on every SQL-serving node now. `source` comes from whether the state store holds limits.

- [ ] **Step 1: Write the failing tests.**
  - `admission.rs`:
    - `raising_the_cap_admits_the_queue`: `(1, 10)`; one admitted, two queued; `set_limits(3, 10)` → both queued waits resolve, and the snapshot shows `running == 3`.
    - `lowering_below_running_queues_new_queries` (Review Focus 5): `(4, 10)` with 4 running; `set_limits(2, 10)` → a new admit is queued. Release one: still queued (running 3 ≥ 2). Release two more → the queued query is granted. The FIFO order of two queued queries is kept.
    - `zero_means_unlimited_at_runtime`: queued waiters are all granted by `set_limits(0, 10)`.
    - `shrinking_the_queue_keeps_queued_queries`: 5 queued, `set_limits(1, 2)` → all 5 stay queued; a new admit is `QueueFull`.
  - `config.rs`:
    - `stored_admission_wins_and_reports_divergence`: config `(8, 256)` with stored `(4, 256)` → limits `(4, 256)`, source `state`, `diverging == [("max_running_queries", 8, 4)]`; stored `None` → config, nothing diverging.
  - `state`: `admission_limits_survive_reopen`.
  - `web/admission.rs` (password auth; one `seed_users`; `admin_users = ["root"]`):
    - `get_reports_limits_and_counters`: the shape and `source == "config"`.
    - `put_is_partial_persisted_and_audited`: root `PUT {"max_running_queries": 2}` → `result.max_running_queries == 2`, `max_queued_queries` unchanged, `source == "state"`. The state store has `(2, <old>)`, the controller snapshot agrees, and the audit has `admission.set` with `parameters == {"max_running_queries": {"from": 8, "to": 2}}`.
    - `put_validation`: `{}` → `invalid_argument` (`send max_running_queries and/or max_queued_queries`); `{"max_running_queries": -1}` → `invalid_argument` with a hint containing `non-negative integer`; `{"bogus": 1}` → `invalid_argument` naming `bogus`.
    - `non_manager_and_dry_run`: alice PUT and alice PUT `?dry_run=true` → `403`. Root `?dry_run=true` → a summary matching `sets max_running_queries 8 → 2` that mentions the running count; the controller is unchanged and nothing is audited.
    - `restart_restores_runtime_limits`: a persistent store → PUT, then reopen the store and `resolve_admission_limits` with the config `(8, 256)` → `(2, 256)` from `state`.
- [ ] **Step 2: Run, confirm the failures.**
- [ ] **Step 3: Implement** per D9. The WARN text follows D9; log one WARN per diverging knob.
- [ ] **Step 4: Tier 1 commands.** Also run `cargo test -p arneb-protocol` in full: the protocols now always get a controller.
- [ ] **Step 5: Commit** `feat(manage): change admission limits at runtime, persisted`.

### Task 7: MCP manage tools

**Files:**
- Modify: `crates/server/src/web/mcp/tools.rs`:
  - the new argument structs and tools;
  - `cancel_query` widened;
  - `list_workers` bounded;
  - `copy_values` gains the D10 placeholders;
  - `TOOL_NAMES`, `tool_list_contract` and the annotation expectations.
- Modify: `crates/server/src/web/mcp/mod.rs`: `INSTRUCTIONS` gains one `Operations:` paragraph and stays ≤ 1500 characters once filled (trim existing copy if needed). The paragraph says:
  - observe tools need no extra privilege;
  - manage tools need the manage privilege (see `whoami.privileges`);
  - every manage tool takes `dry_run`, so preview first;
  - actions are audited.
- Modify: `docs/guide/mcp.md` (the tool table gains D10's rows; a short "Managing the cluster" section).

**Interfaces:**
- Consumes:
  - `v2::cancel_query_for` (Task 4)
  - `workers::{list_workers_for, get_worker_for, worker_action_for, WorkerOp, DEFAULT_WORKER_LIMIT, MAX_WORKER_LIMIT}` (Task 5)
  - `admission::{get_admission_for, set_admission_for, AdmissionUpdate}` (Task 6)
  - `audit::{audit_page_for, AuditParams, DEFAULT_AUDIT_LIMIT, MAX_AUDIT_LIMIT}` (Task 3)
  - `Authorizer` (Task 1)
- Produces: the D10 tools. The argument structs are `WorkerIdArgs { id }`, `WorkerActionArgs { id, dry_run: Option<bool> }`, `AdmissionArgs { max_running_queries: Option<i64>, max_queued_queries: Option<i64>, dry_run: Option<bool> }`, `AuditArgs { limit, cursor, user, action }` and `CancelArgs { id, dry_run: Option<bool> }`. Each field has a `///` description that uses the placeholders.

- [ ] **Step 1: Write the failing tests.**
  - `tool_list_contract`:
    - the 25 names exactly;
    - the D10 annotations;
    - every manage tool has a `dry_run` property;
    - descriptions of 40–600 characters;
    - `cordon_worker` mentions `drain_worker` and `uncordon_worker`;
    - `list_workers` mentions `cordon_worker`;
    - `set_admission_limits` mentions `get_admission`.
  - `copy_quotes_the_configured_limits` is extended: `list_workers` quotes `100`/`1000`, `list_audit_log` quotes `50`/`500`.
  - `manage_tools_match_the_api`: for root, each of `get_worker`, `get_admission`, `list_audit_log`, `list_workers` equals the matching `/api/v2` GET body. `cordon_worker {id:"w1", dry_run:true}` equals `POST /workers/w1/cordon?dry_run=true`.
  - `manage_tools_refuse_non_managers`: alice `cordon_worker`, `drain_worker`, `uncordon_worker` and `set_admission_limits` → `isError`, `code == "forbidden"`; the same calls with `dry_run: true` → the same. Under `none` → `manage_requires_auth`.
  - `cancel_query_widened`:
    - alice `cancel_query` on her own agent statement → the H3 statement status (unchanged shape);
    - root `cancel_query` on alice's statement id → the D5 envelope with `result.state == "cancelled"`, audited;
    - alice on bob's tracker query → `forbidden`;
    - `dry_run: true` on her own statement → the envelope with `dry_run: true`, and the statement still runs.
  - `set_admission_limits_round_trip`: root sets `max_queued_queries: 5` → `get_admission.max_queued_queries == 5`; `max_running_queries: -1` → `invalid_argument`.
  - `instructions_follow_the_workflow_and_fit_the_budget`: still ≤ 1500 characters and mentions `dry_run`.
- [ ] **Step 2: Run, confirm the failures.**
- [ ] **Step 3: Implement** every tool as `identity(&ctx)?` → the matching `*_for` → `respond`.
- [ ] **Step 4: Tier 1 commands.**
- [ ] **Step 5: Commit** `feat(mcp): manage tools with dry runs, widened cancel_query`.

### Task 8: OpenSpec change, upgrade notes and the Tier 2 manage smoke

**Files:**
- Create: `openspec/changes/manage-api/`:
  - `.openspec.yaml` (`schema: spec-driven`, `created: 2026-10-31`)
  - `proposal.md` (why/what, linking the spec)
  - `design.md` (D1–D12 and the Notes below)
  - `tasks.md` (Tasks 1–8 as checked items, in the H3 style)
  - `specs/manage-api/spec.md`: `## ADDED Requirements`, each with SHALL text and at least one `#### Scenario` (WHEN/THEN), for: privilege layers; `admin_users`; manage under `none`; write privileges per protocol; dry run; response envelope; audit log durability, retention and failure; cancel any query; cordon / drain / uncordon and persistence; admission runtime limits and precedence; MCP manage tools; bounded listings.
  - `specs/access-control/spec.md`: `## MODIFIED Requirements` for the rules-file sections, now including `system_information`, and for write privileges. Copy the current requirement text from `openspec/specs/access-control/spec.md` and edit it.
  - `specs/query-admission/spec.md` and `specs/worker-graceful-drain/spec.md`: `## MODIFIED` or `## ADDED` requirements, as above.
- Create: `scripts/manage-smoke.sh`. Requirements:
  - bash 3.2 compatible, with `curl`, `jq` and `psql`;
  - prints `PASS <step>` / `FAIL <step>: reason` and exits 1 on the first failure;
  - never echoes a credential.

  Inputs:
  - `ARNEB_URL` (Web port base) and `ARNEB_PG` (`host:port`)
  - `ARNEB_ADMIN_USER` / `ARNEB_ADMIN_PASSWORD` (a user in `admin_users`)
  - `ARNEB_USER2` / `ARNEB_PASSWORD2` (no manage; only `SELECT` in the rules)
  - `ARNEB_CATALOG` (default `datalake`) and `ARNEB_SCHEMA` (default `tpch`)
  - `ARNEB_LONG_SQL` (default: the 3-way lineitem self-join from `scripts/agent-api-smoke.sh`)
  - `--after-restart`, which runs only step 10
- Modify: `docs/guide/configuration.md` (the existing `**Upgrading.**` paragraph style). Add an `### Upgrading to v0.2.0 (manage API)` list:
  - write privileges;
  - `/api/v2` `DELETE /queries/{id}` answers `200` with a body;
  - `/api/v1` cancel under `none` is refused for other users' queries;
  - observe gating when a `system_information` section exists;
  - an admission controller always exists;
  - new state in the state file and `audit.jsonl`.

  Link it from `docs/guide/access-control.md`'s upgrade section.

Steps the smoke script checks:
1. `whoami`: admin `privileges.manage == true`; user2 `manage == false`.
2. A user2 `POST /workers/worker-1/cordon` and the same call with `?dry_run=true` → both `403 forbidden`.
3. An admin `?dry_run=true` cordon of worker-1 → `dry_run: true`; `GET /workers/worker-1` still `schedulable`.
4. Admin cordon worker-1 → `cordoned`.
   - Submit `ARNEB_LONG_SQL` as user2 and poll until it is `running`.
   - `GET /workers/worker-1` `running_queries == 0` and `GET /workers/worker-2` `running_queries >= 1` (new queries do not use the cordoned worker).
5. Admin drain worker-2 → `draining`. Admin `DELETE /queries/{id}` on user2's query → `changed: true`. Poll `GET /workers/worker-2` until `drained` (at most 30 s). Uncordon both → `schedulable`. TPC-H Q6 then finishes correctly as user2.
6. Admission:
   - admin `PUT {"max_running_queries": 1}`;
   - start two long queries as user2: the second reports `queued`;
   - cancel both;
   - `PUT` the original value back;
   - `GET /admission` `source == "state"`.
7. Write privilege:
   - `psql` as user2: `CREATE TABLE <catalog>.<schema>.smoke_t AS SELECT 1 AS x` → SQLSTATE `42501`;
   - the same through `POST /api/v2/statements` → `state: failed`, `error.code == "forbidden"`.
8. `GET /audit?limit=50` contains `worker.cordon`, `worker.drain`, `worker.uncordon`, `query.cancel` and `admission.set` by the admin, and no entry by user2.
9. MCP: `tools/list` has 25 tools; `get_worker` as admin works; `cordon_worker` as user2 is `isError forbidden`.
10. (`--after-restart`) The cordon made just before the restart (step 4 leaves worker-1 cordoned when run with `KEEP_CORDON=1`) is still `cordoned`; the step-8 audit entries are still listed; `source` is still `state`. Then uncordon to clean up.

- [ ] **Step 1: Write the openspec change; `openspec validate manage-api --strict` passes** (paste the output).
- [ ] **Step 2: Write the script; `bash -n` and `shellcheck` (if installed) are clean.**
- [ ] **Step 3: Tier 2.** Run it against the local distributed stack:
  - coordinator plus 2 workers, password auth, the hive `datalake`;
  - a rules file with a `tables` section granting user2 `SELECT` only;
  - `admin_users = [<admin>]`;
  - a persistent `[state] path`;
  - put `/opt/homebrew/opt/libpq/bin` on `PATH` for `psql`.

  Run order: once in full with `KEEP_CORDON=1`, then restart only the coordinator, then once with `--after-restart`.

  Do not stop, restart or reconfigure any Docker container. Ports 5432 and 8080 are taken: use `--port` / `--trino-port` as in the H2/H3 Tier 2 runs.

  Paste the raw output of both runs.
- [ ] **Step 4: Tier 1 commands; the docs build** if `docs/package.json` has a build script and `node_modules` is present (skip otherwise and say so).
- [ ] **Step 5: Commit** `docs(manage): openspec change, upgrade notes and the Tier 2 manage smoke`.

---

## Notes (where the code shaped or limited the decisions)

- **DDL/DML do not execute in any protocol today.** `ExecutionContext::create_physical_plan` refuses every DDL/DML plan (`"DDL/DML plans are handled at the protocol level"`), and nothing calls `DDLProvider`. H4 puts the write check right after planning in `execute_query_capped`. So:
  - a denied user gets `42501` / `PERMISSION_DENIED` / `forbidden`;
  - a permitted user still gets today's not-supported error;
  - when writes start executing, the check already sits in front of them.

  The acceptance negative check ("refused DDL over pgwire and the API") is met as written.
- **No `UPDATE` statement exists** in the parser or planner. The `UPDATE` privilege is accepted in rules files and maps to nothing (D3).
- **`QueryTracker::cancel_query` only changes the recorded state.** Before this change, `DELETE /api/v1|v2/queries/{id}` did not stop a pgwire or Trino query. H4 adds the per-query canceller (D7) so the kill is real. The pgwire handler runs the statement inline rather than as a task, hence `futures::future::Abortable` there.
- **Kill under the existing convention.** With no rules file, or no `queries` section, any authenticated user may still cancel any query (D2, case 3). This keeps the "missing section allows everything" convention the spec keeps for the query layer, rather than making cancel manage-only. Under `none`, cancelling someone else's query is now refused everywhere, including `/api/v1`, which the Web UI uses. That is a visible change for the default config and is in the upgrade notes. If the controller prefers keeping `/api/v1` open under `none` until the Web UI moves to `/api/v2`, only Task 4's `v1_cancel_uses_the_same_rule` and the upgrade note change.
- **Worker routes.** I followed the given `/api/v2/workers/{id}/…` paths. `GET /api/v2/workers[/{id}]` is added beside the existing `/api/v2/cluster/workers` (an unbounded array kept for compatibility) so that the bounded listing and the state of absent cordoned ids have a home.
- **Drain completion is coordinator-side:** the in-flight count of the coordinator's distributed queries per worker. A worker may still hold a task for a few hundred ms after the coordinator's query future ends (its release is asynchronous). `drained` means "no coordinator query uses it", which is what a planned shutdown needs. The SIGTERM path (#178) is unchanged.
- **The state store exists only under `[auth] type = "password"`** (main.rs opens it only then). Since manage is refused under `none`, nothing needs persisting there. Under password with no `[state] path`, cordons, limits and the audit log live in memory. That gets the startup WARN and a per-response `warning`.
- **`statement_timeout` stays startup-only.** It is handed by value to three protocol builders. Making it runtime would touch every protocol, and the spec's "admission limits" are the two counters.
- **Downgrade.** `StateData` gains `cordons` and `settings` with `#[serde(default)]`. The version stays 1, because an H1–H3 binary ignores unknown fields when reading. Its next write would drop them, though, so a downgrade loses cordons and runtime limits but not users or tokens. This is noted in `design.md`.

## Self-review

1. **Spec coverage.**

   | Spec item | Where |
   |---|---|
   | Privilege layers | Task 1 |
   | Write privileges on every protocol | Task 2 |
   | Cancel any query | Task 4 |
   | Cordon / drain / uncordon | Task 5 |
   | Admission limits | Task 6 |
   | Dry runs | Tasks 4–7 |
   | Durable audit log | Task 3 |
   | Restart survival (Acceptance 9, H4 subset) | Tasks 3, 5 and 6 restart tests; Task 8 step 10 |
   | MCP parity | Task 7 |
   | Acceptance 4–6 and 10 | Task 8 steps 4–8 |
   | Negative checks | Task 8 steps 2, 7 and 9; Tasks 5–7 tests |
   | Bounded results | Tasks 3, 5, 7 |
   | Untraceable actions | D6 + Task 3 |
   | Wider unauthenticated access | D1, D2, `manage_requires_auth` |
   | Silent runtime changes | D9 WARN + D8 restore + Task 6 restart test |

   Catalog, user and rule edits and re-seeding are H5; OAuth is H6.
2. **Step scan.** Every test names its assertions and values. Every code step names its file, signature and the decision it implements. The bodies are left to the implementer, except the D6 order and failure algorithm, which the tests alone do not determine.
3. **Type consistency.** These names are spelled the same in every task that uses them:
   - `Grant` / `KillGrant` (Task 1) feed `run_manage` (Task 4);
   - `audited` / `AuditSubject` (Task 3) are used by `run_manage` (Task 4);
   - `ManageOutcome<T>` is returned by `cancel_query_for` (Task 4), `worker_action_for` (Task 5) and `set_admission_for` (Task 6), and consumed by Task 7;
   - `CordonMode` (scheduler) and `CordonKind` (state) are mapped only in `web/workers.rs` and `main.rs`;
   - `AdmissionLimits` (state) is used by `resolve_admission_limits` (config) and `set_admission_for`.
4. **Review Focus.** Each of the five lines has a named test in its owning task. One more pinned case outside the five, the stale cordon, has `stale_cordon_is_listed_and_can_be_cleared` in Task 5.
5. **Proportion.** The plan is mostly decisions, interfaces and test assertions; it has no function bodies.
