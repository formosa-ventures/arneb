# Runtime Config (H5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A remote agent with the manage privilege can, without a restart, edit the access rules, create / change / remove users and their API tokens, add / change / remove catalogs (pushed to every worker), change the mutable settings, and re-seed the state from the config file. Each change has a dry run, is audited and survives a restart.

**Architecture:**
- **The state store becomes the source of truth** for catalogs, access rules and mutable settings, the way H1 made it the source for users. On the first start that finds no seeded copy, the coordinator copies the config file's `[[catalogs]]`, rules file and settings into the state. On every later start a diverging config entry logs a WARN on `arneb::config`. Re-seeding is an explicit manage action.
- **Live handles replace start-time snapshots.**
  - `arneb_catalog::RulesHandle` is a swappable `Option<Arc<AccessRules>>` shared by pgwire, Trino, `/api/v2`, MCP and the `Authorizer`.
  - `ConnectorRegistry` gains interior mutability.
  - A new `LiveCatalogs` (server crate) owns building, installing and removing hive / postgresql catalogs on one node.
- **Workers receive catalogs from the coordinator** over Flight RPC:
  - a push (`sync_catalogs`) on every change;
  - a catalog generation in every heartbeat reply, so a worker that missed a push, or just registered, pulls (`get_catalogs`);
  - a pull-once retry when a task names a catalog the worker does not have.
- **Every change goes through H4's `run_manage`** (dry run, audit, D5 envelope), serialized by one `WebState::config_changes` lock.

**Tech Stack:** Rust, axum 0.8, rmcp (already a dependency), arrow-flight (already), serde/serde_json, tokio, thiserror, tracing. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`. This plan implements stage H5: Goal 4 (Manage: edit the access rules; add, change and remove catalogs; create, change and remove users and their API tokens), Design decisions → Runtime state (seed, divergence, re-seed, workers receive catalogs, catalog changes affect new queries only), the Stages row H5, and Acceptance steps 7–9 plus the negative checks for steps 7–8. It builds on:
- H1: `docs/superpowers/plans/2026-10-30-headless-api-auth.md`
- H2: `docs/superpowers/plans/2026-10-31-agent-query-api.md`
- H3: `docs/superpowers/plans/2026-10-31-remote-mcp.md`
- H4: `docs/superpowers/plans/2026-10-31-manage-api.md` (branch `feat/manage-api`, this branch's base)

H6 (OAuth) is out of scope.

## Global Constraints

- **Errors and logs.** Library code uses `thiserror`; `web/` uses `ApiError` and never `anyhow`. Use `tracing` for every log line. Config lines go on `arneb::config`; the audit log's own target is `arneb::audit`.
- **Doc comments** on every public item. Internal functions don't need them.
- **Runtime knobs.** Each knob has a TOML field and an `ARNEB_*` env override (env > file > default), and its effective value is logged once at startup on `arneb::config`.
- **Secrets.** Never log, print, audit or return:
  - a full API token, a session token or a password, in code, test output or the smoke script;
  - a catalog's `password`, the password inside a `connection_url`, or any `secret_access_key` / service-account key.

  Audit `parameters` carry a catalog spec only in its redacted form (Task 4's `redacted_spec`) and never carry a user's password. Audit entries name a token by its 16-hex id only.
- **Test seeding.** Tests seed every user of a state store in ONE `seed_users` call (seeding is a full sync).
- **Equal rights** (Anti-goal). Every MCP tool calls the same `*_for` function as its `/api/v2` route.
- **Bounded results** (Anti-goal). Every new listing takes `limit` with a ceiling and reports `truncated`. A value above the ceiling is refused with `invalid_argument`, never clamped (same rule as H2–H4).
- **No silent runtime changes** (Anti-goal). A runtime change is persisted before the response. A config value that no longer takes effect logs a WARN naming the object at every start.
- **Clippy.** CI runs Rust 1.99. Run `cargo +1.99.0 clippy --workspace --all-targets -- -D warnings` before every commit. Watch `result_large_err` (match the existing H1–H4 boxing) and the deprecated `fetch_update`.
- **Git.** Never `git stash`. Don't touch other worktrees. Commit at the end of every task.
- **Housekeeping.** Run `./scripts/target-housekeeping.sh` at the start of the session; run `--sweep` at 🟡.
- **Validation tiers.**
  - **Tier 1, every task:** `cargo fmt -- --check`, the clippy command above, and `cargo test --workspace`. Paste the raw `test result:` lines.
  - **Tier 2, Task 8:** a local coordinator plus 2 workers under password auth, driven by `scripts/runtime-config-smoke.sh`.
  - No Tier 3: H5 has no performance claim.

## Decisions

My calls, on top of the spec. Task 8 copies them into `openspec/changes/runtime-config/design.md`.

- **D1. What is seeded, and what wins.**

  | Object | Seeded on | Later starts |
  |---|---|---|
  | Users | every start (H1 D2, unchanged) | config wins for config-seeded users |
  | Catalogs (`[[catalogs]]`) | the first start whose state has `seeded.catalogs == false` | state wins; each config catalog that is new, changed or gone relative to its seeded copy logs one WARN |
  | Access rules (`rules_file`) | the first start whose state has `seeded.rules == false` | state wins; a rules file whose text differs from the seeded copy logs one WARN |
  | Settings (admission, slow-query threshold) | never copied; a stored value wins (H4 D9) | WARN per diverging knob (H4 D9, extended to `slow_query_threshold_ms`) |

  - An H4 state file has no `seeded` field. It loads as `seeded.catalogs = seeded.rules = false`, so the first H5 start seeds both.
  - Every divergence WARN ends with the hint `preview a re-seed with POST /api/v2/state/reseed?dry_run=true`.
  - **Not runtime-editable (startup-only):** `[[tables]]` (the `file` catalog), the `memory` catalog, the default catalog and schema, `admin_users`, `[audit]` retention, ports, bind addresses and TLS. `[audit]` retention stays startup-only on purpose, so a manager cannot shorten the trail of their own actions.
- **D2. Re-seed** (`POST /api/v2/state/reseed`, action `state.reseed`, target `state`).
  - It re-reads the config file from the path the coordinator started with, using `AppConfig::load` with env overrides applied.
  - It replaces:
    - the catalogs, so runtime-created catalogs are removed;
    - the rules, so a missing rules file means no rules;
    - the stored settings, which are cleared, so the config values apply;
    - the users, through `seed_users(.., reseed = true)`, and the tombstones (D4) are cleared.
  - Tokens, cordons and the audit log are left alone, except the tokens that user seeding revokes.
  - The preview lists the catalogs added / changed / removed, whether the rules change, the settings that reset and the user report.
  - A config file that fails to load is `invalid_argument` with the loader's message.
  - With no config path (started without `--config` and no `arneb.toml`), the config is the defaults.
  - The `--reseed-state` CLI flag does the same at startup for all four objects, not only users.
- **D3. Access rules API.**
  - `GET /api/v2/access-rules` (manage) returns `{"rules": <json|null>, "from_config": bool}`.
  - `PUT` (action `access_rules.set`) takes the full rules JSON as the body. It is validated with `AccessRules::from_json`; a parse error is `invalid_argument` with the parser's message.
  - `DELETE` (action `access_rules.clear`) removes all rules, which means "allow everything" in the query layer. Its preview `warning` says so.
  - **Lockout guard.** A change after which the caller would no longer hold manage is refused with `conflict`, hint `keep a system_information write rule for yourself, or ask a user in [access_control] admin_users`. A user in `admin_users` always keeps manage, so the guard never fires for them. The guard is part of validation, so a dry run is refused the same way.
  - A rules change applies to every statement planned after the response, including on connections already open (each statement reads `RulesHandle::current()`). A statement already running keeps the rules it was checked with.
- **D4. Users.**
  - `POST /api/v2/users` (action `user.create`) takes `{name, password}`. Name rules: 1–64 chars of `[A-Za-z0-9_.@-]`. Password: 8–1024 chars. An existing name is `conflict`. The user gets a fresh SCRAM verifier (random 16-byte salt, iteration count 4096, the H1 default) and `from_config = false`.
  - `PATCH /api/v2/users/{name}` (action `user.set_password`) takes `{password}`:
    - It stores a new verifier and revokes all of the user's tokens (the preview says how many).
    - On a config-seeded user it also **detaches** the user (`from_config = false`). The preview `warning` says the config file no longer manages this user. From then on, H1's existing "conflict" WARN fires at every start while the name is still in the config.
  - `DELETE /api/v2/users/{name}` (action `user.delete`) deletes the user and their tokens. Deleting a config-seeded user records a tombstone (`StateData.deleted_config_users`), so the next start does not re-add it. Instead it logs a WARN naming the user, with the re-seed hint. A re-seed clears the tombstones.
  - After a password change or a delete, the server:
    - drops the user's Basic-auth cache entries (`BasicAuthenticator::forget_user`);
    - ends the user's Web sessions (`SessionStore::end_user`).

    pgwire connections that are already open stay open; this is documented, matching Trino and PostgreSQL.
  - Deleting yourself is allowed, with a preview warning.
  - `GET /api/v2/users` (manage, `limit` default 100, ceiling 1000, name order) lists `{name, from_config, tokens}`, where `tokens` is the live token count.
- **D5. Tokens for another user** (manage).
  - `GET /api/v2/users/{name}/tokens`, `POST /api/v2/users/{name}/tokens` and `DELETE /api/v2/users/{name}/tokens/{id}` reuse H1/H4's `issue_token_for` / `revoke_token_for` and their audit actions (`token.issue`, `token.revoke`).
  - The audit `target` is the user. The issued secret is shown once, to the manager.
  - The self-service routes `/api/v2/tokens*` are unchanged.
- **D6. Catalog definitions.**
  - A catalog spec is `config::CatalogConfig`, now also `Serialize` (and so are `StorageConfig` and `RedactedString`, which serialize the raw value for the state file and the RPC).
  - The state stores it as JSON in `CatalogRecord.spec`, so `arneb-state` does not depend on the server crate.
  - Names: 1–63 chars of `[a-z0-9_]`, starting with a letter. `memory`, `file`, `system`, `information_schema` and `pg_catalog` are reserved (`invalid_argument`).
  - Types: `hive` (needs `metastore_uri`) and `postgresql` (needs `connection_url`); anything else is `invalid_argument`.
  - `ARNEB_CATALOG_<NAME>_PASSWORD` is resolved on the coordinator when the config loads, so a seeded spec stores the resolved password. Workers never read catalog env vars. This goes in the upgrade notes, together with "set `[cluster.tls]` when catalogs carry credentials" (they now cross the Flight connection).
  - **Redaction.** `redacted_spec` replaces `password`, `secret_access_key`, `service_account_path` and the userinfo password of `connection_url` with `"<redacted>"`. Every API response, MCP result and audit entry uses it.
  - **Updates.** `PUT` with a redacted value keeps the stored secret: a field equal to `"<redacted>"` is filled from the stored spec, so an agent can round-trip a `GET` body.
- **D7. Catalog API.**
  - `GET /api/v2/catalog-configs` (manage, `limit` default 100, ceiling 1000) lists the redacted specs plus `from_config`, `generation` and `default` (whether it is the default catalog).
  - `GET /api/v2/catalog-configs/{name}` returns one.
  - `POST /api/v2/catalog-configs` (action `catalog.create`) and `PUT /api/v2/catalog-configs/{name}` (action `catalog.update`):
    - **probe first:** build the provider on the coordinator (connect to HMS / PostgreSQL) with a 10 s timeout, before `run_manage`, for a dry run too. A probe failure is `invalid_argument` with hint `the coordinator could not connect: <error>`, and nothing changes.
    - **then apply:** persist the spec (generation + 1), install the probed provider on the coordinator, and push the new set to every registered worker. Each push has a 5 s timeout.
    - **result:** `{generation, workers: [{worker_id, synced: bool, error?}]}`. A failed push is not an error: the heartbeat pull (D8) repairs it, and the envelope `warning` names the workers.
  - `DELETE /api/v2/catalog-configs/{name}` (action `catalog.drop`) removes it everywhere. Dropping the default catalog is allowed, with a preview warning that unqualified table names then fail. Dropping an unknown name is `not_found`.
  - `POST` on an existing name is `conflict`. The `/api/v2/catalogs` discovery routes (H2) are unchanged and show a new catalog as soon as the response returns.
  - **Snapshot rule** (spec): a query keeps the provider `Arc` it was planned with. A task submitted after a change, for a query that was already running, uses the worker's current catalog set. This is documented as a known limit.
- **D8. Catalog sync over Flight** (`arneb-rpc`).
  - The heartbeat reply body becomes JSON `HeartbeatReply { catalog_generation: Option<u64> }`. A body that is not JSON (`"ok"`) decodes as `None`.
  - A worker whose applied generation differs from the reply's calls `get_catalogs` on the coordinator and applies the result. This covers registration (a fresh worker's applied generation is `0`) and missed pushes.
  - `sync_catalogs` (coordinator → worker) and `get_catalogs` (worker → coordinator) carry `CatalogSet { generation: u64, catalogs: Vec<serde_json::Value> }`. Both run under the existing cluster security (secret / TLS), like every Flight action.
  - A worker applies a set only if its generation is newer than the one it holds, so a stale push never wins over a newer pull.
  - Applying a set builds every new or changed catalog, then swaps them in, removing catalogs not in the set. A catalog that fails to build is skipped with a WARN; the rest still apply, and the applied generation still advances. The coordinator's probe makes such a failure rare.
  - **Workers ignore `[[catalogs]]` in their own config** from now on. If it has any, startup logs one INFO on `arneb::config` saying they are ignored and catalogs come from the coordinator.
  - **Pull-once on a miss.** When a task's `TableScan` names a catalog with no registered connector, the worker pulls once and retries the lookup before failing. The error then names the catalog: `catalog '<name>' is not registered on worker <id>`.
- **D9. Settings.**
  - `GET /api/v2/settings` (observe) returns `{slow_query_threshold_ms, source}`.
  - `PATCH /api/v2/settings` (manage, action `settings.update`) takes `{slow_query_threshold_ms}`, where `0` turns the log off. It is applied through `arneb_common::metrics::set_slow_query_threshold` and stored in `Settings.slow_query_threshold_ms`.
  - Admission stays at `/api/v2/admission` (H4). The settings `GET` does not repeat it.
- **D10. Locking.** One `WebState::config_changes: Arc<tokio::sync::Mutex<()>>` serializes every H5 action (rules, users, user tokens, catalogs, settings, re-seed). It is held through `ManageAction::change_lock` until the audit entry is appended (the H4 pattern). Catalog probes run *before* the lock is taken, so a slow HMS does not block other changes. Under the lock, the action re-checks that the spec it probed is still the one being applied (create: name still absent; update: stored spec unchanged since the probe), and otherwise answers `conflict`, hint `retry`.
- **D11. Error code.** New `ErrorCode::Conflict` (`"conflict"`, HTTP 409).
- **D12. MCP.** 18 new tools (43 in total), each calling its route's `*_for`:
  - catalogs: `list_catalog_configs`, `get_catalog_config`, `create_catalog`, `update_catalog`, `drop_catalog`;
  - access rules: `get_access_rules`, `set_access_rules`, `clear_access_rules`;
  - users: `list_users`, `create_user`, `set_user_password`, `delete_user`;
  - user tokens: `list_user_tokens`, `issue_user_token`, `revoke_user_token`;
  - settings and re-seed: `get_settings`, `update_settings`, `reseed_state`.

  Manage tools take `dry_run`. The descriptions of the tools that take a password say the password is not logged or audited, and that the agent should not repeat it back to the user.

## Review Focus

1. **A rules change on an open connection.** A pgwire session that ran a query before the rules changed must be denied on its next statement. Test: `rules_change_applies_to_an_open_pgwire_session` (Task 1).
2. **Old Basic credentials after a password change.** A Trino client whose Basic credential is cached must be refused right after `PATCH /users/{name}`, not 5 minutes later. Test: `password_change_drops_the_basic_cache` (Task 2).
3. **A secret round-trip.** `GET` then `PUT` of a catalog's redacted body must keep the stored password, and no response, audit line or log line may contain it. Tests: `put_with_redacted_password_keeps_the_stored_one` and `catalog_secrets_never_leave_redacted` (Task 5).
4. **A worker that misses the push.** A worker that was down during a `catalog.create` must serve the catalog within one heartbeat of coming back, and a task that reaches it first must not fail. Tests: `heartbeat_generation_mismatch_pulls` and `task_with_unknown_catalog_pulls_once` (Task 4).
5. **Lockout.** A manager whose only grant is a `system_information` rule must not be able to remove it from themselves, by `PUT`, `DELETE` or re-seed. Tests: `rules_change_that_drops_own_manage_is_refused` (Task 1) and `reseed_that_drops_own_manage_is_refused` (Task 6).

---

### Task 1: Live access rules (state, handle, API)

**Files:**
- Modify: `crates/catalog/src/access.rs` (add `RulesHandle`)
- Create: `crates/state/src/rules.rs`; modify `crates/state/src/file.rs` (`StateData.rules`, `StateData.seeded`), `crates/state/src/lib.rs`
- Modify: every holder of `Option<Arc<AccessRules>>`, so they hold a `RulesHandle` and call `current()` per statement / request instead:
  - `crates/protocol/src/server.rs`, `handler.rs`, `trino/mod.rs`, `trino/engine.rs`, `agent/service.rs`
  - `crates/server/src/web/mod.rs` (rename `WebState.access_rules` → `rules: RulesHandle`; add `config_changes`), `web/privilege.rs`
- Modify: `crates/server/src/web/error.rs` (`ErrorCode::Conflict`, D11)
- Create: `crates/server/src/web/rules.rs` (routes + `*_for`); register it in `web/v2.rs`
- Modify: `crates/server/src/main.rs` (seed rules at startup, D1)
- Test: unit tests in the files above; `crates/protocol/tests/access_control.rs` for the pgwire case

**Interfaces:**
- Produces:
  - `arneb_catalog::RulesHandle`: `Clone + Default`, `new(Option<Arc<AccessRules>>) -> Self`, `current(&self) -> Option<Arc<AccessRules>>`, `replace(&self, Option<Arc<AccessRules>>)`.
  - `arneb_state::StoredRules { pub json: String, pub from_config: bool }`.
  - `StateStore::access_rules(&self) -> Option<StoredRules>`, `set_access_rules(&self, Option<StoredRules>) -> Result<(), StateError>`.
  - `StateStore::seed_rules(&self, config_json: Option<&str>, reseed: bool) -> Result<RulesSeed, StateError>`, where `RulesSeed` is `Seeded | Unchanged | Diverged`.
  - `StateStore::seeded(&self) -> Seeded`, with `pub struct Seeded { pub catalogs: bool, pub rules: bool }` (`#[serde(default)]`).
  - `Authorizer::new(rules: RulesHandle, admin_users, password_auth)`.
  - `Authorizer::would_manage(&self, who: &Identity, rules: Option<&AccessRules>) -> bool`, the lockout check against a candidate rule set.
  - `pub(super) async fn get_rules_for`, `set_rules_for(state, who, dry_run, json: String)` and `clear_rules_for(state, who, dry_run)`, used by Task 7.

- [ ] **Step 1: Write the failing tests**
  - `rules_handle_replace_is_seen_by_clones` (catalog): `h.clone().current()` reflects `h.replace(..)`.
  - `seed_rules_first_start_then_diverged` (state): the first call returns `Seeded` and stores `from_config = true`. The same text gives `Unchanged`; other text gives `Diverged` and keeps the stored copy; `reseed = true` replaces it.
  - `h4_state_file_loads_unseeded` (state): `{"version":1,"users":[],"tokens":[]}` gives `seeded() == Seeded { catalogs: false, rules: false }`.
  - `rules_change_applies_to_an_open_pgwire_session` (protocol test): open a session as bob with rules allowing `tpch`, run a query (ok), `replace` the rules with ones denying bob, then run the same query on the same session, which fails with SQLSTATE `42501`.
  - Web tests:
    - `put_rules_requires_manage`: bob gets `forbidden`.
    - `put_rules_bad_json_is_invalid_argument`.
    - `put_rules_dry_run_changes_nothing`.
    - `put_rules_applies_and_audits`: an audit entry `access_rules.set` with `effect = applied`, where `parameters` holds a SHA-256 of the new JSON and not the JSON itself.
    - `delete_rules_warns_allow_all`.
    - `rules_change_that_drops_own_manage_is_refused`: carol is granted manage only by a `system_information` rule; a `PUT` without it is `conflict`, for a dry run too.
    - `admin_user_is_never_locked_out`.
- [ ] **Step 2: Run them; they fail** (`cargo test -p arneb-catalog -p arneb-state -p arneb-protocol -p arneb-server rules`).
- [ ] **Step 3: Implement**
  - `RulesHandle` uses `Arc<RwLock<Option<Arc<AccessRules>>>>`: the read lock is held only to clone.
  - Swap every `Option<Arc<AccessRules>>` field for the handle. A statement reads `current()` once and uses that snapshot throughout planning and the privilege check.
  - Startup on the coordinator under password auth:
    - read the rules file text, if set;
    - call `seed_rules`;
    - build the handle from the state copy, so the state wins;
    - `Diverged` logs the D1 WARN.
  - A rules file that fails to parse on a later start is a WARN, not fatal, because the state copy is used. On the first start it stays fatal, as today.
  - Under `none` (no state store), the handle holds the file's rules as today, and `PUT`/`DELETE` are refused by `manage_requires_auth`.
  - The `ManageAction` impls for set and clear: `persisted() = true`, `change_lock()` from `config_changes`. `apply` persists, then calls `replace`.
- [ ] **Step 4: Run the tests; they pass.** Then run Tier 1.
- [ ] **Step 5: Commit** `feat(runtime-config): live access rules with seed, divergence WARN and /api/v2/access-rules`

### Task 2: Users and their tokens

**Files:**
- Modify: `crates/state/src/users.rs` (`create_user`, `set_user_password`, `delete_user`, `list_users`, tombstones in `apply_seed`); `crates/state/src/file.rs` (`StateData.deleted_config_users`, `StateError::UserExists`)
- Modify: `crates/protocol/src/trino/auth.rs` (`BasicAuthenticator::forget_user`)
- Modify: `crates/server/src/web/session.rs` (`SessionStore::end_user`)
- Create: `crates/server/src/web/users.rs`; register it in `web/v2.rs`
- Modify: `crates/server/src/web/v2.rs` (let `issue_token_for` / `revoke_token_for` / the list take a target user; the audit `target` is that user)

**Interfaces:**
- Consumes: `ErrorCode::Conflict` and `WebState::config_changes` (Task 1).
- Produces:
  - `StateStore::create_user(&self, name: &str, verifier: ScramVerifier) -> Result<(), StateError>`.
  - `StateStore::set_user_password(&self, name: &str, verifier: ScramVerifier) -> Result<PasswordChange, StateError>`, with `pub struct PasswordChange { pub revoked_tokens: usize, pub detached: bool }`.
  - `StateStore::delete_user(&self, name: &str) -> Result<UserDeletion, StateError>`, with `pub struct UserDeletion { pub revoked_tokens: usize, pub tombstoned: bool }`.
  - `StateStore::list_users(&self) -> Vec<UserInfo>`, with `pub struct UserInfo { pub name: String, pub from_config: bool, pub tokens: usize }`.
  - `SeedReport.tombstoned: Vec<String>` (config users skipped because they were deleted at runtime).
  - `pub(super) async fn list_users_for`, `create_user_for(state, who, dry_run, name, password)`, `set_password_for(..)`, `delete_user_for(..)`, plus the per-user token `*_for` functions, all used by Task 7.

- [ ] **Step 1: Write the failing tests**
  - State:
    - `create_user_then_conflict`.
    - `set_password_revokes_tokens_and_detaches_config_user`.
    - `delete_config_user_tombstones_and_seed_skips_it`: a later `seed_users(.., false)` does not re-add the user and lists it in `tombstoned`; `seed_users(.., true)` re-adds it and clears the tombstone.
    - `created_user_authenticates_with_password_and_token`: through `CredentialSource`.
  - Protocol: `forget_user_drops_only_that_users_entries`.
  - Web:
    - `user_routes_require_manage`: bob gets `forbidden` on all seven routes.
    - `create_user_validates_name_and_password`: an empty name, a 65-char name, a 7-char password and a name with `/` are all `invalid_argument`.
    - `create_user_dry_run_changes_nothing`.
    - `password_never_in_audit_or_response`: grep the audit JSON and the body for the password.
    - `password_change_drops_the_basic_cache`: a Trino Basic request with the old password is `401` right after the `PATCH`.
    - `delete_user_ends_sessions`: a session token for the user gets `401` afterwards.
    - `manager_issues_and_revokes_token_for_other_user`: audit `token.issue` with target `dave`; the token works for dave on `/api/v2/whoami`; after the revoke, `401`.
    - `list_users_bounded`: `limit=1001` is `invalid_argument`.
- [ ] **Step 2: Run them; they fail.**
- [ ] **Step 3: Implement.**
  - Hash passwords in `spawn_blocking` under H1's `ISSUE_PERMITS` semaphore (the PBKDF2 bound).
  - `apply` calls `forget_user` and `end_user` after persisting.
  - `PATCH` and `DELETE` of an unknown user are `not_found`.
- [ ] **Step 4: Run the tests; they pass.** Then run Tier 1.
- [ ] **Step 5: Commit** `feat(runtime-config): manage users and their API tokens at runtime`

### Task 3: Catalog definitions in the state, and `LiveCatalogs`

**Files:**
- Create: `crates/state/src/catalogs.rs`; modify `crates/state/src/file.rs` (`StateData.catalogs`, `StateData.catalog_generation`)
- Modify: `crates/connectors/src/traits.rs`: `ConnectorRegistry` holds `RwLock<HashMap<..>>`, with `register(&self, ..)` and `deregister(&self, name) -> Option<Arc<dyn ConnectorFactory>>`. Update the call sites from `&mut` to `&`.
- Modify: `crates/server/src/config.rs` (`Serialize` on `CatalogConfig`, `StorageConfig` and its sub-structs, and `RedactedString`)
- Create: `crates/server/src/catalogs.rs` (`LiveCatalogs`, build / validate / redact)
- Modify: `crates/server/src/main.rs`: replace the inline hive / postgresql registration (section 6.5 and `register_postgresql_catalog`) with `LiveCatalogs`; seed catalogs on the coordinator (D1)

**Interfaces:**
- Produces:
  - `arneb_state::CatalogRecord { pub name: String, pub spec: serde_json::Value, pub from_config: bool }`.
  - `StateStore::catalogs(&self) -> (u64 /* generation */, Vec<CatalogRecord>)`.
  - `StateStore::put_catalog(&self, CatalogRecord) -> Result<u64, StateError>` and `remove_catalog(&self, name: &str) -> Result<u64, StateError>` (`StateError::CatalogNotFound(String)`).
  - `StateStore::replace_catalogs(&self, Vec<CatalogRecord>) -> Result<u64, StateError>`.
  - `StateStore::seed_catalogs(&self, config: &[(String, serde_json::Value)], reseed: bool) -> Result<CatalogSeedReport, StateError>`, with `pub struct CatalogSeedReport { pub seeded: bool, pub added: Vec<String>, pub changed: Vec<String>, pub removed: Vec<String> }`. When the state is already seeded and `reseed` is false, those lists name the divergences and nothing is written.
  - `crate::catalogs::LiveCatalogs` (server crate):
    - `new(manager: Arc<CatalogManager>, connectors: Arc<ConnectorRegistry>, global_storage: StorageConfig) -> Arc<Self>`;
    - `generation(&self) -> u64`;
    - `specs(&self) -> Vec<CatalogConfig>`;
    - `async fn build(&self, spec: &CatalogConfig) -> Result<BuiltCatalog, CatalogBuildError>`, with a 10 s timeout;
    - `install(&self, spec: CatalogConfig, built: BuiltCatalog)`, `remove(&self, name: &str) -> bool`, `set_generation(&self, g: u64)`;
    - `async fn apply_set(&self, generation: u64, specs: Vec<CatalogConfig>) -> ApplyReport`, with `pub struct ApplyReport { pub applied: bool, pub failed: Vec<(String, String)> }`.
  - `crate::catalogs::validate_spec(spec: &CatalogConfig) -> Result<(), String>` (D6 name and type rules).
  - `crate::catalogs::redacted_spec(spec: &CatalogConfig) -> serde_json::Value` (D6).
  - `crate::catalogs::fill_redacted(new: &mut CatalogConfig, stored: &CatalogConfig)` (D6 round-trip).
  - `CatalogBuildError` (thiserror): `Invalid(String)`, `Unreachable(String)`, `Timeout`.

- [ ] **Step 1: Write the failing tests**
  - State:
    - `catalog_generation_increments_and_survives_reopen`.
    - `seed_catalogs_first_start_then_reports_divergence_without_writing`.
    - `reseed_catalogs_replaces_runtime_ones`.
  - Connectors: `registry_register_and_deregister_through_shared_ref`.
  - Server:
    - `validate_spec_rejects_reserved_and_bad_names`: `memory`, `File`, `1x`, a 64-char name, `type = "mysql"`, a hive spec without `metastore_uri`.
    - `redacted_spec_hides_every_secret`: password, `connection_url` userinfo password, `secret_access_key`, `service_account_path`. Assert the serialized JSON contains none of the raw values.
    - `fill_redacted_restores_stored_secrets`.
    - `apply_set_ignores_an_older_generation`.
    - `apply_set_removes_catalogs_not_in_the_set`: use a postgresql spec pointing at a closed port, so the build fails. The report lists it in `failed`, and a stale catalog of another name is removed.
  - Existing server tests that build catalogs keep passing.
- [ ] **Step 2: Run them; they fail.**
- [ ] **Step 3: Implement**
  - Move the hive and postgresql build code out of `main.rs` into `LiveCatalogs::build`, unchanged in behavior, including the per-catalog storage merge.
  - `LiveCatalogs` keeps a `RwLock<BTreeMap<String, CatalogConfig>>` of installed specs.
  - Coordinator startup under password auth:
    - `seed_catalogs` from `config.catalogs`, serialized;
    - build the catalogs from the state;
    - log the D1 WARN for each divergence.
  - Standalone and `none` build from the config as today, through `LiveCatalogs`.
  - Do **not** change worker startup here; Task 4 does.
- [ ] **Step 4: Run the tests; they pass.** Then run Tier 1. TPC-H integration tests must stay green.
- [ ] **Step 5: Commit** `feat(runtime-config): catalog definitions in the state store and LiveCatalogs`

### Task 4: Catalog sync to workers over Flight

**Files:**
- Create: `crates/rpc/src/catalog_sync.rs`; modify `crates/rpc/src/heartbeat.rs`, `crates/rpc/src/flight_service.rs` and `crates/rpc/src/lib.rs`
- Modify: `crates/server/src/main.rs` (coordinator: the heartbeat callback returns the generation, and serve `get_catalogs`; worker: the sync callback, the pull on mismatch, ignoring `[[catalogs]]`)
- Modify: `crates/server/src/task_manager.rs` (pull-once on a missing connector, D8)

**Interfaces:**
- Consumes: `LiveCatalogs::apply_set` and `generation` (Task 3).
- Produces:
  - `arneb_rpc::CatalogSet { pub generation: u64, pub catalogs: Vec<serde_json::Value> }`, with `encode` / `decode`.
  - `arneb_rpc::HeartbeatReply { pub catalog_generation: Option<u64> }`.
  - `HeartbeatCallback` becomes `Arc<dyn Fn(HeartbeatMessage) -> Result<HeartbeatReply, String> + Send + Sync>`.
  - `send_heartbeat_with(..) -> Result<HeartbeatReply, HeartbeatError>`.
  - `FlightState::set_catalog_sync_callback(Arc<dyn Fn(CatalogSet) -> BoxFuture<'static, Result<(), String>> + Send + Sync>)` (worker).
  - `FlightState::set_catalog_source(Arc<dyn Fn() -> CatalogSet + Send + Sync>)` (coordinator).
  - `arneb_rpc::push_catalogs(addr: &str, set: &CatalogSet, security: &ClusterSecurity) -> Result<(), String>`.
  - `arneb_rpc::fetch_catalogs(coordinator: &str, security: &ClusterSecurity) -> Result<CatalogSet, String>`.
  - A server-side `CatalogPuller`, cloneable and shared by the heartbeat loop and the task manager. `async fn pull(&self) -> Result<(), String>` single-flights concurrent pulls.

- [ ] **Step 1: Write the failing tests**
  - RPC:
    - `heartbeat_reply_ok_body_decodes_as_none`.
    - `catalog_set_round_trips`.
    - `sync_catalogs_without_callback_is_unavailable`.
    - `get_catalogs_returns_the_source`: in-process Flight server, as in the existing `flight_service` tests.
  - Server:
    - `heartbeat_generation_mismatch_pulls`: a worker loop with applied generation 0 and a coordinator replying 3 fetches once and records 3. A second heartbeat replying 3 does not fetch.
    - `task_with_unknown_catalog_pulls_once`: the task manager with an empty registry and a puller stub that installs a memory-backed factory under `cat1`. The scan registers, and the puller was called exactly once. With a puller that installs nothing, the error is `catalog 'cat1' is not registered on worker w1`.
    - `worker_ignores_config_catalogs`: the worker startup helper with `[[catalogs]]` set registers none and logs the INFO (use `tracing-test` if the crate already uses it; otherwise assert the returned list).
- [ ] **Step 2: Run them; they fail.**
- [ ] **Step 3: Implement.**
  - The coordinator's catalog source reads `LiveCatalogs::specs()` and `generation()`.
  - The worker's sync callback calls `apply_set`.
  - Keep `HeartbeatMessage` unchanged: the version handshake already rejects mixed releases.
- [ ] **Step 4: Run the tests; they pass.** Then run Tier 1, including the `arneb-rpc` broadcast tests. Those two were flaky under load before this branch; rerun once on failure and record it.
- [ ] **Step 5: Commit** `feat(runtime-config): workers receive catalogs from the coordinator`

### Task 5: Catalog manage API

**Files:**
- Create: `crates/server/src/web/catalogs.rs`; register it in `web/v2.rs`
- Modify: `crates/server/src/web/mod.rs` (`WebState.catalogs: Option<Arc<LiveCatalogs>>`, `WebState.node_registry` if not present), `crates/server/src/main.rs` (wire it)

**Interfaces:**
- Consumes:
  - `LiveCatalogs`, `validate_spec`, `redacted_spec` and `fill_redacted` (Task 3);
  - `push_catalogs` (Task 4);
  - `StateStore::put_catalog` / `remove_catalog` (Task 3);
  - `config_changes` (Task 1).
- Produces: `pub(super) async fn list_catalog_configs_for`, `get_catalog_config_for`, `create_catalog_for(state, who, dry_run, spec: serde_json::Value)`, `update_catalog_for(state, who, dry_run, name, spec)` and `drop_catalog_for(state, who, dry_run, name)`, used by Task 7.

- [ ] **Step 1: Write the failing tests** (use a `postgresql` spec when a live server is needed; gate it on `ARNEB_TEST_POSTGRES_URL` like the existing postgresql tests; the other tests use a closed port so the probe fails deterministically)
  - `catalog_routes_require_manage`: all five, for bob.
  - `create_with_unreachable_backend_is_invalid_argument_and_changes_nothing`, for a dry run too.
  - `create_existing_name_is_conflict`.
  - `drop_unknown_is_not_found`.
  - `drop_default_catalog_warns`.
  - `catalog_secrets_never_leave_redacted`: create (gated, live) or seed a state record, then `GET` the list, `GET` one, the envelope and `/api/v2/audit`; none contains the raw password.
  - `put_with_redacted_password_keeps_the_stored_one`: the stored spec still holds the real password after a `PUT` of the redacted `GET` body.
  - `create_pushes_to_registered_workers`: a stub worker Flight server records the `sync_catalogs` body. The result lists it with `synced: true`. A second, unreachable worker is `synced: false`, and the envelope `warning` names it.
  - `create_then_discovery_lists_it` (gated, live): `/api/v2/catalogs` shows the new name.
  - `stale_probe_is_conflict`: update the stored spec between the probe and the apply (test hook), and expect `conflict`.
- [ ] **Step 2: Run them; they fail.**
- [ ] **Step 3: Implement** the D7 flow: probe outside the lock, then `run_manage`. `apply` runs inside `spawn_blocking`, so it uses `tokio::runtime::Handle::current().block_on` for the worker pushes, which run in parallel with `futures::future::join_all`.
- [ ] **Step 4: Run the tests; they pass.** Then run Tier 1.
- [ ] **Step 5: Commit** `feat(runtime-config): add, change and drop catalogs at runtime`

### Task 6: Settings and re-seed

**Files:**
- Modify: `crates/state/src/settings.rs` (`slow_query_threshold_ms`, `clear_settings`)
- Create: `crates/server/src/web/settings.rs` and `crates/server/src/web/reseed.rs`; register them in `web/v2.rs`
- Modify: `crates/server/src/web/mod.rs` (`WebState.config_path: Option<PathBuf>`), `crates/server/src/main.rs` (the stored slow-query threshold wins at startup with the D1 WARN; `--reseed-state` covers all four objects, D2), `crates/server/src/web/admission.rs` (re-seed applies the config admission limits through the same `set_limits` path)

**Interfaces:**
- Consumes: everything from Tasks 1–5. Re-seed calls `seed_users(.., true)`, `seed_rules(.., true)`, `seed_catalogs(.., true)` and `clear_settings`, then installs the result in `RulesHandle`, `LiveCatalogs` and the admission controller, then pushes the catalog set to the workers.
- Produces:
  - `StateStore::slow_query_threshold_ms(&self) -> Option<u64>`, `set_slow_query_threshold_ms(&self, ms: u64) -> Result<(), StateError>`, `clear_settings(&self) -> Result<(), StateError>`.
  - `pub(super) async fn get_settings_for`, `update_settings_for(state, who, dry_run, slow_query_threshold_ms: u64)` and `reseed_for(state, who, dry_run)`, used by Task 7.

- [ ] **Step 1: Write the failing tests**
  - `settings_update_applies_and_persists`: `arneb_common::metrics::slow_query_threshold()` reflects it, and a reopened store returns it.
  - `settings_get_is_observe`.
  - `reseed_dry_run_lists_every_change`: a runtime catalog, a runtime rules change, a stored admission limit and a tombstoned config user all appear in the preview, and nothing changes.
  - `reseed_restores_config`: after a real run, the state equals a first-start seed of the same config, and the audit has `state.reseed`.
  - `reseed_with_broken_config_is_invalid_argument`.
  - `reseed_that_drops_own_manage_is_refused`.
  - `cli_reseed_state_covers_catalogs_and_rules`: the startup seeding helper with `reseed = true` replaces a stored runtime catalog.
- [ ] **Step 2: Run them; they fail.**
- [ ] **Step 3: Implement.** Re-seed builds and probes the config's catalogs *before* taking the lock, as in D10. A catalog that fails its probe is still stored (it came from the config) but is reported in the preview `warning`, matching startup's skip-with-WARN.
- [ ] **Step 4: Run the tests; they pass.** Then run Tier 1.
- [ ] **Step 5: Commit** `feat(runtime-config): runtime settings and the re-seed action`

### Task 7: MCP tools

**Files:**
- Modify: `crates/server/src/web/mcp/tools.rs` (18 tools, D12; the INSTRUCTIONS text gains a "Change configuration" paragraph), `crates/server/src/web/mcp/mod.rs` (the tool-count test)

**Interfaces:**
- Consumes: every `*_for` from Tasks 1, 2, 5 and 6.

- [ ] **Step 1: Write the failing tests**
  - `tool_list_has_43_tools`.
  - `each_config_tool_matches_its_route`: for each new tool, the structured result equals the route's JSON for the same input (the H3/H4 pattern).
  - `config_tools_refused_for_non_manager` (bob).
  - `create_user_tool_never_echoes_the_password`.
- [ ] **Step 2: Run them; they fail.**
- [ ] **Step 3: Implement** the tools as thin wrappers.
- [ ] **Step 4: Run the tests; they pass.** Then run Tier 1.
- [ ] **Step 5: Commit** `feat(runtime-config): MCP tools for catalogs, rules, users, settings and re-seed`

### Task 8: OpenSpec change, docs and the Tier 2 smoke

**Files:**
- Create: `openspec/changes/runtime-config/` (proposal, design with D1–D12, specs, tasks)
- Modify:
  - `docs/guide/configuration.md`: the upgrade notes "Upgrading to v0.2.0 (runtime config)", covering:
    - the state now wins for catalogs and rules;
    - workers ignore `[[catalogs]]`;
    - `[cluster.tls]` when catalogs carry credentials;
    - a resolved `ARNEB_CATALOG_<NAME>_PASSWORD` is stored in the state file;
    - open pgwire sessions survive a user delete.
  - `docs/guide/access-control.md` (runtime edits and the lockout guard).
  - `docs/guide/http-api.md` (the new routes).
  - `docs/guide/mcp.md` (the new tools).
- Create: `scripts/runtime-config-smoke.sh` (shellcheck-clean; never prints a token or password)

- [ ] **Step 1: Write the smoke script.** It takes the same environment as `scripts/manage-smoke.sh` (coordinator URL, alice as admin, bob without manage, `PG_URL` for the postgresql catalog):
  1. As alice, create a postgresql catalog `pg2` (dry run first, then real); `/api/v2/catalogs` lists it.
  2. Run a query that scans `pg2` across both workers (`EXPLAIN` shows remote fragments). It returns the expected row count, and each worker's log shows the catalog sync.
  3. `PUT` rules that deny bob `pg2`. Bob is denied over `/api/v2` and over pgwire (`42501`), with no restart.
  4. Create user `erin`, issue her a token, and query as erin over the API and over pgwire (token as password). Revoke the token; both are refused.
  5. As bob, try steps 1, 3 and 4; each is `forbidden`.
  6. Change `slow_query_threshold_ms`.
  7. The audit log lists every action above with its identity, and contains no password or token.

  Plus `--after-restart`: after a coordinator-only restart, `pg2`, the rules, erin and the threshold are all still there; restarting one worker and querying `pg2` through it works (heartbeat pull).
- [ ] **Step 2: Run Tier 2** on the local stack: coordinator + 2 workers, password auth, hive SF10 plus the docker postgres on :5433 (`docker compose up -d postgres` only if it is not already running; never restart the user's containers). Run steps 1–7, then `--after-restart`. Also run `scripts/manage-smoke.sh`, to show H4 still passes on this branch. Record the outputs in the report.
- [ ] **Step 3: Write the docs and the OpenSpec change;** `openspec validate runtime-config --strict` passes.
- [ ] **Step 4: Run Tier 1 once more.**
- [ ] **Step 5: Commit** `docs(runtime-config): openspec change, upgrade notes, API/MCP docs and smoke script`

## Notes

- **Older binaries** read the new state file (every new field is `serde(default)`), but their next write drops `catalogs`, `rules`, `seeded`, `deleted_config_users` and `slow_query_threshold_ms`. A downgrade after H5 therefore re-seeds from the config. This goes in the upgrade notes.
- **Helm.** The chart already mounts the state PVC (H1). No chart change is needed. The worker ConfigMap may keep `[[catalogs]]`; it is ignored, with the INFO log.
- **Follow-ups, not in H5:** a runtime `[[tables]]` / file catalog; per-catalog access to `catalog-configs` (today it is manage only); ending open pgwire sessions of a deleted user.
