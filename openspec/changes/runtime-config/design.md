## Context

Stage H5 of the headless agent interface, built on H1 (`headless-api-auth`: identity, API tokens, the state store), H2 (`agent-query-api`), H3 (`remote-mcp`) and H4 (`manage-api`: privilege layers, `run_manage`, dry runs, the envelope and the audit log). Every H5 action is one `*_for` function that a `/api/v2` route and an MCP tool both call, and every change goes through H4's `run_manage` (dry run, audit, envelope).

Before H5 the access rules, catalogs and slow-query threshold were start-time snapshots, and users could only change through the config file. H5 makes the state store the source of truth for them and replaces the snapshots with live handles:

- `arneb_catalog::RulesHandle`: a swappable `Option<Arc<AccessRules>>` shared by pgwire, Trino, `/api/v2`, MCP and the `Authorizer`.
- `ConnectorRegistry` gains interior mutability.
- `LiveCatalogs` (server crate) builds, installs and removes `hive` / `postgresql` catalogs on one node.

## Goals / Non-Goals

**Goals**
- A manager can edit the access rules, create / change / remove users and their tokens, add / change / remove catalogs (pushed to every worker), change the slow-query threshold, and re-seed from the config file, without a restart.
- Each change has a dry run, is audited and survives a restart.
- No runtime change is silent: a config value that no longer takes effect logs a WARN at every start.
- Secrets never leave the server in answers, audit entries or logs.

**Non-Goals**
- A runtime `[[tables]]` / `file` catalog; the `memory` catalog; default catalog and schema; `admin_users`; `[audit]` retention; ports, bind addresses and TLS. They stay startup-only.
- Per-catalog access to `catalog-configs` (it needs manage).
- Ending open pgwire sessions of a deleted user.
- OAuth (H6).

## Decisions

**D1. What is seeded, and what wins.**

| Object | Seeded on | Later starts |
|---|---|---|
| Users | every start (H1 D2, unchanged) | config wins for config-seeded users |
| Catalogs (`[[catalogs]]`) | the first start whose state has `seeded.catalogs == false` | state wins; each config catalog that is new, changed or gone relative to its seeded copy logs one WARN |
| Access rules (`rules_file`) | the first start whose state has `seeded.rules == false` | state wins; a rules file whose text differs from the seeded copy logs one WARN |
| Settings (admission, slow-query threshold) | never copied; a stored value wins (H4 D9) | WARN per diverging knob (H4 D9, extended to `slow_query_threshold_ms`) |

- An H4 state file has no `seeded` field. It loads as `seeded.catalogs = seeded.rules = false`, so the first H5 start seeds both.
- Every divergence WARN ends with the hint `preview a re-seed with POST /api/v2/state/reseed?dry_run=true`.
- Not runtime-editable (startup-only): `[[tables]]` (the `file` catalog), the `memory` catalog, the default catalog and schema, `admin_users`, `[audit]` retention, ports, bind addresses and TLS. `[audit]` retention stays startup-only on purpose, so a manager cannot shorten the trail of their own actions.
- **R4 (standalone).** A standalone server under `[auth] type = "password"` seeds and reads catalogs from the state store exactly like a coordinator. Only without a state store (`[auth] type = "none"`) does it build catalogs from the config.
- **R5.** Config catalog names are not validated against D6's name rule at startup (only API input is), so existing configs such as `pg-prod` keep loading. Such a seeded catalog can be updated and dropped through the API, but not created.
- **R2.** On the coordinator, catalogs are built after the state store opens; worker and standalone startup order is unchanged.

**D2. Re-seed** (`POST /api/v2/state/reseed`, action `state.reseed`, target `state`).
- It re-reads the config file with `AppConfig::load(<the --config argument the process started with>)` and env overrides applied (R3): without `--config` it reads `./arneb.toml` when present, exactly like startup, and otherwise the defaults.
- It replaces:
  - the catalogs, so runtime-created catalogs are removed;
  - the rules, so a missing rules file means no rules;
  - the stored settings, which are cleared, so the config values apply;
  - the users, through `seed_users(.., reseed = true)`, and the tombstones (D4) are cleared.
- Tokens, cordons and the audit log are left alone, except the tokens that user seeding revokes. Runtime-created users are kept unless a config user of the same name overwrites them.
- The preview lists the catalogs added / changed / removed, whether the rules change, the settings that reset and the user report.
- A config file that fails to load is `invalid_argument` with the loader's message. A re-seed that would drop the caller's own manage privilege is refused like a rules change (D3).
- The `--reseed-state` CLI flag does the same at startup for all four objects.

**D3. Access rules API.**
- `GET /api/v2/access-rules` (manage) returns `{"rules": <json|null>, "from_config": bool}`. `from_config` is `false` when there are no rules.
- `PUT` (action `access_rules.set`) takes the full rules JSON as the body, validated with `AccessRules::from_json`; a parse error is `invalid_argument` with the parser's message.
- `DELETE` (action `access_rules.clear`) removes all rules, which means "allow everything" in the query layer; its preview `warning` says so.
- **Lockout guard.** A change after which the caller would no longer hold manage is refused with `conflict`, hint `keep a system_information write rule for yourself, or ask a user in [access_control] admin_users`. A user in `admin_users` always keeps manage, so the guard never fires for them. The guard is part of validation, so a dry run is refused the same way.
- A rules change applies to every statement planned after the response, including on connections already open (each statement reads `RulesHandle::current()`). A statement already running keeps the rules it was checked with.

**D4. Users.**
- `POST /api/v2/users` (action `user.create`) takes `{name, password}`. Name: 1–64 chars of `[A-Za-z0-9_.@-]`. Password: 8–1024 chars. An existing name is `conflict`. The user gets a fresh SCRAM verifier (random 16-byte salt, 4096 iterations, the H1 default) and `from_config = false`.
- `PATCH /api/v2/users/{name}` (action `user.set_password`) takes `{password}`. It stores a new verifier and revokes all of the user's tokens (the preview says how many). On a config-seeded user it also detaches the user (`from_config = false`), with a preview warning that the config no longer manages the user; H1's "conflict" WARN then fires at every start while the name is still in the config.
- `DELETE /api/v2/users/{name}` (action `user.delete`) deletes the user and their tokens. Deleting a user that was ever seeded from the config (including a detached one) records a tombstone (`StateData.deleted_config_users`), so the next start does not re-add it and logs a WARN naming the user with the re-seed hint. A re-seed clears the tombstones.
- After a password change or a delete the server drops the user's Basic-auth cache entries (`BasicAuthenticator::forget_user`) and ends their Web sessions (`SessionStore::end_user`). pgwire connections already open stay open, matching Trino and PostgreSQL.
- Deleting yourself is allowed, with a preview warning.
- `GET /api/v2/users` (manage, `limit` default 100, ceiling 1000, name order) lists `{name, from_config, tokens}`, where `tokens` is the live token count.
- Passwords are never logged, audited or returned.

**D5. Tokens for another user** (manage).
- `GET /api/v2/users/{name}/tokens`, `POST /api/v2/users/{name}/tokens` and `DELETE /api/v2/users/{name}/tokens/{id}` reuse H1/H4's `issue_token_for` / `revoke_token_for` and their audit actions (`token.issue`, `token.revoke`), with the manage envelope.
- The audit `target` is the user; the token is named by its 16-hex id. The issued secret is shown once, to the manager.
- The self-service routes `/api/v2/tokens*` are unchanged.

**D6. Catalog definitions.**
- A catalog spec is `config::CatalogConfig`, now also `Serialize` (and so are `StorageConfig` and `RedactedString`, which serialize the raw value for the state file and the RPC; their `Debug` stays redacted).
- The state stores it as JSON in `CatalogRecord.spec`, so `arneb-state` does not depend on the server crate.
- Names: 1–63 chars of `[a-z0-9_]`, starting with a letter. `memory`, `file`, `system`, `information_schema` and `pg_catalog` are reserved (`invalid_argument`).
- Types: `hive` (needs `metastore_uri`) and `postgresql` (needs `connection_url`); anything else is `invalid_argument`.
- `ARNEB_CATALOG_<NAME>_PASSWORD` is resolved on the coordinator when the config loads, so a seeded spec stores the resolved password. Workers never read catalog env vars.
- **Redaction.** `redacted_spec` replaces `password`, `secret_access_key` (and Azure `access_key`), `service_account_path`, the userinfo password of `connection_url` and a `password` query parameter of `connection_url` with `"<redacted>"`. Every API response, MCP result and audit entry uses it.
- **Updates.** A `PUT` field equal to `"<redacted>"` is filled from the stored spec, so an agent can round-trip a `GET` body.
- **R10.** A `PUT` that changes where a kept secret is sent (the `connection_url` host, port, database, user or parameters; the hive `metastore_uri`; a storage endpoint, region or bucket host) must send that secret again. A field left as `"<redacted>"` is then refused with `invalid_argument`, hint `send the secret again when you change where it is sent`. Otherwise a manager, or a stolen manager token, could redirect a stored credential it never saw to a host it controls.
- A create, or an update whose stored spec cannot be decoded, that carries a literal `"<redacted>"` is refused: a placeholder is never stored as a secret.
- **R9.** Updating a config-seeded catalog makes it a runtime object (`from_config = false`), with a preview warning that the config no longer manages it. Update or drop of a reserved name, or of a catalog outside `LiveCatalogs`, is `not_found`.

**D7. Catalog API.**
- `GET /api/v2/catalog-configs` (manage, `limit` default 100, ceiling 1000) lists the redacted specs with `from_config`, `generation` and `default` (whether it is the default catalog). `GET /api/v2/catalog-configs/{name}` returns one.
- `POST /api/v2/catalog-configs` (action `catalog.create`) and `PUT /api/v2/catalog-configs/{name}` (action `catalog.update`):
  - **probe first:** build the provider on the coordinator (connect to HMS / PostgreSQL) with a 10 s timeout, before `run_manage`, for a dry run too. A probe failure is `invalid_argument` with hint `the coordinator could not connect: <error>`, and nothing changes.
  - **then apply:** persist the spec (generation + 1), install the probed provider on the coordinator, and push the new set to every alive worker (draining ones included), in parallel, each push with a 5 s timeout.
  - **result:** `{name, generation, workers: [{worker_id, synced, error?, failed_catalogs?}]}`. A failed push is not an error: the heartbeat pull (D8) repairs it, and the envelope `warning` names the workers. A worker that applied the set but could not build some catalogs is `synced: true` with their names in `failed_catalogs` and in the warning (R7).
- `DELETE /api/v2/catalog-configs/{name}` (action `catalog.drop`) removes it everywhere. Dropping the default catalog is allowed, with a preview warning that unqualified table names then fail. Dropping an unknown name is `not_found`.
- `POST` on an existing name is `conflict`. The `/api/v2/catalogs` discovery routes (H2) are unchanged and show a new catalog as soon as the response returns.
- **Snapshot rule:** a query keeps the provider `Arc` it was planned with. A task submitted after a change, for a query that was already running, uses the worker's current catalog set. Known limit.
- The 10 s build bound also applies to catalogs built at startup, so a PostgreSQL `connect_timeout_secs` above 10 is cut to 10 s there.

**D8. Catalog sync over Flight** (`arneb-rpc`), as amended by R1, R6, R7 and R8.
- The heartbeat reply body is JSON `HeartbeatReply { catalog_generation: Option<u64>, catalog_epoch: Option<u64> }`. A body that is not JSON (`"ok"`, an older coordinator; or a standalone reply) decodes as `None`, and the worker does not sync.
- **R6 epoch.** The coordinator picks a random nonzero `u64` epoch at each start, sent in every heartbeat reply and every `CatalogSet`. The worker tracks `(epoch, generation)`: a mismatch in either triggers a pull (`get_catalogs`). A set from the same epoch applies only if its generation is newer; a set from a different epoch always applies. Generations restart or regress after a coordinator restart, so "only newer" alone would never converge.
- **R1.** The coordinator's generation is never 0: a config-built set (no state store) starts at 1, and a seeded state's generation is at least 1 even with zero catalogs. A fresh worker (applied 0) therefore always pulls on its first heartbeat.
- `sync_catalogs` (coordinator → worker, push) and `get_catalogs` (worker → coordinator, pull) carry `CatalogSet { epoch, generation, catalogs: Vec<serde_json::Value> }` with the **raw** specs, since the worker must connect. Both run under the existing cluster security (`[cluster] secret`, `[cluster.tls]`), like every Flight action. A pull has a 10 s timeout.
- Applying a set builds every new or changed catalog, then swaps them in and removes catalogs not in the set. A catalog that fails to build is skipped with a WARN; the rest apply and the applied generation advances.
- **R7.** `sync_catalogs` answers Ok when the set was applied, even if some catalogs failed to build, and lists the failed names.
- **R8.** The worker remembers the catalogs that failed. A set with the same `(epoch, generation)` rebuilds the specs that are not installed instead of being rejected as stale. While failures exist, the heartbeat loop triggers a retry pull at most once per 30 s; the task pull-once path always retries them.
- Workers ignore `[[catalogs]]` in their own config. If it has any, startup logs one INFO on `arneb::config` naming them.
- **Pull-once on a miss.** When a task's `TableScan` names a catalog with no registered connector, the worker pulls once (joining a pull in flight, or starting a fresh one if the joined pull fetched before the catalog existed) and retries the lookup before failing with `catalog '<name>' is not registered on worker <id>`.
- A pull build holds the worker's apply mutex, so a push that lands during it can exceed its 5 s timeout; it is reported `synced: false` and the next heartbeat repairs it.

**D9. Settings.**
- `GET /api/v2/settings` (observe) returns `{slow_query_threshold_ms, source}`, `source` `config` or `state`.
- `PATCH /api/v2/settings` (manage, action `settings.update`) takes `{slow_query_threshold_ms}`, `0` turning the log off. It is applied through `arneb_common::metrics::set_slow_query_threshold` and stored in `Settings.slow_query_threshold_ms`.
- Admission stays at `/api/v2/admission` (H4); the settings `GET` does not repeat it.

**D10. Locking.** One `WebState::config_changes: Arc<tokio::sync::Mutex<()>>` serializes every H5 action (rules, users, user tokens, catalogs, settings, re-seed). It is held through `ManageAction::change_lock` until the audit entry is appended (the H4 pattern); a re-seed also holds the admission lock through its audit append. Catalog probes run before the lock is taken, so a slow HMS does not block other changes. Under the lock the action re-checks that what it probed is still current (create: name still absent; update: stored spec unchanged since the probe), and otherwise answers `conflict`, hint `retry`.

**D11. Error code.** New `ErrorCode::Conflict` (`"conflict"`, HTTP 409).

**D12. MCP.** 18 new tools (43 in total), each calling its route's `*_for`:
- catalogs: `list_catalog_configs`, `get_catalog_config`, `create_catalog`, `update_catalog`, `drop_catalog`;
- access rules: `get_access_rules`, `set_access_rules`, `clear_access_rules`;
- users: `list_users`, `create_user`, `set_user_password`, `delete_user`;
- user tokens: `list_user_tokens`, `issue_user_token`, `revoke_user_token`;
- settings and re-seed: `get_settings`, `update_settings`, `reseed_state`.

Manage tools take `dry_run`. The descriptions of the tools that take a password say the password is not logged or audited, and that the agent should not repeat it back to the user.

## Notes

- **Older binaries** read the new state file (every new field is `serde(default)`), but their next write drops `catalogs`, `rules`, `seeded`, `deleted_config_users` and `slow_query_threshold_ms`. A downgrade after H5 therefore re-seeds from the config.
- **Helm.** The chart already mounts the state volume (H1); no chart change. The worker ConfigMap may keep `[[catalogs]]`; it is ignored, with the INFO log.
- **Follow-ups, not in H5:** a runtime `[[tables]]` / file catalog; per-catalog access to `catalog-configs`; ending open pgwire sessions of a deleted user.

## Risks / Trade-offs

- **Catalog secrets cross the Flight connection.** `get_catalogs` serves raw specs to any peer that passes cluster security. With no `[cluster] secret` and no `[cluster.tls]`, anyone who reaches the coordinator's RPC port can read every catalog password. The docs require both when catalogs carry credentials.
- **The state file holds catalog secrets**, including a password resolved from `ARNEB_CATALOG_<NAME>_PASSWORD` (mode `0600`, as for users).
- **R8 retry cost.** During a backend outage a worker reconnects at most once per 30 s, and a task that reaches a worker with a permanently failing catalog costs one fetch and build (about 10 s) before it fails.
- **Snapshot rule.** A running query's late tasks see the worker's current catalog set.
- **Open pgwire sessions** of a deleted or re-passworded user keep running until they disconnect.
