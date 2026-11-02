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
| Access rules (`rules_file`) | the first start whose state has `seeded.rules == false` | never changed at runtime: the file wins (re-stored, INFO); changed at runtime: kept while the file's fingerprint equals the one last seeded (WARN, R11), startup refused when the file changed since (R14) |
| Settings (admission, slow-query threshold) | never copied; a stored value wins (H4 D9) | WARN per diverging knob (H4 D9, extended to `slow_query_threshold_ms`) |

- An H4 state file has no `seeded` field. It loads as `seeded.catalogs = seeded.rules = false`, so the first H5 start seeds both.
- Every divergence WARN ends with the hint `Preview a re-seed with POST /api/v2/state/reseed?dry_run=true`.
- **R18.** (Review round 2.) A statement pins its catalogs at its start: `ConnectorRegistry::pinned` copies the connectors together with `CatalogManager::pinned` (a frozen view keeping defaults, qualification and access), under a lock that `LiveCatalogs::install`/`remove` take for writing (`ConnectorRegistry::swap`), so the pair is never half-swapped; planning and data-source creation then use only the pinned pair. Workers are not pinned to the plan's catalog version: a task resolves each connector once when it starts (all its scans share it). Pinning workers would need the catalog generation in every task and workers keeping old catalog versions alive until their tasks end; deferred.
- **R17.** (Review round 2.) The default catalog and schema come from the specs the node serves (the state store's once seeded, the config's on a seeding start or without a store): the first `[[catalogs]]` entry when it is served, else the first served name in name order, else `file` with `[[tables]]`, else `memory`; the schema is the chosen spec's `effective_default_schema`. The state store is now opened before the `CatalogManager` is built so this is known up front. The same pair is logged and sent to workers with every set.
- **R16.** (Review round 2.) Flight `get_catalogs` hands raw secrets to the caller, so it needs `[cluster] secret`. A coordinator without one refuses to start, before any state write, while any spec it would serve (the stored ones once seeded, else the config's; built or not; effective specs with the global `[storage]` merged into hive specs) carries a secret; it then arms `LiveCatalogs::exposure_check`, and the catalog API and re-seed refuse a spec that would serve a secret. Without secrets it WARNs; a worker without a secret WARNs about `sync_catalogs`. A GCS `service_account_path` is a path on the node, not a key, and does not count. Refusing every coordinator with a global `[storage]` key but no hive catalog was rejected: nothing is served then.
- **R15.** (Review round 2.) The API re-seed writes users, rules, settings and catalogs in one `StateStore::reseed` mutation, after `run_manage` has synced its intent (manage-api D6: outcome `pending`, the plan's names; the completion replaces it, a crash leaves it `interrupted` at the next start). The in-memory swaps (credentials, rules handle, admission limits, threshold, live catalogs) happen only after the write succeeds. The startup `--reseed-state` path uses the same `StateStore::reseed` write, and every fatal startup check (the rules file loads, a rules file edited since its seed against runtime rules, catalog secrets without a cluster secret) runs in `prepare_state_store` before any state write (stored runtime rules that stay in force must still parse), and the pure `[cluster]` (heartbeat, drain, transport, retry), `[tls]`, Trino transport, `[web] bind_address` and `[[tables]]` (format, CSV schema, column types) checks run before it too, so a refused start leaves the state untouched; a failure after the write (a bind) leaves the atomic, config-equal state. Startup reads the rules file and decodes the served catalog specs once in `prepare_state_store` and reuses them.
- **R14.** (Review round 2.) Rules seeded from the file follow the file, like `[[auth.users]]` in H1: the state records `rules_origin` (`file_sha256` of the text last seeded, and `runtime`, set by `PUT`/`DELETE /api/v2/access-rules`). Without a runtime change, a changed rules file is stored again at startup. With one, the runtime rules are kept while the file is unchanged since the seed; a file edited since then fails startup, telling the operator to restart with `--reseed-state` or restore the file to its last seeded version, so tightening the file can never leave more permissive rules silently in force. A state without `rules_origin` counts `from_config: false` rules as runtime ones and records the current file as seeded. Rules equal to the file (compared and fingerprinted as normalized JSON, so reformatting is not a change) are marked as the file's again, so runtime rules copied into the file follow its later edits. An invalid rules file is always fatal.
- **R11.** The rules divergence WARN compares the rules file with the current stored rules, not only with the originally seeded copy. After a runtime `PUT` or `DELETE` the file no longer takes effect, so the WARN fires at every start until a re-seed (or until the file matches the stored rules). This is what the "no silent runtime changes" anti-goal asks for.
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
- Names (checked on create only; an update or drop takes any existing name, R5): 1–63 chars of `[a-z0-9_]`, starting with a letter. `memory`, `file`, `system`, `information_schema` and `pg_catalog` are reserved (`invalid_argument`).
- Types: `hive` (needs `metastore_uri`) and `postgresql` (needs `connection_url`); anything else is `invalid_argument`.
- `ARNEB_CATALOG_<NAME>_PASSWORD` is resolved on the coordinator when the config loads, so a seeded spec stores the resolved password. Workers never read catalog env vars.
- **R13.** A spec decoded from API or MCP input refuses a field the spec does not have, at any depth (`max_conections`, `storage.s3.secret_acess_key`), with `invalid_argument` naming the field. The TOML config and the state file stay lenient, so existing configs keep loading.
- **Redaction.** `redacted_spec` replaces `password`, `secret_access_key` (and Azure `access_key`), `service_account_path`, the userinfo password of `connection_url` and a `password` query parameter of `connection_url` (its key percent-decoded, as tokio-postgres does: `pass%77ord=` counts) with `"<redacted>"`. Every API response, MCP result and audit entry uses it.
- **Updates.** A `PUT` field equal to `"<redacted>"` is filled from the stored spec, so an agent can round-trip a `GET` body.
- **R10.** A `PUT` that changes where a kept secret is sent (the `connection_url` host, port, database, user or parameters; the hive `metastore_uri`; a storage endpoint, region or bucket host) must send that secret again. A field left as `"<redacted>"` is then refused with `invalid_argument`, hint `send the secret again when you change where it is sent`. Otherwise a manager, or a stolen manager token, could redirect a stored credential it never saw to a host it controls.
- A create, or an update whose stored spec cannot be decoded, that carries a literal `"<redacted>"` is refused: a placeholder is never stored as a secret.
- **R9.** Updating a config-seeded catalog makes it a runtime object (`from_config = false`), with a preview warning that the config no longer manages it. Update or drop of a reserved name, or of a catalog outside `LiveCatalogs`, is `not_found`.

**D7. Catalog API.**
- `GET /api/v2/catalog-configs` (manage, `limit` default 100, ceiling 1000) lists the redacted specs with `from_config`, `generation` and `default` (whether it is the default catalog). `GET /api/v2/catalog-configs/{name}` returns one.
- `POST /api/v2/catalog-configs` (action `catalog.create`) and `PUT /api/v2/catalog-configs/{name}` (action `catalog.update`):
  - **probe first:** build the provider on the coordinator (connect to HMS / PostgreSQL) with a 10 s timeout, before `run_manage`, for a dry run too. A probe failure is `invalid_argument` with hint `the coordinator could not connect: <error>`, and nothing changes.
  - **no-op:** a `PUT` of the spec already stored (as a runtime catalog) and served answers `changed: false` (audit effect `no_change`) with no generation bump and no push.
  - **then apply:** persist the spec (generation + 1), install the probed provider on the coordinator, and push the new set to every alive worker (draining ones included), in parallel, each push with a 5 s timeout.
  - **result:** `{name, generation, workers: [{worker_id, synced, error?, failed_catalogs?}]}`. A failed push is not an error: the heartbeat pull (D8) repairs it, and the envelope `warning` names the workers. A worker that applied the set but could not build some catalogs is `synced: true` with their names in `failed_catalogs` and in the warning (R7).
- `DELETE /api/v2/catalog-configs/{name}` (action `catalog.drop`) removes it everywhere. Dropping the default catalog is allowed, with a preview warning that unqualified table names then fail. Dropping an unknown name is `not_found`.
- `POST` on an existing name is `conflict`. The `/api/v2/catalogs` discovery routes (H2) are unchanged and show a new catalog as soon as the response returns.
- **Snapshot rule:** a query keeps the provider `Arc` it was planned with. Each change swaps the providers, connectors and the registry's `(epoch, generation)` tag in one step, and the statement's pinned tag goes out with every task (`TaskDescriptor.catalog_version`). A worker whose set differs pulls once (not when it is already past the planned generation of the same epoch) and fails the task with `catalog set on worker <id> is at generation N, the query was planned at M; retry the query` when it still differs, so a distributed query fails rather than mix catalog versions. The failure is an ordinary task failure (query retry re-runs only after a lost worker).
- The 10 s build bound also applies to catalogs built at startup, so a PostgreSQL `connect_timeout_secs` above 10 is cut to 10 s there.

**D8. Catalog sync over Flight** (`arneb-rpc`), as amended by R1, R6, R7 and R8.
- The heartbeat reply body is JSON `HeartbeatReply { catalog_generation: Option<u64>, catalog_epoch: Option<u64> }`. A body that is not JSON (`"ok"`, an older coordinator; or a standalone reply) decodes as `None`, and the worker does not sync.
- **R6 epoch.** The coordinator picks an epoch at each start, `(unix_millis << 16) | random16` (never 0), so a later start has a larger epoch; it is sent in every heartbeat reply and every `CatalogSet`. The worker tracks `(epoch, generation)`: a mismatch in either triggers a pull (`get_catalogs`). A set from the same epoch applies only if its generation is newer. The worker keeps the coordinator's current epoch: the largest a push or pull named (`fetch_max`), or the one the last heartbeat reply named, taken as is so a coordinator whose clock went back is still followed. A set from an older epoch is refused, so neither a slow build of a set fetched just before a restart nor a delayed push from the old coordinator overwrites the new coordinator's set. Generations restart or regress after a coordinator restart, so "only newer" alone would never converge. Concurrent applies of the same `(epoch, generation)` (a push and a heartbeat pull of one change) share one build of each changed catalog.
- **R1.** The coordinator's generation is never 0: a config-built set (no state store) starts at 1, and a seeded state's generation is at least 1 even with zero catalogs. A fresh worker (applied 0) therefore always pulls on its first heartbeat.
- `sync_catalogs` (coordinator → worker, push) and `get_catalogs` (worker → coordinator, pull) carry `CatalogSet { epoch, generation, catalogs: Vec<serde_json::Value>, default_catalog, default_schema }` with the **raw** specs, since the worker must connect.
- **R12.** Each pushed spec is the coordinator's effective spec: a hive spec carries the coordinator's global `[storage]` merged in, and a worker merges it over no storage of its own. The set also carries the coordinator's default catalog and schema (`serde(default)`), which the worker uses for a scan with no catalog or schema; until a set with them arrives, the worker keeps its own defaults, and a task with an unqualified scan on a worker that never applied a set pulls once first. A worker therefore needs no catalog or storage config of its own. Both run under the existing cluster security (`[cluster] secret`, `[cluster.tls]`), like every Flight action. A pull has a 10 s timeout.
- Applying a set builds every new or changed catalog, then swaps them in and removes catalogs not in the set. A catalog that fails to build is skipped with a WARN; the rest apply and the applied generation advances.
- **R7.** `sync_catalogs` answers Ok when the set was applied, even if some catalogs failed to build, and lists the failed names.
- **R8.** The worker remembers the catalogs that failed. A changed catalog whose rebuild fails is removed, not kept at its previous spec, because the registry is tagged with the new generation (a task planned at it then retries the build once and otherwise fails with `catalog '<name>' failed to build on worker <id> at generation N; retry the query`). A set with the same `(epoch, generation)` rebuilds the specs that are not installed instead of being rejected as stale. While failures exist, the heartbeat loop triggers a retry pull at most once per 30 s; the task pull-once path always retries them.
- Workers ignore `[[catalogs]]` in their own config, and their `[storage]` for catalogs (it still serves their `[[tables]]`). If it has any `[[catalogs]]`, startup logs one INFO on `arneb::config` naming them.
- Without a cluster secret, any peer can push `sync_catalogs` to a worker, like it can fetch `get_catalogs`; the worker replaces such a set with the coordinator's on its next heartbeat.
- **Pull-once on a miss.** When a task's `TableScan` names a catalog with no registered connector, the worker pulls once (joining a pull in flight, or starting a fresh one if the joined pull fetched before the catalog existed) and retries the lookup before failing with `catalog '<name>' is not registered on worker <id>`.
- A pull build holds the worker's apply mutex, so a push that lands during it can exceed its 5 s timeout; it is reported `synced: false` and the next heartbeat repairs it.

**D9. Settings.**
- `GET /api/v2/settings` (observe) returns `{slow_query_threshold_ms, source}`, `source` `config` or `state`.
- `PATCH /api/v2/settings` (manage, action `settings.update`) takes `{slow_query_threshold_ms}`, `0` turning the log off. It is applied through `arneb_common::metrics::set_slow_query_threshold` and stored in `Settings.slow_query_threshold_ms`.
- Admission stays at `/api/v2/admission` (H4); the settings `GET` does not repeat it.

**D10. Locking.** One `WebState::config_changes: Arc<tokio::sync::Mutex<()>>` serializes every H5 action (rules, users, user tokens, catalogs, settings, re-seed). It is held through `ManageAction::change_lock` until the audit entry is appended (the H4 pattern); a re-seed also holds the admission lock through its audit append. Catalog probes run before the lock is taken, so a slow HMS does not block other changes. Likewise `LiveCatalogs::apply` (a worker's push or pull, the startup load, the failed-build retry) builds every new or changed catalog in parallel without any lock and holds its `applying` mutex only to re-check the generation and swap: a set overtaken by a newer one while building changes nothing, and one whose skipped catalogs another apply changed in between rebuilds those (again outside the lock) before swapping. Under the lock the action re-checks that what it probed is still current (create: name still absent; update: stored spec unchanged since the probe), and otherwise answers `conflict`, hint `retry`. Manage is also checked again once the lock is held, so a change queued behind a rules change that removed the caller's manage is refused with `forbidden`.

**D11. Error code.** New `ErrorCode::Conflict` (`"conflict"`, HTTP 409).

**D12. MCP.** 18 new tools (44 in total), each calling its route's `*_for`:
- catalogs: `list_catalog_configs`, `get_catalog_config`, `create_catalog`, `update_catalog`, `drop_catalog`;
- access rules: `get_access_rules`, `set_access_rules`, `clear_access_rules`;
- users: `list_users`, `create_user`, `set_user_password`, `delete_user`;
- user tokens: `list_user_tokens`, `issue_user_token`, `revoke_user_token`;
- settings and re-seed: `get_settings`, `update_settings`, `reseed_state`.

Manage tools take `dry_run`. The descriptions of the tools that take a password say the password is not logged or audited, and that the agent should not repeat it back to the user.

## Notes

- **Older binaries** read the new state file (every new field is `serde(default)`), but their next write drops `catalogs`, `rules`, `seeded`, `deleted_config_users` and `slow_query_threshold_ms`. A downgrade after H5 therefore re-seeds from the config.
- **Helm.** The chart already mounts the state volume (H1); no chart change. The worker ConfigMap may keep `[[catalogs]]` (ignored, with the INFO log) and `[storage]`, or drop both (R12).
- A catalog that fails to build at coordinator startup is stored but not served or pushed until a `PUT`, a re-seed or a restart builds it; a re-seed that builds it reports it as changed.
- **Follow-ups, not in H5:** a runtime `[[tables]]` / file catalog; per-catalog access to `catalog-configs`; ending open pgwire sessions of a deleted user.

## Risks / Trade-offs

- **Catalog secrets cross the Flight connection.** `get_catalogs` serves raw specs to any peer that passes cluster security. With no `[cluster] secret` and no `[cluster.tls]`, anyone who reaches the coordinator's RPC port can read every catalog password. The docs require both when catalogs carry credentials.
- **The state file holds catalog secrets**, including a password resolved from `ARNEB_CATALOG_<NAME>_PASSWORD` (mode `0600`, as for users).
- **R8 retry cost.** During a backend outage a worker reconnects at most once per 30 s, and a task that reaches a worker with a permanently failing catalog costs one fetch and build (about 10 s) before it fails.
- **Snapshot rule.** A running query's late tasks see the worker's current catalog set.
- **Open pgwire sessions** of a deleted or re-passworded user keep running until they disconnect.
