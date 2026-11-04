## ADDED Requirements

### Requirement: The state store is the source of truth
Under `[auth] type = "password"` the coordinator (and a standalone server) SHALL keep the catalogs, the access rules and the slow-query threshold in the state store. The first start whose state has no seeded copy of the catalogs SHALL copy `[[catalogs]]` into it, and the first start with no seeded copy of the rules SHALL copy the `rules_file`. On later starts the stored catalogs SHALL be used. Stored rules that were never changed at runtime SHALL follow the rules file: a rules file whose text differs SHALL be stored again, with an INFO on `arneb::config`. Rules changed at runtime SHALL be kept while the rules file is unchanged since it was last seeded (the state SHALL keep a SHA-256 fingerprint of the seeded text); if the rules file changed since then, startup SHALL fail with an error naming `--reseed-state` and reverting the file. Each config catalog that is new, changed or gone relative to its seeded copy, rules changed at runtime that differ from the rules file, and a config setting overridden by a stored value SHALL log one WARN on `arneb::config` naming the object and ending with `Preview a re-seed with POST /api/v2/state/reseed?dry_run=true`. A state file without a `seeded` field SHALL load as not seeded. Under `[auth] type = "none"` the config SHALL be used as before.

#### Scenario: Upgrade seeds once
- **WHEN** a coordinator starts on an H4 state file with `[[catalogs]]` naming `datalake`
- **THEN** the state stores `datalake` with `from_config: true`, and the next start reads it from the state

#### Scenario: Rules changed at runtime
- **WHEN** a manager replaces the rules with `PUT /api/v2/access-rules` and the coordinator restarts with the rules file unchanged
- **THEN** the stored rules are used and startup logs the rules divergence WARN, at every start until a re-seed

#### Scenario: Tightened rules file
- **WHEN** the rules were never changed at runtime and an operator removes bob's table rule from the rules file and restarts
- **THEN** the state stores the file's rules and bob can no longer read the table

#### Scenario: Rules changed at runtime and the file edited
- **WHEN** a manager replaced the rules with `PUT /api/v2/access-rules` and an operator then edits the rules file and restarts
- **THEN** startup fails with an error naming `--reseed-state`, and the stored rules are unchanged

#### Scenario: Config edit after seeding
- **WHEN** an operator changes the `metastore_uri` of a seeded catalog in the config file and restarts
- **THEN** the stored catalog is used and startup logs a WARN naming the catalog with the re-seed hint

### Requirement: Runtime changes are manage actions
Every change to the access rules, users, user tokens, catalogs, settings and the re-seed SHALL need the manage privilege, take `dry_run`, answer with the manage envelope, be audited (except dry runs), be persisted in the state store before the response, and survive a restart. Changes SHALL be serialized by one lock held until the audit entry is appended. A dry run SHALL apply the same authorization and validation as the real call and change nothing. Every listing SHALL take `limit` (default 100, ceiling 1000) and report `truncated`, and a value above the ceiling SHALL be `invalid_argument`. A new error code `conflict` (HTTP 409) SHALL report a name that exists, an object that changed while the call was being prepared, and a change that removes the caller's own manage privilege.

#### Scenario: Non-manager is refused
- **WHEN** bob, without manage, sends `POST /api/v2/catalog-configs`, `PUT /api/v2/access-rules` or `POST /api/v2/users`, as a dry run or not
- **THEN** each answers `403 forbidden` and nothing changes or is audited

#### Scenario: Survives a restart
- **WHEN** a manager creates catalog `pg2`, sets rules, creates user `erin` and sets `slow_query_threshold_ms`, then the coordinator restarts
- **THEN** all four are still in effect after the restart

### Requirement: Users at runtime
`POST /api/v2/users` SHALL create a user from `{name, password}` (name 1–64 characters of `[A-Za-z0-9_.@-]`, password 8–1024 characters, an existing name `conflict`) with a fresh SCRAM verifier and `from_config: false`. `PATCH /api/v2/users/{name}` SHALL store a new verifier, revoke all of the user's tokens and, for a config-seeded user, detach it from the config. `DELETE /api/v2/users/{name}` SHALL delete the user and their tokens, and SHALL record a tombstone for a user that was ever seeded from the config, so the next start does not re-add it and logs a WARN instead. After a password change or a delete the user's cached Basic credentials SHALL be dropped and their Web sessions ended at once; open pgwire connections MAY stay open. `GET /api/v2/users` SHALL list `{name, from_config, tokens}` in name order. A password SHALL never be logged, audited or returned.

#### Scenario: Password change drops cached credentials
- **WHEN** a Trino client authenticated with Basic as alice and a manager changes alice's password
- **THEN** the client's next request with the old password is refused

#### Scenario: Deleted config user stays deleted
- **WHEN** a manager deletes config user bob and the coordinator restarts with bob still in `[[auth.users]]`
- **THEN** bob does not exist and startup logs a WARN naming bob

### Requirement: Tokens for another user
`GET`, `POST /api/v2/users/{name}/tokens` and `DELETE /api/v2/users/{name}/tokens/{id}` SHALL list, issue and revoke the named user's API tokens for a manager, audited as `token.issue` and `token.revoke` with the user as `target`. The issued token SHALL be returned once and the audit log SHALL name a token by its 16-hex id only. The self-service `/api/v2/tokens` routes SHALL be unchanged.

#### Scenario: Issue and revoke for erin
- **WHEN** a manager issues a token for erin, erin uses it on `/api/v2` and as her pgwire password, and the manager revokes it
- **THEN** both uses succeed before the revoke, `/api/v2` answers `401` after it and pgwire refuses the login

### Requirement: Catalog definitions at runtime
`/api/v2/catalog-configs` SHALL list, show, create (`POST`), update (`PUT`) and drop (`DELETE`) `hive` and `postgresql` catalogs. A created name SHALL be 1–63 characters of `[a-z0-9_]` starting with a letter, and `memory`, `file`, `system`, `information_schema` and `pg_catalog` SHALL be reserved; an update or drop SHALL accept any existing name (a config-seeded `pg-prod` stays updatable). A create or update SHALL first build the catalog on the coordinator within 10 s, also for a dry run, and a failure SHALL be `invalid_argument` with nothing changed. A real change SHALL persist the spec with the catalog generation increased by one, install it on the coordinator and push the new set to every alive worker with a 5 s timeout each, reporting each worker as `synced` or not; a failed push SHALL NOT fail the call. A spec sent through the API or MCP SHALL be refused with `invalid_argument` naming any field the spec does not have, at any depth. A `PUT` of the spec already stored and served SHALL answer `changed: false` with no generation bump and no push. A `POST` of an existing name SHALL be `conflict`, and an update or drop of an unknown name `not_found`. The `/api/v2/catalogs` discovery routes SHALL list a new catalog as soon as the change returns. A running query SHALL keep, on the coordinator, the catalog provider and connector it was planned with. Every distributed task SHALL carry the catalog version the query was planned at: the coordinator epoch and catalog generation, a fingerprint of each catalog's spec, and the default catalog and schema, in which the task SHALL resolve an unqualified table name. A worker whose copy of a catalog the task scans has another fingerprint SHALL pull once and, when it still differs, fail the task with `catalog '<name>' on worker <id> is not the version the query was planned with (worker at generation N, query planned at M); retry the query`, so a query never mixes catalog versions; a change to a catalog the task does not scan SHALL NOT fail it. All scans of one catalog in a task SHALL use one connector.

#### Scenario: Create and query across workers
- **WHEN** a manager creates postgresql catalog `pg2` on a coordinator with two workers
- **THEN** both workers report `synced: true`, and `SELECT count(*) FROM pg2.tpch_mixed.customer` runs distributed on both workers with the database's row count

#### Scenario: Change during a distributed query
- **WHEN** a query planned at generation 4 dispatches a task that scans `pg2` to a worker that holds generation 5, where `pg2` changed
- **THEN** the task fails with `catalog 'pg2' on worker <id> is not the version the query was planned with (worker at generation 5, query planned at 4); retry the query` and the query fails without reading the changed `pg2`

#### Scenario: Unrelated change during a distributed query
- **WHEN** a query planned at generation 4 scans only `datalake`, and catalog `x` is created (generation 5) before one of its tasks starts on a worker
- **THEN** the task runs, and an unqualified table name in it resolves in the default catalog and schema the query was planned with

#### Scenario: Unreachable backend
- **WHEN** a manager creates a catalog whose database cannot be reached
- **THEN** the call is `invalid_argument` with hint `the coordinator could not connect: <error>` and no catalog is stored

### Requirement: Catalog secrets stay redacted
Every catalog answer, MCP result and audit entry SHALL replace `password`, `secret_access_key`, `service_account_path` and the password in `connection_url` (userinfo or a `password` parameter, its key percent-decoded) with `"<redacted>"`; a stored `connection_url` that fails URL validation SHALL be shown as `postgresql://<redacted>` (or `postgres://<redacted>`) when it starts with that scheme in any case, and as `"<redacted>"` otherwise, wherever a `://` appears further in. URL validation SHALL refuse a `connection_url` whose first `@` lies past the authority (after the first `/`, `?` or `#` following `//`), whose `password` query parameter is followed by another parameter (a raw `&` in it would make its tail a driver parameter), or whose user as written (not percent-decoded) or parsed host holds a URL delimiter or an `@`; a create or update SHALL refuse it with `invalid_argument` that echoes no part of the URL. Percent-encoded characters in the password SHALL be accepted. A `PUT` field equal to `"<redacted>"` SHALL keep the stored secret, unless the update changes where that secret is sent (the `connection_url` host, port, database, user or parameters, the `metastore_uri`, or a storage endpoint, region or bucket), which SHALL be `invalid_argument` with hint `send the secret again when you change where it is sent`. A literal `"<redacted>"` SHALL never be stored as a secret.

#### Scenario: Round-trip keeps the password
- **WHEN** a manager sends back the body of `GET /api/v2/catalog-configs/pg2` with a changed `max_connections`
- **THEN** the stored password is kept, and no answer, audit entry or log line contains it

#### Scenario: Redirected secret
- **WHEN** a manager changes the host of `pg2`'s `connection_url` and leaves its password as `"<redacted>"`
- **THEN** the call is `invalid_argument` and the stored spec is unchanged

#### Scenario: Raw ampersand in a query password
- **WHEN** a manager sends `connection_url = "postgresql://db/x?password=Xy&host=9zK"`
- **THEN** the call is `invalid_argument` naming the last-parameter rule and `%26`, and a stored copy of that URL is shown as `postgresql://<redacted>`

### Requirement: Workers receive catalogs from the coordinator
Workers SHALL ignore `[[catalogs]]` in their own config, logging one INFO on `arneb::config` that names them, and SHALL take their catalogs from the coordinator over Flight under the cluster security. Each set SHALL carry the coordinator's default catalog and schema, which a worker SHALL use for a scan with no catalog or schema, and each hive spec SHALL carry the coordinator's global `[storage]` merged in, so a worker needs no catalog or storage config of its own. Every heartbeat reply SHALL carry the coordinator's catalog generation and an epoch chosen at random at each coordinator start; a worker whose applied `(epoch, generation)` differs SHALL pull the set. A pushed or pulled set from the same epoch SHALL apply only when its generation is newer, and a set from another epoch SHALL always apply. Applying a set SHALL build new or changed catalogs, swap them in and remove catalogs not in the set; a catalog that fails to build SHALL be skipped with a WARN, the applied generation SHALL still advance, and the worker SHALL retry the failed catalogs at most once per 30 s. A task that names a catalog the worker does not have SHALL pull once and retry before failing with `catalog '<name>' is not registered on worker <id>`.

A coordinator without a `[cluster] secret` SHALL refuse to start while a served catalog's effective spec carries a secret (`password`, a `connection_url` password (a `connection_url` that fails URL validation counts when it may hold one: an `@` with a `:` before it, or `password` anywhere once percent-decoded, in any case; one with neither, such as a mistyped parameter, is a validation failure reported when the catalog is checked or built, not a secret), an S3 `secret_access_key` or an Azure `access_key`), naming each catalog and field but never a value, and SHALL refuse a catalog create, update or re-seed that would serve one with `invalid_argument` and a hint to set `[cluster] secret`. Without such secrets it SHALL start and log a WARN on `arneb::config` that cluster RPC is unauthenticated; a worker without a secret SHALL log a WARN that `sync_catalogs` is unauthenticated.

#### Scenario: Catalog secret without a cluster secret
- **WHEN** a coordinator with no `[cluster] secret` starts with a postgresql catalog that has a `password`
- **THEN** startup fails with an error naming `[cluster] secret` and `catalog 'pg': password`

#### Scenario: Invalid URL with no password without a cluster secret
- **WHEN** a coordinator with no `[cluster] secret` starts with a postgresql catalog whose `connection_url` is `postgresql://db/x?sslmod=require` (a typo, no password)
- **THEN** it starts, and the catalog fails to build with a WARN

#### Scenario: Worker restarted
- **WHEN** a worker restarts after catalog `pg2` was created
- **THEN** its first heartbeat pulls the set, and a query on `pg2` runs on it

#### Scenario: Worker without catalog config
- **WHEN** both workers' configs have no `[[catalogs]]` and no `[storage]`, and a client runs `SELECT count(*) FROM tpch.lineitem` with default catalog `datalake`
- **THEN** the query runs distributed on both workers, reading object storage with the coordinator's `[storage]`

#### Scenario: Coordinator restarted
- **WHEN** the coordinator restarts and its generation is the same as before
- **THEN** each worker sees a new epoch and pulls the set again

#### Scenario: Slow set from the previous epoch
- **WHEN** a worker's pull of epoch E1's set is still building after the restarted coordinator (epoch E2) pushed its set
- **THEN** the E1 set changes nothing when its build finishes, and a later push from E1 is refused with an error, so E1's coordinator never counts the worker as synced: a worker follows the epoch its last heartbeat reply named (before the first, the largest a push or pull named), refuses a set from any other epoch, and concurrent applies of the same epoch and generation build each catalog once

#### Scenario: Coordinator moved to a host whose clock is behind
- **WHEN** a heartbeat names epoch E2 smaller than the applied E1, and a delayed push from E1's coordinator arrives afterwards
- **THEN** the push is refused with an error and does not move the epoch back to E1, and the live coordinator's pulls and pushes at E2 keep applying

### Requirement: Runtime settings
`GET /api/v2/settings` (observe) SHALL return `{slow_query_threshold_ms, source}`. `PATCH /api/v2/settings` (manage) SHALL set `slow_query_threshold_ms` (`0` turns the slow-query log off), apply it at once and store it, and a stored value SHALL win over `[observability]` and `ARNEB_SLOW_QUERY_THRESHOLD_MS` at every start.

#### Scenario: Threshold change
- **WHEN** a manager sets `slow_query_threshold_ms` to 54321
- **THEN** `GET /api/v2/settings` reports 54321 with `source: state`, also after a restart

### Requirement: Re-seed
`POST /api/v2/state/reseed` (manage) and `--reseed-state` SHALL reload the config file the server was started with, env overrides applied, and replace the stored catalogs (removing runtime-created ones), the access rules (none without a rules file), the stored settings (cleared) and the config users, clearing the tombstones. Tokens, worker cordons, runtime-created users and the audit log SHALL be kept, except tokens that user seeding revokes. The dry run SHALL list the catalogs added, changed and removed, whether the rules change and the settings reset. A config that fails to load SHALL be `invalid_argument`, and a re-seed that removes the caller's manage privilege or deletes the caller's user SHALL be `conflict`. The API re-seed SHALL have its `state.reseed` intent synced to the audit log (outcome `pending`, the manage-api intent/completion mechanism) before it changes anything, and SHALL persist the catalogs, rules, settings and users in one atomic state write; a failed write SHALL change nothing, stored or live.

#### Scenario: Re-seed removes a runtime catalog
- **WHEN** a manager re-seeds after creating catalog `pg2`, which is not in the config
- **THEN** `pg2` is gone on the coordinator and every worker, and the rules and threshold are the config's

### Requirement: MCP tools for the runtime configuration
The MCP server SHALL offer 18 more tools, 44 in total, each calling the same function as its route: `list_catalog_configs`, `get_catalog_config`, `create_catalog`, `update_catalog`, `drop_catalog`, `get_access_rules`, `set_access_rules`, `clear_access_rules`, `list_users`, `create_user`, `set_user_password`, `delete_user`, `list_user_tokens`, `issue_user_token`, `revoke_user_token`, `get_settings`, `update_settings` and `reseed_state`. Every manage tool SHALL take `dry_run`. The descriptions of tools that take a password SHALL say that it is not logged or audited and that the agent should not repeat it back to the user.

#### Scenario: Tool list
- **WHEN** a client calls `tools/list`
- **THEN** it receives 44 tools

#### Scenario: Equal rights
- **WHEN** bob, without manage, calls `create_user`
- **THEN** the result is an error with code `forbidden`, as `POST /api/v2/users` answers
