# Production Checklist

Walk this list before a team relies on Arneb in place of an internal Trino
cluster: a few people pointing BI and ad-hoc tools (Superset, Metabase, dbt,
notebooks) at one cluster. The goal is that concurrent and oversized queries
do not take a node down, unauthenticated users cannot connect, a lost worker
does not wedge the cluster, and operators can see what is happening.

Arneb's defaults suit a laptop, not a shared cluster. Out of the box:

- No authentication: `[auth] type = "none"`.
- No TLS.
- Flight RPC trusts every peer.
- `/metrics` is open.
- MCP is on.
- The Trino listener is on.

The one safe default is `bind_address = "127.0.0.1"`, so a fresh install is
reachable only from its own host. Every section below that opens a port
also closes one of these gaps.

Each item names the setting to use and how to confirm it took effect. Most
settings log their effective value at startup on the `arneb::config` target,
so `RUST_LOG=info` plus a grep is usually the check.

## 1. Who can connect

- [ ] **Password authentication is on.**
  - Set `[auth] type = "password"` with `[[auth.users]]` entries.
  - Generate each `password_hash` with `arneb hash-password --iterations 100000` (the default of 4096 iterations is low).
  - Under `none`, any client connects as any user it names.
  - See [Authentication](/guide/configuration#authentication).
- [ ] **pgwire and the Trino listener use TLS.**
  - Set `[tls] cert_path` and `key_path`. pgwire then requires TLS (`pgwire = "required"`) and the Trino listener serves HTTPS only.
  - Password auth with the Trino listener on and no `[tls]` refuses to start. Turn the listener off with `--no-trino` or `ARNEB_TRINO_ENABLED=false` if you do not use it.
  - The certificate is read at startup, so rotating it needs a restart.
  - See [TLS](/guide/configuration#tls).
- [ ] **The Web port is behind a TLS-terminating proxy.**
  - The Web UI, the HTTP API and MCP share one plain-HTTP port (pgwire + 1000).
  - Under password auth that port binds to `127.0.0.1` unless `[web] bind_address` is set. Set it only when a proxy terminates TLS in front of it.
  - Under `none`, anyone who reaches this port can cancel any query, so do not expose it.
  - See [Web UI](/guide/configuration#web-ui) and [Security notes](/guide/http-api#security-notes).
- [ ] **Login attempts are rate-limited at the proxy.** Arneb has no lockout after failed logins.
- [ ] **`/metrics` is protected or kept internal.**
  - Set `[web] metrics_auth = true`, or make sure only Prometheus can reach the port.
  - `/metrics` serves worker ids and admission gauges, and `system_information` rules do not hide them.

**Check:**
- `psql "host=<coordinator> sslmode=disable"` is refused.
- A connection with a wrong password is refused.
- `curl https://<proxy>/api/v2/queries` without a session or token answers `401`.

## 2. What each user can read

- [ ] **Access rules are loaded.**
  - Point `[access_control] rules_file` (`ARNEB_ACCESS_CONTROL_RULES_FILE`) at a Trino-style rules file.
  - With no file, every authenticated user reads every table, including federated PostgreSQL catalogs.
  - Rules only mean something under password auth.
  - See [Access Control](/guide/access-control).
- [ ] **Every section you rely on is present.** A missing section (`catalogs`, `schemas`, `tables`, `queries`, `system_information`) allows everything it governs.
- [ ] **Allow rules match the stored name.** A read is checked against both the name as written and the name the connector resolved, so a rule must match the table's stored name too. See [How rules match](/guide/access-control#how-rules-match).
- [ ] **Admins are named.**
  - `admin_users` (`ARNEB_ADMIN_USERS`) grants the manage API.
  - Nothing grants it by default, and under `none` every manage call is refused.
- [ ] **PostgreSQL catalogs use a least-privilege database role.** Arneb's rules cover the tables a user names, but a PostgreSQL view's underlying tables are governed only by the catalog's own role.

**Check:**
- A user without a rule for a schema gets a permission error on `SELECT`.
- That user does not see the schema in `SHOW SCHEMAS` or `information_schema`.

## 3. The cluster's own traffic

- [ ] **A cluster secret is set.**
  - Set `[cluster] secret` (`ARNEB_CLUSTER_SECRET`, at least 32 characters) to the same value on every node. Every Flight call then carries it as a bearer token.
  - Without it, any host that reaches a node's RPC port can run tasks and read exchange data.
  - A coordinator refuses to start without it when a catalog carries credentials.
  - The Helm chart generates one.
  - See [Securing the Cluster](/guide/distributed#securing-the-cluster).
- [ ] **Cluster TLS is on when nodes talk over an untrusted network.**
  - Set `[cluster.tls]`. Without it, the secret crosses the network in clear text.
  - Cluster TLS encrypts but does not authenticate peers; the secret does that.
  - On Kubernetes each pod needs a certificate with its pod IP as an IP SAN, because the chart does not yet give workers stable DNS names. See [Secrets](/guide/kubernetes#secrets).

**Check:** a worker started with a different secret is never registered. It logs an error naming `[cluster] secret`, and the coordinator logs the rejection.

## 4. Oversized and concurrent queries

#258 is the case this section guards against: on a worker with no memory
limit, a large join grew until the OS killed the process.

Since then:
- the partial aggregate runs inside the join's fragment (#282);
- spill is bounded (#280);
- a failed task fails its query at once (#281);
- pgwire cancel works (#278).

The limits below are what keep one query from taking a node with it.

- [ ] **Admission limits fit the team.**
  - Size `[admission] max_running_queries` (default 8) and `max_queued_queries` (default 256) to the workload.
  - `0` means unlimited, which is risky without the memory limits below.
  - See [Admission Control](/guide/configuration#admission-control).
- [ ] **A per-query memory limit is set.**
  - Set `[memory] query_max_memory_per_node` (`ARNEB_QUERY_MAX_MEMORY_BYTES`). It is unset by default.
  - A query over the limit fails with `53200` (`EXCEEDED_LOCAL_MEMORY_LIMIT`) and the others keep running.
  - The limit is per node, not cluster-wide. Start around 0.5-0.8 × the node's spill budget.
- [ ] **The memory pool is bounded.**
  - In containers the pool follows the cgroup limit × `cgroup_ratio` (default 0.5).
  - On bare hosts, set `[memory] spill_budget_bytes`; otherwise the pool is unbounded.
  - Confirm with the startup line `memory pool installed … spill_budget_source=…`. It should not say `unbounded`.
- [ ] **Spill has its own disk and a cap.**
  - Point `TMPDIR` at a volume sized for spill.
  - `ARNEB_SPILL_MIN_FREE_BYTES` (default 1 GiB) keeps spill from filling the disk.
  - `ARNEB_SPILL_MAX_BYTES` caps one node's spill and has no default limit.
- [ ] **A statement timeout is set.**
  - The server-wide `[admission] statement_timeout` is off by default.
  - Clients can still set their own: `SET statement_timeout` on pgwire, `query_max_execution_time` on Trino.
- [ ] **Container memory limits are set on every pod.** The chart's `resources` default is empty, which leaves the pool unbounded on Kubernetes too.

**Check:**
- Run a query that needs more than the per-query limit, such as a `COUNT(DISTINCT ...)` over the largest table. It should fail with `53200` while a concurrent small query succeeds.
- `arneb_memory_pool_limit_bytes` appears in `/metrics`.

## 5. A lost worker

- [ ] **At least two workers.** The chart deploys two, with a PodDisruptionBudget that allows one disruption.
- [ ] **Heartbeat settings are understood.**
  - Workers heartbeat every 2 s (`heartbeat_interval_secs`). One missing for 10 s (`heartbeat_timeout_secs`) is probed before its queries fail, so they fail about 15 s after its last heartbeat.
  - A failed task fails its query at once.
  - See [When a Worker Fails](/guide/distributed#when-a-worker-fails).
- [ ] **Query retry is a deliberate choice.**
  - `retry_policy = "query"` (`ARNEB_RETRY_POLICY`) reruns a failed read-only query up to `query_retry_attempts` times (default 4). It is off by default.
  - A retried query's results are collected in full on the coordinator before the first row reaches the client.
  - Task-level retry (#248) does not exist yet.
  - See [Query retry](/guide/distributed#query-retry).
- [ ] **Drain fits the grace period.**
  - On SIGTERM a worker drains for up to `drain_timeout_secs` (default 120).
  - Kubernetes' `terminationGracePeriodSeconds` must be longer. The chart uses drain + 30 s.
  - See [Stopping a Worker](/guide/distributed#stopping-a-worker).
- [ ] **The coordinator is a single point of failure, and the team knows it.**
  - There is one coordinator. It does not drain, so when it restarts the queries in flight fail.
  - Upgrading to a new release is a short full restart: workers must run the coordinator's version. Schedule upgrades when the cluster is quiet.
  - See [Restarts and upgrades](/guide/kubernetes#restarts-and-upgrades).

**Check:**
- Delete one worker pod during a query. The query fails within about 15 s (or succeeds under `retry_policy = "query"`), and the next query runs on the remaining worker.
- `arneb_worker_up` drops to `0` for that worker.

## 6. Seeing what happens

- [ ] **Prometheus scrapes the coordinator's `/metrics`.**
  - There is a Grafana dashboard in `docker/observability/grafana/dashboards/arneb.json`.
  - Workers have no HTTP listener, so their memory pools are not exported.
  - See [Observability](/guide/observability).
- [ ] **Alerts are set on the metrics that matter:**
  - `arneb_worker_up == 0`
  - `arneb_admission_queued` staying high
  - `arneb_queries_total{outcome="failed"}` rising
  - `arneb_exchange_stream_breaks_total{outcome="failed"}` above zero (see #139 below)
- [ ] **Logs are collected.**
  - The slow-query log (`arneb::slow_query`, threshold `ARNEB_SLOW_QUERY_THRESHOLD_MS`, default 10 s) logs each slow statement with its duration, outcome and SQL.
  - The `arneb::admission`, `arneb::cluster` and `arneb::exchange` targets log queueing, worker state and exchange breaks.
- [ ] **Query history is sized.** `query_history_size` (default 100) sets how many finished queries `system.runtime.queries` keeps.
- [ ] **The audit log persists.**
  - Under password auth, manage and token actions are written to `<state>.audit.jsonl`.
  - With no `[state] path` it is in memory only and lost on restart.
  - See [Audit Log](/guide/configuration#audit-log).

## 7. Deployment

- [ ] **Pin the image version.** Use `ghcr.io/formosa-ventures/arneb:<version>` (amd64 and arm64), not `latest`.
- [ ] **Configuration secrets live in a Secret.** Put `[auth]` users and catalog credentials in `existingConfigSecret`, not in values files.
- [ ] **The coordinator's state is on a volume you keep.**
  - Under password auth, users, tokens, runtime catalogs, rules and admission overrides live in the state file (`[state] path`).
  - Use `coordinator.state.persistence.existingClaim`, because `helm uninstall` deletes a PVC the chart created.
  - See [Coordinator state](/guide/kubernetes#coordinator-state).
- [ ] **`helm upgrade` keeps the cluster secret.** The kind CI job checks that it does. With a secret you manage yourself, keep it in the Secret the chart reads.

## 8. Clients

- [ ] **Your tools are on the tested list.**
  - Every PR runs psycopg2, psycopg 3, trino-python-client, the SQLAlchemy Trino dialect (Superset's path), dbt-trino, the SQLAlchemy PostgreSQL dialect and dbt-postgres against a 1+2 cluster.
  - Metabase over pgwire was checked by hand, and CI replays its statements.
  - Superset itself, Metabase's Trino driver and Trino JDBC have not been run against Arneb.
  - See [Checked in CI](/guide/clients#checked-in-ci).
- [ ] **The known gaps are acceptable.**
  - The gaps are listed in [Known gaps](/guide/clients#known-gaps), for example ISO day of week, one-part intervals only, and Rust regular-expression syntax.
  - dbt models must be ephemeral, because Arneb does not write to catalogs.
- [ ] **Your tools work with your security settings.** CI runs the client checks on a cluster with no auth and no TLS. Connect each tool the team uses to a staging cluster with your `[auth]` and `[tls]` settings.

## 9. Agent interface

Skip this section if nobody uses the HTTP API or MCP.

- [ ] **MCP is off unless it is used.**
  - Set `[mcp] enabled = false`.
  - When it is used, keep `allowed_hosts` to the names clients use and `allowed_origins` unset unless a browser client needs it.
  - See [MCP](/guide/mcp#security).
- [ ] **Tokens expire.**
  - Create tokens with `expires_in_days`; without it a token never expires.
  - There is no automatic rotation, so reissue and revoke on a schedule.
  - Revoking a token does not close pgwire connections already open with it.
  - See [API tokens](/guide/http-api#api-tokens).
- [ ] **Agent API limits fit the memory budget.** `[agent_api]` caps rows, page bytes and retained result bytes (64 MiB per statement, 256 MiB per user, 1 GiB total by default).
- [ ] **OAuth for MCP goes through a TLS ingress.** `[oauth] issuer` turns it on. It needs password auth and MCP, and works on `/mcp` only. See [OAuth for MCP](/guide/kubernetes#oauth-for-mcp).

The headless acceptance scenario has run on a kind cluster every night and on
relevant PRs since v0.2.1 (`.github/workflows/acceptance.yml`). It covers
password auth, the agent API, MCP with Keycloak OAuth, the manage API,
runtime config, a coordinator restart and a worker delete.

## 10. Known limitations

These are open issues, not configuration. Weigh them against the workload
before relying on the cluster.

- **Exchange stream breaks under concurrency (#139).**
  - With about 20 concurrent SF10 queries on a local cluster, roughly one query per run failed with an h2 `BrokenPipe` or `ConnectionReset` on a worker-to-worker stream.
  - No break has reproduced since the instrumentation landed.
  - A broken broadcast stream is read again from the first missing row; a broken partitioned stream fails its query.
  - Watch `arneb_exchange_stream_breaks_total`.
- **Memory outside the pool (#247).** Some memory is still not reserved against the pool.
  - At SF30, TPC-DS q04, q11 and q78 get 12 GiB workers OOM-killed while the pool counts 2-3 GB.
  - The growth in #258 most likely came from the same place, on the Hive + S3 scan path.
  - Size container limits with headroom well above the pool.
- **Aggregate spill is partial (#316, #317).** Aggregate spill is off unless `ARNEB_AGG_SPILL=1` is set. When it is on:
  - Only an aggregate whose input arrives as a single partition can spill. An aggregate over several input partitions, the usual case on a cluster, fails with `53200` when the pool is full, as it does with spill off.
  - Groups admitted before memory ran short stay in memory while the rest spill. If their state keeps growing (`MIN`/`MAX` over long strings, `ARRAY_AGG`, percentiles), the query can still run out of memory (#316).
  - A spilling aggregate's 64 spill writers hold about 37 MB of buffers outside the pool (#317). That is small next to a multi-GiB pool, but it adds up when many queries spill at once.
- **A small memory pool can fail a query with an exchange error (#325).** With a 64 MiB pool on a local SF1 cluster, TPC-H q21 fails with `partition 0 already consumed` instead of spilling or failing with `53200`. Pools sized for production are far larger, but treat this error as a sign that the pool is too small.
- **No task-level retry (#248).** A query that loses a task fails, or reruns from the start under `retry_policy = "query"`.
- **PostgreSQL aggregate pushdown has no switch.** A supported aggregate over one PostgreSQL table runs in PostgreSQL, with its `work_mem` and CPU (see [Aggregate pushdown](/connectors/postgresql#aggregate-pushdown)). A `GROUP BY` on a high-cardinality column moves the whole aggregation into that database, and no catalog option turns this off. Before attaching a production database, check that it can take those queries, or point the catalog at a replica.
- **Broadcast joins on one core (#283).** A broadcast hash join uses about one core of a multi-core worker, where a hash-partitioned plan uses several.
- **Large-scale validation.** TPC-H is cell-identical to Trino at SF10 and SF30, but the memory validation at SF30 is still open, and q18 at SF30 can still run out of memory.
- **Results are materialized for Trino clients.** A Trino-protocol query's result is collected in full before the first page is served, so very large results cost coordinator memory.

## Before go-live

- [ ] Sections 1-8 are checked on the cluster that will serve the team, not only on staging.
- [ ] Each BI tool the team uses connects to that cluster and runs its usual queries.
- [ ] `benchmarks/tpch/scripts/concurrency_soak.sh 5` passes on a staging cluster with the same limits and TPC-H data, so that 20 queries at once queue instead of failing.
- [ ] A worker pod delete during a query behaves as section 5 describes.
- [ ] Prometheus shows the cluster, and an alert fires when a worker is stopped.
