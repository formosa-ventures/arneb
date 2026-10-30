# Configuration

Arneb loads configuration from three sources with the following precedence (highest wins):

1. **CLI arguments** (`--port`, `--config`, `--role`)
2. **Environment variables** (`ARNEB_PORT`, `ARNEB_BIND_ADDRESS`, etc.)
3. **Configuration file** (`arneb.toml`)
4. **Built-in defaults**

## Configuration File

By default, Arneb looks for `arneb.toml` in the current directory. Specify a different path with:

```bash
cargo run --bin arneb -- --config /path/to/config.toml
```

## Server Settings

| Field | Type | Default | Env Var | Description |
|-------|------|---------|---------|-------------|
| `bind_address` | string | `"127.0.0.1"` | `ARNEB_BIND_ADDRESS` | Address to bind the server to |
| `port` | integer | `5432` | `ARNEB_PORT` | PostgreSQL wire protocol port |
| `max_worker_threads` | integer | (CPU count) | `ARNEB_MAX_WORKER_THREADS` | Maximum worker threads for query execution |
| `max_memory_mb` | integer | (system dependent) | `ARNEB_MAX_MEMORY_MB` | Maximum memory in MB |

### Ports

| Service | Port | Roles |
|---------|------|-------|
| pgwire (PostgreSQL protocol) | `port` | standalone, coordinator |
| Trino client protocol (HTTP, or HTTPS with [`[tls]`](#tls)) | `[trino] port` (default `8080`) | standalone, coordinator |
| Web UI (binds to loopback under password auth, see [Web UI](#web-ui)) | `port + 1000` | standalone, coordinator |
| Flight RPC | `9090` | all roles |

## Trino Client Protocol

| Field | Type | Default | Env Var | CLI | Description |
|-------|------|---------|---------|-----|-------------|
| `[trino] enabled` | bool | `true` | `ARNEB_TRINO_ENABLED` | `--no-trino` | Serve the Trino client REST protocol |
| `[trino] port` | integer | `8080` | `ARNEB_TRINO_PORT` | `--trino-port` | HTTP port of the Trino listener |

See [Trino Client Compatibility](./trino-clients.md) for what the listener supports.

## Tuning Knobs: Build-Time vs Runtime

Arneb separates configuration into two classes. Knowing which is which keeps
builds reproducible and avoids silent misconfiguration.

**Runtime-tunable** — anything that can change without recompiling (per-node
memory budget, parallelism, log level, allocator decay). These are exposed as
`ARNEB_*` environment variables / `arneb.toml` fields, follow the precedence
above, and the effective value is logged at startup. Override freely per
deployment or experiment.

**Build-time** — anything that can only be decided at compile/link time (Cargo
features, codegen flags). These live in version-controlled build config
(`Cargo.toml`, `.cargo/config.toml`) and are changed in source, **never** via an
environment variable. A binary's behaviour must be a pure function of its
committed inputs.

Two rules follow:

- **Never override a build-time parameter with an environment variable.** To
  experiment with a build-time setting, use an explicit `cargo --features` /
  `--profile` invocation, then commit the chosen default.
- **Never rely on a third-party allocator/runtime's own environment variable**
  (e.g. jemalloc's `MALLOC_CONF` / `_RJEM_MALLOC_CONF`). Those are silent on
  typos and prefix mismatches. Every runtime knob goes through an `ARNEB_*`
  variable that Arneb reads, applies, and logs — so a wrong value is visible,
  not silently ignored.

### Memory / Allocator Tuning

Arneb uses jemalloc and returns freed pages to the OS promptly so the cgroup
memory peak reflects the engine's true working set, not allocator history. The
page-decay interval is **runtime-tunable** and set in-binary at startup (default
shown), with the effective value logged.

| Knob | Default | Env Var | Description |
|------|---------|---------|-------------|
| dirty/muzzy page decay | `500` ms | `ARNEB_DIRTY_DECAY_MS` | How long jemalloc holds freed pages before `madvise`-ing them back to the OS. Lower = tighter RSS but more page re-faults; `0` returns immediately (slowest); higher lets RSS drift up. `500` ms is the measured sweet spot. Applied via `mallctl` — do **not** set `MALLOC_CONF`, it is ignored. |
| spill budget | config / cgroup | `ARNEB_SPILL_BUDGET_BYTES` | Per-node budget (bytes) a spillable operator (SemiJoin/HashJoin build) reserves against before spilling to disk. Overrides the `[memory] spill_budget_bytes` config field. Exists as an env var because the bench config is COPYed into the docker image at build time — the env override retunes without an image rebuild. |
| query memory limit | config | `ARNEB_QUERY_MAX_MEMORY_BYTES` | Per-query limit on each node (bytes), see [Admission control](#admission-control). Overrides `[memory] query_max_memory_per_node`. |

The startup log line `memory pool installed … spill_budget_bytes=… spill_budget_source=…`
confirms the resolved value and its source (`env` / `config` / `cgroup_v2` / `cgroup_v1`
/ `unbounded`); a `source=env` line confirms the override took effect.

### Query Engine Levers

Every planner/executor optimisation that has been validated at TPC-H SF30
(cell-identical to Trino, lower latency and peak memory) ships **on by default**.
The `ARNEB_*` variable exists only as a kill switch for A/B experiments; the
effective value is logged at startup on the `arneb::config` target. Nothing in
`docker/arneb-bench/docker-compose.bench.yml` needs to set these — the bench
runs the in-binary defaults.

| Knob | Default | Disable with | What it does |
|------|---------|--------------|--------------|
| Grace hash join | on | `ARNEB_GRACE_HJ=0` | An inner join without a residual predicate that has to spill partitions both sides by hash instead of re-scanning the probe side once per build chunk. |
| Selective-dim-first join order | on | `ARNEB_SELECTIVE_DIM_FIRST=0` | Join-order scoring charges each left-deep intermediate at full cardinality, so filtered dimensions join before the fact table. |
| Eager aggregation | on | `ARNEB_EAGER_AGG=0` | Pre-aggregates below a join when the group keys are the join keys (q13: orders 45M → 4.5M rows before the join). |
| Parallel final aggregate | on | `ARNEB_PARALLEL_FINAL_AGG=0` | Hash-partitions the final aggregate across workers instead of a single-partition merge. |
| Aggregate pre-size | on | `ARNEB_AGG_PRESIZE=0` | Sizes hash-aggregate tables from the planner's group estimate (capped) to avoid rehash cascades. |
| Adaptive aggregate pre-size | on | `ARNEB_AGG_PRESIZE_ADAPTIVE=0` | Exact final-from-partials sizing plus data-driven partial growth for high-cardinality `GROUP BY`. |
| Data-parallel scan | on | `ARNEB_DATA_PARALLEL_SCAN=0` | Splits a fact-table scan across all workers (M×N fan-out) instead of one worker per table. |
| Broadcast build-side swap | on | `ARNEB_BROADCAST_BUILD_SIDE_SWAP=0` | Builds the estimated-smaller side of a broadcast join and streams the larger side. |
| Broadcast build cap | `1000000000` bytes | `ARNEB_BROADCAST_MAX_BUILD_BYTES=<bytes>` (`0` disables broadcast) | Largest estimated build side that is broadcast rather than hash-partitioned. |
| Scan batch size | `16384` rows | `ARNEB_SCAN_BATCH_SIZE=<rows>` | Arrow batch size for file and Hive scans; larger amortises per-batch overhead at the cost of in-flight memory. |
| Probe prefetch | on | `ARNEB_PROBE_PREFETCH=0` | Hash-join probe prefetches the bucket a few rows ahead (CPU hint, no effect on results). |
| Aggregate prefetch | on | `ARNEB_AGG_PREFETCH=0` | Same prefetch hint for hash-aggregate lookups. |
| Compact semi/anti `<>` build | on | `ARNEB_COMPACT_SEMI_NE=0` | Semi/anti joins with a `<>` residual keep a per-key `{distinct, first value}` map instead of the full build rows (q21: no build spill). |
| Hash-partitioned semi/anti joins | on | `ARNEB_PARTITIONED_SEMI_JOIN=0` | Hash-partitions both sides of a `SemiJoin`/`AntiJoin` on the join key across workers instead of running it on one worker that receives the whole probe and build (SF10 warm: q21 −21%, q04 −22%, q22 −20%). |
| Probe-side semi/anti filters | on | `ARNEB_SEMI_PROBE_FILTER=0` | On a partitioned semi/anti join whose build side is estimated at ≥ 20M rows and ≥ 4× the probe side, and whose probe side scans ≤ 100M rows, each join task drains the probe side first and publishes its key set (Bloom above 20K keys) to the build-side scan, which drops non-matching rows before the shuffle. If the set misses the wait it is applied to the scan output once it arrives. Needs partitioned semi/anti joins (SF10 warm: q21 −51%, q04 −23%). |
| Spill compression | `lz4` | `ARNEB_SPILL_COMPRESSION=0` (`zstd` for Zstandard) | Arrow IPC compression for exchange/build spill files. |
| CTE self-aggregate window | on | `ARNEB_CTE_SELF_AGG_WINDOW=0` | A CTE referenced both directly and in a `col >= (SELECT MAX(col) FROM cte)` subquery is computed once as a window (q15). |

`ARNEB_NARROW_KEYS` is deliberately **not** a default: its column allowlist is a
TPC-H-specific stopgap for the Hive connector's missing plan-time min/max stats.

## Admission Control

The coordinator (or standalone node) caps how many queries run at once,
queues the rest, and can limit each query's memory and run time.

```toml
[admission]
max_running_queries = 8      # ARNEB_MAX_RUNNING_QUERIES; 0 = unlimited
max_queued_queries  = 256    # ARNEB_MAX_QUEUED_QUERIES
statement_timeout   = "0"    # ARNEB_STATEMENT_TIMEOUT; "30s", "5min", 30000 (ms); 0 = off

[memory]
query_max_memory_per_node = 4294967296   # ARNEB_QUERY_MAX_MEMORY_BYTES; unset = no per-query limit
```

| Setting | Default | Description |
|---|---|---|
| `max_running_queries` | `8` | Statements that execute a plan at the same time, across pgwire and the Trino listener. `0` disables the cap. |
| `max_queued_queries` | `256` | Statements waiting for a slot. Beyond that a statement fails at once with SQLSTATE `53000` (pgwire) or `QUERY_QUEUE_FULL` (Trino). |
| `statement_timeout` | `0` (off) | Server default for sessions that set no timeout of their own; it counts queue time. |
| `query_max_memory_per_node` | unset | Each query's memory limit **on each node**. |

**Queue.** A statement takes a slot after session and metadata handling and
before planning, so a queued query holds no plan, memory or workers. Slots
go to waiters in arrival order. Only statements that execute a plan queue
(queries, `INSERT`, `DELETE`, `CREATE TABLE AS`, `EXPLAIN ANALYZE`); `SET`,
`SHOW`, metadata queries, catalog DDL and plain `EXPLAIN` never wait. A Trino
query reports `QUEUED` while it waits. The counters are on
`/api/v1/cluster` (`running_queries`, `queued_queries`,
`max_running_queries`, `max_queued_queries`) and the Web UI dashboard, and
`arneb::admission` logs each queued and admitted query (with its position
and wait time). Workers have no queue; the coordinator's cap bounds what
reaches them.

**Memory limit.** With `query_max_memory_per_node` set, every query gets its
own budget of that many bytes on each node it runs on (per statement on the
coordinator, per query on each worker), on top of the node's spill budget.
A query that exceeds it fails with SQLSTATE `53200` (pgwire) or
`EXCEEDED_LOCAL_MEMORY_LIMIT` (Trino), and the other queries on that node
keep running. The limit is per node, not cluster-wide: a query on two
workers can use up to twice the limit in total, and a skewed query hits it
first on the node with the most data. Start around 0.5–0.8 × the node's
spill budget.

**Timeouts and cancellation.** pgwire sessions use `SET statement_timeout`,
and Trino sessions use the `query_max_execution_time` (excludes queue time)
and `query_max_run_time` (includes it) session properties; see
[SQL Overview](/sql/overview#session-settings) and
[Trino Clients](/guide/trino-clients#time-limits-and-queuing). A query that
times out, is cancelled (Trino `DELETE`), fails in one fragment or loses its
client is cancelled on every worker: its tasks stop and their memory and
output buffers are released. `arneb::cluster` logs `cancelling query on
workers` on the coordinator and `query cancelled on worker` on each worker.

The effective values are logged at startup on `arneb::config`. Set
`max_running_queries = 0` and leave the other two unset to turn admission
off.

## Query History

The coordinator, or a standalone server, keeps every running query and the
most recent finished ones. That covers every statement from pgwire or the
Trino protocol that reaches the engine. Session commands and metadata queries
are not kept. `[observability] query_history_size` (`ARNEB_QUERY_HISTORY_SIZE`)
sets how many finished queries are kept (default `100`; must be positive).

Query them as a table, over either protocol:

```sql
SELECT query_id, state, "user", source, elapsed_time_ms, rows, error_message
FROM system.runtime.queries
WHERE state <> 'FINISHED'
ORDER BY created DESC;
```

| Column | Meaning |
|---|---|
| `query_id`, `query` | Id and SQL text |
| `state` | `QUEUED`, `RUNNING`, `FINISHED` or `FAILED`. As in Trino, a cancelled query (the client went away) is `FAILED`, with `Query was canceled` as its error |
| `user`, `source` | Who submitted it, and over `pgwire` or `trino` |
| `queued_time_ms`, `elapsed_time_ms` | Time waiting for admission, and total time (so far, if still running) |
| `rows` | Rows returned, once finished |
| `created`, `started`, `end` | Timestamps (UTC) |
| `error_message` | Why it failed, including statement timeouts and admission refusals |

The Web UI's query list (`/api/v1/queries`) shows the same history.
`system.runtime.queries` is a snapshot taken when the statement starts, and
it cannot be joined with other catalogs' tables.

## Table Registration

Register tables directly in the config file:

```toml
[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[[tables]]
name = "orders"
path = "/data/orders.csv"
format = "csv"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Table name used in SQL queries |
| `path` | string | yes | File path (local or remote `s3://`, `gs://`, `az://`) |
| `format` | string | yes | File format: `"parquet"` or `"csv"` |

## Object Store Configuration

### S3

```toml
[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"   # For MinIO/LocalStack; omit for AWS
allow_http = true                     # Required when endpoint uses HTTP
# access_key_id = "s3admin"       # Optional: falls back to env/IAM
# secret_access_key = "s3adminsecret"
```

Credential precedence: config file → `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars → IAM role / instance profile.

### GCS

```toml
[storage.gcs]
service_account_path = "/path/to/service-account.json"
```

## Catalog Configuration

Register external catalogs: a Hive Metastore (its Iceberg tables are read through the same catalog) or a PostgreSQL database:

```toml
[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"
default_schema = "default"

# Per-catalog storage override (merges with global [storage])
[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Catalog name (used as first part of `catalog.schema.table`) |
| `type` | string | yes | `"hive"` (Iceberg tables in the metastore are included, see [Iceberg](/connectors/iceberg)) or `"postgresql"` |
| `metastore_uri` | string | hive | `host:port` of the Hive Metastore (no scheme prefix) |
| `default_schema` | string | no | Default schema within the catalog (`default`; `public` for PostgreSQL) |

A PostgreSQL catalog takes `connection_url`, `user`, `password` and the pool/split
knobs `max_connections`, `split_min_rows`, `max_splits`,
`unconstrained_numeric_scale`, `connect_timeout_secs`,
`pool_wait_timeout_secs` and `domain_compaction_threshold` (dynamic filters
longer than this are sent as a range). `ARNEB_CATALOG_<NAME>_PASSWORD`
overrides the password. See [PostgreSQL](/connectors/postgresql):

```toml
[[catalogs]]
name = "pg"
type = "postgresql"
connection_url = "postgresql://127.0.0.1:5433/arneb"
user = "arneb"
password = "arneb"
```

## Cluster Configuration

For distributed mode (worker nodes):

```toml
[cluster]
rpc_port = 9091
coordinator_address = "127.0.0.1:9090"
worker_id = "worker-1"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `rpc_port` | integer | yes | Flight RPC port for this worker |
| `coordinator_address` | string | yes | `host:port` of the coordinator's Flight RPC (`ARNEB_CLUSTER_COORDINATOR_ADDRESS`) |
| `worker_id` | string | no | Unique identifier for this worker (`ARNEB_CLUSTER_WORKER_ID`; a random `worker-<uuid>` when unset) |
| `advertised_address` | string | no | `host:port` peers dial to reach this worker (`ARNEB_CLUSTER_ADVERTISED_ADDRESS`; defaults to `bind_address:rpc_port`, which only works on one host) |
| `secret` | string | no | Shared cluster secret, see [Cluster security](#cluster-security) |
| `heartbeat_interval_secs` | integer | no | Seconds between a worker's heartbeats (default `2`, `ARNEB_HEARTBEAT_INTERVAL_SECS`). Read on workers. |
| `heartbeat_timeout_secs` | integer | no | Seconds without a heartbeat before the coordinator treats a worker as lost and fails its running queries (default `10`, `ARNEB_HEARTBEAT_TIMEOUT_SECS`). Read on the coordinator; must be greater than the interval. |
| `drain_timeout_secs` | integer | no | Longest a worker waits on SIGTERM for its running queries to finish before it exits (default `120`, `ARNEB_DRAIN_TIMEOUT_SECS`). Read on workers; set the orchestrator's grace period (Kubernetes `terminationGracePeriodSeconds`) above it. See [Stopping a worker](/guide/distributed#stopping-a-worker). |
| `retry_policy` | string | no | `none` (default) or `query`: re-run a read-only query that failed because one of its workers was lost, on the remaining workers (`ARNEB_RETRY_POLICY`). Read on the coordinator. `task` is refused. See [When a worker fails](/guide/distributed#when-a-worker-fails). |
| `query_retry_attempts` | integer | no | Most re-runs of one query under `retry_policy = "query"` (default `4`, at least `1`, `ARNEB_QUERY_RETRY_ATTEMPTS`). Read on the coordinator. |

The three `ARNEB_CLUSTER_*` variables above override the file (env > file),
so one config file can serve every worker; the Helm chart sets them per
pod. An empty value is an error.

See [Distributed Mode](/guide/distributed) for full setup instructions.

### Cluster security

By default the Flight RPC port accepts any peer: anyone who can reach it can
register a worker, submit tasks, or read another task's output. Two
independent settings close that, and you should use both:

```toml
[cluster]
secret = "…"                          # >= 32 characters; prefer ARNEB_CLUSTER_SECRET

[cluster.tls]
cert_path = "/etc/arneb/node.crt"     # this node's certificate (PEM)
key_path  = "/etc/arneb/node.key"     # its private key (PEM)
ca_path   = "/etc/arneb/ca.crt"       # CA that signed every node's certificate
```

| Field | Env var | Description |
|-------|---------|-------------|
| `[cluster] secret` | `ARNEB_CLUSTER_SECRET` | Every Flight call carries `authorization: Bearer <secret>`; every node checks it before any handler runs. Minimum 32 characters, printable ASCII. |
| `[cluster.tls] cert_path` | `ARNEB_CLUSTER_TLS_CERT_PATH` | This node's certificate chain |
| `[cluster.tls] key_path` | `ARNEB_CLUSTER_TLS_KEY_PATH` | This node's private key |
| `[cluster.tls] ca_path` | `ARNEB_CLUSTER_TLS_CA_PATH` | The cluster CA; peers must present a certificate it signed |

- **Secret.** A call with a missing or wrong secret gets `UNAUTHENTICATED`,
  so a worker with the wrong secret is never registered. The coordinator
  logs a WARN on `arneb::cluster` (at most once per peer per minute), and
  the worker logs an ERROR naming `[cluster] secret` on every heartbeat and
  keeps retrying, so fixing the secret recovers without restarting it. The
  secret never appears in logs or `Debug` output; prefer the environment
  variable (for example from a Kubernetes Secret) to keep it out of files.
- **TLS.** Flight servers serve TLS with the node certificate, and clients
  verify peers against `ca_path`. Peer addresses in `coordinator_address` /
  `advertised_address` stay `host:port`; they are upgraded to `https://`
  automatically. Every node must agree: a plaintext node cannot talk to a
  TLS node. The certificate must name the host peers dial (for example the
  `advertised_address` host).
- **Without TLS, the secret crosses the network in the clear.** Startup logs
  a WARN when a secret is set without `[cluster.tls]` and `bind_address` is
  not loopback.
- The effective mode is logged on `arneb::config`
  (`cluster_auth="secret"|"off" cluster_tls="on"|"off"`). A short secret, a
  missing `[cluster.tls]` path, or an unreadable file stops startup.

## Authentication

`[auth]` is the server's client authentication: one credential store for
both client-facing listeners. pgwire runs a SCRAM-SHA-256 exchange, and the
Trino listener checks HTTP Basic credentials against the same
`[[auth.users]]` (see [Trino clients](#trino-listener-under-password-auth)).

By default every connection is accepted without a password
(`type = "none"`), so existing setups keep working. To require passwords,
add an `[auth]` section:

```toml
[auth]
type = "password"   # "none" (default) | "password"

[[auth.users]]
name = "alice"
password_hash = "SCRAM-SHA-256$4096:Mzusu25I7rTxxYUaRIIPag==$qQmyyOdRet5ULfbhSwiX8OP4QInk+Monhm0zSkWcgGs=:SgOFTmuXsLnO/AHKLu3a+BAiJ0Yx3JcIj/wNsJX1MxY="
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | no | `"none"` (default) or `"password"` |
| `users[].name` | string | yes | Login name, matched against the client's `user` |
| `users[].password_hash` | string | yes | SCRAM-SHA-256 verifier (see below) |

`password` mode uses **SCRAM-SHA-256**, the same mechanism as PostgreSQL's
`password_encryption = scram-sha-256`. psql, JDBC, psycopg2/3, DBeaver,
tokio-postgres and other modern clients support it. The password never
crosses the network, and the config file holds only a salted verifier in
PostgreSQL's `pg_authid.rolpassword` format, not the password. A verifier
copied from PostgreSQL (`SELECT rolpassword FROM pg_authid WHERE rolname = '…'`)
works as-is. To generate one:

```bash
echo -n 'my-password' | arneb hash-password     # or run it and type at the prompt
echo -n 'my-password' | arneb hash-password --iterations 100000
```

`--iterations` sets the PBKDF2 iteration count stored in the verifier. The
default, 4096, matches PostgreSQL. For production, use a much higher count
(for example 100000 or more) to slow down offline guessing if the config
file leaks. The cost is paid by the client on every connection, so measure
connection latency for your clients before going very high.

Behavior:

- A wrong password or an unknown user is rejected with
  `FATAL 28P01 password authentication failed for user "…"`. Unknown users
  get the same challenge as real ones, so an attacker can't tell which user
  names exist.
- One startup handler serves both the Simple and Extended Query protocols,
  so every client path gets the same check.
- The effective mode is logged at startup on the `arneb::config` target
  (`pgwire authentication effective mode auth="password (scram-sha-256)" users=N`).
- Invalid `[auth]` config stops startup. This covers an unknown `type`, a
  malformed hash, duplicate or empty user names, `type = "password"` with no
  users, and a plaintext `password` key.

<a id="trino-listener-under-password-auth"></a>
**Trino listener under password auth.** Every endpoint except the
`/v1/info`, `/v1/info/state` and `/v1/status` health checks requires
`Authorization: Basic`, verified against the same verifiers in constant
time. A wrong password, an unknown user or a missing header gets `401` with
`WWW-Authenticate: Basic realm="arneb"`. The authenticated user is the
query's user: any non-blank `X-Trino-User` / `X-Presto-User` naming
someone else gets `403`, and a query's paging, cancel and info URIs only
work for its owner. Successful checks are cached for 5 minutes, so a
changed password keeps working until the cache entry expires or the server
restarts. Uncached checks run off the request threads, at most four at a
time; further ones wait their turn.

**Fail closed.** Trino clients refuse to send a password over plain HTTP, so
`type = "password"` with the Trino listener enabled and no [`[tls]`](#tls)
**stops startup** with an error naming both fixes: configure `[tls]`, or
disable the listener with `--no-trino` / `ARNEB_TRINO_ENABLED=false`.

<a id="auth-limitations"></a>
Limitations:

- The **Web UI and Web API** need a sign-in under password auth, and bind to
  `127.0.0.1` by default; see [Web UI](#web-ui) and [HTTP API](/guide/http-api).
- The **Flight RPC** port is protected by [Cluster security](#cluster-security),
  not by `[auth]`.
- Channel binding (`SCRAM-SHA-256-PLUS`) and client-certificate
  authentication are not supported. Per-user permissions are set with
  [access control](#access-control).

## Access Control

```toml
[access_control]
rules_file = "/etc/arneb/rules.json"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rules_file` | path | unset | Rules in Trino's file-based access control format (`ARNEB_ACCESS_CONTROL_RULES_FILE`). Read at startup on the coordinator and standalone server; an unreadable or unsupported file stops startup. Unset: every user reads every table. |

See [Access Control](/guide/access-control) for the rule format.

## TLS

`[tls]` turns on TLS for both client-facing listeners with one
certificate:

```toml
[tls]
cert_path = "/etc/arneb/server.crt"   # PEM chain, leaf first
key_path  = "/etc/arneb/server.key"   # PEM private key (PKCS#8, PKCS#1 or SEC1)
pgwire    = "required"                # "required" (default) | "optional"
```

| Field | Env var | Description |
|-------|---------|-------------|
| `cert_path` | `ARNEB_TLS_CERT_PATH` | Certificate chain. Required. |
| `key_path` | `ARNEB_TLS_KEY_PATH` | Private key matching the certificate. Required. |
| `pgwire` | `ARNEB_TLS_PGWIRE` | `required`: plaintext pgwire connections are rejected with `FATAL 28000 SSL connection is required`. `optional`: both are accepted (for migrations). |

Setting both path variables enables TLS without a `[tls]` section.

- **pgwire** answers `SSLRequest` with `S` and also accepts PostgreSQL 17
  direct TLS, so `sslmode=require` and `verify-full` work. The `required`
  check applies in every `[auth]` mode.
- **Trino** serves **HTTPS only** on the same port (HTTP/1.1 or HTTP/2). A
  plain-HTTP request to it fails the handshake and only that connection is
  closed.
- The certificate is read once at startup; a missing file, an empty chain,
  or a key that does not match the certificate stops startup. Restart to
  pick up a new certificate.
- The effective mode is logged on `arneb::config`
  (`tls_pgwire="required"` / `tls="off"`, and `tls_trino="https"|"http"`).

For local testing, `./scripts/dev-certs.sh` writes a throwaway CA and a
`localhost` certificate to `.dev-certs/`:

```bash
./scripts/dev-certs.sh
psql "host=localhost sslmode=verify-full sslrootcert=.dev-certs/ca.crt user=alice"
trino --server https://localhost:8080 --truststore-path .dev-certs/ca.crt --user alice --password
```

## Web UI

The Web UI and Web API (pgwire port + 1000) show query text and cluster
state, and let a caller cancel queries. Under `[auth] type = "password"`
the Web UI asks you to sign in, and every `/api/v1` and `/api/v2` route
except the health and sign-in routes needs credentials; see
[HTTP API](/guide/http-api). The port also serves Prometheus metrics at
`/metrics` (see [Observability](/guide/observability)), which stays
unauthenticated unless `metrics_auth` is set. Where it listens depends on
`[auth]`:

```toml
[web]
bind_address = "0.0.0.0"   # optional; ARNEB_WEB_BIND_ADDRESS
metrics_auth = false       # optional; ARNEB_WEB_METRICS_AUTH
```

| `[auth] type` | `[web] bind_address` | Web UI binds to |
|---|---|---|
| `none` | unset | the server's `bind_address` (unchanged) |
| `password` | unset | `127.0.0.1` |
| any | set | the configured address |

Startup logs the effective address and the reason (`default`,
`loopback (auth=password)` or `explicit`) on `arneb::config`, and a WARN
when an explicit non-loopback address is used under password auth. In
docker or Kubernetes, set `[web] bind_address` (or reach the Web UI through
a port-forward) if you need it from outside the container.

`metrics_auth = true` makes `/metrics` require the same credentials as the
rest of the Web API (a bearer API token or Basic), so a scraper needs a
token. It has no effect under `type = "none"`. The default keeps `/metrics`
open so existing scrapers keep working.

**Upgrading.** Under `[auth] type = "password"`, `/api/v1/*` (except
`/api/v1/info`) now needs credentials, and the Web UI asks you to sign in.
`/api/v1` query list, detail and cancel now apply the `queries` rules from
[access control](/guide/access-control). Scrapers of `/api/v1` need a token
(or Basic). Under `type = "none"` nothing changes.

## State

```toml
[state]
path = "/var/lib/arneb/state.json"   # optional; ARNEB_STATE_PATH
```

The coordinator (or standalone server) keeps its users and
[API tokens](/guide/http-api#api-tokens) in one JSON state file. Workers never
open it. Every write is atomic (write, fsync, rename) and the file is
created with mode `0600`. The path's directory must be writable.

- **The config file is a seed.** `[[auth.users]]` seeds the state on first
  start. Afterwards the state wins: if the config's users later differ from
  the state, startup logs a WARN on `arneb::config` naming the user and keeps
  the state. Start once with `--reseed-state` to overwrite the state's
  config-seeded users with the config.
- **No `path`** keeps the state in memory. Startup logs a WARN, `no [state]
  path: API tokens are lost on restart`, and the token API says so when it
  issues a token.
- The effective path (and `--reseed-state`) is logged on `arneb::config`.
- The state store is only used under `[auth] type = "password"`. Under
  `none`, `[state]` and `--reseed-state` have no effect (`--reseed-state`
  logs an INFO line).
- On Kubernetes the chart mounts a volume for it; see
  [Kubernetes](/guide/kubernetes#coordinator-state).

## Observability

```toml
[observability]
slow_query_threshold_ms = 10000   # ARNEB_SLOW_QUERY_THRESHOLD_MS; 0 = slow-query log off
query_history_size      = 100     # ARNEB_QUERY_HISTORY_SIZE; finished queries kept, > 0
```

Statements running at least `slow_query_threshold_ms` are logged on
`arneb::slow_query` (see [Observability](/guide/observability));
`query_history_size` bounds the [query history](#query-history). An invalid
value fails startup, and the effective values are logged on `arneb::config`.

## CLI Arguments

```
arneb [OPTIONS] [COMMAND]

Commands:
  hash-password      Read a password from stdin and print a SCRAM-SHA-256
                     verifier for [[auth.users]] password_hash

Options:
  --config <PATH>    Path to configuration file
  --port <PORT>      Override the pgwire port
  --role <ROLE>      Server role: standalone, coordinator, or worker
  --reseed-state     Overwrite config-seeded users in the state file with
                     the [[auth.users]] from the config
```

## Example: Standalone with Local Files

```toml
bind_address = "127.0.0.1"
port = 5432

[[tables]]
name = "lineitem"
path = "/data/tpch/lineitem.parquet"
format = "parquet"

[[tables]]
name = "orders"
path = "/data/tpch/orders.parquet"
format = "parquet"
```

## Example: Distributed with Hive Catalog

```toml
bind_address = "0.0.0.0"
port = 5432

[storage.s3]
region = "us-east-1"
endpoint = "http://s3:9000"
allow_http = true

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "hive-metastore:9083"
default_schema = "default"

[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://s3:9000"
allow_http = true
```
