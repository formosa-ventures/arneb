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

## Agent Query API

The coordinator (or standalone node) bounds every agent query API response and
wait with these limits. A request value above its ceiling is refused, not
clamped. The effective values are logged once on `arneb::config` at startup.

```toml
[agent_api]
default_max_rows   = 1000
max_rows           = 10000
```

| `[agent_api]` field | Env var | Default | Meaning |
|---|---|---|---|
| `default_max_rows` | `ARNEB_AGENT_DEFAULT_MAX_ROWS` | 1000 | Rows a statement retains when the request names no `max_rows`, and the default listing `limit` |
| `max_rows` | `ARNEB_AGENT_MAX_ROWS` | 10000 | Ceiling for a statement's `max_rows` and a listing's `limit` |
| `default_page_bytes` | `ARNEB_AGENT_DEFAULT_PAGE_BYTES` | 262144 | Bytes per page when the request names no limit |
| `max_page_bytes` | `ARNEB_AGENT_MAX_PAGE_BYTES` | 4194304 | Ceiling for a requested page size |
| `max_cell_bytes` | `ARNEB_AGENT_MAX_CELL_BYTES` | 8192 | Longest cell, in bytes of a string's UTF-8 text (other values: of their JSON); longer cells are cut and end with `…` |
| `max_retained_bytes` | `ARNEB_AGENT_MAX_RETAINED_BYTES` | 67108864 | Arrow memory one statement keeps, while collecting and after; rows past it are dropped (`truncated_by: "max_retained_bytes"`) |
| `max_user_retained_bytes` | `ARNEB_AGENT_MAX_USER_RETAINED_BYTES` | 268435456 | Arrow memory one user's statements (running and ended) keep together, so one user cannot take the whole budget; when short, the user's own ended statements are evicted if that covers the shortfall (fully read first, then least recently used), otherwise a statement keeps what fits (`truncated_by: "user_budget"`) |
| `max_total_retained_bytes` | `ARNEB_AGENT_MAX_TOTAL_RETAINED_BYTES` | 1073741824 | Arrow memory all statements (running and ended) keep together; when short, ended statements are evicted the same way (the user's own first) if that covers the shortfall, otherwise a statement keeps what fits (`truncated_by: "server_budget"`); it frees up as statements are deleted, evicted or reaped |
| `max_wait_ms` | `ARNEB_AGENT_MAX_WAIT_MS` | 10000 | Longest any request waits before answering "still running" |
| `idle_timeout_secs` | `ARNEB_AGENT_IDLE_TIMEOUT_SECS` | 600 | Idle time before a statement is reaped |

All values must be greater than 0, `default_max_rows <= max_rows`,
`default_page_bytes <= max_page_bytes`,
`max_user_retained_bytes <= max_total_retained_bytes` (a `max_user_retained_bytes`
below `max_retained_bytes` is raised to it, with a warning, so a user can
always retain one full statement), and `idle_timeout_secs` must not
be shorter than `max_wait_ms` (a statement being waited on never counts as
idle, so a long-poll cannot outlast it); a bad combination stops startup.

A catalog, schema or table listing returns `default_max_rows` names unless
the request passes `limit`, which must be in `1..=max_rows`.

A query stops producing rows after `max_rows + 1` of them, so `max_rows`
bounds what it emits as well as what it retains; a result cut by `max_rows`
reports `total_rows` as `null` (one cut only by bytes still reports the
count). A page always holds at least one row, so a single wide row can
exceed the page byte limit; each cell is still capped at `max_cell_bytes`.

## MCP

The coordinator (or standalone node) serves a Model Context Protocol endpoint
at `/mcp` on the Web port; see [MCP Server](/guide/mcp). It is on by default
and has no effect on a worker, which has no Web port. The effective values
are logged once on `arneb::config` at startup.

```toml
[mcp]
enabled            = true
allowed_hosts      = ["db.internal", "db.internal:6432"]
allowed_origins    = ["https://app.example:443"]
default_page_bytes = 32768
```

| `[mcp]` field | Env var | Default | Meaning |
|---|---|---|---|
| `enabled` | `ARNEB_MCP_ENABLED` | `true` | Serve `/mcp`. When `false`, `/mcp` answers `404` (without asking for credentials, under any auth mode). |
| `allowed_hosts` | `ARNEB_MCP_ALLOWED_HOSTS` (comma-separated) | unset | `Host` header values `/mcp` accepts, as `host` or `host:port`. Unset means any host under password auth and loopback (`localhost`, `127.0.0.1`, `::1`) under `type = "none"`. A `*` anywhere in the list means any host. |
| `allowed_origins` | `ARNEB_MCP_ALLOWED_ORIGINS` (comma-separated) | unset | Browser `Origin` header values `/mcp` accepts. Each entry needs a scheme: `scheme://host`, `scheme://host:port`, `scheme://host:*` (any port), or `null`. A request without an `Origin` header always passes. Unset refuses every request that carries an `Origin`, so a browser page cannot call `/mcp`. |
| `default_page_bytes` | `ARNEB_MCP_DEFAULT_PAGE_BYTES` | 32768 | Bytes per page of a tool result when the call names no `max_bytes`, including the inline first page of `submit_query`, `get_query_status` and `sample_table`. |

Blank entries in either list are dropped. `allowed_hosts = []` (or a list
of only blanks) is a startup error (omit it for the default, or use
`["*"]`); so is an empty `allowed_origins` or an entry without a scheme.
An empty `ARNEB_MCP_ALLOWED_HOSTS` or `ARNEB_MCP_ALLOWED_ORIGINS` counts as
unset. Env vars
override the file. `default_page_bytes` must be in `1..=` `[agent_api]`
`max_page_bytes`, so a bad value stops startup. Row, wait and cell limits of
the tools are the `[agent_api]` ones above.

## OAuth

`[oauth]` lets `/mcp` accept OAuth 2.0 access tokens from an external OpenID
Connect provider; see [MCP Server: OAuth](/guide/mcp#oauth) for how it works
and for the identity provider checklist. It is startup-only (a restart
applies a change) and needs `[auth] type = "password"` and `[mcp] enabled =
true`; otherwise startup fails naming the setting. Setting `issuer` turns it
on. The effective values are logged once on `arneb::config`.

```toml
[oauth]
issuer     = "https://idp.example/realms/arneb"
resource   = "https://db.example/mcp"
user_claim = "sub"
```

| `[oauth]` field | Env var | Default | Meaning |
|---|---|---|---|
| `issuer` | `ARNEB_OAUTH_ISSUER` | unset | Issuer URL of the IdP; the required `iss`, and the base of the discovery document. Setting it enables OAuth. `https`, or `http` for a loopback host. |
| `resource` | `ARNEB_OAUTH_RESOURCE` | unset | Public URL of `/mcp`; required with `issuer`, must end in `/mcp`, no query or fragment. Published in the metadata document. |
| `audience` | `ARNEB_OAUTH_AUDIENCE` | `resource` | Value `aud` must contain. Any value other than the `resource` URL (a client id, including colon-bearing ones such as `arneb:mcp` and URL client ids, but also `api://…`, `urn:…` and other `https://…` identifiers) needs `required_scopes`, or startup fails: an ID token for a client with that id would carry the same `aud`. |
| `user_claim` | `ARNEB_OAUTH_USER_CLAIM` | `sub` | Claim holding the Arneb user name: 1 to 64 characters of `A-Z a-z 0-9 _ . @ -`. A value outside that set (e.g. Auth0's `auth0\|...` subjects) answers `403`; use a claim that holds a valid name. With `email`, the token's `email_verified` must be `true`. With another claim whose value contains `@` (`sub` included), the token's `email` must equal it and be verified. Otherwise the answer is `403` (see `allow_missing_email_verified`). |
| `jwks_uri` | `ARNEB_OAUTH_JWKS_URI` | from discovery | The IdP's JWKS URL, when its discovery document cannot be used. |
| `required_scopes` | `ARNEB_OAUTH_REQUIRED_SCOPES` (comma-separated) | none | Scopes a token must carry, in `scope` or `scp` (each a space-separated string or an array of strings; a claim of any other shape, such as `null`, a number or `["mcp", 1]`, grants nothing). A valid token without one gets `403` with `error="insufficient_scope"`. |
| `leeway_secs` | `ARNEB_OAUTH_LEEWAY_SECS` | 60 | Clock-skew allowance for `exp` / `nbf`, `0` to `300`. |
| `jwks_cache_secs` | `ARNEB_OAUTH_JWKS_CACHE_SECS` | 3600 | How long fetched signing keys are fresh, at least `60`. From 80% of it they are refetched in the background. |
| `jwks_min_refresh_secs` | `ARNEB_OAUTH_JWKS_MIN_REFRESH_SECS` | 30 | Minimum gap between JWKS fetches (a token with an unknown `kid` triggers one), `1` to `jwks_cache_secs`. |
| `jwks_max_stale_secs` | `ARNEB_OAUTH_JWKS_MAX_STALE_SECS` | 86400 | While refetches fail (IdP down), how long after the last successful fetch its keys keep deciding; at least `jwks_cache_secs`. After that, `/mcp` answers `503` for JWTs. |
| `require_access_token_typ` | `ARNEB_OAUTH_REQUIRE_ACCESS_TOKEN_TYP` | `false` | Accept only tokens whose header `typ` is `at+jwt` (RFC 9068). Off by default because many IdPs send `typ: JWT` on access tokens; ID tokens are refused either way. |
| `allow_missing_email_verified` | `ARNEB_OAUTH_ALLOW_MISSING_EMAIL_VERIFIED` | `false` | Accept an email-address user name (from a claim other than `email`, `sub` included) when the token has no `email` or no `email_verified` claim, for an IdP such as Entra ID that never sends it. `email_verified: false`, and a verified `email` other than the user name, are refused regardless; an unverified `email` that differs (Entra's `upn` beside its `email`) is ignored. Risk: nothing then checks the address in `user_claim`, so if users can choose that claim (a self-service `preferred_username`, a self-registered login in `sub`) they can claim another user's address; turn it on only when the claim is admin-assigned (Entra's `upn`, or an IdP-assigned `sub`). Logged, with a WARN when on. |

An empty env var counts as unset; env vars override the file. The server
reaches the IdP over the network (5 s timeout per request), so its pod needs
a route to the issuer.

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

The effective values are logged at startup on `arneb::config`, with
`admission_source = config|state`. Set `max_running_queries = 0` and leave
the other two unset to turn admission off, unless limits were set at
runtime (see below): then `PUT {"max_running_queries": 0}` instead.

**Runtime changes.** A manager can change `max_running_queries` and
`max_queued_queries` while the server runs, with `PUT /api/v2/admission` or
the MCP `set_admission_limits` tool (see
[HTTP API](/guide/http-api#admission-limits)). The change applies at once:

- lowering the cap below the running count stops nothing; new queries queue
  until fewer than the new cap run;
- raising it, or setting `0` (unlimited), admits queued queries at once, in
  arrival order, up to the new cap;
- a shorter queue keeps the queries already queued, and refuses new ones
  while the queue is at or over the new limit.

The new limits are kept in the state store (`[state] path`) and win over
`[admission]` and the `ARNEB_*` variables at the next start. Each knob whose
config value differs from the stored one logs a WARN on `arneb::config`:

```text
[admission] max_running_queries / ARNEB_MAX_RUNNING_QUERIES = 8 is overridden by the runtime value 4 in the state store (DELETE /api/v2/admission returns to the config)
```

A stored value pins the limits across restarts and config edits: changing
`[admission]` or the `ARNEB_*` variables has no effect on them until the
stored value is gone. To go back to the config, call
[`DELETE /api/v2/admission`](/guide/http-api#admission-limits) (MCP
`reset_admission_limits`): it clears the stored limits and applies the
config values the server started with, at once. `statement_timeout` and
`query_max_memory_per_node` change only at startup.

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

Under `[auth] type = "password"`, `[[catalogs]]` only seeds the catalogs:
the first start copies them into the [state store](#state), and from then on
they are added, changed and dropped through
[`/api/v2/catalog-configs`](/guide/http-api#catalogs) without a restart.
Workers ignore `[[catalogs]]` and receive the catalogs from the coordinator,
together with its default catalog and schema and with its global `[storage]`
merged into each Hive catalog, so a worker needs no catalog or storage
config of its own.

**Default catalog.** Unqualified table names resolve in the default catalog
and its `default_schema` (`public` for PostgreSQL, `default` otherwise). It
is picked at startup from the catalogs the node serves (under password auth,
the state store's): the first `[[catalogs]]` entry when it is served, else
the first served catalog by name, else `file` when `[[tables]]` are set,
else `memory`. So it never names a catalog that is not defined, and a
config edit that the state store does not follow does not move it. The
effective value is logged on `arneb::config` at startup, and workers
receive the same one. It changes only at a restart: dropping the default
catalog at runtime leaves unqualified names failing until then.

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
- **Catalog credentials cross this port.** Workers receive the catalog
  definitions, passwords and storage keys included, from the coordinator
  over Flight. Without a secret, any peer that reaches the coordinator's RPC
  port can fetch them; without TLS they travel in the clear. Without a
  secret, any peer can also push a catalog set to a worker
  (`sync_catalogs`); the worker replaces it with the coordinator's on its
  next heartbeat. So a coordinator without a secret refuses to start while
  a catalog it serves carries a secret, refuses API changes that add one,
  and otherwise starts with a WARN; a worker without a secret WARNs too
  (see [Upgrading to v0.2.0 (runtime config)](#upgrading-to-v020-runtime-config)).
  Use TLS as well whenever a catalog carries credentials.
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
admin_users = ["root"]
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rules_file` | path | unset | Rules in Trino's file-based access control format (`ARNEB_ACCESS_CONTROL_RULES_FILE`). Read at startup on the coordinator and standalone server; an unreadable or unsupported file stops startup. Unset: every user reads every table. |
| `admin_users` | list of strings | `[]` | Users who hold the manage privilege (`ARNEB_ADMIN_USERS`, comma-separated; whitespace is trimmed, blanks are dropped, an empty value counts as unset). Only effective under `[auth] type = "password"`; under `none` a WARN is logged at startup. Names that are not users in the state store are also warned about. |

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

### Upgrading to v0.2.0 (manage API)

- **Write privileges.** `INSERT`, `DELETE` and `OWNERSHIP` are checked
  against the `tables` rules on every protocol, and a `read-only` catalog
  refuses writes (SQLSTATE `42501`, `PERMISSION_DENIED`, `forbidden`). A
  rules file whose `tables` section grants only `SELECT` now refuses DDL and
  DML; without a `tables` section nothing changes. See
  [access control](/guide/access-control#supported-rules).
- **`DELETE /api/v2/queries/{id}` answers `200` with a body** (the
  [manage envelope](/guide/http-api#dry-runs-and-the-response-envelope))
  instead of `204`, and now stops the query on pgwire and Trino too. Clients
  that check for `204` must accept `200`. `DELETE /api/v1/queries/{id}` of a
  query that already ended answers `204`.
- **Observe is gated by `system_information`.** Once the rules file has a
  `system_information` section, the cluster, worker, admission and audit
  views need a rule that allows `read` (or the manage privilege). Without
  the section, or without a rules file, they stay open as before.
- **An admission controller always exists.** It is built even for
  `max_running_queries = 0`, so the admission gauges are now reported at cap
  0 (see [Observability](/guide/observability)).
- **Stored admission limits pin values.** A limit set with `PUT
  /api/v2/admission` is kept in the state file and wins over `[admission]`
  and `ARNEB_MAX_*_QUERIES` at every start, each overridden knob logging a
  WARN. To go back to the config, call `DELETE /api/v2/admission` (MCP
  `reset_admission_limits`), which clears the stored limits and applies the
  config values at once.
- **New state.** The state file gains `cordons` and `settings` (readable by
  older binaries, whose next write drops them), and the coordinator writes
  the [audit log](#audit-log) next to it (`state.json` →
  `state.json.audit.jsonl`). With no `[state] path` these live
  in memory and are lost on restart.
- **Revoking a token with a malformed id is `400 invalid_argument`**, and
  the refusal is not audited.
- **Under `[auth] type = "none"`, a Web API cancel now really stops the
  query.** Before, cancelling a pgwire or Trino query only changed its
  recorded state. Now any peer that can reach the Web port can stop any
  query through `DELETE /api/v1/queries/{id}`, and `/api/v2` and MCP accept
  a claimed `X-Arneb-User` as the query's owner. Do not expose the Web port
  under `none`: bind it to loopback (`[web] bind_address = "127.0.0.1"`) or
  use `[auth] type = "password"`.
- **Under `none`, cancelling another user's query over `/api/v2` or MCP is
  refused** with `403 manage_requires_auth` (it was allowed before). The
  owner (the claimed `X-Arneb-User`) may still cancel it, and `/api/v1`
  still lets any caller cancel any query.
- **Killing another user's query needs a rule or manage.** Under password
  auth with no rules file, or a rules file without a `queries` section, only
  the query's owner or a manager may cancel it (on `/api/v1`, `/api/v2` and
  MCP `cancel_query`). An explicit `queries` rule that allows `kill` still
  grants it; add `{"allow": ["execute", "view", "kill"]}` for the old
  open behavior.
- **`DELETE /api/v1/queries/{id}` under password auth** follows the
  [kill rule](/guide/http-api#cancel-any-query): cancelling another user's
  query is audited, and the call can answer `503 audit_unavailable` (with an
  error body and `Retry-After: 5`) while the audit log is unwritable.
- **Hash fan-out counts only schedulable workers.** Distributed queries size
  their hash partitions from the workers they are scheduled on, not from
  every alive worker. A cordoned, draining or SIGTERM-draining worker no
  longer counts, so queries use fewer partitions while one is out.
- **`/metrics` is outside the observe layer.** A `system_information`
  section does not hide the admission gauges, worker ids, `arneb_worker_up`
  or heartbeat ages that `/metrics` serves. Set
  [`metrics_auth = true`](#web-ui) (and keep the port private) if they must
  not be public.

### Upgrading to v0.2.0 (runtime config)

Under `[auth] type = "password"` the access rules, users, catalogs and the
slow-query threshold can now change while the server runs (see
[Runtime configuration](/guide/http-api#runtime-configuration)). That moves
the source of truth from the config file to the [state store](#state):

- **The state wins for catalogs; rules from the file follow the file.**
  The first start on this version copies `[[catalogs]]` and the
  `rules_file` into the state (an older state file has no seeded copy, so
  this happens once on upgrade). From then on the stored catalogs are used.
  Editing `[[catalogs]]` no longer takes effect at a restart: each config
  catalog that is new, changed or gone relative to its seeded copy logs one
  WARN on `arneb::config` at every start, ending `Preview a re-seed with
  POST /api/v2/state/reseed?dry_run=true`. The access rules depend on
  whether they were changed at runtime:
  - **Never changed at runtime** (they are the seeded copy of the file):
    the file wins. A changed `rules_file` is stored again at the next start,
    with an INFO line, so tightening the file revokes access on restart as
    before.
  - **Changed at runtime** (`PUT` or `DELETE /api/v2/access-rules`) and the
    rules file is unchanged since it was last seeded: the runtime rules are
    kept, with a WARN at every start until a re-seed.
  - **Changed at runtime, then copied into the rules file:** once the file
    holds the same rules as the state (compared as JSON, so formatting and
    key order do not count), the rules count as the file's again and follow
    later edits of it.
  - **Changed at runtime, and the rules file was also edited since it was
    last seeded:** startup refuses, since neither copy can be assumed to be
    the one you want. Restart with `--reseed-state` to take the config file
    (users, catalogs, rules and settings), or restore the rules file to its
    last seeded version to keep the runtime rules (copy them into it
    afterwards to make them the file's).

  An unreadable or invalid rules file stops startup, as before v0.2.0.
  Every startup refusal that depends on the users, catalogs or rules (an
  invalid or edited rules file, stored rules that no longer parse, a catalog
  secret without `[cluster] secret`), and the `[cluster]`, retry, `[tls]`,
  Trino-transport, `[web] bind_address` and `[[tables]]` checks, happen
  before anything is written to the state file, so fixing the config and
  restarting is enough. The write itself, a re-seed included, is atomic; a
  later failure (a port in use) leaves the state as the config made it. Apply
  `[[catalogs]]` with a [re-seed](/guide/http-api#re-seed) or
  `--reseed-state`, which replaces catalogs, rules, settings and users in
  one atomic write of the state file. A
  standalone server under password auth behaves like a coordinator here.
  Under `[auth] type = "none"` nothing changes: there is no state store, and
  the config is used as before.
- **A PostgreSQL `connection_url` password must be the last query
  parameter.** In `?password=pw&sslmode=require` the driver used to read the
  password `pw`, but a password that really is `pw&sslmode=require` would
  have been split the same way, with its tail sent as a driver parameter.
  Such a URL is now refused (with or without runtime config): the catalog
  fails to build with a WARN at startup and queries on it fail with "not
  registered", and the API shows its URL as `postgresql://<redacted>`.
  Before: `connection_url = "postgresql://app@db/sales?password=pw&sslmode=require"`.
  After: `connection_url = "postgresql://app@db/sales?sslmode=require&password=pw"`,
  or `connection_url = "postgresql://app@db/sales?sslmode=require"` with
  `password = "pw"`. Percent-encode an `&` inside the password as `%26`. A
  catalog already in the state store is fixed with a `PUT` (send the full
  `connection_url`, password included), or from `[[catalogs]]` with a
  re-seed.
- **Workers ignore `[[catalogs]]`, and `[storage]` for catalogs.** A worker
  gets its catalogs from the coordinator over Flight: a push on every
  change, and a pull when its heartbeat shows it is behind (after it starts,
  after a missed push, after a coordinator restart). The set carries the
  coordinator's default catalog and schema, which the worker uses for a
  table named without a catalog or schema, and each Hive catalog arrives
  with the coordinator's global `[storage]` already merged in, so the
  worker reads object storage with the coordinator's settings and keys (a
  worker whose own `[storage]`, for example IAM-only, differed now uses the
  coordinator's). A worker's `[storage]` still applies to its own
  `[[tables]]`. A worker config that still has `[[catalogs]]` logs one INFO
  naming them; you can leave them, or remove them together with a
  `[storage]` that only served them. Catalog environment variables
  (`ARNEB_CATALOG_<NAME>_PASSWORD`) are no longer read on workers. Only the
  alive workers get a push; a worker that joins later pulls on its first
  heartbeat.
- **A coordinator with catalog secrets needs `[cluster] secret`.** The
  catalog definitions, passwords included, now cross the cluster RPC port
  from the coordinator to the workers (`get_catalogs`, `sync_catalogs`).
  Without a secret anyone who reaches the port could read every catalog
  password and storage key, or replace a worker's catalogs. So a
  coordinator without `[cluster] secret` (`ARNEB_CLUSTER_SECRET`) now
  **refuses to start** while a served catalog carries a secret: a
  `password`, a password in `connection_url` (a `connection_url` that fails
  validation counts when it may hold one: a `:` before its first `@`
  after `scheme://` (outside an IPv6 host), unless that `@` plainly sits
  in a query value such as `db:5432/x?user=a@b`; a query parameter whose
  key, split on every `&` and percent-decoded, contains `password` in any
  case (`?sslmode&password=`, `?PASSWORD=`); or, with no `scheme://` (a
  libpq `host=... password=...` string), any `password` or `user:pass@`
  shape; a mistyped URL with no password, such as `?sslmod=require`,
  does not stop the start, its catalog fails to build with a WARN),
  an S3 `secret_access_key` or an Azure `access_key`, including the global `[storage]` keys merged into
  every Hive catalog. The error names each catalog and field (never the
  value). The check runs before anything is stored, on every catalog that
  would be served (built or not): on a first start, removing the secret
  from `[[catalogs]]` and restarting is enough; a secret already in the
  state store has to be removed with a `PUT` on a server with a cluster
  secret, or from `[[catalogs]]` followed by one start with
  `--reseed-state`. A catalog create, update or re-seed that would add such a secret
  is refused the same way (`invalid_argument`, hint `set [cluster] secret
  ...`). Without secrets the coordinator starts with a WARN that cluster RPC
  is unauthenticated, and a worker without a secret WARNs that any peer can
  replace its catalogs. Set the same secret on every node (the Helm chart
  already does) and add `[cluster.tls]`, or the secret and the catalog
  passwords cross the network in the clear. A standalone server serves no
  catalogs over RPC and is not affected.
- **A resolved `ARNEB_CATALOG_<NAME>_PASSWORD` is stored in the state
  file.** The coordinator resolves it when it loads the config, so the
  seeded catalog keeps the password in `state.json` (mode `0600`). Rotating
  the variable alone no longer changes it: update the catalog through
  `PUT /api/v2/catalog-configs/{name}` or re-seed.
- **Open pgwire sessions survive a user delete.** Deleting a user, or
  changing their password, revokes their tokens, drops their cached Trino
  logins and ends their Web sessions at once, but pgwire connections already
  open stay open until they disconnect (as in Trino and PostgreSQL).
- **A catalog build has a 10 s limit**, at startup too. A PostgreSQL
  `connect_timeout_secs` above 10 is cut to 10 s while the catalog is built,
  and a catalog that cannot be reached is skipped with a WARN; workers retry
  it at most every 30 s.
- **A catalog that fails to build at coordinator startup is stored but not
  served.** It is not pushed to the workers either, until a
  `PUT /api/v2/catalog-configs/{name}`, a re-seed or a restart builds it.
- **Stored settings.** `PATCH /api/v2/settings` stores
  `slow_query_threshold_ms`; a stored value wins over `[observability]` and
  `ARNEB_SLOW_QUERY_THRESHOLD_MS` at every start, with a WARN.
- **`GET /api/v2/access-rules` reports `from_config: false` when there are
  no rules**, not only after a runtime change.
- **Changing where a catalog sends its secret needs the secret again.** A
  `PUT /api/v2/catalog-configs/{name}` that changes the host, port,
  database, user or parameters of `connection_url`, the `metastore_uri`, or
  a storage endpoint, region or bucket, must send the password or key
  again instead of `"<redacted>"`.
- **Downgrading.** An older binary reads the new state file, but its next
  write drops `catalogs`, `rules`, `rules_origin`, `seeded`, `deleted_config_users` and
  `slow_query_threshold_ms`. After a downgrade (and a later upgrade) the
  catalogs and rules are seeded from the config file again, and users
  deleted at runtime that are still in the config come back.
- **Kubernetes.** The chart already mounts the state volume; no chart change
  is needed. The worker ConfigMap may keep `[[catalogs]]` and `[storage]`,
  or drop both: workers take catalogs, defaults and catalog storage from
  the coordinator.

### Upgrading to v0.2.0 (MCP OAuth)

Nothing changes unless you add an `[oauth]` section: without it `/mcp`,
every other route, and every response header are byte-identical to the
previous release, and the `WWW-Authenticate` challenge stays
`Bearer realm="arneb"`. The one difference is that
`/.well-known/oauth-protected-resource[/...]` answers `404` instead of the
single-page app. With `[oauth]`:

- **Only `/mcp` accepts JWTs.** `/api/*` and `/metrics` treat one as an
  invalid bearer.
- **Create the users first.** A valid token for a name with no Arneb user is
  `403`; there is no auto-provisioning.
- **Check `user_claim`.** The default is `sub`, so Arneb user names are
  subject ids. `preferred_username` and `email` work but must not be
  editable by users at your IdP; `email` also needs `email_verified =
  true` in the token, and an email-address user name under another claim
  (`sub` included) needs the token's `email` to be that same address,
  verified (or `allow_missing_email_verified` for an IdP that never sends
  them).
- **Client-id audiences need a scope.** An `audience` other than the
  `resource` URL refuses to start without `required_scopes`.
- **Unmarked tokens without a scope are refused.** A token with neither an
  `at+jwt` header nor a Keycloak payload `typ` is taken for a possible ID
  token when it has a `nonce`, an `at_hash` or no scope (no `scope` /
  `scp`, or only empty or malformed ones; string and string-array forms of
  either claim both count); set
  `required_scopes` for an IdP that issues such access tokens. App-only
  tokens without a scope are refused: Entra client-credentials tokens
  (only `roles`, never `scp` or `at+jwt`) cannot be used, so give those
  clients an Arneb API token; Auth0 M2M tokens work once granted a
  permission, which puts it in `scope`.
- **Metadata paths.** The metadata document is at
  `/.well-known/oauth-protected-resource` plus the path of `resource`
  (normally `…/mcp`), and at the bare well-known path only when that path is
  exactly `/mcp` (behind a path prefix the bare path is `404`).
- **`[oauth]` is startup-only** and refuses to start without `[auth] type =
  "password"` and `[mcp] enabled = true`. A plain-`http` issuer or resource
  is refused unless the host is loopback.
- **The Web port needs TLS in front** for any non-loopback `resource`; see
  [Kubernetes](/guide/kubernetes#oauth-for-mcp).
- **Revoking at the IdP is not enough for minted API tokens.** An OAuth
  identity can create API tokens like any user; they outlive the IdP
  session until revoked or until the Arneb user is deleted, but never the
  access token that minted them: the expiry is clamped to the JWT's `exp`.

## State

```toml
[state]
path = "/var/lib/arneb/state.json"   # optional; ARNEB_STATE_PATH
```

The coordinator (or standalone server) keeps its users and
[API tokens](/guide/http-api#api-tokens) in one JSON state file, and the
[audit log](#audit-log) in the same directory, named after the state file
(`state.json` → `state.json.audit.jsonl`), so two state files in one directory
keep separate audit logs. Workers never
open them. Every write is atomic (write, fsync, rename) and the file is
created with mode `0600`. The path's directory must be writable.

- **Config users follow the config file.** On every start, the users from
  `[[auth.users]]` are applied to the state. A new user is added. A changed
  `password_hash` replaces the stored verifier and revokes all of that
  user's API tokens, so the old password and its tokens stop working. A user
  removed from the config is deleted together with their tokens. Each change
  logs a line on `arneb::config` naming the user.
- **Catalogs and access rules are seeded once.** The first start whose state
  has no seeded copy copies `[[catalogs]]` and the `rules_file` into it; from
  then on the stored catalogs are used, they change through the
  [runtime configuration](/guide/http-api#runtime-configuration) API, and a
  config entry that differs logs a WARN at every start. Rules never changed
  at runtime follow the rules file at every start; rules changed at runtime
  are kept while the file is unchanged, and a file edited since then stops
  startup until `--reseed-state`. See
  [Upgrading to v0.2.0 (runtime config)](#upgrading-to-v020-runtime-config).
  The state file then holds catalog passwords too.
- **Users created through the state store are never touched** by this. If a
  config user has the same name as one of them, startup keeps the state user
  and logs a WARN. Start once with `--reseed-state` to overwrite it with the
  config user (revoking its tokens). The exception is a state-created user
  whose stored verifier is identical to the config's `password_hash`: it is
  adopted (marked config-managed, tokens kept) with an INFO line on
  `arneb::config`, so later config edits or removal apply to it.
  `--reseed-state` re-seeds the catalogs, the access rules and the stored
  settings too, like [`POST /api/v2/state/reseed`](/guide/http-api#re-seed).
- **No `path`** keeps the state, and the audit log, in memory. Startup logs a
  WARN, `no [state] path: API tokens, worker cordons, admission limits and
  the audit log are lost on restart`, and the token API says so when it
  issues a token.
- The effective path (and `--reseed-state`) is logged on `arneb::config`.
- The state store is only used under `[auth] type = "password"`. Under
  `none`, `[state]` and `--reseed-state` have no effect (`--reseed-state`
  logs an INFO line).
- On Kubernetes the chart mounts a volume for it; see
  [Kubernetes](/guide/kubernetes#coordinator-state).

## Audit Log

```toml
[audit]
max_entries    = 10000   # ARNEB_AUDIT_MAX_ENTRIES
retention_days = 90      # ARNEB_AUDIT_RETENTION_DAYS; 0 = no age limit
```

Under `[auth] type = "password"`, the coordinator records every audited
action (token issue and revoke, and the manage actions) as JSON lines in
`<state file name>.audit.jsonl`, in the directory of [`[state] path`](#state)
and named after its full file name (`/var/lib/arneb/prod.json` →
`/var/lib/arneb/prod.json.audit.jsonl`), mode `0600`. A log under the
earlier name `audit.jsonl` in that directory is neither read nor migrated:
the server logs a WARN on `arneb::audit` at startup naming it, and its
entries do not appear in `GET /api/v2/audit`. Keep or archive it.
An action writes an intent entry (outcome `pending`) before it is applied
and a completion entry after; each is synced to disk, the completion before
the response returns. Read it with
[`GET /api/v2/audit`](/guide/http-api#audit-log).

| Knob | Env | Default | Meaning |
|---|---|---|---|
| `max_entries` | `ARNEB_AUDIT_MAX_ENTRIES` | `10000` | Most entries kept, `1` to `1000000`. |
| `retention_days` | `ARNEB_AUDIT_RETENTION_DAYS` | `90` | Days an entry is kept; `0` keeps entries of any age. |

- **Compaction.** The file is rewritten atomically (temporary file, fsync,
  rename, directory fsync), keeping at most the newest `max_entries` entries
  younger than `retention_days`. The newest entry is always kept, so `seq`
  keeps increasing across restarts. Compaction runs at startup when there is
  something to drop, and whenever the file holds `max(max_entries / 10, 100)`
  more lines than `max_entries`. Reads hide entries past `retention_days` at
  once, the kept newest entry included. The `pending` intent of an action in
  flight is never dropped, by age or by the cap, until its completion is
  written.
- **Torn lines.** A line that does not parse (for example, cut short by a
  crash) is skipped with a WARN on `arneb::audit` naming its line number.
  Startup never fails on it, and the startup compaction drops it.
- **A failed write.** If the intent cannot be written, the action is
  refused with `503 audit_unavailable` and nothing is applied. When the
  write was attempted (it may have reached the disk) and the log reopens at
  once, the intent is closed with outcome `{"error": "audit_unavailable"}`
  (best effort), so a restart does not report it as interrupted; an intent
  refused without a write gets no closing entry. If the completion cannot
  be written, the action has already happened: the entry is logged in full
  at ERROR on `arneb::audit`, its intent stays `pending` for now, and every
  later audited action is refused with `503 audit_unavailable` until the
  file can be written again. The server keeps that completion and appends
  it first when the log reopens, so the known outcome still reaches the
  file. It tries to reopen the log every 5 seconds in the background (no
  audited action is needed), and once more when the server exits (a
  graceful shutdown, or a fatal pgwire or Flight listener error); a
  completion still unwritten then is logged at ERROR and its action is
  recorded as `interrupted` at the next start. Before that last attempt, the
  Web API stops gracefully: it accepts no new connections, closes idle ones,
  and gives requests in progress up to 5 seconds to finish. Then new audited
  actions are refused with `503 audit_unavailable` ("the server is shutting
  down"; nothing is applied or written, including one still queued on the
  blocking pool), and the server waits up to 10 seconds for audited actions
  already running (a cancel waiting on its kill, a token issue), so their
  completions get the same retry; one still running after that is logged at
  ERROR and recorded as `interrupted` at the next start. An action that panics, or a cancel whose
  kill ended without an outcome, is completed as `interrupted`, since it may
  have taken effect. A `token.issue` that failed
  has an empty `target`: its token id never became a token.
- **Interrupted actions.** An intent with no completion at startup (the
  server stopped while applying it, or before a failed completion could be
  appended again, or the action panicked) is logged at
  WARN on `arneb::audit` and recorded once as an entry with outcome
  `interrupted`: the action may or may not have been applied. So no applied
  action is ever missing from the log.
- **No `[state] path`** keeps the log in memory, with a startup WARN; it is
  lost on restart. Under `type = "none"` nothing is audited and the log stays
  empty.
- The effective values, the path and the number of entries loaded are logged
  on `arneb::config` as `audit effective settings ([audit] / ARNEB_AUDIT_*)`.
  An invalid value fails startup.

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
  --reseed-state     Re-seed the state from the config file, like
                     POST /api/v2/state/reseed: catalogs, access rules and
                     settings, and let [[auth.users]] overwrite
                     state-created users of the same name
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
