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
| Web UI | `port + 1000` | standalone, coordinator |
| Flight RPC | `9090` | all roles |

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
| query memory cap | config | `ARNEB_QUERY_MAX_MEMORY_BYTES` | Per-task cumulative allocation cap (bytes). When the query's tracked `MemoryReservation` crosses it, the query fails cleanly with `ResourceExhausted` instead of OOM-killing the worker. Overrides `[memory] query_max_memory_per_node`. |

The startup log line `memory pool installed … spill_budget_bytes=… spill_budget_source=…`
confirms the resolved value and its source (`env` / `config` / `cgroup_v2` / `cgroup_v1`
/ `unbounded`); a `source=env` line confirms the override took effect.

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
# access_key_id = "minioadmin"       # Optional: falls back to env/IAM
# secret_access_key = "minioadmin"
```

Credential precedence: config file → `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars → IAM role / instance profile.

### GCS

```toml
[storage.gcs]
service_account_path = "/path/to/service-account.json"
```

## Catalog Configuration

Register external catalogs (e.g., Hive Metastore):

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
| `type` | string | yes | Catalog type (currently `"hive"`) |
| `metastore_uri` | string | yes | `host:port` of the Hive Metastore (no scheme prefix) |
| `default_schema` | string | no | Default schema within the catalog |

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
| `coordinator_address` | string | yes | `host:port` of the coordinator's Flight RPC |
| `worker_id` | string | yes | Unique identifier for this worker |

See [Distributed Mode](/guide/distributed) for full setup instructions.

## CLI Arguments

```
arneb [OPTIONS]

Options:
  --config <PATH>    Path to configuration file
  --port <PORT>      Override the pgwire port
  --role <ROLE>      Server role: standalone, coordinator, or worker
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
endpoint = "http://minio:9000"
allow_http = true

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "hive-metastore:9083"
default_schema = "default"

[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://minio:9000"
allow_http = true
```
