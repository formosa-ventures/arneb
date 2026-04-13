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
