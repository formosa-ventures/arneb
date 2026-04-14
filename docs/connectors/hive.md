# Hive Connector

Arneb connects to [Apache Hive Metastore](https://hive.apache.org/) (HMS) to discover and query tables managed by Hive catalogs. The connector supports HMS 4.x via an async Thrift client.

## Configuration

```toml
[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"
default_schema = "default"

[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Catalog name used in 3-part table references (`catalog.schema.table`) |
| `type` | string | yes | Must be `"hive"` |
| `metastore_uri` | string | yes | `host:port` of the Hive Metastore Thrift service (no scheme) |
| `default_schema` | string | no | Default schema to use when schema is not specified |

## Three-Part Table References

Tables in Hive catalogs use three-part naming:

```sql
SELECT * FROM datalake.demo.cities;
--            ^^^^^^^^ ^^^^ ^^^^^^
--            catalog  schema table
```

## Storage Configuration

Hive tables are typically stored in object stores. Configure storage credentials either globally or per-catalog:

```toml
# Global storage (used by all catalogs unless overridden)
[storage.s3]
region = "us-east-1"

# Per-catalog override
[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
```

Per-catalog settings merge with and override global `[storage]` settings.

## Local Demo Walkthrough

Arneb includes a Docker Compose setup with HMS 4.2.0 and MinIO for local development.

### Prerequisites

- Docker and Docker Compose
- Rust toolchain

### Step 1: Start Services

```bash
docker compose up -d
```

This starts:
- **MinIO** — S3-compatible object store on port `9000` (API) and `9001` (console)
- **Hive Metastore** — HMS 4.2.0 on port `9083`

### Step 2: Seed Demo Data

```bash
cargo run --bin hive-demo-setup
```

This creates two demo tables in the `demo` schema:
- `demo.cities` — sample city data
- `demo.orders` — sample order data

### Step 3: Start Arneb

```bash
cargo run --bin arneb -- --config scripts/arneb-hive-demo.toml
```

The demo config (`scripts/arneb-hive-demo.toml`) is pre-configured to connect to the local HMS and MinIO:

```toml
bind_address = "127.0.0.1"
port = 5432

[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
access_key_id = "minioadmin"
secret_access_key = "minioadmin"

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"
default_schema = "demo"
```

### Step 4: Run Queries

```bash
psql -h 127.0.0.1 -p 5432 -c "SELECT * FROM datalake.demo.cities;"
psql -h 127.0.0.1 -p 5432 -c "SELECT * FROM datalake.demo.orders LIMIT 10;"
```

### Step 5: Tear Down

```bash
docker compose down
```

## HMS Compatibility

The Hive connector uses auto-generated Thrift bindings from the Hive 4.2.0 IDL. It communicates with HMS using plain TBinaryProtocol (buffered codec), compatible with standard HMS deployments.

The Thrift bindings are in the `hive-metastore` crate. To regenerate after modifying the IDL:

```bash
cargo run -p hive-metastore-thrift-build
```
