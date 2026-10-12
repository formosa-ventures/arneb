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

## Storage Formats

The file format comes from the table's HMS storage descriptor:

| Input format / SerDe | Read as |
|----------------------|---------|
| `MapredParquetInputFormat` / `ParquetHiveSerDe` | Parquet |
| `OrcInputFormat` / `OrcSerde` | ORC (Trino's default Hive storage format) |
| anything else (text, Avro, RCFile, ...) | error: `unsupported Hive storage format` |

Both formats are read-only and share the same file listing, splitting and
projection. Hidden files and directories (names starting with `.` or `_`, e.g.
`_SUCCESS`, `.trino-staging/`) are skipped.

### ORC

- **Column mapping** is by name, case-insensitively. Files written by old Hive
  versions with positional names (`_col0`, `_col1`, ...) are mapped by position.
- **Missing columns** (added to the table after a file was written) read as NULL.
- **Type differences** that Hive allows are widened: `tinyint`→`smallint`→`int`→`bigint`,
  integers to `float`/`double`/`decimal`, `float`→`double`, decimal precision/scale
  changes (a value that no longer fits is an error, never NULL). Any other mismatch
  fails the query with an error naming the column, the ORC type and the table type.
- **Types**: `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `float`, `double`,
  `decimal`, `string`/`varchar`/`char`, `binary`, `date`, `timestamp`.
- **Timestamps** are the wall-clock values in the writer time zone recorded in each
  stripe, which is what Hive and Trino return for `timestamp` columns.
- **Compression**: ZLIB, Snappy, LZO, LZ4, Zstd.
- Filters are evaluated above the scan; ORC row-index (min/max) pruning is not
  used yet.

## Partitioned Tables

Partition key columns follow the data columns (`SELECT *` order, as in Hive and
Trino). As in Trino and Hive, a partitioned table reads exactly the partitions
registered in HMS, each at its own location:

- The partitions (values and locations) are fetched from HMS when the query is
  planned and travel with the plan to workers.
- A partition registered at a custom location, even outside the table directory
  or in another bucket (`ALTER TABLE ... ADD PARTITION ... LOCATION`), is read.
- Directories under the table location that are not registered partitions (files
  written before `ADD PARTITION` / `MSCK REPAIR`, or left behind by a drop) and
  stray files at the table root are ignored.
- `__HIVE_DEFAULT_PARTITION__` reads as NULL. A partition without a location in
  HMS uses Hive's default `key=value` layout under the table location.

Filters on partition columns do not yet prune partitions.

## Unsupported Tables

Hive ACID (transactional) tables are rejected with a clear error rather than
misread: tables with `transactional=true`, locations containing `base_N` /
`delta_N_M` / `delete_delta_N_M` directories, and ACID-layout ORC files.
Complex column types (`array`, `map`, `struct`, `uniontype`) are skipped when the
table is loaded.

## Iceberg Tables

Iceberg tables registered in the same metastore (`table_type=ICEBERG`) are
read through the Hive catalog automatically, using their current Iceberg
snapshot rather than a listing of the table location. See
[Iceberg](/connectors/iceberg).

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
access_key_id = "s3admin"
secret_access_key = "s3adminsecret"
```

Per-catalog settings merge with and override global `[storage]` settings.

## Local Demo Walkthrough

Arneb includes a Docker Compose setup with HMS 4.2.0 and RustFS for local development.

### Prerequisites

- Docker and Docker Compose
- Rust toolchain

### Step 1: Start Services

```bash
docker compose up -d
```

This starts:
- **RustFS** — S3-compatible object store on port `9000` (API) and `9001` (console)
- **Hive Metastore** — HMS 4.2.0 on port `9083`

### Step 2: Seed TPC-H Data

```bash
docker compose run --rm tpch-seed
```

This creates 8 TPC-H tables in the `tpch` schema on RustFS via Trino CTAS.

### Step 3: Start Arneb

```bash
cargo run --bin arneb -- --config benchmarks/tpch/tpch-hive.toml
```

The config (`benchmarks/tpch/tpch-hive.toml`) connects to the local HMS and RustFS:

```toml
bind_address = "127.0.0.1"
port = 5432

[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
access_key_id = "s3admin"
secret_access_key = "s3adminsecret"

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"
default_schema = "tpch"
```

### Step 4: Run Queries

```bash
psql -h 127.0.0.1 -p 5432 -c "SELECT COUNT(*) FROM datalake.tpch.nation;"
psql -h 127.0.0.1 -p 5432 -c "SELECT COUNT(*) FROM datalake.tpch.lineitem;"
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
