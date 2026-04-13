## Context

Hive Metastore (HMS) is a Thrift-based metadata service that stores table definitions, schemas, partition information, and file storage locations. In a typical data lake setup, tables are registered in HMS and their data files (usually Parquet) are stored on S3/GCS/Azure. Arneb needs to connect to HMS, discover tables, and read their data via the already-implemented `object_store` layer.

The Rust crate `hive_metastore` (by Xuanwo) provides a complete HMS Thrift client using the Volo runtime. This eliminates the need to generate Thrift code from the HMS IDL.

Architecture after all three changes:

```
┌──────────────────────────────────────────────────────┐
│                    Arneb Server                       │
│                                                       │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │ FileCatalog  │  │ HiveCatalog  │  │ MemoryCatalog│ │
│  │ (local/cloud)│  │ (HMS Thrift) │  │ (in-memory)  │ │
│  └──────┬──────┘  └──────┬───────┘  └─────────────┘ │
│         │                │                            │
│         ▼                ▼                            │
│  ┌─────────────────────────────────┐                 │
│  │       StorageRegistry           │                 │
│  │  (ObjectStore instance cache)   │                 │
│  └──────┬──────┬──────┬───────────┘                 │
│         │      │      │                              │
│         ▼      ▼      ▼                              │
│     LocalFS   S3    GCS   Azure                      │
└──────────────────────────────────────────────────────┘
```

## Goals / Non-Goals

**Goals:**
- Connect to Hive Metastore via Thrift and list databases/tables
- Map HMS databases → Arneb schemas, HMS tables → Arneb TableProviders
- Read Parquet data from HMS table storage locations via ObjectStore
- Support configuring multiple Hive catalogs in `arneb.toml`
- Support per-catalog object store credentials

**Non-Goals:**
- Partition pruning (future change: `partition-pruning`)
- ORC, Avro, or other Hive storage formats (Parquet only in v1)
- Write support (CREATE TABLE, INSERT INTO on Hive tables)
- HMS DDL operations (ALTER TABLE, etc.)
- Schema evolution or type mapping beyond Arrow-compatible types

## Decisions

### 1. Use `hive_metastore` crate for HMS Thrift client

**Choice**: `hive_metastore` crate (Volo-based, pure Rust).

**Alternatives considered**:
- **Generate from Thrift IDL**: More control but significant upfront work and maintenance.
- **`iceberg-catalog-hms`**: Designed for Iceberg tables, not general Hive tables. Would add unnecessary Iceberg abstractions.

**Rationale**: Production-ready, maintained by Xuanwo (also author of OpenDAL). Covers the full HMS API. Pure Rust with no external Thrift compiler needed.

### 2. New crate: `crates/hive/`

**Choice**: Create a dedicated `arneb-hive` crate rather than adding to `crates/connectors/`.

**Rationale**: The Hive connector has unique dependencies (`hive_metastore`, `volo-thrift`) that shouldn't be forced on all connector users. Separate crate allows optional compilation and cleaner dependency management.

### 3. HiveCatalogProvider implements async CatalogProvider

**Choice**: `HiveCatalogProvider` implements the async `CatalogProvider` trait directly, making live HMS calls for schema/table resolution.

**Implementation detail**: On `schema()` call, query HMS for database. On `table()` call, query HMS for table metadata. No upfront caching in v1 — keep it simple, add caching if latency becomes an issue.

### 4. HiveDataSource wraps ObjectStore-based ParquetDataSource

**Choice**: `HiveDataSource` reads the table's `sd.location` from HMS metadata (e.g., `s3://warehouse/db/table/`), lists Parquet files at that location via ObjectStore, and reads them.

For non-partitioned tables: read all `.parquet` files in the location directory.
For partitioned tables (v1): read ALL partitions (no pruning). Partition pruning is a separate future change.

### 5. Multi-catalog configuration via `[[catalogs]]`

**Choice**: New top-level `[[catalogs]]` array in `arneb.toml`:

```toml
[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "thrift://hms.internal:9083"
default_schema = "default"

[catalogs.storage]
type = "s3"
region = "us-east-1"
```

**Rationale**: Supports multiple Hive Metastore instances (e.g., production + analytics). Per-catalog storage config allows different credentials per HMS.

## Risks / Trade-offs

- **[Volo runtime compatibility]** → `hive_metastore` uses Volo (not tonic) for Thrift. Need to verify it works alongside tokio without conflicts. Mitigation: Volo is tokio-compatible. Test early in a spike.
- **[No partition pruning in v1]** → Querying large partitioned tables will be slow (reads all files). Mitigation: Document the limitation. Partition pruning is planned as a follow-up change.
- **[HMS connection lifecycle]** → Thrift connections are stateful. Need to handle reconnection on failure. Mitigation: v1 creates a new connection per catalog operation. Connection pooling can be added later.
- **[Type mapping gaps]** → HMS types (Hive SerDe) may not map cleanly to Arrow types. Mitigation: Support common types (INT, BIGINT, STRING, DOUBLE, DECIMAL, TIMESTAMP, DATE, BOOLEAN, BINARY). Error on unsupported types with clear message.

## Open Questions

- Should we support HMS with Kerberos authentication in v1, or defer to a later iteration?
- What is the maximum number of files we should scan for a single table before warning the user (performance guard)?
