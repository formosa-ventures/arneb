## Why

Most Trino deployments read Apache Iceberg tables, not plain Hive tables. Arneb can read Hive tables through a Hive Metastore (HMS), but it can't read the Iceberg tables in that same metastore. That's the biggest gap for a Trino user evaluating Arneb. It's also a correctness risk today. An Iceberg table registered in HMS looks like a normal external table, so the Hive connector would list its location and read every Parquet file under `data/`, including files that later snapshots deleted, and every row covered by a delete file. That would silently return wrong results.

## What Changes

- Add a new `crates/iceberg/` crate (`arneb-iceberg`) that reads Iceberg tables tracked by an HMS (`table_type=ICEBERG`, `metadata_location=...`), the convention Trino, Spark, and Flink use.
- **Table redirection in the Hive catalog**: a `type = "hive"` catalog serves Iceberg tables from the same metastore. `HiveSchemaProvider` returns the Iceberg table provider for `table_type=ICEBERG` tables, and `HiveConnectorFactory` delegates their scans to the Iceberg reader. No new catalog type or config.
- Parse Iceberg table metadata JSON (format v1/v2, gzip or plain). Expose the **current schema**. Pin the **current snapshot** at planning time and carry it in the logical plan's table properties, so the coordinator and every worker read the same snapshot.
- Read the snapshot's manifest list and manifests (Avro, via the `apache-avro` crate) to find the live data files.
- Scan data files through Arneb's existing Parquet path, moved from `crates/hive` into `arneb_connectors::parquet_scan` so Hive and Iceberg share one implementation (row-group pruning, row-filter pushdown, projection pushdown, intra-file splits). Resolve columns **by Iceberg field ID**, so renamed, reordered, added, and type-promoted columns read correctly.
- Fail clearly, instead of returning wrong results, on unsupported table states: live position or equality delete files (merge-on-read), and non-Parquet data files.
- Propagate `ConnectorFactory::create_data_source` errors to the client, in both the protocol handler and worker tasks. Previously these errors were swallowed and turned into a generic "data source not found" error.
- Docker: add a Trino `iceberg` catalog on the same HMS and MinIO, plus an `iceberg-seed` compose service that creates sample tables (partitioned, schema-evolved, and with delete files).
- Docs: add `docs/connectors/iceberg.md` and mention the connector in the README, CLAUDE.md, the connector overview, the Hive connector page, and the configuration guide.

## Capabilities

### New Capabilities
- `iceberg-catalog`: resolve HMS-tracked Iceberg tables (reached through Hive catalog redirection) to their current schema and pinned snapshot. Map Iceberg types to Arneb types. Expose snapshot statistics.
- `iceberg-data-source`: plan scans from the manifest list and manifests, resolve columns by field ID, and reject delete files.

### Modified Capabilities
- `hive-catalog`: Iceberg tables in HMS are redirected to the Iceberg reader instead of being read as plain Hive tables.
- `server-config`: data source creation errors reach the client.

## Impact

- **New crate**: `crates/iceberg/` (a workspace member). `arneb-hive` depends on it for redirection; it depends only on the engine crates (`common`, `catalog`, `connectors`, `execution`, `planner`).
- **New dependencies**: `apache-avro` 0.22 (snappy and zstandard features; deflate is built in) and `flate2` (already in the dependency tree through parquet). There's no second copy of arrow, parquet, or object_store in the dependency tree.
- **Crates modified**: `connectors` (new `parquet_scan` module, moved verbatim from `hive`, plus a one-line clippy fix for a pre-existing `unnecessary_cast` lint), `hive` (exposes table parameters, redirects Iceberg tables, calls the shared scan; `HiveCatalogProvider::new` now takes the catalog's `StorageRegistry`), `server` (passes the storage registry, error propagation in the worker task manager), `protocol` (error propagation).
- **Behavior change**: when a connector fails to create a data source, the query now fails with the connector's error instead of a later "data source not found for table" error. Iceberg tables queried through a Hive catalog now return their current snapshot instead of a raw listing of their location.
- **No config changes**: existing `type = "hive"` catalogs keep working unchanged and gain Iceberg tables.
