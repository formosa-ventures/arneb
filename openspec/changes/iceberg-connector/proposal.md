## Why

Most Trino deployments read Apache Iceberg tables, not plain Hive tables. Arneb can read Hive tables through a Hive Metastore (HMS), but it can't read the Iceberg tables in that same metastore. That's the biggest gap for a Trino user evaluating Arneb. It's also a correctness risk today. An Iceberg table registered in HMS looks like a normal external table, so the Hive connector would list its location and read every Parquet file under `data/`, including files that later snapshots deleted, and every row covered by a delete file. That would silently return wrong results.

## What Changes

- Add a new `crates/iceberg/` crate (`arneb-iceberg`) that reads Iceberg tables tracked by an HMS (`table_type=ICEBERG`, `metadata_location=...`). This is the Trino/Spark/Flink "hive" Iceberg catalog.
- Parse Iceberg table metadata JSON (format v1/v2, gzip or plain). Expose the **current schema**. Pin the **current snapshot** at planning time and carry it in the logical plan's table properties, so the coordinator and every worker read the same snapshot.
- Read the snapshot's manifest list and manifests (Avro, via the `apache-avro` crate) to find the live data files.
- Scan data files through Arneb's existing Parquet path (row-group pruning, row-filter pushdown, projection pushdown, intra-file splits). Resolve columns **by Iceberg field ID**, so renamed, reordered, added, and type-promoted columns read correctly.
- Prune whole files before opening them, using manifest column lower/upper bounds and identity-partition values.
- Fail clearly, instead of returning wrong results, on unsupported table states: live position or equality delete files (merge-on-read), and non-Parquet data files.
- Add a `type = "iceberg"` catalog type to `[[catalogs]]` in `arneb.toml`.
- Hive catalog: reject Iceberg tables with an error that names the `iceberg` catalog type. Iceberg catalog: reject non-Iceberg tables the same way.
- Propagate `ConnectorFactory::create_data_source` errors to the client, in both the protocol handler and worker tasks. Previously these errors were swallowed and turned into a generic "data source not found" error.
- Docker: add a Trino `iceberg` catalog on the same HMS and MinIO, plus an `iceberg-seed` compose service that creates sample tables (partitioned, schema-evolved, and with delete files).
- Docs: add `docs/connectors/iceberg.md` and mention the connector in the README, CLAUDE.md, the connector overview, and the configuration guide.

## Capabilities

### New Capabilities
- `iceberg-catalog`: resolve HMS-tracked Iceberg tables to their current schema and pinned snapshot. Map Iceberg types to Arneb types. Expose snapshot statistics.
- `iceberg-data-source`: plan scans from the manifest list and manifests, resolve columns by field ID, prune files, and reject delete files.

### Modified Capabilities
- `server-config`: `[[catalogs]]` accepts `type = "iceberg"`.
- `hive-catalog`: Iceberg tables in HMS are rejected with a clear error instead of being read as plain Hive tables.

## Impact

- **New crate**: `crates/iceberg/` (a workspace member). It depends on `arneb-hive` for `HmsClient` and the Parquet split helpers.
- **New dependencies**: `apache-avro` 0.22 (snappy and zstandard features; deflate is built in) and `flate2` (already in the dependency tree through parquet). There's no second copy of arrow, parquet, or object_store in the dependency tree.
- **Crates modified**: `hive` (exposes table parameters and the `table_type` property, rejects Iceberg tables, makes two Parquet helpers `pub`), `server` (catalog wiring, error propagation in the worker task manager), `protocol` (error propagation), `connectors` (a one-line clippy fix for a pre-existing `unnecessary_cast` lint).
- **Behavior change**: when a connector fails to create a data source, the query now fails with the connector's error instead of a later "data source not found for table" error.
- **No breaking config changes**: existing `type = "hive"` catalogs keep working unchanged.
