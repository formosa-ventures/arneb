# Iceberg Connector

Arneb reads [Apache Iceberg](https://iceberg.apache.org/) tables that are
tracked by a Hive Metastore (HMS). This is the setup Trino calls
`iceberg.catalog.type=hive_metastore`, and it's also what Spark and Flink use
with a `hive` Iceberg catalog. If Trino, Spark, or Flink already write your
Iceberg tables through HMS, Arneb can query them in place.

The connector is **read-only** and reads the table's **current snapshot**.

## Configuration

```toml
[[catalogs]]
name = "lake"
type = "iceberg"
metastore_uri = "127.0.0.1:9083"   # host:port, no scheme
default_schema = "default"

[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Catalog name used in 3-part table references (`catalog.schema.table`) |
| `type` | string | yes | Must be `"iceberg"` |
| `metastore_uri` | string | yes | `host:port` of the Hive Metastore Thrift service (no scheme) |
| `default_schema` | string | no | Default schema when a query doesn't name one |
| `storage` | table | no | Per-catalog object store settings. They merge with the global `[storage]` settings, as they do for [Hive](/connectors/hive) |

A Hive catalog and an Iceberg catalog can point at the same metastore. That's
the usual setup when a lake holds both kinds of table:

```toml
[[catalogs]]
name = "hive"
type = "hive"
metastore_uri = "hms:9083"

[[catalogs]]
name = "iceberg"
type = "iceberg"
metastore_uri = "hms:9083"
```

Each catalog reads only its own kind of table. If you query an Iceberg table
through a `hive` catalog, or a Hive table through an `iceberg` catalog, the
query fails with a message that names the right catalog type. It never
returns rows from the wrong format. Trino behaves the same way when table
redirection is off.

## How a Query Reads an Iceberg Table

1. **Resolve** – HMS returns the table's parameters: `table_type=ICEBERG` and
   `metadata_location=<uri>`. Arneb reads that metadata JSON (format v1 or
   v2, gzip or plain) and exposes the table's **current schema**. Arneb
   ignores the HMS storage-descriptor columns because they can be stale.
2. **Pin a snapshot** – the current snapshot ID is fixed when the query is
   planned. It travels with the plan, so the coordinator and every worker
   read the same snapshot, even if a writer commits while the query runs.
3. **Plan files** – Arneb reads the snapshot's manifest list and manifests
   (Avro) to get the live data files. Deleted (tombstoned) entries are
   skipped. Manifests are fetched in parallel.
4. **Prune files** – before opening a data file, Arneb checks the file's
   manifest column bounds (and its identity-partition values) against the
   pushed-down filters. If no row in the file can match, Arneb skips the
   file without making a single storage request.
5. **Scan** – each file goes through Arneb's Parquet reader. It gets
   row-group pruning, row-level predicate pushdown, projection pushdown, and
   intra-file splits for parallelism.

## Schema Evolution

Arneb matches data-file columns to table columns **by Iceberg field ID**, not
by name or position. So evolved tables read correctly:

| Change | Behavior |
|--------|----------|
| Rename a column | Old files still hold the old name. They're read by field ID and show up under the new name |
| Add a column | Files written before the column existed return `NULL` for it |
| Drop a column | The column disappears from the table. Old files still contain it, and it's ignored |
| Reorder columns | The physical order in each file doesn't matter |
| Widen a type (`int`→`long`, `float`→`double`, decimal precision) | Old values are cast to the current type |

If a data file has no Parquet field IDs (for example, a table migrated from
Hive), Arneb uses the table's `schema.name-mapping.default` property. If that
property isn't set, it matches columns by their current names.

## Type Mapping

| Iceberg | Arneb / Arrow |
|---------|---------------|
| `boolean` | Boolean |
| `int` / `long` | Int32 / Int64 |
| `float` / `double` | Float32 / Float64 |
| `decimal(P,S)` | Decimal128(P,S) |
| `date` | Date32 |
| `timestamp` / `timestamp_ns` | Timestamp(µs / ns) |
| `timestamptz` / `timestamptz_ns` | Timestamp(µs / ns, UTC) |
| `string` | Utf8 |
| `uuid`, `fixed[L]`, `binary` | Binary |
| `time`, `struct`, `list`, `map` | Not supported yet. The column is hidden and a warning is logged |

## Supported and Unsupported Features

| Feature | Status |
|---------|--------|
| HMS-backed Iceberg catalog | ✅ |
| Format v1 and v2 metadata | ✅ |
| Current-snapshot reads, pinned per query | ✅ |
| Parquet data files | ✅ |
| Schema evolution resolved by field ID | ✅ |
| File pruning from manifest column bounds | ✅ |
| File pruning from identity-partition values | ✅ |
| Row-group pruning and predicate pushdown inside files | ✅ (shared with the Hive/File connectors) |
| Table statistics for the planner (`total-records`, `total-files-size`) | ✅ |
| **Row-level deletes** (position or equality delete files) | ❌ The query fails with an error (see below) |
| ORC and Avro data files | ❌ The query fails with an error |
| Writes (`INSERT`, `CTAS`, `DELETE`, `MERGE`) | ❌ Read-only |
| Time travel (`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`) | ❌ Planned follow-up |
| REST, Glue, Nessie, and JDBC catalogs | ❌ Planned follow-up |
| Partition pruning through non-identity transforms (`bucket`, `truncate`) | ⚠️ Not used directly. Column bounds already prune most `day`/`month`/`year`-partitioned files |

### Row-level deletes

A merge-on-read `DELETE`, `UPDATE`, or `MERGE` writes delete files that
readers must apply. Arneb doesn't apply them yet. If it returned the data
files alone, the result would include rows that were already deleted.
Instead, when the pinned snapshot has any live delete file, the query fails:

```
ERROR: unsupported operation: Iceberg table lake.ice.orders_deletes has position
delete files in snapshot 4518039616381... (row-level deletes / merge-on-read ...);
reading tables with delete files is not supported yet. Compact the table
(e.g. Trino `ALTER TABLE ... EXECUTE optimize`) or rewrite it with
copy-on-write to make it readable
```

A compaction that rewrites the data files, such as Trino's
`ALTER TABLE t EXECUTE optimize` or Spark's `rewrite_data_files` with
delete-file removal, makes the table readable again.

## Local Walkthrough

The Docker Compose stack includes an Iceberg catalog for Trino
(`docker/trino/catalog/iceberg.properties`) that shares the same HMS and
MinIO. A seed service creates sample Iceberg tables, including partitioned,
schema-evolved, and delete-file cases:

```bash
docker compose up -d
docker compose run --rm iceberg-seed            # TPCH_SF=sf1 for 1.5M-row orders
```

Then point Arneb at the metastore:

```toml
# ice.toml
port = 5432

[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
access_key_id = "minioadmin"
secret_access_key = "minioadmin"

[[catalogs]]
name = "lake"
type = "iceberg"
metastore_uri = "127.0.0.1:9083"
default_schema = "ice"
```

```bash
cargo run --bin arneb -- --config ice.toml
psql -h 127.0.0.1 -p 5432 -c "SELECT o_orderpriority, count(*) FROM lake.ice.orders GROUP BY 1 ORDER BY 1"
psql -h 127.0.0.1 -p 5432 -c "SELECT * FROM lake.ice.nation_evolved ORDER BY n_nationkey"
```

The seeded tables:

| Table | Shape |
|-------|-------|
| `ice.nation` | Plain CTAS |
| `ice.orders` | Identity-partitioned on `o_orderpriority` |
| `ice.orders_month` | Partitioned by `month(o_orderdate)` |
| `ice.nation_evolved` | Promoted `int`→`bigint`, a renamed column, an added column, and several snapshots |
| `ice.orders_deletes` | Has position delete files. Arneb rejects it until it's compacted |

## Implementation Notes

- The code lives in the `arneb-iceberg` crate (`crates/iceberg`). It reuses
  the Hive connector's `HmsClient` and Parquet split helpers.
- Arneb doesn't use the `iceberg` crate (apache/iceberg-rust). The latest
  release pins arrow/parquet 58, while Arneb is on 59, and it uses OpenDAL
  instead of `object_store`. Depending on it would put two copies of Arrow
  in the dependency tree and bypass Arneb's tuned Parquet scan.
  `openspec/changes/iceberg-connector/design.md` explains the decision in
  full.
