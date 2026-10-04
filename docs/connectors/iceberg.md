# Iceberg Tables

Arneb reads [Apache Iceberg](https://iceberg.apache.org/) tables that are
tracked by a Hive Metastore (HMS). This is the setup Trino calls
`iceberg.catalog.type=hive_metastore`, and it's also what Spark and Flink use
with a `hive` Iceberg catalog. If Trino, Spark, or Flink already write your
Iceberg tables through HMS, Arneb can query them in place.

Reads are **read-only** and use the table's **current snapshot**.

## Configuration

There's nothing Iceberg-specific to configure. A [Hive](/connectors/hive)
catalog serves both kinds of table from its metastore: when HMS marks a table
with `table_type=ICEBERG`, Arneb redirects it to the Iceberg reader, the way
Trino's table redirection does.

```toml
[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"   # host:port, no scheme
default_schema = "default"

[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
```

```sql
SELECT count(*) FROM datalake.tpch.lineitem;   -- plain Hive table
SELECT count(*) FROM datalake.ice.orders;      -- Iceberg table, same catalog
```

The catalog's storage settings are used for the metadata, manifest, and data
files.

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
| Iceberg tables in a Hive Metastore (via a `hive` catalog) | ✅ |
| Format v1 and v2 metadata | ✅ |
| Current-snapshot reads, pinned per query (consistent across workers) | ✅ |
| Parquet data files | ✅ |
| Schema evolution resolved by field ID | ✅ |
| Row-group pruning and predicate pushdown inside files | ✅ (same Parquet scan as Hive tables) |
| Table statistics for the planner (`total-records`, `total-files-size`) | ✅ |
| File pruning from manifest column bounds / partition values | ❌ Planned follow-up |
| **Row-level deletes** (position or equality delete files) | ❌ The query fails with an error (see below) |
| ORC and Avro data files | ❌ The query fails with an error |
| Writes (`INSERT`, `CTAS`, `DELETE`, `MERGE`) | ❌ Read-only |
| Time travel (`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`) | ❌ Planned follow-up |
| REST, Glue, Nessie, and JDBC catalogs | ❌ Planned follow-up |

### Row-level deletes

A merge-on-read `DELETE`, `UPDATE`, or `MERGE` writes delete files that
readers must apply. Arneb doesn't apply them yet. If it returned the data
files alone, the result would include rows that were already deleted.
Instead, when the pinned snapshot has any live delete file, the query fails:

```
ERROR: unsupported operation: Iceberg table datalake.ice.orders_deletes has position
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
MinIO. A seed service uses it to create sample Iceberg tables in the `ice`
schema, including partitioned, schema-evolved, and delete-file cases:

```bash
docker compose up -d
docker compose run --rm iceberg-seed            # TPCH_SF=sf1 for 1.5M-row orders
cargo run --bin arneb -- --config benchmarks/tpch/tpch-hive.toml
psql -h 127.0.0.1 -p 5432 -c "SELECT o_orderpriority, count(*) FROM datalake.ice.orders GROUP BY 1 ORDER BY 1"
psql -h 127.0.0.1 -p 5432 -c "SELECT * FROM datalake.ice.nation_evolved ORDER BY n_nationkey"
```

| Table | Shape |
|-------|-------|
| `ice.nation` | Plain CTAS |
| `ice.orders` | Identity-partitioned on `o_orderpriority` |
| `ice.nation_evolved` | Promoted `int`→`bigint`, a renamed column, an added column, and several snapshots |
| `ice.orders_deletes` | Has position delete files. Arneb rejects it until it's compacted |

Design notes (why a native reader instead of `iceberg-rust`, scan planning,
field-ID resolution) are in `openspec/changes/iceberg-connector/design.md`.
