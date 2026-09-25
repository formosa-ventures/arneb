## ADDED Requirements

### Requirement: Resolve HMS-tracked Iceberg tables
The `IcebergCatalogProvider` SHALL map HMS databases to schemas. It SHALL resolve a table whose HMS parameters include `table_type=ICEBERG` (case-insensitive) by reading the metadata file named by the `metadata_location` parameter.

#### Scenario: Resolve an Iceberg table
- **WHEN** `lake.ice.orders` is queried and HMS reports `table_type=ICEBERG`, `metadata_location=s3://warehouse/ice/orders-…/metadata/00001-….metadata.json`
- **THEN** the system SHALL read and parse that metadata file (format v1 or v2, plain or gzip)
- **AND** SHALL expose the columns of the table's current schema, in declaration order

#### Scenario: Missing metadata location
- **WHEN** an HMS table has `table_type=ICEBERG` but no `metadata_location`
- **THEN** resolution SHALL fail and log a warning. No data SHALL be read

### Requirement: Pin the current snapshot at planning time
The table provider SHALL expose `metadata_location`, `snapshot_id` (the current snapshot), and `table_type=ICEBERG` as table properties. Every node that executes the scan SHALL read that snapshot.

#### Scenario: Concurrent commit during a query
- **WHEN** a writer commits a new snapshot after the query is planned
- **THEN** the coordinator and all workers SHALL still read the snapshot that was pinned at planning time

#### Scenario: Empty table
- **WHEN** the metadata has no current snapshot (`current-snapshot-id` is `-1` or `null`)
- **THEN** the table SHALL scan zero rows, and its statistics SHALL report `row_count = 0`

### Requirement: Iceberg type mapping
The system SHALL map Iceberg primitive types to Arneb types as follows: `boolean`→Boolean, `int`→Int32, `long`→Int64, `float`→Float32, `double`→Float64, `decimal(P,S)`→Decimal128(P,S), `date`→Date32, `timestamp[_ns]`→Timestamp(µs|ns), `timestamptz[_ns]`→Timestamp(µs|ns, UTC), `string`→Utf8, `uuid`/`fixed[L]`/`binary`→Binary.

#### Scenario: Unsupported column types
- **WHEN** a table has a `time`, `struct`, `list`, or `map` column
- **THEN** the column SHALL be hidden from the table schema and a warning SHALL be logged. The rest of the table SHALL stay queryable

### Requirement: Table statistics from the snapshot summary
The table provider SHALL report `row_count` from the current snapshot's `total-records` and `size_bytes` from its `total-files-size`, when present.

#### Scenario: Statistics available
- **WHEN** the current snapshot summary contains `total-records = 1500000`
- **THEN** `statistics().row_count` SHALL be `Some(1500000)`

### Requirement: Reject non-Iceberg tables
An `iceberg` catalog SHALL NOT read HMS tables that are not Iceberg tables.

#### Scenario: Query a Hive table through an Iceberg catalog
- **WHEN** `lake.tpch.nation` is queried and the HMS table has no `table_type=ICEBERG`
- **THEN** the query SHALL fail with an error stating that the table is not an Iceberg table and should be queried through a catalog with `type = "hive"`
