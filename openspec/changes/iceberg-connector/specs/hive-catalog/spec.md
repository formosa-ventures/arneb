## ADDED Requirements

### Requirement: Reject Iceberg tables in Hive catalogs
The Hive connector SHALL NOT read HMS tables whose `table_type` parameter is `ICEBERG`. Listing an Iceberg table's location would also read files from deleted snapshots and rows covered by delete files.

#### Scenario: Query an Iceberg table through a Hive catalog
- **WHEN** `datalake.ice.nation` is queried through a `type = "hive"` catalog and HMS reports `table_type=ICEBERG`
- **THEN** the query SHALL fail with an error stating that the table is an Iceberg table and should be queried through a catalog with `type = "iceberg"`

#### Scenario: Table type forwarded as a property
- **WHEN** a Hive table is resolved and its HMS parameters contain `table_type`
- **THEN** `HiveTableProvider::properties()` SHALL include `table_type` with that value
