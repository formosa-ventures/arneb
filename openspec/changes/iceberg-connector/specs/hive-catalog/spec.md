## ADDED Requirements

### Requirement: Redirect Iceberg tables to the Iceberg reader
A Hive catalog SHALL serve HMS tables whose `table_type` parameter is `ICEBERG` (case-insensitive) through the Iceberg reader, never by listing the table's location. Listing an Iceberg table's location would also read files from deleted snapshots and rows covered by delete files.

#### Scenario: Resolve an Iceberg table through a Hive catalog
- **WHEN** `datalake.ice.nation` is resolved through a `type = "hive"` catalog and HMS reports `table_type=ICEBERG`
- **THEN** the schema provider SHALL return the Iceberg table provider for it (current Iceberg schema, pinned snapshot), not a `HiveTableProvider`

#### Scenario: Scan an Iceberg table through a Hive catalog
- **WHEN** the Hive connector factory creates a data source for a table whose properties contain `table_type=ICEBERG`
- **THEN** it SHALL delegate to the Iceberg reader, which plans the pinned snapshot's live data files
- **AND** it SHALL NOT list the table location

#### Scenario: Plain Hive tables are unchanged
- **WHEN** an HMS table has no `table_type=ICEBERG` parameter
- **THEN** it SHALL be resolved and scanned exactly as before
