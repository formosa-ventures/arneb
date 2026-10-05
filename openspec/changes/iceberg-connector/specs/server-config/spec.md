## ADDED Requirements

### Requirement: Surface data source creation errors
If a connector fails to create a data source for a scanned table, the query SHALL fail with that connector error. This applies both on the coordinator and standalone path and in worker tasks.

#### Scenario: Connector rejects a table
- **WHEN** a connector's `create_data_source` returns an error (for example, an Iceberg table with delete files)
- **THEN** the client SHALL receive that error message, not a generic "data source not found" error
