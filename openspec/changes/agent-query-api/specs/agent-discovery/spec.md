## ADDED Requirements

### Requirement: Bounded catalog discovery
The coordinator (or standalone server) SHALL serve `GET /api/v2/catalogs`, `GET /api/v2/catalogs/{c}/schemas` and `GET /api/v2/catalogs/{c}/schemas/{s}/tables` to authenticated callers. Each SHALL return `{"items":[{"name"}],"total","truncated"}`, accept a case-insensitive `prefix` and a `limit`, and list only objects the caller may see. A catalog, schema or table the caller may not see SHALL answer exactly like one that does not exist (`catalog_not_found`, `schema_not_found`, `table_not_found`). Workers SHALL NOT serve these routes.

#### Scenario: TPC-H catalog
- **WHEN** alice lists catalogs, then the tables of `datalake.tpch` with `prefix=line`
- **THEN** `datalake` is listed, and `lineitem` is listed

#### Scenario: Hidden table
- **WHEN** a table is hidden from bob by the access rules
- **THEN** bob's listing omits it, and describing it answers `table_not_found` as for a missing table

### Requirement: Table description
`GET /api/v2/catalogs/{c}/schemas/{s}/tables/{t}` SHALL return the table's columns with Trino type names and nullability, and its statistics (`row_count`, `size_bytes`), each `null` when the connector does not know it.

#### Scenario: Describe lineitem
- **WHEN** alice describes `datalake.tpch.lineitem`
- **THEN** the columns include `l_quantity`

### Requirement: Table sample
`GET .../tables/{t}/sample?rows=n` SHALL run `SELECT * FROM "c"."s"."t" LIMIT n` as the caller's statement, waiting up to `max_wait_ms`, and return its statement status. `rows` SHALL default to 10; a value above 100 SHALL be refused with `invalid_argument`. The row count SHALL then be lowered to the server's `max_rows` when that is smaller, and the sample SHALL NOT fail because of a limit the caller did not send. A table missing from (or hidden in) the schema's table names SHALL answer the same not-found error as describe without registering, admitting or running a statement; the check SHALL load no table metadata, so the query resolves the table once.

#### Scenario: Sample nation
- **WHEN** alice samples `nation` with `rows=5`
- **THEN** the statement finishes and returns 5 rows

#### Scenario: Sample a missing table
- **WHEN** alice samples `natoin`
- **THEN** the response is `table_not_found` with `nation` among the candidates, and no statement is registered or queued

### Requirement: Explain with estimates
`POST /api/v2/explain` with `{"sql","catalog","schema"}` SHALL plan the statement without running it and return `{"plan","scans":[{"table","row_count","size_bytes"}],"estimated_scan_bytes"}`. Explain SHALL NOT be admitted or tracked and SHALL NOT run `ANALYZE`.

#### Scenario: Explain Q6
- **WHEN** alice explains TPC-H Q6
- **THEN** the `plan` is non-empty and `scans` lists `datalake.tpch.lineitem`
