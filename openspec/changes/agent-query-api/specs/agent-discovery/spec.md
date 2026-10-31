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
`GET /api/v2/catalogs/{c}/schemas/{s}/tables/{t}` SHALL return the table's columns with Trino type names and nullability, and its statistics (`row_count`, `size_bytes`), each `null` when the connector does not know it. It SHALL accept a `limit` (default `default_max_rows`; outside `1..=max_rows` SHALL be refused with `invalid_argument`, as for the listings), return at most `limit` columns in table order, and set `truncated` to true when columns beyond the limit were dropped.

#### Scenario: Describe lineitem
- **WHEN** alice describes `datalake.tpch.lineitem`
- **THEN** the columns include `l_quantity`

#### Scenario: Wide table
- **WHEN** alice describes a 30-column table with `limit=10`
- **THEN** the first 10 columns are returned and `truncated` is true, and `limit` above `max_rows` is refused with `invalid_argument`

### Requirement: Table sample
`GET .../tables/{t}/sample?rows=n` SHALL run `SELECT * FROM "c"."s"."t" LIMIT n` as the caller's statement, waiting up to `max_wait_ms`, and return its statement status. `rows` SHALL default to 10; a value above 100 SHALL be refused with `invalid_argument`.

#### Scenario: Sample nation
- **WHEN** alice samples `nation` with `rows=5`
- **THEN** the statement finishes and returns 5 rows

### Requirement: Explain with estimates
`POST /api/v2/explain` with `{"sql","catalog","schema"}` SHALL plan the statement without running it and return `{"plan","plan_truncated","scans":[{"table","row_count","size_bytes"}],"estimated_scan_bytes"}`. The `plan` text SHALL be cut at `[agent_api] default_page_bytes` on a character boundary, with `plan_truncated` true when cut. Explain SHALL NOT be admitted or tracked and SHALL NOT run `ANALYZE`.

#### Scenario: Explain Q6
- **WHEN** alice explains TPC-H Q6
- **THEN** the `plan` is non-empty and `scans` lists `datalake.tpch.lineitem`

#### Scenario: Long plan
- **WHEN** the plan text is longer than `default_page_bytes`
- **THEN** `plan` is at most `default_page_bytes` bytes of valid UTF-8 and `plan_truncated` is true
