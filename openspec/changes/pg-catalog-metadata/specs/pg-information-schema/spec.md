## ADDED Requirements

### Requirement: information_schema.tables lists all tables
The system SHALL return a result set for queries against `information_schema.tables` containing one row per table. Each row SHALL include: `table_catalog` (Utf8), `table_schema` (Utf8), `table_name` (Utf8), `table_type` (Utf8, 'BASE TABLE').

#### Scenario: Query information_schema.tables
- **WHEN** client sends `SELECT table_name FROM information_schema.tables WHERE table_schema = 'default'`
- **THEN** server returns rows for all tables registered in the 'default' schema

#### Scenario: Query all tables
- **WHEN** client sends `SELECT table_catalog, table_schema, table_name FROM information_schema.tables`
- **THEN** server returns rows for all tables across all catalogs and schemas

### Requirement: information_schema.columns lists all columns
The system SHALL return a result set for queries against `information_schema.columns` containing one row per column. Each row SHALL include: `table_catalog` (Utf8), `table_schema` (Utf8), `table_name` (Utf8), `column_name` (Utf8), `ordinal_position` (Int64), `data_type` (Utf8), `is_nullable` (Utf8, 'YES' or 'NO').

#### Scenario: Query columns for a specific table
- **WHEN** client sends `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'lineitem'`
- **THEN** server returns rows for all columns in the lineitem table with correct data types

### Requirement: information_schema.schemata lists all schemas
The system SHALL return a result set for queries against `information_schema.schemata` containing one row per schema. Each row SHALL include: `catalog_name` (Utf8), `schema_name` (Utf8).

#### Scenario: Query schemata
- **WHEN** client sends `SELECT schema_name FROM information_schema.schemata`
- **THEN** server returns rows for all schemas across all catalogs
