## ADDED Requirements

### Requirement: Structured query errors
The `/api/v2` error envelope SHALL gain optional `location` (`{"line","column"}` in the SQL) and `candidates` (nearest existing names). The codes `syntax_error` (400), `catalog_not_found` (404), `schema_not_found` (404), `table_not_found` (404), `column_not_found` (400), `function_not_found` (400), `type_mismatch` (400), `not_supported` (400), `query_failed` (400), `exceeded_memory` (400), `exceeded_time` (400) and `cancelled` (409) SHALL be added; `forbidden`, `overloaded`, `invalid_argument`, `not_found` and `internal` are reused. For a statement the error SHALL appear in the status `error` object; for explain, discovery and a bad request it SHALL be the HTTP error. Candidates SHALL only name objects the caller may see.

#### Scenario: Misspelled column
- **WHEN** alice submits `SELECT l_quantiy FROM lineitem`
- **THEN** the error code is `column_not_found`, it carries a `location`, and `candidates[0]` is `l_quantity`

#### Scenario: Fix from the hint
- **WHEN** alice resubmits with the first candidate substituted
- **THEN** the statement finishes

### Requirement: Trino error location
Trino client error JSON SHALL gain `errorLocation` (`lineNumber`, `columnNumber`) when the error has a SQL position. No existing Trino JSON field SHALL change.

#### Scenario: Trino syntax error
- **WHEN** a Trino client sends SQL with a syntax error
- **THEN** the error carries `errorLocation` and its existing fields are unchanged
