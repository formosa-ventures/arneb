## ADDED Requirements

### Requirement: Dynamic-filter pushdown
`ScanContext` SHALL carry `dynamic_filters_start`, the index in `filters` at
which runtime dynamic-filter conjuncts begin. `ScanExec` SHALL set it whenever
it appends same-fragment or cross-fragment dynamic filters to the static
filters, and leave it unset otherwise. Connectors that do not read it SHALL
behave exactly as before.

The framework SHALL translate a dynamic-filter conjunct with the same rules
as a static one, plus a size bound. A dynamic `IN` list with more literals than
the catalog's `domain_compaction_threshold` (default 1000) SHALL be compacted
to `col BETWEEN min AND max` when all its literals share one orderable type
(`Int32`, `Int64`, `Date32`, or `Decimal128` of one scale). Otherwise it SHALL
NOT be pushed. A compacted conjunct SHALL mark the translation incomplete, so
`LIMIT` is not pushed. Static `IN` lists SHALL never be compacted. A dynamic
filter that is a `Bloom` domain, that cannot be translated, or that did not
arrive before the wait timeout SHALL NOT be pushed and SHALL NOT fail the
query. Query results SHALL be identical whether a dynamic filter was pushed
exactly, compacted, or not pushed at all.

#### Scenario: Small dynamic IN list is pushed exactly
- **WHEN** a hash join's build side yields 12 distinct `BIGINT` keys for a PostgreSQL probe scan
- **THEN** the remote SQL contains `"key" IN (…)` with those 12 values

#### Scenario: Large dynamic IN list is compacted to a range
- **WHEN** a dynamic filter carries 5000 `BIGINT` keys between 7 and 9000 and the threshold is 1000
- **THEN** the remote SQL contains `("key" BETWEEN 7 AND 9000)` and no `IN` list, and the query result equals the uncompacted result

#### Scenario: Static IN list is never compacted
- **WHEN** a user query filters `WHERE k IN (…)` with 2000 literals on a PostgreSQL table
- **THEN** the remote SQL contains all 2000 literals

#### Scenario: Compaction disables LIMIT pushdown
- **WHEN** a scan with a `LIMIT` receives a dynamic filter that is compacted
- **THEN** the remote SQL contains no `LIMIT` clause

#### Scenario: Unpushable dynamic filter does not fail the query
- **WHEN** a dynamic filter's literals cannot be rendered by the dialect, or it is over the threshold with mixed literal types
- **THEN** the remote SQL omits it and the query returns the same rows

#### Scenario: File connectors are unaffected
- **WHEN** a Hive or file scan receives dynamic filters
- **THEN** it receives the same `filters` as before this change, regardless of `dynamic_filters_start`

## MODIFIED Requirements

### Requirement: Observable remote SQL
Each remote query the framework issues SHALL be logged at `debug` level on
the `arneb::rdbms` tracing target, with the catalog name, the partition index
and the full SQL text. The same event SHALL carry the counts `dynamic_pushed`
(dynamic-filter conjuncts pushed exactly), `dynamic_compacted` (dynamic lists compacted to
a range) and `dynamic_skipped` (dynamic conjuncts not pushed), all zero when the
scan has no dynamic filters. Credentials SHALL never appear in that log or in
error messages.

#### Scenario: Remote SQL is logged
- **WHEN** `RUST_LOG=arneb::rdbms=debug` is set and a PostgreSQL table is scanned
- **THEN** one log event per partition contains the generated SQL

#### Scenario: Dynamic-filter conjuncts are counted
- **WHEN** a PostgreSQL probe scan receives one dynamic filter that is pushed exactly and one that is compacted
- **THEN** its log event reports `dynamic_pushed=1`, `dynamic_compacted=1`, `dynamic_skipped=0`
