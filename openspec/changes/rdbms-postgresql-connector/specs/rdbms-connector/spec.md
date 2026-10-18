## ADDED Requirements

### Requirement: Dialect abstraction
The `arneb-rdbms` crate SHALL define a `Dialect` trait that owns everything
specific to one remote database's SQL. It SHALL include identifier quoting,
literal rendering per Arneb `ScalarValue`, translation of supported binary
operators, and `LIKE` rendering. The query builder SHALL NOT embed any
database-specific syntax outside a `Dialect` implementation.

#### Scenario: Identifiers are quoted by the dialect
- **WHEN** a table `Order Items` with a column `select` is scanned through a dialect whose quote character is `"`
- **THEN** the generated SQL references `"Order Items"` and `"select"`, and an embedded `"` in an identifier is doubled

#### Scenario: String literals are escaped by the dialect
- **WHEN** a pushed filter compares a column with the string literal `O'Brien`
- **THEN** the generated SQL renders the literal as `'O''Brien'`

### Requirement: Filter pushdown with safe fallback
The framework SHALL split each pushed-down filter into its top-level `AND`
conjuncts and translate each one independently. It SHALL translate
comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`) between a column and a literal,
`IS NULL` / `IS NOT NULL`, `IN` / `NOT IN` lists of literals, `BETWEEN` with
literal bounds, `LIKE` / `NOT LIKE` with a literal pattern, and `AND` / `OR` /
`NOT` over translatable operands. A conjunct that contains anything else
(function calls, casts, arithmetic, subqueries, parameters, column-to-column
comparisons of differing types) SHALL be omitted from the remote `WHERE`
clause and never cause the query to fail. Arneb's own `FilterExec` above the
scan SHALL keep evaluating the full predicate, so results are identical
whether or not a conjunct was pushed.

#### Scenario: Supported conjuncts are pushed
- **WHEN** a query filters `WHERE n_regionkey = 1 AND n_name LIKE 'A%'`
- **THEN** the remote SQL contains both conditions in its `WHERE` clause

#### Scenario: Unsupported conjunct is kept local
- **WHEN** a query filters `WHERE upper(n_name) = 'CHINA' AND n_regionkey = 2`
- **THEN** the remote SQL's `WHERE` clause contains only the `n_regionkey` condition, and the query returns exactly the rows a full scan plus local filtering would return

#### Scenario: OR with one untranslatable branch is kept local entirely
- **WHEN** a query filters `WHERE n_regionkey = 1 OR upper(n_name) = 'CHINA'`
- **THEN** that conjunct is not pushed, and no remote `WHERE` condition is generated from it

#### Scenario: Text range comparison is kept local
- **WHEN** a query filters `WHERE n_name < 'C'` on a VARCHAR column
- **THEN** the conjunct is not pushed, because remote collation order can differ from Arneb's byte order

#### Scenario: NULL literal comparison is not pushed
- **WHEN** a conjunct compares a column to a NULL literal with `=`
- **THEN** it is not pushed (Arneb evaluates it locally with SQL three-valued logic)

### Requirement: Projection pushdown
When the scan context carries a projection, the remote `SELECT` list SHALL
contain exactly the projected columns in the projected order, and the
emitted record batches SHALL have exactly those columns in that order. With no
projection, all table columns SHALL be selected in table order. An empty
projection SHALL select no columns but still return the correct row count.

#### Scenario: Only projected columns are fetched
- **WHEN** a query reads `SELECT n_name FROM pg.public.nation`
- **THEN** the remote SQL selects only `n_name`

#### Scenario: Projection order is preserved
- **WHEN** the scan projection is `[2, 0]`
- **THEN** the batches have column 2 first and column 0 second

### Requirement: Limit pushdown only when exact
The framework SHALL append a remote `LIMIT n` only when the scan context
carries a limit and every filter conjunct in the scan context was
translated. If any conjunct stayed local, it SHALL NOT push the limit. Arneb's
`LimitExec` above the scan SHALL still apply the final limit.

#### Scenario: Limit with no filter is pushed
- **WHEN** a query reads `SELECT * FROM pg.public.nation LIMIT 5`
- **THEN** each remote query carries `LIMIT 5` and the result has 5 rows

#### Scenario: Limit with an untranslated filter is not pushed
- **WHEN** a scan has a limit of 5 and a filter conjunct that could not be translated
- **THEN** the remote SQL has no `LIMIT`

### Requirement: Primary-key range splits
The framework SHALL split a table that has a single-column integer primary key
and an estimated row count of at least `split_min_rows`, exposing
`min(max_splits, ceil(estimated_rows / split_min_rows))` partitions. Each
partition SHALL read a disjoint half-open key range, and the ranges together
SHALL cover `[min(pk), max(pk)]`. The first and last ranges SHALL be open-ended,
so rows inserted outside the sampled bounds are still read exactly once. Rows
with a NULL key cannot exist under a primary key and need no range. Any other
table SHALL be read as a single partition. Split bounds SHALL be computed once
when the table's data source is created on the coordinator and SHALL travel
with the plan, so every node scans the same ranges.

#### Scenario: Large integer-keyed table is split
- **WHEN** a table with `PRIMARY KEY (id bigint)` has an estimated 1,000,000 rows, `split_min_rows = 100000` and `max_splits = 8`
- **THEN** the data source exposes 8 partitions whose key ranges are disjoint and together cover every row

#### Scenario: Small or non-integer-keyed table is not split
- **WHEN** a table has fewer than `split_min_rows` estimated rows, or a composite / non-integer / missing primary key
- **THEN** the data source exposes 1 partition

#### Scenario: Splits return every row exactly once
- **WHEN** a split table is fully scanned across all partitions
- **THEN** the union of the partitions' rows equals the table's rows, with no duplicates and none missing

### Requirement: Observable remote SQL
Each remote query the framework issues SHALL be logged at `debug` level on
the `arneb::rdbms` tracing target, with the catalog name, the partition index
and the full SQL text. Credentials SHALL never appear in that log or in error
messages.

#### Scenario: Remote SQL is logged
- **WHEN** `RUST_LOG=arneb::rdbms=debug` is set and a PostgreSQL table is scanned
- **THEN** one log event per partition contains the generated SQL

### Requirement: Read-only remote catalogs
RDBMS catalogs SHALL be read-only in this change. A DDL or DML statement
targeting an RDBMS catalog SHALL fail with an error stating that the catalog is
read-only. It SHALL NOT send any statement to the remote database.

#### Scenario: INSERT is rejected
- **WHEN** a client runs `INSERT INTO pg.public.nation VALUES (...)`
- **THEN** the statement fails with a read-only catalog error and no remote write occurs
