# G6 stage 2b-2: Metabase over pgwire — design

Status: draft, 2026-11-21. Part of the production-ready v0.2.x gates in
`2026-10-16-phase3-roadmap-design.md` ("G6 stage 2b"). Builds on stage 2b-1
(`2026-11-21-g6-stage-2b1-pg-reflection-design.md`, PRs #275/#276):
arrays, `= ANY`, `unnest`, `ProjectSet`, correlated `SELECT` subqueries and
the queryable `pg_catalog`.

## Intent

A team replacing Trino points Metabase at Arneb with Metabase's built-in
PostgreSQL driver. Today that fails at the first step: Metabase's sync finds
**0 tables**, so nothing in the UI works, although native SQL questions do
run. This change makes a fresh Metabase (LTS v0.64.8.3, pgJDBC) sync an
Arneb database and answer the common query-builder questions. As in 2b-1 it
does this by adding the engine and catalog features those statements need,
not by recognising Metabase's queries.

## Evidence: what fails today

Measured 2026-11-21 on `feat/g6-2b-pg-reflection` (5ff7898), with Arneb as
1 coordinator + 2 workers on the TPC-H SF1 `file` tables of the client smoke
cluster (schema `default`), and Metabase v0.64.8.3 in Docker.

**Method.** The same Metabase was also pointed at a PostgreSQL 17 with
`log_statement=all`. That PostgreSQL held the same schema (DDL generated
from Arneb's `information_schema.columns`, a few rows loaded). The probe ran
a full sync and 25 query-builder questions against it. Every statement
pgJDBC and Metabase sent was taken from the log, with bind parameters
inlined, and replayed on Arneb.
- Against PostgreSQL the sync completes (8 tables) and every question
  passes.
- Against Arneb the sync aborts in `sync-tables`, with "The column name
  TABLE_SCHEM was not found in this ResultSet".
- The corpus is 79 distinct statements, with setup DDL removed and session
  ids and query hashes masked. It is in
  `docs/superpowers/specs/2026-11-21-g6-stage-2b2/metabase-captured.sql`,
  separated by `;;;;` lines.

Of the 79 statements, 64 run on Arneb today. Some of those 64 succeed only
through the `pg_catalog` fallback, with the wrong shape:
- 3× JDBC `getSchemas`: the regex `!~` operator, then
  `(current_schemas(true))[1]`.
- 16× JDBC `getPrimaryKeys`:
  `(information_schema._pg_expandarray(con.conkey)).n`.

The rest fail. Replaying with `!~` and `INTERVAL 'n unit'` rewritten by hand
shows the next layer. Those layers are combined here:

| Step | Statement (corpus) | Missing on Arneb |
|---|---|---|
| sync-tables | `pg_class ⋈ pg_namespace ⟕ pg_description ⟕ pg_stat_user_tables` | `!~`; table `pg_stat_user_tables`; `int = '0'` and `NULLIF(int8, '0')` (untyped string literal against a number) |
| sync-fields (×8 tables) | `information_schema.columns ⟕ (table_constraints ⋈ key_column_usage)` `UNION ALL` pg_catalog part | `!~`; `information_schema.*` not queryable in the engine (only intercepted by substring, and the quoted form `"information_schema"."columns"` is not intercepted); `format()` with `%s` and `%I`; `col_description(oid, int)` |
| sync enums | `… WHERE t.oid IN (SELECT enumtypid FROM pg_enum)` | table `pg_enum` |
| sync-fks | `pg_constraint ⋈ pg_attribute … attnum = ANY(c.conkey)`, `'f'::char` | `!~`; `pg_constraint.conkey` / `confkey` |
| JDBC getSchemas | `nspname !~ '^pg_temp_' OR nspname = (current_schemas(true))[1]` | `!~`, `current_schemas(bool)`, array subscript |
| JDBC getPrimaryKeys | `(information_schema._pg_expandarray(con.conkey)).n`, `(result.KEYS).x` | `_pg_expandarray` (a set-returning function yielding `(x, n)`), composite field access `(expr).field` |
| "count by week" | `DATE_TRUNC('week', d + INTERVAL '1 day') + INTERVAL '-1 day'` | the PostgreSQL interval literal `INTERVAL '1 day'` (today only `INTERVAL '1' DAY`) |
| relative date filter | `d >= DATE_TRUNC('year', NOW() + INTERVAL '-30 year')` | interval units other than DAY; timestamp ± interval (`now() + INTERVAL '1' DAY` is a type error today) |
| median / percentile | `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` | ordered-set aggregates (2b-1 refuses `WITHIN GROUP`) |
| datetime-diff (days) | `CAST('2000-01-01' AS DATE) - CAST(d AS DATE)` | `date - date` (fails at runtime: Duration cast to Date32) |

Notes on the evidence:
- These checks pass today: native SQL, table preview, count/sum/avg/min/max/
  stddev, distinct count, group by month/year/quarter-of-year/day-of-week,
  date ranges, `contains`/`starts-with`, binning, custom expressions,
  explicit joins, nested questions, `CASE`, string functions, field values
  (`GROUP BY … ORDER BY … LIMIT`), the JDBC privileges query
  (`has_any_column_privilege`), and fingerprinting
  (`SELECT … SUBSTRING(col, 1, 1234) … LIMIT 10000`).
- The replay sent each statement through the simple-query protocol with
  parameters inlined. Metabase itself uses the extended protocol, with
  pgJDBC server-prepared statements after 5 executions. The end-to-end run
  (Testing, below) covers the extended path.
- More layers may be hiding behind the first error of each statement. The
  acceptance test is the whole corpus, not this table.

## Goals

1. A fresh Metabase v0.64.8.3 syncs an Arneb database. On the 1+2 cluster,
   all 8 TPC-H tables and all 61 columns appear, with the same Metabase
   `base_type` and the same `database_type` as against PostgreSQL holding
   the same DDL.
2. **No fallback.** Every statement in the corpus runs in the engine, with
   `arneb_pg_catalog_fallbacks_total` unchanged.
3. The metadata statements in the corpus return the same rows as
   PostgreSQL, modulo oids and row order. This covers sync-tables,
   sync-fields, enums, FKs, getSchemas and getPrimaryKeys. The FK, enum and
   PK results are empty on both, because Arneb has no constraints.
4. The 25 probe questions and native SQL with a `{{variable}}` parameter
   pass end to end against Arneb, with the same results as DuckDB on SF1
   where the result is data.
5. These features are general engine features, documented, and usable in
   user SQL:
   - regex operators `~`, `!~`, `~*`, `!~*`;
   - PostgreSQL interval literals and non-DAY units;
   - timestamp ± interval;
   - `date - date`;
   - `percentile_cont` / `percentile_disc … WITHIN GROUP`;
   - `format()`;
   - array subscripts;
   - composite field access;
   - untyped string literals compared with numbers.

## Non-goals

- Metabase features beyond the probe:
  - writes (actions, uploads, model persistence);
  - "database routing";
  - Metabase's own SQL editor autocomplete.
- PostgreSQL POSIX regex syntax beyond what Rust's `regex` crate accepts:
  back-references and lookaround are refused with a clear error.
- A general interval type. Intervals stay a parse-time construct; see
  Design §3.
- Real table statistics: `pg_stat_user_tables.n_live_tup` is 0.
- Retiring the `pg_catalog` fallback or the substring interception of
  `information_schema`. Both stay as the net for statements the engine
  cannot run. The engine is tried first.
- The pre-existing `bind_parameters` `$N` injection and the binary result
  format. Both are tracked separately. pgJDBC gets text results, which it
  handles.

## Anti-goals (failure even if the tests pass)

- Matching Metabase's SQL text: any `if sql.contains("metabase")` style
  routing.
- Changing a TPC-H plan. `EXPLAIN` q1–q22 must stay byte-identical, in
  particular for `date ± INTERVAL 'n' DAY`, which must keep today's
  constant-folded form.
- Making an untyped string literal compare as a number where PostgreSQL
  would refuse it: a column against a column, or a non-numeric string. The
  coercion is for literals only.
- An `information_schema` engine table that shows objects the session's
  access view hides. The rule is the same as for `pg_catalog`.

## Design

### 1. Regex operators (sql-parser → planner)

- sqlparser yields `PGRegexMatch`, `PGRegexNotMatch`, `PGRegexIMatch` and
  `PGRegexNotIMatch`.
- These map onto the existing `regexp_like`:
  - `~` → `regexp_like(a, p)`;
  - `~*` → the case-insensitive form, `(?i)` prepended to a constant
    pattern, or concatenated for a non-constant one;
  - `!~` / `!~*` → `NOT` of the above.
- NULL in either operand gives NULL.
- Patterns with constructs Rust's `regex` rejects fail with an error that
  names the operator.

### 2. Untyped string literals against numbers (planner)

PostgreSQL types a quoted literal as `unknown` and resolves it from the
other operand. Arneb does this for dates already, and Metabase relies on it
for numbers (`objsubid = '0'`, `NULLIF(n_live_tup, '0')`).

- A string **literal** compared with, or `NULLIF`/`COALESCE`d with, an
  integer, decimal or float expression is cast to that type at plan time.
- A literal that does not parse as a number raises PostgreSQL's error,
  `invalid input syntax for type bigint: "x"`.
- Columns, and other non-literal strings, still raise today's type
  mismatch.

### 3. Intervals (sql-parser, planner)

- **Literal forms accepted:**
  - `INTERVAL 'n' UNIT`;
  - `INTERVAL 'n unit'` and `INTERVAL '-n units'`, with a signed integer
    and a singular or plural unit;
  - for units `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`,
    `second`.
  - Multi-part strings (`'1 day 2 hours'`) are refused clearly.
- **`date ± INTERVAL 'n' DAY`** keeps today's lowering, so TPC-H plans do
  not change.
- **Every other `x ± INTERVAL`** is rewritten when the AST is built into
  `date_add('<unit>', ±n, x)`. Arneb's `date_add` already handles dates and
  timestamps with Trino semantics: month ends clamp, so `2000-01-31` minus
  1 month is `1999-12-31`. The rewrite covers:
  - other units on a date;
  - any unit on a timestamp, or on an expression of unknown type such as
    `NOW()`.

  The builder picks where the "is it a DATE?" decision lives. It needs the
  operand's type, so the rewrite probably belongs in the planner, not the
  parser.
- **Result types:** these follow `date_add`, not PostgreSQL. This is the
  builder's call.
  - A DATE with a unit of year, quarter, month, week or day stays DATE.
    That includes both spellings `INTERVAL '1 day'` and `INTERVAL '1' DAY`,
    which go through one lowering path.
  - A DATE with hour, minute or second becomes TIMESTAMP.
  - A TIMESTAMP stays TIMESTAMP.
  - PostgreSQL would return TIMESTAMP for every `date + interval`. Metabase
    wraps each such expression in `CAST(… AS date)`, as the "count by week"
    statement does, so its results are the same either way.
- `timestamp - timestamp` and `timestamp - date` stay unsupported; they are
  non-goals.
- An interval anywhere other than an operand of `+` or `-` stays
  unsupported.

### 4. `date - date` (planner/execution)

- Returns `INTEGER` days, as PostgreSQL does (Trino returns an interval;
  this is a pgwire-facing choice).
- Typed in `function_return_type`/the binary-operator typing, so that
  `ProjectionExec` and the planner agree.

### 5. Ordered-set aggregates (planner, execution)

- `percentile_cont(f) WITHIN GROUP (ORDER BY x)` and `percentile_disc(f) …`
  are added, with one sort key and a constant fraction `f` in [0, 1].
  - The ORDER BY may be ASC or DESC. DESC sorts the values descending and
    then applies the same rule, as PostgreSQL does. It is not computed as
    `1 - f`, which differs for `percentile_disc` on ties.
  - `percentile_cont` interpolates linearly and returns DOUBLE.
  - `percentile_disc` returns the value type.
  - NULLs are ignored. An empty group gives NULL.
- They are single-phase, like `array_agg ORDER BY`: values are buffered per
  group and never split into partial and final.
- Memory is accounted like `array_agg`'s, with the same follow-up caveat.
- `WITHIN GROUP` on any other aggregate stays refused.
- Arrays of fractions (`percentile_cont(ARRAY[...])`) are refused.

### 6. Arrays and composites

- **Subscript:** `expr[i]` on a `List` is 1-based and gives NULL when out of
  range or for a NULL array. A constant or column index is allowed. Slices
  `[a:b]` are refused.
- **`current_schemas(include_implicit bool)` → `VARCHAR[]`:** the session's
  search path, with `pg_catalog` first when the argument is true, matching
  PostgreSQL's ordering.
- **`information_schema._pg_expandarray(anyarray)`:** a set-returning
  function, a new `SrfKind` in `ProjectSet`. It yields
  `STRUCT(x elem, n INT4)` per element; `n` is 1-based.
- **Composite field access:** `(expr).field` on a `Struct` expression is a
  new plan expression, or a get-field function; the builder picks.
  - It also works through a column of struct type in an outer query, as in
    `(result.KEYS).x`.
  - Unknown field names are a plan-time error.
- Struct values over pgwire are outside this change: getPrimaryKeys
  selects them only inside the subquery. A struct reaching the client is
  refused with a clear error, unless it's already supported.

### 7. Catalog surface (crates/protocol)

**`pg_catalog` additions.** Column lists use the `TABLES` notation in
`pg_catalog.rs`: a trailing `#` is BIGINT and `?` is BOOLEAN, `[]` makes an
array, and a column with no marker is VARCHAR.
- `pg_stat_user_tables`: `relid#`, `schemaname`, `relname`, `n_live_tup#`
  (0). One row per visible table.
- `pg_enum`: `oid#`, `enumtypid#`, `enumsortorder#`, `enumlabel`. Always
  empty.
- `pg_constraint`: adds `conkey[]#` and `confkey[]#`.

**Functions:**
- `format(fmt, args…)` with `%s`, `%I`, `%L` and `%%`.
  - `%s` of NULL is the empty string; `%I` of NULL is an error; `%L` of
    NULL is `NULL`.
  - Positional (`%1$s`) and width forms are refused.
  - It is a general scalar function, registered with its return type
    (`planner_and_registry_return_types_agree`).
- `col_description(oid, int)` and `obj_description(oid [, name])` return
  NULL VARCHAR. Arneb has no comments. If a future change fills
  `pg_description`, these should read it.

**`information_schema` in the engine:**
- It becomes a second schema in the per-statement memory catalog, next to
  `pg_catalog`, built from the same access view.
- Tables and their PostgreSQL column names:
  - `schemata`;
  - `tables`;
  - `columns`, including at least `table_catalog`, `table_schema`,
    `table_name`, `column_name`, `ordinal_position`, `column_default`,
    `is_nullable`, `data_type`, `udt_catalog`, `udt_schema`, `udt_name`,
    `is_identity`, `is_generated`, the length/precision/scale columns, and
    `character_maximum_length`;
  - `views` (empty);
  - `table_constraints` (empty);
  - `key_column_usage` (empty).
- `udt_schema` is `pg_catalog` and `udt_name` is the `pg_type.typname` used
  in `pg_attribute`. Metabase's `database_type` comes from these, and goal 1
  needs them to match PostgreSQL.
- **Routing:** a statement that names an `information_schema` table, in any
  quoting, runs in the engine first, the same way a `pg_catalog` statement
  does today.
  - "Names" is the same word-split test as `references_pg_catalog` in
    `crates/protocol/src/pg_catalog.rs`. Quotes and dots are separators, so
    `"information_schema"."columns"` matches.
  - The test is: the word `information_schema`, followed by one of its
    table names.
  - The memory catalog is still built only for statements that match, so
    other statements cost nothing extra. If the engine fails, the existing substring handlers
  (`handle_info_tables/columns/schemata`) answer as before, and that counts
  as a fallback. This order means dbt, DBeaver and the existing
  `information_schema` smoke get correct `WHERE` filtering instead of the
  whole table. Their current tests must still pass.
- The Trino protocol's virtual `information_schema` is a separate code path
  and is not touched.

### 8. Tests and tooling

**Tier 1 (CI):**
- A new test binary `crates/protocol/tests/metabase_corpus.rs`.
  - It loads the corpus file.
  - It runs each statement against a coordinator with in-memory TPC-H-shaped
    tables in schema `default`.
  - It asserts that no statement errors and that the fallback counter does
    not move. It is a separate binary because the counter is per process,
    as in 2b-1.
- **The golden rows** for the 35 metadata statements in the corpus are in
  `docs/superpowers/specs/2026-11-21-g6-stage-2b2/metabase-pg17-expected.json`.
  - They were dumped from the PostgreSQL 17 capture instance: same DDL,
    `nation` loaded, 2 rows in `orders`, 1 in `customer`.
  - `index` is the 0-based position of the statement in the corpus.
  - The test compares Arneb's result for each statement with them.
  - It masks columns that hold oids, and compares rows order-insensitively
    unless the statement has `ORDER BY`.
  - Some columns differ for real reasons (estimated row counts, or
    sizes/typmods PostgreSQL derives that Arneb doesn't have). Each of
    those is listed in the test with the reason, never silently skipped.
  - The test's memory tables must have the same rows as the capture
    instance for the data-dependent statements, or those statements are
    compared on columns only.
- Each feature in §1–§7 gets its own unit or integration tests in its
  crate. That includes the Review Focus inputs the plan names, for example:
  - a NULL pattern;
  - `INTERVAL '-1 months'`;
  - a month-end clamp;
  - `percentile_cont` over an empty group, a single row, and DESC;
  - subscript 0 and negative subscripts;
  - `format('%I', 'Mixed Case')`, which gives `"Mixed Case"`.

**Tier 2 (manual, documented):**
- `scripts/client-smoke/metabase/` holds:
  - `compose.yml`: Metabase v0.64.8.3, plus a PostgreSQL 17 capture
    instance with `log_statement=all`. It runs under compose project
    `arneb-mb`, on ports `127.0.0.1:13300` and `127.0.0.1:15499`.
  - `probe.py`, stdlib only: setup, add the database, wait for sync,
    report the metadata, run the questions, x-ray.
  - `capture.py`, which turns a PostgreSQL log into a corpus file and
    replays it on Arneb.
  - A `README.md` with the run steps: it starts against the cluster from
    `cluster.sh start`.
- Not in CI. The image is about 1.4 GB, and a run takes minutes of JVM
  start-up and sync. This is the builder's call, and the user can overrule
  it. The corpus test is the CI guard.
- The Tier 2 run is the acceptance for goals 1 and 4: the probe's output
  against Arneb matches its output against PostgreSQL. Data results are
  compared with DuckDB (`tpch.duckdb`).

**Regression:**
- The full workspace tests.
- `scripts/client-smoke/run.sh`, all steps.
- TPC-H `EXPLAIN` q1–q22 byte-identical to the 2b-1 branch.

### 9. Docs

- `docs/guide/clients.md`: a Metabase section covering the driver, the
  connection settings (SSL off unless TLS is set up), what works, and the
  known gaps.
- `docs/sql/expressions.md`: regex operators, intervals, date subtraction,
  subscripts and field access.
- `docs/sql/functions.md`: `format`, `percentile_cont`/`percentile_disc`,
  `current_schemas`, `col_description`/`obj_description`.
- The roadmap status line: G6 stage 2b done.

## Delivery

- Branch `feat/g6-2b2-metabase`, stacked on `feat/g6-2b-pg-reflection`
  (#276); it is rebased when those merge.
- One PR if it lands below about 5k lines. Otherwise:
  - (a) engine features §1–§6;
  - (b) catalog surface, corpus test, tooling and docs §7–§9.

## Left to the builder

- Where the interval rewrite and the literal coercion live (parser vs
  analyzer), within the anti-goals.
- The `(expr).field` representation.
- The exact `information_schema.columns` column list beyond the minimum
  above.
- The `percentile_*` buffer layout.
- Whether `format()` lives in `functions/pg_catalog.rs` or the string
  functions.

### Calls made in this design (not yet confirmed by the user)

- Metabase end to end is a manual Tier 2 script; CI runs only the corpus
  test.
- Exact `percentile_cont`/`percentile_disc` are in scope, because Metabase
  "Median" and "Percentile" are core aggregations.
- `date - date` returns an integer number of days (PostgreSQL), not an
  interval (Trino).
- `information_schema` goes to the engine first, with the old handlers as
  the fallback.
- Metabase LTS v0.64.8.3 is the pinned version.
