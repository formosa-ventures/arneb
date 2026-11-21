# G6 stage 2b-2: Metabase over pgwire Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fresh Metabase v0.64.8.3 (pgJDBC) syncs an Arneb database and answers the common query-builder questions over pgwire, with no `pg_catalog` fallback.

**Architecture:**
- General engine features:
  - regex operators;
  - literal coercion;
  - intervals and `date - date`;
  - ordered-set percentiles;
  - array subscripts and composite field access.
- A wider queryable catalog: more `pg_catalog`, plus `information_schema` in the same per-statement memory catalog, tried before the old fixed handlers.
- A Tier 1 test replays the captured Metabase corpus and checks it against PostgreSQL 17 golden rows.
- A manual Tier 2 Metabase script, documented.

**Tech Stack:** Rust 1.89, sqlparser 0.64, Arrow 60, pgwire 0.42, tokio-postgres (tests), Python stdlib (Tier 2 scripts), Docker Compose (Tier 2 only).

**Spec:** `docs/superpowers/specs/2026-11-21-g6-stage-2b2-metabase-design.md`. Read it with this plan; the evidence table and anti-goals are binding.

## Global Constraints

- **Branch:** `feat/g6-2b2-metabase`, stacked on `feat/g6-2b-pg-reflection`. Never touch `main`.
- **No SQL-text routing on client identity.** No `contains("metabase")`, and no matching Metabase's queries. All features are general.
- **TPC-H plans stay put.** `EXPLAIN` q1–q22 is byte-identical to the base commit 3a04ece. In particular, `date ± INTERVAL 'n' DAY` and the parse-time constant fold of `DATE 'x' ± INTERVAL` (`try_fold_date_interval`, `crates/sql-parser/src/convert.rs`) keep today's output.
- **Literal coercion is narrow.** It applies only to string *literals* against numeric expressions. A column against a column, and non-numeric strings, keep today's error.
- **`information_schema` rows follow the session's access view,** as `pg_catalog` does.
- **Functions are registered fully.** Every new scalar function is registered in `crates/execution/src/functions/registry.rs` and typed in `function_return_type` (`crates/planner/src/analyzer/mod.rs`). `planner_and_registry_return_types_agree` must pass.
- **Separate test binaries.** Tests that assert the fallback counter does not move live in their own test binary, because the counter is per process.
- **`PlanExpr` size.** `plan_expr_stays_small` asserts ≤112 bytes; new variants box their payloads. New walker arms are `#[inline(never)]`, as in 2b-1.
- **Pinned versions:** Metabase `metabase/metabase:v0.64.8.3`, capture PostgreSQL `postgres:17`.
- **Tier 2 isolation:**
  - Compose project `arneb-mb`, on ports `127.0.0.1:13300` (Metabase) and `127.0.0.1:15499` (capture PostgreSQL).
  - Never touch other Docker containers.
- **Commits:** Conventional Commits. Each commit message ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01WwNmnQCEk3V7MvZCQsscN4
  ```
- **Gate:** `cargo fmt -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` pass at the end of every task.

## Review Focus

1. **A NULL operand or a bad pattern to a regex operator.**
   - `NULL ~ 'a'` and `'a' ~ NULL` give NULL.
   - `'a' ~ '(?<=a)b'` (lookbehind) is an error naming the operator, not a panic.
   - Owner: Task 1.
2. **Month-end clamping and plural or negative interval strings on a column, not a literal.**
   - `d - INTERVAL '1 month'` with `d = 2000-03-31` gives `2000-02-29`.
   - `INTERVAL '-2 months'` is accepted.
   - `INTERVAL '1 day 2 hours'` is a clear error.
   - Owner: Task 3.
3. **Degenerate groups for percentiles:**
   - an empty group gives NULL;
   - a single row gives that value;
   - all NULLs give NULL;
   - DESC ordering;
   - a fraction outside [0, 1] is an error.
   - Owner: Task 4.
4. **Subscripts at the edges.**
   - `arr[0]`, `arr[-1]` and `arr[len+1]` give NULL.
   - A NULL array or a NULL index gives NULL.
   - Owner: Task 5.
5. **A table the session may not see** is absent from `information_schema.columns` and `pg_stat_user_tables`, and so from Metabase's sync too. Owner: Task 7. Extend `crates/protocol/tests/access_control.rs`.

---

### Task 1: Regex operators `~ !~ ~* !~*`

**Files:**
- Modify: `crates/sql-parser/src/convert.rs`, in the `sp::Expr::BinaryOp` arm near the `StringConcat` lowering (~line 511).
- Test: unit tests in `crates/sql-parser/src/convert.rs` `mod tests`.
- Test: create `crates/protocol/tests/metabase_features.rs` as an end-to-end file for Tasks 1–6. Base it on `crates/protocol/tests/arrays.rs`: memory tables under `file.default`, and `simple_query` assertions.

**Interfaces:**
- Produces: the AST lowering only, with no new types.
  - `a ~ p` → `Function{name:"regexp_like", args:[a,p]}`.
  - `a ~* p` → `regexp_like(a, CONCAT('(?i)', p))`. When `p` is a string literal, the prefix is folded into the literal.
  - `!~` and `!~*` → `UnaryOp NOT` around the above.

- [ ] **Step 1: Write failing tests.**
  - Parser unit test `regex_operators_lower_to_regexp_like`. It asserts the four lowerings above on `a ~ 'x'`, `a ~* 'x'` (literal `'(?i)x'`), `a !~ 'x'` and `a !~* b` (`CONCAT`).
  - In `metabase_features.rs`, test `regex_operators`, over `file.default.words(w VARCHAR)` with the rows `'pg_toast'`, `'Public'`, `'default'`:
    - `SELECT w FROM words WHERE w !~ '^pg_' ORDER BY w` → `Public`, `default`.
    - `SELECT count(*) FROM words WHERE w ~* '^public$'` → `1`.
    - `SELECT NULL ~ 'a', 'a' ~ NULL` → `NULL`, `NULL`.
    - `SELECT 'ab' ~ '(?<=a)b'` → error containing `~`.
- [ ] **Step 2:** `cargo test -p arneb-sql-parser regex_operators` and `cargo test -p arneb-protocol --test metabase_features regex` fail with "unsupported feature: binary operator".
- [ ] **Step 3: Implement the lowering.**
  - If `regexp_like`'s error on an invalid pattern does not mention the operator, catch it at plan time for constant patterns: the error should read `invalid regular expression in ~: …`.
  - A non-constant pattern keeps `regexp_like`'s own runtime error.
- [ ] **Step 4:** Both test commands pass.
- [ ] **Step 5:** Commit `feat(sql): PostgreSQL regex match operators`.

### Task 2: Untyped string literals against numbers

**Files:**
- Modify: `crates/planner/src/analyzer/type_coercion.rs`: `unify_binary_operands` (~line 625), plus the `NULLIF`/`COALESCE` unification. `COALESCE` is in `unify_case`, or wherever it lives; the implementer finds it.
- Test: `crates/planner/src/analyzer/type_coercion.rs` `mod tests`, and `crates/protocol/tests/metabase_features.rs`.

**Interfaces:**
- Produces: `fn coerce_string_literal_to_numeric(lit: &str, target: &DataType) -> Result<ScalarValue, PlanError>`, private to `type_coercion.rs`.
  - It parses the literal into `target`: Int8/16/32/64, Decimal, or Float.
  - On a parse failure it returns `PlanError` with the message `invalid input syntax for type <pg type name>: "<lit>"`, where the pg name is `bigint`, `integer`, `smallint`, `numeric` or `double precision`.

- [ ] **Step 1: Write failing tests.**
  - Unit test `string_literal_coerces_to_numeric_operand`: an `Int64` column `= Literal(Utf8 "0")` unifies to `Literal(Int64 0)`.
  - Unit test `string_literal_coercion_is_literal_only`: a `Utf8` column `= Int64` column still errors with today's type mismatch.
  - In `metabase_features.rs`, `untyped_literal_numbers`:
    - `SELECT count(*) FROM t WHERE x = '2'` → `1`.
    - `SELECT NULLIF(x, '1') FROM t ORDER BY x` → `NULL`, `2`, `3`.
    - `SELECT count(*) FROM t WHERE x = 'abc'` → error containing `invalid input syntax for type bigint: "abc"`.
- [ ] **Step 2:** The new tests fail with `type mismatch: expected Int64, found Utf8`.
- [ ] **Step 3:** Implement it in the binary-comparison, arithmetic and `NULLIF`/`COALESCE` unification paths. The literal side only.
- [ ] **Step 4:**
  - `cargo test -p arneb-planner type_coercion` passes.
  - `cargo test -p arneb-protocol --test metabase_features untyped` passes.
  - `cargo test -p arneb-protocol --test small_integer_coercion` still passes.
- [ ] **Step 5:** Commit `feat(planner): resolve untyped string literals against numeric operands`.

### Task 3: Intervals and `date - date`

**Files:**
- Modify: `crates/sql-parser/src/convert.rs`.
  - Widen `convert_interval` (~line 980) to accept the `'n unit'` string form.
  - Teach `try_fold_date_interval` the same string form, so `DATE 'x' + INTERVAL '1 month'` still folds.
- Modify: the planner, at the spot that types `BinaryOp Plus/Minus`. The implementer finds it: `crates/planner/src/planner*` or `analyzer/type_coercion.rs`.
- Modify: execution, for `date - date`. Today it produces a Duration; it should produce INT32 days.
- Test: parser unit tests, `crates/protocol/tests/metabase_features.rs`, and the TPC-H `EXPLAIN` check (Step 4).

**Interfaces:**
- Produces: a new AST node `ast::Expr::Interval { value: i64, unit: IntervalUnit, span }`, with `pub enum IntervalUnit { Year, Quarter, Month, Week, Day, Hour, Minute, Second }` in `crates/sql-parser/src/ast.rs`.
- The planner lowers `x ± Interval{..}` by cases:
  - **`x` is DATE and the unit is Day:** today's Int32-days form, a `Literal(Int32 n)` added to the date. This keeps TPC-H plans identical.
  - **`x` is DATE and the unit is Year/Quarter/Month/Week:** `date_add('<unit>', ±n, x)`, which stays DATE.
  - **`x` is DATE and the unit is Hour/Minute/Second:** `date_add('<unit>', ±n, CAST(x AS TIMESTAMP))`, which gives TIMESTAMP.
  - **`x` is TIMESTAMP:** `date_add('<unit>', ±n, x)`, which gives TIMESTAMP.
  - **Anything else, including an `Interval` that is not an operand of `+`/`-`:** `PlanError::Unsupported("interval outside date/timestamp arithmetic")`.
  - **`INTERVAL + x`** is allowed only for `+`.
- `date - date` gives Int32 days.

- [ ] **Step 1: Write failing tests.**
  - Parser unit test `interval_string_forms`. It parses `'1 day'`, `'-30 year'`, `'2 months'`, `'1' DAY` and `'3' MONTH` into `Interval{value,unit}`, and refuses `'1 day 2 hours'` with `UnsupportedFeature` containing `interval`.
  - Parser unit test `date_literal_interval_string_still_folds`: `DATE '2000-01-31' + INTERVAL '1 month'` gives `Literal(Date32 2000-02-29)`.
  - In `metabase_features.rs`, `interval_arithmetic`, over `dt(d DATE)` with the rows `2000-03-31` and `1996-01-02`:
    - `SELECT d - INTERVAL '1 month' FROM dt ORDER BY d` → `1995-12-02`, `2000-02-29`.
    - `SELECT d + INTERVAL '1 day' FROM dt ORDER BY d` → `1996-01-03`, `2000-04-01`. The type is DATE (RowDescription OID 1082).
    - `SELECT CAST(d + INTERVAL '2 hours' AS VARCHAR) FROM dt ORDER BY d LIMIT 1` → `1996-01-02 02:00:00`.
    - `SELECT count(*) FROM dt WHERE d >= DATE_TRUNC('year', NOW() + INTERVAL '-40 year')` → `2` (holds until 2036).
    - `SELECT CAST(DATE_TRUNC('week', d + INTERVAL '1 day') AS DATE) + INTERVAL '-1 day' FROM dt ORDER BY d LIMIT 1` → `1995-12-31`, PostgreSQL's answer for Metabase's Sunday-week.
    - `SELECT CAST('2000-01-01' AS DATE) - d FROM dt ORDER BY d` → `1460`, `-90`. The type is INT4 (OID 23).
    - `SELECT INTERVAL '1 day'` → error containing `interval`.
- [ ] **Step 2:** The tests fail.
- [ ] **Step 3: Implement.** The `x ± Interval` rewrite needs `x`'s type, so it belongs in the planner, where expression types are known.
- [ ] **Step 4: Verify.**
  - The tests pass.
  - `cargo test -p arneb-sql-parser` passes.
  - The TPC-H `EXPLAIN` check is byte-identical. Build release, start `scripts/client-smoke/cluster.sh start <SF1 data> <run dir>`, run `EXPLAIN` for each of `benchmarks/tpch/queries/q*.sql` via psql into a file, and diff it against the same output from base commit 3a04ece, built in a separate `--target-dir`. Expected: an empty diff.
- [ ] **Step 5:** Commit `feat(engine): PostgreSQL interval literals, non-day units, timestamp arithmetic and date - date`.

### Task 4: `percentile_cont` / `percentile_disc … WITHIN GROUP`

**Files:**
- Modify: `crates/sql-parser/src/convert.rs`, at the `WITHIN GROUP` refusal ~line 1140. Allow it for these two names only, and carry the ORDER BY as the aggregate's sort key and argument.
- Modify: `crates/planner/src/planner.rs` and `crates/planner/src/plan.rs`. Type and validate the call; the planner marks it non-decomposable, as `array_agg ORDER BY` is.
- Modify: `crates/execution/src/aggregate.rs`. Add the accumulator, reusing the buffered-values pattern of `array_agg`.
- Test: `crates/execution/src/aggregate.rs` unit tests, and `crates/protocol/tests/metabase_features.rs`.

**Interfaces:**
- Produces: aggregate names `percentile_cont` and `percentile_disc`.
  - Argument 0 is the fraction, a constant Float64 in [0, 1].
  - The sort key is the single WITHIN GROUP expression, with its ASC/DESC.
- Return types:
  - `percentile_cont` returns Float64. The input must be numeric.
  - `percentile_disc` returns the input type.
- Algorithm. PostgreSQL's, which the tests pin:
  - Sort the non-NULL values by the key.
  - `percentile_disc`: the first value whose cumulative position `(i+1)/N >= f`.
  - `percentile_cont`: `rn = f*(N-1)`, then interpolate linearly between `floor(rn)` and `ceil(rn)`.

- [ ] **Step 1: Write failing tests** in `metabase_features.rs`, `ordered_set_percentiles`, over `t(x BIGINT)` with the rows 1, 2, 3, plus `e(x BIGINT)` with no rows:
  - `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t` → `2`.
  - `SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY x) FROM t` → `1.5`.
  - `SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM t` → `2`.
  - `SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY x DESC) FROM t` → `2.5`.
  - `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM e` → `NULL`.
  - `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t WHERE x = 3` → `3`.
  - `SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY x) FROM t` → error containing `between 0 and 1`.
  - `SELECT sum(x) WITHIN GROUP (ORDER BY x) FROM t` → error containing `WITHIN GROUP`.
  - The same median grouped by a key, `GROUP BY x % 2`, gives `2` for odd and `2` for even. This is checked on the 1+2 cluster in Task 9.
- [ ] **Step 2:** The tests fail with `WITHIN GROUP on percentile_cont`.
- [ ] **Step 3:** Implement the accumulator. NULL inputs are skipped; an empty or all-NULL group gives NULL.
- [ ] **Step 4:** `cargo test -p arneb-execution aggregate` and `cargo test -p arneb-protocol --test metabase_features percentile` pass.
- [ ] **Step 5:** Commit `feat(engine): percentile_cont and percentile_disc ordered-set aggregates`.

### Task 5: Array subscripts, `current_schemas`, `_pg_expandarray` and composite field access

**Files:**
- Modify:
  - `crates/sql-parser/src/convert.rs` and `ast.rs`: `sp::Expr::CompoundFieldAccess` with a subscript or an identifier access, in both forms `(expr)[i]` and `(expr).field`.
  - `crates/planner/src/plan.rs`: `PlanExpr::ArrayElement` and `PlanExpr::GetField`, each with a boxed payload; and `SrfKind::ExpandArray`.
  - The planner, which handles the `information_schema._pg_expandarray(arr)` call in a SELECT list as a ProjectSet SRF yielding a Struct.
  - `crates/execution`: evaluation of the expression arms, plus `ProjectSetExec` for `ExpandArray`.
  - `crates/execution/src/functions/pg_catalog.rs` and `registry.rs`: `current_schemas(BOOLEAN) -> VARCHAR[]`.
- Test: planner and execution unit tests, and `crates/protocol/tests/metabase_features.rs`.

**Interfaces:**
- Produces:
  - `PlanExpr::ArrayElement(Box<(PlanExpr, PlanExpr)>)`, with the array and a 1-based index. It returns the element type and gives NULL out of range.
  - `PlanExpr::GetField(Box<(PlanExpr, String)>)`. On an unknown field it raises the plan-time error `column "<f>" not found in data type`, as in PostgreSQL.
  - `SrfKind::ExpandArray`, which yields `DataType::Struct(vec![("x", elem), ("n", Int32)])`.
  - `current_schemas(include_implicit)`: the session's search path, with `pg_catalog` prepended when the argument is true. With the default path that is `{pg_catalog,default}`, and `{default}` for false.
  - The function needs the session search path. Pass it the way `current_schema()` gets it today; the implementer follows that path.
  - Every walker that matches on `PlanExpr` or `SrfKind` gets the new arms: `walk_plan_exprs`, `rewrite_expr_after_column_insert`, the worker register walker in `crates/server/src/task_manager.rs`, and the plan serializers. Grep for `SrfKind::GenerateSubscripts` and `PlanExpr::ArrayContains` to find them all.
  - Structs stay inside the plan. If a projected struct column reaches pgwire encoding unsupported, the error reads `composite values are not supported over pgwire`.

- [ ] **Step 1: Write failing tests** in `metabase_features.rs`, `array_subscripts_and_composites`:
  - `SELECT (ARRAY[10,20,30])[1], (ARRAY[10,20,30])[3], (ARRAY[10,20,30])[0], (ARRAY[10,20,30])[-1], (ARRAY[10,20,30])[4]` → `10`, `30`, NULL, NULL, NULL.
  - `SELECT (CAST(NULL AS BIGINT[]))[1], (ARRAY[1])[CAST(NULL AS BIGINT)]` → NULL, NULL.
  - `SELECT (pg_catalog.current_schemas(true))[1], (current_schemas(false))[1]` → `pg_catalog`, `default`.
  - `SELECT k.n, (k.e).x, (k.e).n FROM (SELECT information_schema._pg_expandarray(ARRAY[7,8]) AS e, 0 AS n) k ORDER BY 2` → rows `(0,7,1)`, `(0,8,2)`. This covers the JDBC getPrimaryKeys shape: an SRF in a subquery, with field access through an outer column.
  - `SELECT (information_schema._pg_expandarray(ARRAY[5])).n` → `1`.
  - `SELECT (information_schema._pg_expandarray(ARRAY[5])).zz` → error containing `zz`.
  - `SELECT (ARRAY[1,2])[1:2]` → error containing `slice`.
- [ ] **Step 2:** The tests fail with `unsupported feature: expression: (ARRAY[...])[1]`.
- [ ] **Step 3:** Implement it. Run `cargo test -p arneb-planner plan_expr_stays_small`; it must pass at ≤112 bytes.
- [ ] **Step 4:**
  - `cargo test -p arneb-protocol --test metabase_features array_subscripts` passes.
  - `cargo test -p arneb-planner` passes.
  - `cargo test -p arneb-protocol --test arrays` passes.
- [ ] **Step 5:** Commit `feat(engine): array subscripts, composite field access, _pg_expandarray and current_schemas`.

### Task 6: `pg_catalog` additions and catalog functions

**Files:**
- Modify: `crates/protocol/src/pg_catalog.rs`, in `TABLES` and the row builders:
  - add `("pg_stat_user_tables", &["relid#", "schemaname", "relname", "n_live_tup#"])`;
  - add `("pg_enum", &["oid#", "enumtypid#", "enumsortorder#", "enumlabel"])`;
  - add `"conkey[]#"` and `"confkey[]#"` to `pg_constraint`.
- Modify: `crates/execution/src/functions/pg_catalog.rs`, `registry.rs`, and `function_return_type` in `crates/planner/src/analyzer/mod.rs`:
  - `format(VARCHAR, ANY…) -> VARCHAR`;
  - `col_description(BIGINT, BIGINT) -> VARCHAR`;
  - `obj_description(BIGINT [, VARCHAR]) -> VARCHAR`.
- Test: `crates/protocol/tests/pg_catalog_surface.rs`, and unit tests next to `format`.

**Interfaces:**
- **`pg_stat_user_tables`:** one row per table that `pg_class` lists with `relkind = 'r'`. `relid` is the same oid as in `pg_class`, `schemaname` is its namespace, and `n_live_tup` is `0`.
- **`pg_enum`:** always empty.
- **`format`:**
  - `%s` gives the text of the argument; NULL gives the empty string.
  - `%I` gives the value as-is when it matches `^[a-z_][a-z0-9_$]*$` and is not a reserved word. Otherwise it is double-quoted, with `"` doubled. NULL is the error `null values cannot be formatted as an SQL identifier`.
  - `%L` gives a single-quoted literal with `'` doubled; NULL gives `NULL`.
  - `%%` gives `%`.
  - Too few arguments is the error `too few arguments for format()`.
  - `%1$s` and width forms are the error `unsupported format specifier`.
- **`col_description` / `obj_description`:** always NULL.

- [ ] **Step 1: Write failing tests.**
  - Unit test `format_specifiers`:
    - `format('%s.%s','a','b')` = `a.b`;
    - `format('%I','Mixed Case')` = `"Mixed Case"`;
    - `format('%I','abc')` = `abc`;
    - `format('%I','select')` = `"select"`;
    - `format('%L','it''s')` = `'it''s'`;
    - `format('%L',NULL)` = `NULL`;
    - `format('%s',NULL)` = the empty string;
    - `format('100%%')` = `100%`;
    - `format('%s')` → error containing `too few arguments`.
  - In `pg_catalog_surface.rs`:
    - `stat_user_tables_lists_visible_tables`: `SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY relname` gives one row per memory table, with `n_live_tup` 0.
    - `enum_is_empty`: `SELECT count(*) FROM pg_enum` gives `0`.
    - `constraint_key_arrays`: `SELECT conkey, confkey FROM pg_constraint` gives zero rows. `EXPLAIN` or `Describe` shows the columns' array types.
    - `col_description_is_null`: `SELECT col_description(CAST('nation'::regclass AS OID), 1)` gives NULL.
- [ ] **Step 2:** The tests fail.
- [ ] **Step 3:** Implement it. `%I`'s reserved words come from sqlparser's keyword list. Use `ALL_KEYWORDS`, or the reserved subset if sqlparser exposes one; the implementer picks and notes it.
- [ ] **Step 4:**
  - `cargo test -p arneb-protocol --test pg_catalog_surface` passes.
  - `cargo test -p arneb-execution format` passes.
  - `cargo test -p arneb-planner planner_and_registry_return_types_agree` passes.
- [ ] **Step 5:** Commit `feat(protocol): pg_stat_user_tables, pg_enum, constraint key arrays, format() and comment functions`.

### Task 7: `information_schema` in the engine

**Files:**
- Modify: `crates/protocol/src/pg_catalog.rs`.
  - Add an `INFO_SCHEMA_TABLES` spec list, in the same notation as `TABLES`.
  - Build an `information_schema` `MemorySchema` beside `pg_catalog` in `catalog_for`, from the same access view.
  - Extend `references_pg_catalog`, or add a sibling `references_information_schema` that it calls. The test: the word `information_schema` followed by a word that is an `INFO_SCHEMA_TABLES` name, after the same word split.
- Modify: `crates/protocol/src/handler.rs`.
  - `metadata()` (~line 490): an `information_schema` statement takes the engine path first.
  - On an engine error it falls back to `crate::metadata::try_handle_metadata` (the old substring handlers), counting `arneb_common::metrics::pg_catalog_fallback()` and logging the same warn line.
  - The other two `references_pg_catalog` call sites (~716 and ~1161, Describe and bind-time inference) use the combined predicate.
- Modify: `crates/protocol/src/metadata.rs`. `try_handle_metadata` keeps its `information_schema` substring arms, but they are reached only as the fallback for statements that reference those tables. Every other statement goes through `try_handle_metadata` as today.
- Test: `crates/protocol/tests/pg_catalog_surface.rs`, `crates/protocol/tests/access_control.rs`, and the existing `information_schema` tests, which must keep passing. Find them with `grep -rln information_schema crates/protocol/tests`.

**Interfaces:**
- The tables and columns. A column with no marker is VARCHAR; `#` is BIGINT.
  - `schemata`: `catalog_name`, `schema_name`, `schema_owner`.
  - `tables`: `table_catalog`, `table_schema`, `table_name`, `table_type`. `table_type` is `BASE TABLE` or `VIEW`.
  - `columns`: `table_catalog`, `table_schema`, `table_name`, `column_name`, `ordinal_position#`, `column_default`, `is_nullable`, `data_type`, `character_maximum_length#`, `numeric_precision#`, `numeric_scale#`, `datetime_precision#`, `udt_catalog`, `udt_schema`, `udt_name`, `is_identity`, `is_generated`.
  - `views`: `table_catalog`, `table_schema`, `table_name`, `view_definition`.
  - `table_constraints`: `constraint_catalog`, `constraint_schema`, `constraint_name`, `table_catalog`, `table_schema`, `table_name`, `constraint_type`.
  - `key_column_usage`: `constraint_catalog`, `constraint_schema`, `constraint_name`, `table_catalog`, `table_schema`, `table_name`, `column_name`, `ordinal_position#`.
- Values:
  - `is_nullable` is `YES`/`NO`. `is_identity` is `NO`. `is_generated` is `NEVER`. `column_default` is NULL.
  - `data_type` is PostgreSQL's spelling: `bigint`, `integer`, `smallint`, `numeric`, `character varying`, `date`, `timestamp without time zone`, `boolean`, `double precision`, `real`, `ARRAY`.
  - `udt_schema` is `pg_catalog`. `udt_name` is the `typname` that `pg_attribute`/`pg_type` already use: `int8`, `int4`, `numeric`, `varchar`, `date`, and so on.
  - `views`, `table_constraints` and `key_column_usage` are empty. `views` stays empty even where Arneb has views; that's a follow-up, noted in docs.
  - The existing handlers' column lists are the floor: every column they return today must exist with the same name.
- This task adds no new public types. The helpers are `pub(crate)`.

- [ ] **Step 1: Write failing tests.**
  - In `pg_catalog_surface.rs`, `information_schema_runs_in_engine`:
    - `SELECT column_name, udt_name, data_type, is_nullable FROM "information_schema"."columns" WHERE table_name = 'nation' ORDER BY ordinal_position` gives the nation columns with `udt_name` `int4`/`varchar`, `data_type` `integer`/`character varying`, and `is_nullable` `NO`.
    - The fallback counter is unchanged.
    - A `WHERE table_name = 'nation'` filter returns only nation's rows. Today the substring handler returns every table's columns.
  - In `pg_catalog_surface.rs`, `information_schema_joins_pg_catalog`:
    - `SELECT c.column_name FROM information_schema.columns c JOIN pg_catalog.pg_namespace n ON n.nspname = c.table_schema WHERE c.table_name = 'nation' ORDER BY 1` gives 4 rows, with no fallback.
  - In `pg_catalog_surface.rs`, `information_schema_fallback_still_answers`:
    - A statement the engine can't plan but the old handler answers, e.g. `SELECT * FROM information_schema.tables WHERE table_name ~~ 'n%' ESCAPE '\'` (pick any construct the engine refuses; the implementer confirms it does), still returns rows.
    - The fallback counter goes up by 1.
  - In `access_control.rs`, `information_schema_and_stat_tables_hide_denied_tables`: a user denied table `orders` sees it neither in `information_schema.columns` nor in `pg_stat_user_tables`.
- [ ] **Step 2:** The tests fail with `table not found: information_schema.columns`.
- [ ] **Step 3:** Implement it.
- [ ] **Step 4:**
  - `cargo test -p arneb-protocol` passes, all test binaries.
  - `scripts/client-smoke/run.sh <SF1 data>` reports ALL STEPS PASSED (dbt `docs generate` reads `information_schema`).
- [ ] **Step 5:** Commit `feat(protocol): queryable information_schema beside pg_catalog`.

### Task 8: Metabase corpus test (Tier 1)

**Files:**
- Create: `crates/protocol/tests/metabase_corpus.rs`, a separate binary.
- Create: `crates/protocol/tests/data/metabase/`, holding copies of `docs/superpowers/specs/2026-11-21-g6-stage-2b2/metabase-captured.sql` and `metabase-pg17-expected.json`. Alternatively, read those docs paths through `env!("CARGO_MANIFEST_DIR")`/`../../docs/...`; pick one and state it in the file header.
- Fix: whatever the corpus exposes that Tasks 1–7 did not cover. Fix it in the owning crate, with a focused test next to the fix. The design's "more layers may be hiding" applies here.

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Test setup:
  - The memory tables are `file.default.{nation, region, part, supplier, partsupp, customer, orders, lineitem}`. Column names and types are exactly those in the corpus DDL. Get them from `information_schema.columns` in the spec, or from the `CREATE TABLE` statements the design's capture used; nullable is false.
  - Rows match the capture instance: the 25 `nation` rows (copy the 25 TPC-H nation rows), 2 `orders` rows `(1,1,'O',100.5,'1996-01-02','1-URGENT','Clerk#1',0,'x')` and `(2,2,'F',200000,'1995-03-04','2-HIGH','Clerk#2',0,'y')`, 1 `customer` row `(1,'Customer#000000001','a',1,'p',1.0,'BUILDING','c')`, and empty tables for the rest.
  - The search path is the default.

- [ ] **Step 1: Write the tests.**
  - `every_corpus_statement_runs_without_fallback`:
    - Split the corpus on `\n;;;;\n`.
    - Run each statement with `simple_query` on one connection, in order. Session statements (`SET`, `BEGIN READ ONLY`, `ROLLBACK`, `SHOW`) are part of the corpus.
    - Assert that none errors. On failure, print the 0-based index and the first 200 characters.
    - Assert that the fallback counter is unchanged.
  - `metadata_statements_match_postgres`:
    - For each entry in `metabase-pg17-expected.json`, run `statements[index]` and compare it with `columns`/`rows`.
    - Mask any column whose name ends in `oid`, `relid` or `attrelid`, or equals `objoid`.
    - Compare as multisets unless the SQL contains `ORDER BY`.
    - A `const KNOWN_DIFFERENCES: &[(usize /*index*/, &str /*column*/, &str /*reason*/)]` lists every column that legitimately differs. Each entry has a reason, for example `estimated_row_count: Arneb has no statistics`. The test fails if an entry is unused.
- [ ] **Step 2:** `cargo test -p arneb-protocol --test metabase_corpus` fails. Read every failure.
- [ ] **Step 3: Fix the gaps.**
  - Each fix is a general feature with its own focused test in the owning crate.
  - Re-run until both tests pass.
  - If a gap needs a feature the spec rules out as a non-goal, stop and report it (status BLOCKED, with the statement index and error). Do not special-case it.
- [ ] **Step 4:** `cargo test -p arneb-protocol --test metabase_corpus` passes, along with the full `cargo test --workspace`.
- [ ] **Step 5:** Commit `test(protocol): Metabase corpus runs in the engine and matches PostgreSQL` (plus a `fix(...)` commit per gap).

### Task 9: Tier 2 tooling, end-to-end run, and docs

**Files:**
- Create in `scripts/client-smoke/metabase/`:
  - `compose.yml`: services `metabase` (`metabase/metabase:v0.64.8.3`, `127.0.0.1:13300:3000`, `MB_DB_TYPE=h2`, `MB_ANON_TRACKING_ENABLED=false`, `MB_CHECK_FOR_UPDATES=false`, `JAVA_TIMEZONE=UTC`, `extra_hosts: host.docker.internal:host-gateway`) and `pgcap` (`postgres:17`, `127.0.0.1:15499:5432`, trust auth, user `smoke`, db `arneb`, `log_statement=all`).
  - `probe.py`, stdlib only, with the usage line `probe.py METABASE_URL HOST PORT [DB_NAME]`. It does the setup (random admin password, kept only in a file next to the script that is git-ignored), adds the database (`ssl=false`), waits for sync, reports the metadata, runs the 25 questions and the native `{{r}}` parameter question, and runs x-ray. It prints `OK`/`FAIL` lines and finishes with a summary line `PROBE ok=<n> fail=<m> tables=<t> fields=<f>`. It exits non-zero when `fail > 0`, or when `tables`/`fields` differ from the optional `--expect-tables 8 --expect-fields 61`.
  - `capture.py`, with the usage line `capture.py PGLOG OUT_SQL [--replay HOST:PORT]`. It extracts the statements from a `log_statement=all` log, inlines parameters, de-duplicates, masks `application_name` uuids and query hashes, and optionally replays them, printing `OK`/`FAIL` lines.
  - `README.md`: the run steps (start the cluster with `cluster.sh`, `docker compose -p arneb-mb up -d`, run the probe against Arneb and against `pgcap`, recapture the corpus), the pinned versions, and teardown with `docker compose -p arneb-mb down -v`.
- Modify: `.gitignore`, adding the probe's password file.
- Modify:
  - `docs/guide/clients.md`: a Metabase section covering the driver, the settings (SSL off unless TLS is configured), what works, and the known gaps. The gaps: `views` and constraints are empty, `n_live_tup` is 0, no POSIX-only regex syntax, and no multi-part interval strings.
  - `docs/sql/expressions.md`: regex operators, intervals, `date - date`, subscripts and field access, and untyped literals.
  - `docs/sql/functions.md`: `format`, `percentile_cont`/`percentile_disc`, `current_schemas`, `col_description`/`obj_description`.
  - `docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`: the status line, "G6 stage 2b done".
- Base `probe.py` and `capture.py` on the scratch prototypes in `/private/tmp/claude-501/-Users-jacky-Projects-arneb/dfd8c7ce-6bb5-4c4e-aceb-c430179be785/scratchpad/mb/` (`mb_probe.py`, `replay.py`). Clean them up; do not copy them blindly.

- [ ] **Step 1: End-to-end run.**
  - Build the release binary and start the 1+2 cluster on SF1: `scripts/client-smoke/cluster.sh start <SF1 data> <run dir>`.
  - `docker compose -p arneb-mb -f scripts/client-smoke/metabase/compose.yml up -d`.
  - Run `probe.py http://127.0.0.1:13300 host.docker.internal 15432 arneb --expect-tables 8 --expect-fields 61`.
  - Expected: `PROBE ok=<all> fail=0 tables=8 fields=61`, and the `/metrics` `arneb_pg_catalog_fallbacks_total` delta is 0.
  - The `base_type`/`database_type` per column equal those of the probe run against `pgcap` with the same DDL.
  - The data results of the count/sum/median questions equal DuckDB on `tpch.duckdb` for the same SQL. Spot-check at least: count orders, sum by status, median `o_totalprice`, and count by year.
- [ ] **Step 2:** Run `scripts/client-smoke/run.sh <SF1 data>`. Expected: ALL STEPS PASSED.
- [ ] **Step 3:** Write the docs.
- [ ] **Step 4:** Run `cargo fmt -- --check`, clippy, and `cargo test --workspace`. All pass.
- [ ] **Step 5:** Commit `test(smoke): Metabase Tier 2 probe and capture tooling; docs`.
