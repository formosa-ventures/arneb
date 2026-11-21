# G6 stage 2b-1: SQLAlchemy reflection over pgwire — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The SQLAlchemy 2.1 postgresql dialect must reflect Arneb tables
over pgwire with both psycopg2 and psycopg 3, with no pg_catalog
fallback. To get there, the engine gains:

- a minimal array type;
- `array_agg … ORDER BY`;
- `unnest` and `generate_subscripts`;
- correlated scalar subqueries in `SELECT`;
- the missing pg_catalog surface.

**Architecture:**

- Arrays are `DataType::List` end to end, with a new
  `ScalarValue::List`.
- Set-returning functions get a new `ProjectSet` plan node below the
  final projection.
- Correlated `SELECT`-list subqueries are rewritten to LEFT JOINs onto a
  subquery grouped by the correlation keys.
- The pg_catalog additions are empty tables and NULL-returning catalog
  functions in the existing per-statement memory catalog.

**Tech Stack:** Rust (arrow 60, sqlparser 0.64, pgwire 0.42); Python 3,
SQLAlchemy 2.1.6, psycopg2 2.9.13, psycopg 3.3.6 and duckdb 2.0.0 for the
smoke suite.

**Spec:** `docs/superpowers/specs/2026-11-21-g6-stage-2b1-pg-reflection-design.md`
(reference SQL: `docs/superpowers/specs/2026-11-21-g6-stage-2b1/captured-fallback-sql.txt`;
probe: `scripts/client-smoke/pg_reflect_probe.py`).

## Global Constraints

- Arneb has no PKs, unique constraints, indexes, defaults, identity
  columns, sequences or collations. The new catalog tables are empty,
  and the new catalog functions return NULL, except
  `pg_collation_is_visible`, which returns true.
- No matching of client SQL text in the protocol layer. Every reflection
  statement must run through the planner and executor.
- Aggregate `ORDER BY` is accepted only in `array_agg`. `FILTER` and
  `WITHIN GROUP` are refused on every function. `DISTINCT` is refused in
  `array_agg`. Nothing is silently dropped.
- A correlated subquery that yields more than one row for an outer row
  raises `more than one row returned by a subquery used as an expression`.
- `array_agg` is non-decomposable and runs as a single phase, so
  `is_decomposable_for_split` does not change.
- `json_build_object` returns text. An odd argument count is a planning
  error.
- Several SRFs in one `SELECT` zip by position and pad with NULL. A NULL
  or empty array yields no rows.
- pgwire text encoding of arrays follows PostgreSQL's `{…}` literal
  rules.
- Queries that planned before must plan identically. TPC-H and TPC-DS
  tests stay green.
- `metadata::fixed_pg_catalog` stays in place (#189).
- New scalar functions need three registrations: the function in
  `crates/execution/src/functions/`, its return type in
  `function_return_type` (`crates/planner/src/analyzer/mod.rs`), and its
  argument types in the `planner_and_registry_return_types_agree` table
  (`crates/execution/src/functions/registry.rs`).
- New aggregates need entries in `is_aggregate_function`
  (`crates/planner/src/planner.rs`), `function_return_type`, and both
  accumulator matches in `crates/execution/src/aggregate.rs`.
- Every commit passes `cargo fmt -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and the tests
  it touches. Run `./scripts/target-housekeeping.sh` before the first
  build.
- Commit trailers:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01WwNmnQCEk3V7MvZCQsscN4`.

## Review Focus

1. **Array elements needing quotes over pgwire.** Elements that are
   empty strings, contain commas, quotes, backslashes, braces or
   whitespace, are NULL, or are nested must round-trip through psycopg's
   array parser to the same Python list. Test: Task 2,
   `array_text_encoding_round_trips`.
2. **`array_agg` over a group whose values are all NULL, and over an
   empty input.** The first gives `{NULL,NULL}`. The second, with no
   `GROUP BY`, gives one NULL row. Test: Task 4,
   `array_agg_nulls_and_empty_input`.
3. **A correlated subquery under a `CASE` branch that is not taken,
   where the key group has several rows.** It must not error. The same
   subquery in a taken branch errors. Test: Task 6,
   `correlated_subquery_multirow_only_errors_when_evaluated`.
4. **`= ANY($1)` with an empty array parameter (`{}`) and a NULL
   element.** `x = ANY('{}')` is false. `x = ANY('{1,NULL}')` is NULL for
   x=2. Test: Task 3, `any_array_param_null_semantics`.
5. **`unnest` over arrays of different lengths, plus a non-SRF column.**
   The rows zip, the shorter side is padded with NULL, and the non-SRF
   column is repeated. Test: Task 5, `srf_zip_pads_with_null`.

---

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `crates/sql-parser/src/ast.rs`, `convert.rs` | `Expr::Array`, `Function.order_by`, refusals, `ANY(expr)` | 1, 2, 3 |
| `crates/common/src/types.rs` | `ScalarValue::List` | 2 |
| `crates/planner/src/plan.rs`, `planner.rs`, `analyzer/mod.rs` | `PlanExpr::Array`, `PlanExpr::ArrayContains`, `Function.order_by`, `LogicalPlan::ProjectSet`, subquery pre-pass | 2–6 |
| `crates/planner/src/fragment.rs`, `cost.rs`, optimizer passes | match arms for `ProjectSet` | 5 |
| `crates/execution/src/expression.rs` | evaluating `Array` and `ArrayContains` | 2, 3 |
| `crates/execution/src/aggregate.rs` | `ArrayAgg`, `BoolAnd` accumulators | 4 |
| `crates/execution/src/project_set.rs` (new), `planner.rs` | `ProjectSetExec` | 5 |
| `crates/execution/src/functions/pg_catalog.rs`, `json.rs` (new), `registry.rs` | catalog functions, `json_build_object`, runtime regclass | 7 |
| `crates/protocol/src/encoding.rs`, `handler.rs` | array OIDs and text encoding, array bind parameters | 2, 3 |
| `crates/protocol/src/pg_catalog.rs` | new columns and tables, array column specs | 7 |
| `crates/protocol/tests/arrays.rs` (new) | Tasks 2–5 integration tests | 2–5 |
| `crates/protocol/tests/correlated_select_subquery.rs` (new) | Task 6 | 6 |
| `crates/protocol/tests/pg_reflection.rs` (new) | captured SQLAlchemy SQL, verbatim | 8 |
| `scripts/client-smoke/clients.py`, `docs/…` | smoke checks and docs | 9 |

The new integration-test files copy the harness from
`crates/protocol/tests/pg_catalog.rs`: `state()`, `pg_server()`,
`connect()` and `rows()`. In `pg_reflection.rs` and `arrays.rs`, add a
`fallbacks()` helper that reads the global counter behind
`arneb_common::metrics::pg_catalog_fallback()`. If no getter exists,
add `pub fn pg_catalog_fallbacks() -> u64` to
`crates/common/src/metrics.rs`.

---

### Task 1: Aggregate call modifiers are never silently dropped

**Files:**
- Modify: `crates/sql-parser/src/ast.rs:284` (`Expr::Function`),
  `crates/sql-parser/src/convert.rs:1104-1350` (`convert_function`)
- Modify: `crates/planner/src/plan.rs:114` (`PlanExpr::Function`),
  `crates/planner/src/planner.rs` (where `ast::Expr::Function` becomes
  `PlanExpr::Function`)
- Test: `crates/sql-parser/src/lib.rs` tests module,
  `crates/protocol/tests/arrays.rs`

**Interfaces:**
- Produces:
  - `ast::Expr::Function { name, args, distinct, order_by: Vec<ast::OrderByExpr>, span }`
  - `PlanExpr::Function { name, args, distinct, order_by: Vec<SortExpr>, span }`.
    This field is serde, so it is on the worker wire format.
  - Every existing constructor passes `order_by: vec![]`.

- [ ] **Step 1: Write failing parser tests.**
  - `function_filter_is_refused`: parsing
    `SELECT count(*) FILTER (WHERE x > 1) FROM t` returns
    `Err(UnsupportedFeature(msg))`, where `msg` contains `"FILTER"`.
  - `function_within_group_is_refused`: the same for
    `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t`, with
    a message containing `"WITHIN GROUP"`.
  - `aggregate_order_by_is_kept`: for `SELECT array_agg(a ORDER BY b DESC) FROM t`,
    the parsed `Function` has `order_by.len() == 1`, the expression is
    column `b`, and it is descending.
- [ ] **Step 2: Run `cargo test -p arneb-sql-parser function_` and
  `cargo test -p arneb-sql-parser aggregate_order_by`.** Expected: FAIL,
  because the clauses are dropped today.
- [ ] **Step 3: Implement the parser changes.** In `convert_function`:
  - read `arg_list.clauses`: `FunctionArgumentClause::OrderBy` goes to
    `order_by`, and any other clause is refused;
  - refuse `func.filter.is_some()` and a non-empty `func.within_group`.
- [ ] **Step 4: Implement the planner check.** The planner keeps
  `order_by` only when the uppercased name is `ARRAY_AGG`. Otherwise it
  returns `PlanError` with the message
  `ORDER BY inside <name>(...) is not supported`.
- [ ] **Step 5: Write the planner integration test**
  `order_by_in_other_aggregate_is_refused` in
  `crates/protocol/tests/arrays.rs`.
  `SELECT sum(x ORDER BY x) FROM t` fails, and its error message
  contains `ORDER BY inside`.
- [ ] **Step 6: Run the tests.** Run
  `cargo test -p arneb-sql-parser && cargo test -p arneb-planner && cargo test -p arneb-protocol --test arrays`.
  Expected: PASS.
- [ ] **Step 7: Commit.** Message:
  `fix(sql): refuse FILTER and WITHIN GROUP, keep ORDER BY in aggregate calls`.

### Task 2: Array values, `ARRAY[...]` literals and pgwire output

**Files:**
- Modify: `crates/common/src/types.rs:420` (`ScalarValue`)
- Modify: `crates/sql-parser/src/ast.rs`, `convert.rs:923`
  (`sp::Expr::Array`)
- Modify: `crates/planner/src/plan.rs`, `planner.rs` (`plan_expr`),
  `analyzer/mod.rs` (type inference)
- Modify: `crates/execution/src/expression.rs`;
  `crates/execution/src/operator.rs:4177` (`extract_scalar`) and
  `:4253` (`scalars_to_array`); `aggregate.rs:1924` (`scalar_from_array`)
- Modify: `crates/protocol/src/encoding.rs:13` (`datatype_to_pg_type`),
  `:44` (`encode_value`); `handler.rs:103` (`arrow_type_to_pg`), `:777`
  (`arneb_type_to_pg_param_type`)
- Test: `crates/protocol/tests/arrays.rs`, plus unit tests in
  `encoding.rs`

**Interfaces:**
- Produces:
  - `ScalarValue::List { element: DataType, values: Vec<ScalarValue> }`.
    Its `data_type()` is `DataType::List(Box::new(element))`.
  - `ast::Expr::Array { elements: Vec<ast::Expr>, span: Span }`
  - `PlanExpr::Array { elements: Vec<PlanExpr>, element_type: DataType, span }`
  - `pub fn encode_pg_array_text(array: &dyn arrow::array::Array, row: usize) -> String`
    in `encoding.rs`. Task 8's expectations depend on it.

- [ ] **Step 1: Write failing tests in `arrays.rs`.** Each one asserts
  the text values that `simple_query` returns.
  - `array_literal_text`: `SELECT ARRAY[1,2,3]` gives `{1,2,3}`, and the
    column type OID is 1007 (int4[]). Read the OID through
    `client.prepare(sql).columns()[0].type_().oid()`.
  - `array_literal_common_supertype`: `SELECT ARRAY[1, 2.5]` gives
    `{1.0,2.5}` or PostgreSQL's `{1,2.5}`. Pin whichever rendering
    Arneb's float or decimal formatting gives, and comment the choice.
  - `empty_array_without_cast_is_refused`: `SELECT ARRAY[]` fails.
  - `array_text_encoding_round_trips` (Review Focus 1): `SELECT
    ARRAY['', 'a,b', 'q"x', 'b\\s', '{c}', 'sp ace', NULL]` gives
    `{"","a,b","q\"x","b\\\\s","\{c\}"…` exactly as PostgreSQL 16 prints
    it.
    - Copy the expected string from `psql` on a real PostgreSQL. The
      docker-compose `postgres` service on 5433 may be used read-only:
      run `SELECT` only, and do not seed anything.
    - Also assert that psycopg 3 parses it back to the same Python list.
      That part lives in Task 9's smoke check; here assert only the text.
- [ ] **Step 2: Run `cargo test -p arneb-protocol --test arrays`.**
  Expected: FAIL with `unsupported feature: expression: ARRAY[...]`.
- [ ] **Step 3: Implement the value model.** Add `ScalarValue::List` and
  extend `data_type()`, `extract_scalar`, `scalars_to_array` and
  `scalar_from_array` for `List`.
- [ ] **Step 4: Implement the parser and planner.** Convert
  `sp::Expr::Array` to `ast::Expr::Array`. `plan_expr` computes the
  element type as the common supertype, reusing the coercion that `CASE`
  and `COALESCE` use, and casts each element to it.
- [ ] **Step 5: Implement evaluation.** Evaluate `PlanExpr::Array` to a
  `ListArray` with one list per input row. If every element folds to a
  literal, `fold_constants` yields `ScalarValue::List`.
- [ ] **Step 6: Implement pgwire output.**
  - `datatype_to_pg_type(List(t))` returns `t`'s `array_oid` from
    `pg_oid::PG_TYPES`, or `TEXT_ARRAY` (1009) when there is none.
  - `encode_value` calls `encode_pg_array_text` for `List` and
    `LargeList`.
  - Update the other two maps the same way.
- [ ] **Step 7: Run the tests.** Run
  `cargo test -p arneb-protocol --test arrays && cargo test -p arneb-common && cargo test -p arneb-execution`.
  Expected: PASS.
- [ ] **Step 8: Commit.** Message:
  `feat(engine): ARRAY[...] literals and array output over pgwire`.

### Task 3: `= ANY (<array>)`, `<> ALL`, and array bind parameters

**Files:**
- Modify: `crates/sql-parser/src/convert.rs:643-656`, `:1661`
  (`array_in_list`)
- Modify: `crates/planner/src/plan.rs`, `planner.rs`
- Modify: `crates/execution/src/expression.rs`
- Modify: `crates/protocol/src/handler.rs` (`param_literal` at about
  :635, the binary decode at about :697, and the Describe parameter
  types)
- Test: `crates/protocol/tests/arrays.rs`

**Interfaces:**
- Consumes: Task 2's `ast::Expr::Array` and `ScalarValue::List`.
- Produces:
  - `ast::Expr::AnyAll { expr, op: AnyAllOp, array, span }`, where
    `AnyAllOp` is one of `EqAny` or `NotEqAll`. A literal `ARRAY[...]`
    on the right keeps the existing `InList` desugar.
  - `PlanExpr::ArrayContains { expr, array, negated, span }`.

- [ ] **Step 1: Write failing tests.**
  - `any_over_array_expression`:
    `SELECT n FROM (VALUES (1),(2),(3)) v(n) WHERE n = ANY (ARRAY[1,3])`
    gives `1,3`. Write it with a non-literal array, for example
    `ARRAY[n, 3]`, so that the new path is used.
  - `any_array_param_text` (tokio_postgres):
    `client.query("SELECT r_name FROM region WHERE r_regionkey = ANY($1) ORDER BY 1", &[&vec![1i64, 3]])`
    gives the two names. tokio_postgres sends int8[] in binary.
  - `any_array_param_text_format`: the same query, with the parameter
    sent as a text `{1,3}` and declared as `int8[]`.
  - `any_array_param_null_semantics` (Review Focus 4):
    - `SELECT 2 = ANY('{}'::int[])` gives `f`.
    - `SELECT 2 = ANY(ARRAY[1,NULL])` gives NULL.
    - `SELECT 2 <> ALL(ARRAY[1,NULL])` gives NULL.
    - `SELECT 1 = ANY(ARRAY[1,NULL])` gives `t`.
- [ ] **Step 2: Run `cargo test -p arneb-protocol --test arrays any_`.**
  Expected: FAIL.
- [ ] **Step 3: Implement it.**
  - The parser emits `AnyAll` whenever the right side is not a literal
    array. That includes parameters, casts and columns.
  - The planner type-checks that the right side is `List(t)`, coerces
    the left side to `t`, and emits `ArrayContains` with
    three-valued logic.
  - `param_literal` turns a text array (`{…}`, honouring quotes and
    `NULL`) or a binary array into
    `CAST(ARRAY[...] AS <elem>[])`. Binary decoding covers the array
    OIDs of every element type already decoded: 1005, 1007, 1016, 1021,
    1022, 1000, 1182, 1115, 1185, 1009, 1015 and 1014. Any other binary
    array OID keeps the `0A000` error.
  - `'…'::int[]` casts from text parse the same array syntax.
- [ ] **Step 4: Run the tests.** Expected: PASS.
- [ ] **Step 5: Commit.** Message:
  `feat(engine): = ANY / <> ALL over array expressions and array parameters`.

### Task 4: `array_agg` with `ORDER BY`, and `bool_and`

**Files:**
- Modify: `crates/planner/src/planner.rs:3184` (`is_aggregate_function`),
  `analyzer/mod.rs:297`
- Modify: `crates/execution/src/aggregate.rs` (the `create_accumulator`
  match at about :905 and the grouped match at about :1795)
- Modify: the aggregate operator's input wiring, wherever aggregate
  argument arrays are evaluated
- Test: `crates/protocol/tests/arrays.rs`, plus the existing
  `planner_and_accumulator_types_agree`

**Interfaces:**
- Consumes: Task 1's `PlanExpr::Function.order_by` and Task 2's
  `ScalarValue::List`.
- Produces:
  - `ARRAY_AGG(T) -> List(T)`.
  - `BOOL_AND(Boolean) -> Boolean`.
  - The accumulators receive value and sort keys packed as one
    `StructArray` argument (`{v, k0, k1, …}`), built by the planner when
    `order_by` is non-empty. Use this packing, not multi-column traits;
    that is the planner's call. This keeps the `Accumulator` and
    `GroupedAccumulator` signatures unchanged.

- [ ] **Step 1: Write failing tests.**
  - `array_agg_order_by`:
    `SELECT n_regionkey, array_agg(n_name ORDER BY n_name DESC) FROM nation GROUP BY 1 ORDER BY 1`.
    Use a memory `nation` with 6 rows across 2 regions. Expect
    `{C,B,A}` and `{F,E,D}`.
  - `array_agg_multi_key_order_with_nulls_last`: `ORDER BY k1, k2 DESC
    NULLS LAST` gives the expected element order.
  - `array_agg_nulls_and_empty_input` (Review Focus 2):
    - two all-NULL values give `{NULL,NULL}`;
    - `SELECT array_agg(x) FROM t WHERE false` gives one row, NULL.
  - `array_agg_distinct_is_refused`: the error contains `DISTINCT`.
  - `bool_and_basic`: `bool_and` over `{t,t}` gives `t`, over `{t,f}`
    gives `f`, and over all NULL gives NULL.
- [ ] **Step 2: Run `cargo test -p arneb-protocol --test arrays array_agg bool_and`.**
  Expected: FAIL.
- [ ] **Step 3: Implement the accumulators.**
  - `ArrayAggAccumulator` and `GroupedArrayAgg` buffer `(sort keys,
    value)` rows per group. `evaluate` sorts stably by the keys
    (`ASC`/`DESC`, `NULLS FIRST`/`LAST` as in PostgreSQL: NULLS LAST for
    ASC by default) and builds `ScalarValue::List`.
  - `BoolAnd` mirrors `BoolOr`.
  - Leave `is_decomposable_for_split` unchanged.
- [ ] **Step 4: Run the tests.** Run
  `cargo test -p arneb-protocol --test arrays && cargo test -p arneb-execution planner_and_accumulator_types_agree`.
  Expected: PASS.
- [ ] **Step 5: Commit.** Message:
  `feat(engine): array_agg with ORDER BY, and bool_and`.

### Task 5: `unnest` and `generate_subscripts` in `SELECT` (`ProjectSet`)

**Files:**
- Modify: `crates/planner/src/plan.rs`: the `LogicalPlan::ProjectSet`
  variant, plus `schema()` at :779 and Display at :1015.
- Modify: `crates/planner/src/planner.rs` `plan_select` at :632. Place
  the node below the final projection, like `plan_windows`.
- Modify: every exhaustive `LogicalPlan` match. The compiler lists them;
  at least `fragment.rs` :336, :1173 and :2188, `cost.rs`, and the
  optimizer and analyzer passes.
- Create: `crates/execution/src/project_set.rs`. Wire it in
  `crates/execution/src/planner.rs` near :1588.
- Test: `crates/protocol/tests/arrays.rs`, plus planner unit tests.

**Interfaces:**
- Consumes: Task 2's arrays.
- Produces:
  - `LogicalPlan::ProjectSet { input: Arc<LogicalPlan>, srfs: Vec<(SrfKind, PlanExpr)>, schema: Vec<ColumnInfo> }`.
  - `enum SrfKind { Unnest, GenerateSubscripts }`.
  - The output schema is the input columns followed by one column per
    SRF:
    - `unnest(List(t))` gives `t`, nullable;
    - `generate_subscripts(arr, 1)` gives `Int32`.
  - The final projection references these columns by name.

- [ ] **Step 1: Write failing tests.**
  - `unnest_literal`: `SELECT unnest(ARRAY[1,2,3])` gives the 3 rows
    `1,2,3`.
  - `generate_subscripts_basic`:
    `SELECT generate_subscripts(ARRAY['a','b'], 1)` gives `1,2`.
  - `srf_zip_pads_with_null` (Review Focus 5):
    `SELECT 'x' AS c, unnest(ARRAY[1,2,3]), unnest(ARRAY['a'])` gives
    `(x,1,a), (x,2,NULL), (x,3,NULL)`.
  - `srf_null_and_empty_yield_no_rows`:
    `SELECT unnest(a) FROM (VALUES (CAST(NULL AS int[])), (CAST('{}' AS int[]))) v(a)`
    gives 0 rows.
  - `srf_over_array_agg_subquery`:
    `SELECT unnest(arr) FROM (SELECT array_agg(n_name ORDER BY n_name) arr FROM nation) s`
    gives all the names in order.
  - `srf_in_where_is_refused` and `srf_nested_in_expression_is_refused`:
    `SELECT unnest(ARRAY[1]) + 1` fails with
    `set-returning function … not supported here`.
  - `generate_subscripts_dim_other_than_1_is_refused`.
- [ ] **Step 2: Run `cargo test -p arneb-protocol --test arrays srf_ unnest generate_subscripts`.**
  Expected: FAIL.
- [ ] **Step 3: Implement it.**
  - Planning extracts SRF calls that appear as whole `SELECT` items, or
    as the item under an alias, into `ProjectSet`.
  - `ProjectSetExec` streams per batch. For each input row it emits
    `max(len_i)` rows, padding with NULL; a NULL or empty array
    counts as length 0. It builds output with `take` indices on the
    input columns.
  - The fragmenter treats `ProjectSet` like `Projection`: it stays in
    its input's fragment.
- [ ] **Step 4: Run the tests, plus the existing TPC-H and TPC-DS plan
  tests.** Run
  `cargo test -p arneb-protocol --test arrays && cargo test -p arneb-planner`.
  Expected: PASS.
- [ ] **Step 5: Commit.** Message:
  `feat(engine): unnest and generate_subscripts in SELECT lists`.

### Task 6: Correlated scalar subqueries in `SELECT` lists

**Files:**
- Modify: `crates/planner/src/planner.rs`:
  - `plan_projection` and `plan_aggregate_projection` (around :902);
  - `plan_expr` `Subquery` arm (:2323);
  - reuse `try_make_leftjoin_spec` (:1612), `LeftJoinSpec`,
    `try_match_correlation`, `references_outer`,
    `factor_or_correlations`.
- Modify: `crates/execution/src/functions/`: a new internal scalar
  function `__single_row_check`.
- Test: `crates/protocol/tests/correlated_select_subquery.rs`.

**Interfaces:**
- Produces: a planner pre-pass `rewrite_select_list_subqueries`. It
  runs before projection planning and replaces each correlated
  `ast::Expr::Subquery` at any depth with a reference to a column of a
  LEFT JOIN added to the `FROM` plan.
  - Join keys come from the correlation equalities, which may be
    expressions on either side.
  - Outer-only conjuncts become a guard.
  - **Aggregate subquery:** reuse `LeftJoinSpec`, including its
    `empty_value` (COUNT → 0).
  - **Non-aggregate subquery** with value `v`: the joined side is
    `SELECT keys…, min(v) AS v, count(*) AS n FROM … WHERE inner-only GROUP BY keys`.
    The expression becomes
    `CASE WHEN <guard> THEN __single_row_check(n, v) END`, where
    `__single_row_check(n, v)` returns `v` when `n` is NULL or ≤ 1 and
    otherwise raises the multi-row error.
  - For `v` types without `min` (lists), refuse with the unsupported
    error.
  - `CASE` must evaluate only the taken branch for these rows. Check
    how `CaseExpr` is evaluated in `expression.rs`. If it evaluates
    every branch eagerly, evaluate the `THEN` side only on the rows
    where `WHEN` is true (filter, evaluate, scatter).

- [ ] **Step 1: Write failing tests.** Use memory tables
  `o(id, k)` and `i(k, v)`.
  - `select_list_correlated_scalar`: `SELECT id, (SELECT v FROM i WHERE
    i.k = o.k) FROM o ORDER BY id`. Unmatched rows get NULL.
  - `select_list_correlated_aggregate`: `(SELECT count(*) FROM i WHERE
    i.k = o.k)`. Unmatched rows get `0`.
  - `correlated_inside_case_condition_and_branch`: `CASE WHEN (SELECT v
    FROM i WHERE i.k = o.k) > 1 THEN (SELECT v FROM i WHERE i.k = o.k)
    END`.
  - `correlated_with_outer_only_guard`: `(SELECT v FROM i WHERE o.flag
    AND i.k = o.k)` is NULL when `flag` is false.
  - `correlated_with_expression_key`: `(SELECT v FROM i WHERE i.k =
    o.k + 1)`.
  - `correlated_inner_join_in_subquery_from`: the subquery `FROM i JOIN
    j ON …` works.
  - `correlated_subquery_multirow_only_errors_when_evaluated` (Review
    Focus 3):
    - with a duplicate key in `i`,
      `CASE WHEN o.id < 0 THEN (SELECT v FROM i WHERE i.k = o.k) END`
      succeeds;
    - without the `CASE` it fails with
      `more than one row returned by a subquery used as an expression`.
  - `unsupported_correlation_is_clear`: a non-equality-only correlation
    `i.k > o.k` fails with an error containing
    `correlated subquery` and not `column not found`.
  - `uncorrelated_select_subquery_unchanged`: the existing fold path is
    still used. Assert the result, and that `EXPLAIN` shows no join.
- [ ] **Step 2: Run `cargo test -p arneb-protocol --test correlated_select_subquery`.**
  Expected: FAIL with `column not found`.
- [ ] **Step 3: Implement the pre-pass, `__single_row_check`, and the
  lazy `CASE` branch if it is needed.**
- [ ] **Step 4: Run the tests, plus the full planner suite and the
  TPC-DS SF1 correctness tests.** Run
  `cargo test -p arneb-protocol --test correlated_select_subquery && cargo test -p arneb-planner && cargo test -p arneb-protocol`.
  Expected: PASS, with existing plans unchanged.
- [ ] **Step 5: Commit.** Message:
  `feat(planner): decorrelate scalar subqueries in SELECT lists`.

### Task 7: pg_catalog surface: columns, tables, catalog functions, `json_build_object`, runtime regclass

**Files:**
- Modify: `crates/protocol/src/pg_catalog.rs`: `TABLES` at :20-153, the
  `build()` rows from :251, and the column-spec parser.
- Modify: `crates/protocol/src/memory_table.rs`: a `Cell` that is NULL
  or a list, if array columns need it.
- Modify: `crates/execution/src/functions/pg_catalog.rs` (:16
  `all_pg_catalog_functions`).
- Create: `crates/execution/src/functions/json.rs`.
- Modify: `crates/execution/src/functions/registry.rs`,
  `crates/planner/src/analyzer/mod.rs:397`.
- Modify: `crates/planner/src/planner.rs:2225-2257` (regclass handling)
  and `crates/sql-parser/src/convert.rs` around :655.
- Test: `crates/protocol/tests/pg_catalog.rs`, plus unit tests in
  `json.rs`.

**Interfaces:**
- Produces:
  - **Column-spec suffix `[]`:** `indkey[]#` is `List(Int64)`, `name[]`
    is `List(Utf8)`. It is combined with the existing `#` and `?`
    markers.
  - **New columns:**
    - `pg_attribute`: `atthasdef?`=false, `attidentity`='',
      `attgenerated`='', `attcollation#`=0;
    - `pg_type`: `typcollation#`=0;
    - `pg_class`: `relam#`=0, `reloptions[]`=NULL.
  - **New empty tables:**
    - `pg_index`: `indexrelid#`, `indrelid#`, `indisprimary?`,
      `indisunique?`, `indisvalid?`, `indnkeyatts#`,
      `indnullsnotdistinct?`, `indpred`, `indkey[]#`, `indclass[]#`,
      `indoption[]#`;
    - `pg_sequence`: `seqrelid#`, `seqstart#`, `seqincrement#`,
      `seqmin#`, `seqmax#`, `seqcache#`, `seqcycle?`;
    - `pg_collation`: `oid#`, `collname`, `collnamespace#`;
    - `pg_opclass`: `oid#`, `opcname`, `opcdefault?`.
  - **Functions** (with `pg_catalog.` stripped as today):
    - `PG_GET_CONSTRAINTDEF(Int64[, Boolean]) -> Utf8` NULL;
    - `PG_GET_EXPR(Utf8, Int64[, Boolean]) -> Utf8` NULL;
    - `PG_GET_INDEXDEF(Int64, Int32|Int64, Boolean) -> Utf8` NULL;
    - `PG_GET_SERIAL_SEQUENCE(Utf8, Utf8) -> Utf8` NULL;
    - `PG_COLLATION_IS_VISIBLE(Int64) -> Boolean` true;
    - `JSON_BUILD_OBJECT(variadic) -> Utf8`.
  - **Runtime regclass:**
    - `CAST(<int expr> AS REGCLASS)` stays the integer oid, typed Int64.
    - `CAST(<regclass expr> AS TEXT)` is
      `__regclass_name(oid) -> Utf8`. It looks the oid up in the
      statement's catalog: the same `pg_class` rows `build()` produces,
      passed in the way `session_literal` passes session defaults. An
      unknown oid gives the number as text.
    - `CAST(<text expr> AS REGCLASS)` is
      `__regclass_oid(name) -> Int64`. NULL gives NULL, and an unknown
      name raises `relation "<name>" does not exist`.
    - A string literal keeps today's plan-time fold.

- [ ] **Step 1: Write failing tests in `pg_catalog.rs`.**
  - `new_attribute_columns_have_defaults`:
    `SELECT atthasdef, attidentity, attgenerated, attcollation FROM pg_attribute WHERE attnum > 0 LIMIT 1`
    gives `f`, `` (empty string), `` (empty string) and `0`.
  - `new_catalog_tables_are_empty_and_typed`:
    - `SELECT count(*) FROM pg_index` gives `0`, and likewise for
      `pg_sequence`, `pg_collation` and `pg_opclass`;
    - `SELECT unnest(indkey) FROM pg_index` gives 0 rows and does not
      error.
  - `catalog_functions_return_null`: `SELECT pg_get_constraintdef(1,
    true), pg_get_expr('x', 1), pg_get_indexdef(1, 1, true),
    pg_get_serial_sequence('a','b'), pg_collation_is_visible(1)` gives
    `NULL, NULL, NULL, NULL, t`.
  - `json_build_object_encodes_types`:
    `SELECT json_build_object('a', 1, 'b', true, 'c', NULL, 'd', 'x"y')`
    gives `{"a" : 1, "b" : true, "c" : null, "d" : "x\"y"}`. That is
    PostgreSQL's exact spacing; verify it with `psql` against the
    compose postgres.
  - `json_build_object_odd_args_refused`.
  - `regclass_runtime_casts`:
    - `SELECT CAST(CAST(oid AS REGCLASS) AS TEXT) FROM pg_class WHERE relname = 'nation'`
      gives `nation`;
    - `SELECT CAST(CAST(NULL AS TEXT) AS REGCLASS)` gives NULL;
    - `SELECT CAST(CAST('nope' AS TEXT) AS REGCLASS)` fails with an
      error containing `does not exist`.
  - Every test asserts `fallbacks()` did not change.
- [ ] **Step 2: Run `cargo test -p arneb-protocol --test pg_catalog`.**
  Expected: FAIL.
- [ ] **Step 3: Implement it.**
- [ ] **Step 4: Run the tests.** Run
  `cargo test -p arneb-protocol --test pg_catalog && cargo test -p arneb-execution planner_and_registry_return_types_agree`.
  Expected: PASS.
- [ ] **Step 5: Commit.** Message:
  `feat(protocol): pg_catalog columns, tables and functions for SQLAlchemy reflection`.

### Task 8: The captured SQLAlchemy SQL runs through the engine

**Files:**
- Create: `crates/protocol/tests/pg_reflection.rs`
- Create: `crates/protocol/tests/data/sqlalchemy_reflection/*.sql`. Each
  statement goes verbatim from `captured-fallback-sql.txt`, one file per
  family: `columns.sql`, `pk.sql`, `unique.sql`, `fk.sql`, `check.sql`.
  Also add `indexes.sql`, captured in Step 1.

**Interfaces:**
- Consumes: Tasks 1–7.
- Each test loads its `.sql` file and replaces the literal oid
  `1778988401` with the oid the harness's `nation` gets, read from
  `SELECT oid FROM pg_class WHERE relname='nation'`. It runs the
  statement with `simple_query`.

- [ ] **Step 1: Capture the index query.**
  1. Build, and start the standalone from the spec's probe setup.
  2. Run `scripts/client-smoke/pg_reflect_probe.py 25432` with
     `RUST_LOG=arneb::protocol=debug`. Either log statements, or
     temporarily set SQLAlchemy `echo=True` in a scratch copy.
  3. Save the `get_indexes` SQL as `indexes.sql`, with its bound oids
     inlined the same way.
  4. Stop the server.
- [ ] **Step 2: Write the tests.**
  - `reflect_columns`: 4 rows (`n_nationkey`, `n_name`, `n_regionkey`,
    `n_comment`).
    - `format_type` gives `bigint` / `character varying` (or whatever
      `format_type` yields today; pin it);
    - `default`, `identity_options` and `collation` are NULL;
    - `not_null` matches the harness table's nullability;
    - `table_name` is `nation`.
  - `reflect_pk`, `reflect_unique`, `reflect_fk`, `reflect_check`,
    `reflect_indexes`: each succeeds with 0 rows, and its column count
    equals the `SELECT` list length.
  - Every test asserts `fallbacks()` is unchanged.
- [ ] **Step 3: Run `cargo test -p arneb-protocol --test pg_reflection`.**
  Expected: PASS. If one fails, fix the owning task's feature, not the
  test SQL.
- [ ] **Step 4: Run the probe.**
  1. Start the standalone from the spec.
  2. Run
     `<venv>/bin/python -I scripts/client-smoke/pg_reflect_probe.py 25432`.
  3. Expected: 20 lines, all `OK`.
  4. Expected: `curl -s 127.0.0.1:26432/metrics | grep arneb_pg_catalog_fallbacks_total`
     gives `0`.
  5. Stop the server.
- [ ] **Step 5: Commit.** Message:
  `test(protocol): SQLAlchemy reflection SQL runs without the pg_catalog fallback`.

### Task 9: Smoke suite, distributed checks and docs

**Files:**
- Modify: `scripts/client-smoke/clients.py`
- Modify: `docs/guide/clients.md`, `docs/sql/expressions.md`,
  `docs/sql/functions.md`
- Modify: `docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`
  (the status line under Releases)

- [ ] **Step 1: Add SQLAlchemy postgresql reflection checks to
  `clients.py`.** For both drivers:
  - `get_columns("nation")` names equal
    `["n_nationkey","n_name","n_regionkey","n_comment"]`;
  - `get_pk_constraint` gives `{"constrained_columns": [], "name": None, …}`;
  - `get_foreign_keys`, `get_indexes`, `get_unique_constraints` and
    `get_check_constraints` give `[]`;
  - `Table(..., autoload_with=engine)` followed by
    `select(n.c.n_regionkey, func.count()).group_by(...).order_by(...).limit(3)`
    gives 3 rows;
  - then `arneb_pg_catalog_fallbacks_total` from
    `http://127.0.0.1:{PG_PORT+1000}/metrics` is `0`.
- [ ] **Step 2: Add distributed array and subquery checks.** Run these
  over pgwire against the 1+2 cluster and compare them with DuckDB on
  the same Parquet files, using `tpch_check.py`'s `rows_match`. Lists
  compare element-wise, and psycopg returns Python lists.
  1. `SELECT n_regionkey, array_agg(n_name ORDER BY n_name) FROM nation GROUP BY 1 ORDER BY 1`
  2. `SELECT r, unnest(a), generate_subscripts(a, 1) FROM (SELECT n_regionkey r, array_agg(n_nationkey ORDER BY n_nationkey) a FROM nation GROUP BY 1) s ORDER BY 1, 3`.
     DuckDB has no `generate_subscripts`, so its query uses
     `generate_subscripts(a, 1)` → `range(1, len(a)+1)` via
     `unnest(range(...))`. Write the DuckDB SQL by hand.
  3. `SELECT o_orderkey, ARRAY[o_orderkey, o_custkey] FROM orders ORDER BY 1 LIMIT 5`
  4. `SELECT c_custkey, (SELECT count(*) FROM orders WHERE o_custkey = c_custkey), (SELECT n_name FROM nation WHERE n_nationkey = c_nationkey), CASE WHEN c_acctbal < 0 THEN (SELECT r_name FROM region WHERE r_regionkey = c_nationkey % 5) END FROM customer ORDER BY 1 LIMIT 50`
  Also run query 1 through the Trino DB-API client and assert that each
  second column is a Python `list` equal to the pgwire result. This
  covers `array(varchar)` over the Trino protocol.
- [ ] **Step 3: Run the suite locally.** Run:
  ```
  cargo build --release --bin arneb && PYTHON=<venv>/bin/python scripts/client-smoke/run.sh <tpch sf1 dir>
  ```
  Expected: all `PASS`. Use the default ports 15432/18080/16432 and
  check first that nothing else is listening on them.
- [ ] **Step 4: Update the docs.**
  - `clients.md`:
    - the pgwire SQLAlchemy row now lists reflection of columns, keys
      and indexes (empty);
    - in "Not supported yet", drop the array, `json_build_object` and
      correlated-subquery caveats and the "Use the Trino protocol"
      advice;
    - keep the fallback note and the `pg_catalog` / user-table mixing
      note.
  - `expressions.md`: `ARRAY[...]`, `= ANY` / `<> ALL` over arrays, and
    correlated scalar subqueries in `SELECT`, listing their supported
    shapes.
  - `functions.md`: `array_agg`, `bool_and`, `unnest`,
    `generate_subscripts`, `json_build_object`, and the new catalog
    functions.
  - The roadmap: one status sentence saying stage 2b-1 is done.
- [ ] **Step 5: Commit.** Message:
  `test(smoke): SQLAlchemy pgwire reflection and distributed array checks; docs`.

---

## Self-review notes

- **Spec coverage.** Each spec section maps to tasks:
  - §1 → Task 7;
  - §2 value model and literals → Task 2;
  - `ANY` and bind parameters → Task 3;
  - `array_agg` and `bool_and` → Task 4;
  - SRFs → Task 5;
  - pgwire output → Task 2;
  - Trino array output → Task 9 Step 2, a Trino client assertion;
  - §3 → Task 6;
  - §4 → Tasks 8 and 9;
  - §5 → Task 9;
  - anti-goals → Task 1 (silent drops), Task 6 (multi-row), and Tasks 5
    and 6 Step 4 (plans unchanged).
- **Order.** Tasks 1→2→3→4→5 build on each other through the interfaces
  above. Task 6 depends only on Task 1 being merged into the AST shape.
  Task 7 needs Task 2 (array columns) and Task 5 (`unnest` in its test).
  Task 8 needs everything.
- **PR split.** If the diff after Task 5 is over about 3k lines, open
  PR A for Tasks 1–5 and PR B for Tasks 6–9, as the spec allows.
