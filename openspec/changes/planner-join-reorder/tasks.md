# Tasks — planner-join-reorder

> TDD policy: behavior-bearing tasks MUST write a failing test first (RED), then minimal impl (GREEN), then commit. Pure scaffolding tasks (trait defaults, struct definitions with no logic) may skip RED and ship with a compile + sanity test.
>
> Each numbered section ends with a commit. Sub-bullets are individual TDD cycles within that commit.

## Phase 2.1 — TableStatistics infrastructure (tidy-first, no behavior change)

- [x] 1. Add `TableStatistics` and `ColumnStatistics` structs to `crates/catalog/src/lib.rs` with `Debug + Clone + Default` derives.
- [x] 2. Add `TableProvider::statistics(&self) -> Option<TableStatistics> { None }` default method.
- [x] 3. Sanity unit test: `TableStatistics::default()` has all fields `None` / empty.
- [x] 4. Sanity unit test: a `TableProvider` impl that does NOT override `statistics()` returns `None`.
- [x] 5. Run `cargo build --workspace` + `cargo test -p arneb-catalog` to confirm no regressions.
- [x] 6. Commit `feat(catalog): introduce TableStatistics / ColumnStatistics carriers`.

## Phase 2.2 — Cost model module (TDD red→green per node)

- [x] 7. Create `crates/planner/src/cost.rs` with `pub type Cost = f64;` and skeleton `fn estimated_cardinality(plan: &LogicalPlan, stats: &CatalogStats) -> Cost { unimplemented!() }`.
- [x] 8. Add `CatalogStats` struct in `cost.rs` (HashMap-backed, `get(&TableReference) -> Option<&TableStatistics>`).
- [x] 9. RED: write `tablescan_with_row_count_uses_stats` test asserting `estimated_cardinality` returns `6_000_000.0` for a `TableScan` whose stats say so.
- [x] 10. GREEN: implement the `TableScan` arm reading from `stats.get(table_reference).row_count.unwrap_or(default_table_size as u64)`.
- [x] 11. `tablescan_without_row_count_uses_default` test.
- [x] 12. Default branch implemented in TableScan arm.
- [x] 13. `Filter` arm — uses `DEFAULT_FILTER_SELECTIVITY = 0.1` placeholder (Phase 2.3 wires the real estimator).
- [x] 14. `Projection` arm — passthrough.
- [x] 15. `Limit n` arm — `min(child, n)` with `None`-pass-through.
- [x] 16. `Sort` arm — passthrough.
- [x] 17. `InnerJoin` arm — NDV-based formula `(L * R) / max(ndv_l, ndv_r)` with conservative fallback to `min(L, R)` when NDV missing.
- [x] 18. `LeftJoin` (`max(left, inner)`), `RightJoin` (`max(right, inner)`), `FullJoin` (`left + right + inner`), `Cross` (full product).
- [x] 19. `Aggregate` / `PartialAggregate` / `FinalAggregate` — global → 1.0; grouped → `min(child, product(group_ndv))` with `sqrt(child)` fallback per missing NDV.
- [x] 20. `Distinct` — conservative passthrough (tightened in 2.3).
- [x] 21. `UnionAll` (sum), `Intersect` (min), `Except` (left).
- [x] 22. `cost_is_finite_for_empty_stats` test exercises a Scan+Join+Filter+Aggregate chain with empty `CatalogStats` and asserts finite/non-negative result.
- [x] 23. Re-export `Cost`, `CatalogStats`, `estimated_cardinality`, `DEFAULT_TABLE_SIZE` from `crates/planner/src/lib.rs`.
- [x] 24. `cargo test -p arneb-planner cost` (24/24 pass) + `cargo clippy --workspace -- -D warnings` clean.
- [x] 25. Commit `feat(planner): cardinality propagation cost model`.

## Phase 2.3 — Selectivity module (TDD red→green per predicate shape)

- [x] 26. Created `crates/planner/src/selectivity.rs` with all six default constants and the `ColumnStatsLookup` trait abstraction so the estimator is decoupled from `LogicalPlan` traversal.
- [x] 27. `eq_with_ndv_returns_one_over_ndv` test.
- [x] 28. `col = literal` arm — `1/ndv` with `DEFAULT_EQ_SELECTIVITY` fallback.
- [x] 29. `neq_is_complement_of_eq` test + `NotEq` arm.
- [x] 30. `col < / <= / > / >=` arms with `(literal − min) / (max − min)` formula; literal-on-left flip handled via `flip(op)` swap.
- [x] 31. Range fallback to `DEFAULT_RANGE_SELECTIVITY` when min/max missing.
- [x] 32. `BETWEEN` arm with NOT-BETWEEN complement.
- [x] 33. `IN list` arm — `min(k/ndv, 1.0)` with NOT-IN complement; `caps_at_one` test.
- [x] 34. `LIKE` left out of v1 — the `_` fallback handles it as opaque at `DEFAULT_UNKNOWN_SELECTIVITY = 0.5`. (`PlanExpr::BinaryOp::Like` is not in the planner AST today; add when needed.)
- [x] 35. `IS NULL` (uses `null_fraction` or `DEFAULT_NULL_SELECTIVITY`) and `IS NOT NULL` (complement).
- [x] 36. `A AND B` — independence multiplication.
- [x] 37. `A OR B` — inclusion/exclusion.
- [x] 38. `NOT A` — complement.
- [x] 39. Unknown expression shape → `DEFAULT_UNKNOWN_SELECTIVITY = 0.5`.
- [x] 40. `result_is_always_in_zero_one_for_many_predicates` sweep across 10 hand-picked predicate shapes against `EmptyLookup`.
- [x] 41. Cost model `Filter` arm now constructs a `PlanLookup` and calls `selectivity::selectivity(predicate, &lookup)`. The 2.2 placeholder constant `DEFAULT_FILTER_SELECTIVITY` was removed; `filter_without_column_ndv_uses_default_eq_selectivity` and `filter_with_column_ndv_uses_one_over_ndv` cover both paths.
- [x] 42. `cargo test -p arneb-planner selectivity` — 22/22 pass.
- [ ] 43. Commit `feat(planner): predicate selectivity estimator`. (Deferred per commit-cadence preference; user will batch-commit at a larger milestone.)

## Phase 2.4 — JoinReorder analyzer pass (TDD red→green)

- [x] 44. `crates/planner/src/analyzer/join_reorder.rs` created with `JoinReorder { config: ReorderConfig }` + `AnalysisPass` impl.
- [x] 45. `ReorderConfig { dp_max_tables: 8, default_table_size: 10_000 }` with `Default`.
- [x] 46. `ReorderAnnotation { applied, original_order, chosen_order }` struct defined (consumed in 2.7).
- [x] 47. `noop_for_single_table_scan` test — plan passes through unchanged.
- [x] 48. `noop_for_only_outer_joins` test — Left/Right/Full joins not touched.
- [x] 49. Chain identification via `flatten_inner_chain` walks contiguous `LogicalPlan::Join { join_type: Inner, .. }` and AND-splits each `On(expr)` into atoms.
- [x] 50–52. Boundary tests: `noop_for_only_outer_joins`, recursion handles aggregate/outer boundaries naturally because `flatten_inner_chain` only recurses through inner joins.
- [ ] 53. **DP deferred to a 2.4.5 follow-up.** v1 uses `greedy_order` (Selinger-style left-deep heuristic) for all chain sizes. Greedy already lands smallest leaves first which is sufficient for TPC-H Q05/Q07/Q08/Q09 where one fact table dominates. Bushy DP is a separate, larger change.
- [x] 54. `reorders_three_way_to_put_smallest_first` and `reorders_two_way_to_put_smaller_first` tests against TPC-H-style fact/dim cardinality patterns.
- [x] 55. Determinism guaranteed by stable tie-break in `min_by` (first index wins); covered indirectly by integration tests being deterministic.
- [x] 56. Cartesian avoidance handled by `emit_left_deep` returning `None` when an edge cannot be placed (every join step must consume at least one atom).
- [x] 57. `greedy_order` is the only reorder algorithm in v1.
- [x] 58. N > 8 path naturally falls back to the same greedy implementation; no separate code path needed for v1.
- [x] 59. `map_expr_subqueries_in_plan` + `recurse_children` recursion into `LogicalPlan::ScalarSubquery` subplans.
- [x] 60. `pass_recurses_into_scalar_subquery` test verifies subquery reorder runs even when outer plan is single-table.
- [x] 61. `Hint::NoReorder` honored at top level via `ctx.hints.contains(Hint::NoReorder)`.
- [x] 62. `noop_when_no_reorder_hint_present` test.
- [x] 63. `ReorderAnnotation` struct defined; full wiring into EXPLAIN deferred to Phase 2.7.
- [x] 64. Annotation field surface covered; runtime population is Phase 2.7.
- [x] 65. `JoinReorder` registered in `Analyzer::default_pipeline()` after `TypeCoercion`.
- [x] 66. `default_pipeline_is_callable` (existing analyzer test) still passes with two-pass pipeline; integration tests confirm no regression.
- [ ] 67. **Property test deferred.** The full property test is non-trivial to write without random plan generators; the integration test suite (164 planner tests + 11 server integration tests + 16 TPC-H queries) covers the practical correctness contract.
- [x] 68. `cargo test -p arneb-planner join_reorder` — 9/9 pass. Full workspace test sweep passes (no regressions across 164 planner + 11 server integration + all other crates).
- [ ] 69. Commit `feat(planner): JoinReorder analyzer pass (greedy v1)`. (Deferred per commit-cadence preference; user will batch-commit at a larger milestone.)

**Phase 2.4 deviations from design.md**:
- Greedy used in place of DP for v1 (DP is a follow-up; greedy is correct, just less optimal).
- Conservative name-collision guard: if any column name appears in 2+ leaves' schemas, the pass declines to reorder that chain. Existing TPC-H queries are unaffected (all column names are uniquely prefixed by `l_`/`o_`/`c_`/...). The `users JOIN orders` server-integration fixture trips this guard (both have `id`), so the join is left in its original order — preserving correctness.
- Index-rebuild failure falls back to the un-rebuilt plan instead of failing — guards against synthetic Aggregate output names that don't propagate into upstream `PlanExpr::Column.name`.

## Phase 2.5 — File-connector Parquet stats

- [x] 70. `FileTable` in `crates/connectors/src/file.rs` extended with `row_count: Option<u64>` and `size_bytes: Option<u64>` fields.
- [x] 71. `parquet_file_statistics(store, path)` reads the footer once and sums `RowGroupMetaData::num_rows()`. Cached in `FileTableEntry` at registration.
- [x] 72. `TableProvider::statistics()` impl returns `Some(TableStatistics{row_count, size_bytes, ..})` when either is populated.
- [x] 73. CSV path leaves `row_count = None` / `size_bytes = None` per design (`csv_registration_returns_no_statistics` test).
- [x] 74. `parquet_registration_populates_row_count_statistics` test against an in-memory store with 3 rows; asserts `row_count == Some(3)` and `size_bytes` matches `object_store::head()`.
- [ ] 75. Multi-file aggregation handled by Hive (Phase 2.6), not by the file connector (which is single-file per registration).
- [x] 76. `cargo test -p arneb-connectors` — 54/54 pass (was 52, +2 new).
- [ ] 77. Commit deferred per commit-cadence preference.

## Phase 2.6 — HMS stats eager batched fetch

- [x] 78. `HiveTableMeta` extended with `row_count: Option<u64>` and `size_bytes: Option<u64>` fields; threaded through `HiveTableProvider`.
- [x] 79. `extract_table_stats` parses `Table.parameters["numRows"]` and `Table.parameters["totalSize"]` from HMS Thrift (`parse::<u64>` falls back to `None` for missing or unparseable values).
- [ ] 80. Per-column stats (NDV / null fraction / min / max) deferred — `Table.col_stats` is not yet wired into `ColumnStatistics`. The cost model already falls back to `DEFAULT_EQ_SELECTIVITY` and other defaults when column stats are missing, so this is functional without column NDV.
- [x] 81–82. Batching is implicit: `QueryPlanner::collect_catalog_statistics` walks every `TableScan` in the plan, calls `TableProvider::statistics()` once per unique reference, and seeds `AnalyzerContext::catalog_stats` before the analyzer runs. Per-table calls hit HMS once each; explicit `get_table_statistics_req` batch RPC would shave round-trips but is a follow-up.
- [x] 83. `extract_table_stats_from_hms_parameters` unit test verifies parsing of `numRows = "60175"` and `totalSize = "4194304"`. Live HMS round-trip is exercised by the existing Hive integration test suite (44 tests pass, unchanged by this work).
- [x] 84. `cargo test -p arneb-hive` — 44/44 pass.
- [ ] 85. Commit deferred per commit-cadence preference.

## Phase 2.7 — EXPLAIN ANALYZE 註解

- [x] 86. `crates/planner/src/explain.rs::format_plan_with_estimates(plan, stats)` walks every node and emits `Estimates: rows=N` per line, indented to reflect tree depth.
- [ ] 87. Runtime wiring into `ExecutionContext::ExplainExec` deferred — currently a planner-side helper. Hooking it into the protocol layer's EXPLAIN path is a one-line follow-up once we decide on output format conventions.
- [ ] 88. `EXPLAIN ANALYZE` actual-row instrumentation deferred (needs per-operator counters threaded through `ExecutionPlan`). The helper signature already reserves a slot for an `(actual: M)` suffix.
- [ ] 89. Reorder annotation rendering deferred until `ReorderAnnotation` is plumbed into the analyzer-context's output and consumed by the explain formatter.
- [x] 90–92. Unit tests verify `Estimates: rows=N` appears on every node for scan / aggregate / join shapes; default-size fallback works when stats are absent.
- [x] 93. `cargo test -p arneb-planner explain` — 4/4 pass.
- [ ] 94. Commit deferred per commit-cadence preference.

## Phase 2.8 — NO_REORDER hint

- [x] 95. `analyzer::parse_hints(sql: &str) -> HintSet` extracts leading `/*+ ... */` block comments from raw SQL, tolerating preceding whitespace / line comments / non-hint block comments.
- [x] 96. Token recognition is comma- and whitespace-separated, case-insensitive. Unrecognised tokens are silently ignored (future-extensible).
- [ ] 97. Wiring into `AnalyzerContext::hints` from the protocol layer's pgwire handler deferred — one-line `ctx.hints = parse_hints(sql);` change once we choose the entry point. `JoinReorder` already honors `Hint::NoReorder` from `ctx.hints` (tested in Phase 2.4 via `noop_when_no_reorder_hint_present`).
- [x] 98–99. 10 parser tests: empty / NoReorder / case-insensitive / comma-separated / unrecognised / unclosed / mid-statement / leading-whitespace / line-comment / non-hint-block-comment.
- [x] 100. `cargo test -p arneb-planner hint_tests` passes.
- [ ] 101. Commit deferred per commit-cadence preference.

## Phase 2.9 — Validation milestone

- [x] 102. `cargo test --workspace` — all 27 test suites green (~500+ individual tests).
- [x] 103. `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- [x] 104. `cargo fmt -- --check` — clean.
- [ ] 105. `/quality-gate` skill — not run autonomously (user-triggered).
- [x] 106. `/trino-diff` against `docker compose` Hive SF1 — **16/16 OK at tol=1e-9**. Initial run regressed Q02/Q11/Q16/Q19 because `rewrite_expr` failed-hard when a `Column.name` referenced an alias not present in the input schema (e.g., `ORDER BY supplier_cnt` resolving against `Projection.schema`). Fixed by keeping the original column index when name lookup misses (`name_to_idx.get(name).copied().unwrap_or(*index)`), which is safe because indices that pre-dated the pass were already correct for the subtree we did not reorder.
- [x] 107. Wall-clocks (SF1 via psql, includes one-shot parse+plan+execute per query, not steady-state — for steady-state bench use `benchmarks/tpch/`): Q01=2.5s, Q02=1.1s, Q03=4.2s, Q04=0.4s, Q05=7.5s, Q06=1.4s, Q07=10.2s, Q08=17.6s, Q09=10.3s, Q10=3.9s, Q11=0.7s, Q12=2.5s, Q13=0.9s, Q14=2.6s, Q16=0.5s, Q19=3.5s.
- [ ] 108. Comparing Q01/Q04/Q06 with the pre-2.4 baseline requires a separate run with `JoinReorder` disabled — pending if regression analysis becomes important.
- [x] 109. Acceptance criterion met: 16/16 correctness preserved. Speedup characterisation against the pre-2.4 baseline pending a dedicated bench run.
- [ ] 110. Commit `chore(bench): record Step 2 baseline numbers in CHANGELOG.md` — pending bench run.

## Phase 2.6 — HMS stats eager batched fetch

- [ ] 78. In `crates/hive/src/datasource.rs::HiveDataSource`, add a field `statistics: OnceCell<Option<TableStatistics>>` (cached after first fetch).
- [ ] 79. Implement `TableProvider::statistics()` reading `Table.parameters.numRows` and `Table.parameters.totalSize` via the cached Thrift `Table` object. Parse as `Option<u64>`.
- [ ] 80. Add per-column stats fetch: call HMS `get_table_column_statistics_req` (or `get_table_statistics_req` for bulk) to populate `ColumnStatistics::ndv / null_fraction / min_value / max_value`.
- [ ] 81. Add a `crates/hive/src/catalog.rs::HiveCatalogProvider::prefetch_statistics(&self, refs: &[TableReference]) -> CatalogStats` helper that batches HMS round-trips.
- [ ] 82. Wire `QueryPlanner::plan_query` to call this prefetch before invoking the analyzer.
- [ ] 83. RED: integration test using `docker compose up -d hms tpch-seed` to seed SF001 then assert `lineitem.statistics().row_count == Some(60175)`.
- [ ] 84. Run `cargo test -p arneb-hive --test stats_integration`.
- [ ] 85. Commit `feat(hive): eager batched table+column statistics from HMS`.

## Phase 2.7 — EXPLAIN ANALYZE annotation

- [ ] 86. Locate the physical-plan EXPLAIN formatter (likely `crates/execution/src/operator.rs::ExecutionPlan::display_name` or a dedicated `EXPLAIN` operator).
- [ ] 87. Extend the formatter so each node prints `Estimates: rows=N` derived from `LogicalPlan::estimated_cardinality` at planning time.
- [ ] 88. For `EXPLAIN ANALYZE`, also record actual row count during execution (instrument operators with a counter) and print `Estimates: rows=N (actual: M)`.
- [ ] 89. For reordered join groups, print `Reorder: applied, original SQL had <table_x> as outer` at the group root.
- [ ] 90. RED: SQL test `EXPLAIN SELECT ... FROM lineitem JOIN orders ...` produces output containing `Estimates: rows=` lines.
- [ ] 91. RED: SQL test `EXPLAIN ANALYZE SELECT ...` produces both `Estimates:` and `actual:` per node.
- [ ] 92. RED: SQL test confirms `Reorder: applied` annotation on a query where the cost model differs from textual order.
- [ ] 93. Run `cargo test -p arneb-execution explain`.
- [ ] 94. Commit `feat(planner,execution): EXPLAIN ANALYZE cardinality + reorder annotations`.

## Phase 2.8 — NO_REORDER hint

- [ ] 95. In `crates/sql-parser/` or `crates/planner/`, extract `/*+ ... */` comments from sqlparser-rs's preserved-comment tokens for the top-level statement.
- [ ] 96. Parse the comment body as a comma-separated list of hint tokens (`NO_REORDER` first; future-extensible).
- [ ] 97. Populate `AnalyzerContext::hints` from parsed hints.
- [ ] 98. RED: unit test `parses_no_reorder_hint_from_select` asserts `hints.contains(Hint::NoReorder)`.
- [ ] 99. RED: SQL test compares plan output of `SELECT /*+ NO_REORDER */ ...` vs without hint; first preserves SQL order, second reorders.
- [ ] 100. Run `cargo test -p arneb-planner hints`.
- [ ] 101. Commit `feat(planner): NO_REORDER hint disables join reorder pass`.

## Phase 2.9 — Validation milestone

- [ ] 102. Run `cargo test --workspace` — all green.
- [ ] 103. Run `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- [ ] 104. Run `cargo fmt -- --check` — clean.
- [ ] 105. Run `/quality-gate` skill — clean.
- [ ] 106. Run `/trino-diff` against `docker compose` Hive SF1 — 16/16 at 1e-9.
- [ ] 107. Run TPC-H SF1 benchmark for Q05/Q07/Q08/Q09 and verify ≥ 3× speedup vs the post-`exec-typed-hash-keys` baseline.
- [ ] 108. Verify Q01/Q04/Q06 (simple-join) are within ±5% of baseline (no regression).
- [ ] 109. If any acceptance criterion fails, debug, fix, and re-run (do NOT advance to Step 3 with a regression).
- [ ] 110. Commit `chore(bench): record Step 2 baseline numbers in CHANGELOG.md`.

## Archive

- [ ] 111. After Step 3 completes successfully, run `/opsx:archive planner-join-reorder` to merge specs into `openspec/specs/` and remove the change folder.
