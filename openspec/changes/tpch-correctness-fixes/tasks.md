# Tasks — tpch-correctness-fixes

## 1. PB-001 — self-join alias collapse (planner)

- [x] 1.1 Add `group_by_qualifier(&ast::Expr) -> Option<String>` helper in `crates/planner/src/planner.rs` returning `col_ref.table.clone()` for `ast::Expr::Column` and `None` otherwise.
- [x] 1.2 In `plan_query_body`, after building the `Aggregate` node, rebuild `PlanningContext` with per-slot qualifier: group-by slots carry `group_by_qualifier(&body.group_by[i])`; aggregate-output slots carry `None`.
- [x] 1.3 Unit test `test_self_join_alias_projection_does_not_collapse` — self-join with GROUP BY on both aliases' shared column; assert projection has two distinct Column indices.

## 2. PB-002 — duplicate aggregate index collision (planner)

- [x] 2.1 Add `format_ast_unqualified(&ast::Expr) -> String` to `crates/planner/src/planner.rs`. Walk the AST, stripping column qualifiers and unwrapping `ast::Expr::Nested`. Cover `Column`, `Literal`, `BinaryOp`, `UnaryOp`, `Function` (with `DISTINCT` keyword if present), `IsNull`, `IsNotNull`, `Between`, `InList`, `Cast`, `Case`; fall back to Display for other variants.
- [x] 2.2 In `find_aggregate_index`, replace `format!("{expr}")` with `format_ast_unqualified(expr)`.
- [x] 2.3 Delete the name-prefix fallback (the `col.name.to_uppercase().starts_with(&name_upper)` loop). Keep the "exactly one aggregate slot" last-ditch guard.
- [x] 2.4 Unit test `test_two_same_function_aggregates_do_not_collide` — `SELECT SUM(CASE..), SUM(t.col) FROM t` resolves to two distinct Column indices and the Aggregate carries two `aggr_exprs`.
- [x] 2.5 Unit test `test_aggregate_with_nested_parens_resolves` — `SELECT SUM(id * (1 - id)), SUM(age) FROM users` resolves both projections to Column refs (no `ColumnNotFound`).
- [x] 2.6 Add an inline doc comment on `format_ast_unqualified` calling out the contract: it must stay aligned with `PlanExpr` Display.

## 3. PB-003 — COUNT(DISTINCT) over-count (execution)

- [x] 3.1 Extend `create_accumulator` signature: `(func_name, is_count_star, distinct) -> Result<Box<dyn Accumulator>>`. Build the base accumulator as today; wrap in `DistinctAccumulator` when `distinct && !is_count_star && upper != "MIN" && upper != "MAX"`.
- [x] 3.2 Implement `DistinctAccumulator { inner, seen: HashSet<String> }`. `update_batch` walks the array, skips nulls, computes `dedup_key`, forwards first-seen values via `arrow::compute::take`. `evaluate` / `reset` delegate; `reset` also clears `seen`.
- [x] 3.3 Implement `dedup_key(arr, index) -> Result<String>` covering `Int32`, `Int64`, `Float32/64` (via `to_bits()`), `Utf8`, `LargeUtf8`, `Boolean`, `Date32`, `Date64`, `Decimal128(p,s)`, `Timestamp(unit, tz)`. Unsupported types return `ExecutionError::InvalidOperation`.
- [x] 3.4 In `crates/execution/src/operator.rs`, add `distinct: bool` to `AggrInfo`. Populate it from `PlanExpr::Function::distinct` at construction. Pass it through to both `create_accumulator` call sites (the streaming HashAggregate path and the eager `aggregate_batches` path).
- [x] 3.5 Unit test `count_distinct_over_arrays_drops_duplicates_and_nulls` in `aggregate.rs`.
- [x] 3.6 Unit test `count_distinct_resets_between_groups` in `aggregate.rs`.
- [x] 3.7 Integration test `count_distinct_groups_deduplicates` in `operator.rs` — full `HashAggregateExec` with `distinct: true`, two groups, expected count of 3 per group.

## 4. Verification

- [x] 4.1 `cargo fmt -- --check` clean.
- [x] 4.2 `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [x] 4.3 `cargo test --workspace` — all tests green, including the seven new unit / integration tests above.
- [x] 4.4 End-to-end via the `trino-diff` skill: bring up `docker compose up -d` + `tpch-seed`, run the 16 TPC-H queries through both Arneb and Trino on SF1, diff CSV cell-by-cell at relative tolerance `1e-9`. Assert 16/16 queries values-identical.
- [x] 4.5 Run the `quality-gate` skill (fmt + clippy + tests + cargo-deny) as the pre-PR gate.

## 5. Cleanup

- [x] 5.1 Remove PB-001 / PB-002 / PB-003 sections from `TODO.md`. Keep the "Discovery workflow" block as the reference recipe.
- [x] 5.2 Add a cross-reference note in `openspec/changes/archive/2026-04-23-planner-type-coercion/tasks.md` task #53's follow-up paragraph pointing to this change as the resolution.

## 6. PR / archive

- [ ] 6.1 Commit via the `commit` skill. Suggested title: `fix(planner,execution): TPC-H values-equivalence with Trino (16/16)`.
- [ ] 6.2 Open the PR via the `pr` skill. Body references this change name and the four affected TPC-H queries.
- [ ] 6.3 After merge, run `openspec-archive-change` to move this change into `openspec/changes/archive/<date>-tpch-correctness-fixes/`.
