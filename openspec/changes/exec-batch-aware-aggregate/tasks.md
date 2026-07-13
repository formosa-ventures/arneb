# Tasks — exec-batch-aware-aggregate

> TDD policy: behavior-bearing tasks MUST write a failing test first (RED), then minimal impl (GREEN). Tidy-first scaffolding tasks may skip RED. Each phase ends with `cargo test -p arneb-execution` green + a logical checkpoint; commit cadence decided by user (per `feedback_commit_cadence.md`).

## Phase BA.1 — OpenSpec scoping (this phase)

- [x] 1. Write `proposal.md` capturing the per-row → per-batch motivation, the API skeleton, and the expected Q01 speedup.
- [x] 2. Write `design.md` with 6 decisions: two-trait coexistence, trait shape, GroupByHash interface, DISTINCT fallback, parallel merge_from, no-grouping degenerate case.
- [x] 3. Write `specs/grouped-accumulator/spec.md`, `specs/group-by-hash/spec.md`, `specs/hash-aggregate-exec/spec.md`.
- [x] 4. Write `specs/exec-batch-aware-aggregate/tasks.md` (this file).
- [x] 5. Run `openspec validate exec-batch-aware-aggregate --strict` — must pass.

## Phase BA.2.a — GroupedAccumulator trait skeleton (tidy-first, no behavior change)

- [x] 6. Add `pub trait GroupedAccumulator` to `crates/execution/src/aggregate.rs` with the 6 method signatures from design Decision 2.
- [x] 7. Add a one-line `pub use` re-export from `crates/execution/src/lib.rs`.
- [x] 8. `cargo build --workspace` — must compile (trait has no impls yet).

## Phase BA.2.b — GroupedCountAccumulator (TDD red→green)

- [x] 9. RED: write `count_grouped_basic` test in `aggregate.rs::tests` — 3 rows, 2 groups, count_star=true; expect `evaluate(0)==2`, `evaluate(1)==1`.
- [x] 10. GREEN: implement `GroupedCountAccumulator { counts: Vec<i64>, count_star: bool }` with `ensure_capacity`, `add_input` (matches on `count_star` for the inner loop), `evaluate`, `num_groups`, `merge_from`, `as_any`.
- [x] 11. RED: `count_grouped_non_null_skips_null` test.
- [x] 12. GREEN: implement null-skip branch.
- [x] 13. RED: `count_grouped_evaluate_unused_group_returns_zero` (note: COUNT returns 0 for empty group, not Null — matches existing CountAccumulator).

## Phase BA.2.c — GroupedSumAccumulator (TDD red→green)

- [x] 14. RED: `sum_grouped_int64_two_groups` test.
- [x] 15. GREEN: implement `GroupedSumAccumulator` with internal state matching the existing per-instance one (i64, f64, decimal i128, has_values per group). Match on `values.data_type()` once per `add_input` call, then a typed-array iteration.
- [x] 16. RED: `sum_grouped_decimal128_widens_to_precision_38`.
- [x] 17. GREEN: decimal branch.
- [x] 18. RED: `sum_grouped_evaluate_empty_group_returns_null`.

## Phase BA.2.d — GroupedAvgAccumulator (TDD red→green)

- [x] 19. RED: `avg_grouped_int_two_groups`.
- [x] 20. GREEN: `GroupedAvgAccumulator { sum: Vec<f64>, count: Vec<i64> }`.
- [x] 21. RED: `avg_grouped_evaluate_empty_group_returns_null`.

## Phase BA.2.e — GroupedMinAccumulator + GroupedMaxAccumulator (TDD red→green)

- [x] 22. RED: `min_grouped_int_two_groups`, `max_grouped_string_two_groups`.
- [x] 23. GREEN: `GroupedMinAccumulator { state: Vec<Option<OrdScalar>> }` and `GroupedMaxAccumulator { state: Vec<Option<OrdScalar>> }`. Reuse existing `OrdScalar` + `extract_ordscalar` helpers from this module.
- [x] 24. RED: `min_grouped_decimal128`, `max_grouped_timestamp`.

## Phase BA.2.f — `create_grouped_accumulator(name, count_star, distinct)` factory

- [x] 25. Add free function `pub fn create_grouped_accumulator(name: &str, is_count_star: bool, distinct: bool) -> Result<Box<dyn GroupedAccumulator>, ExecutionError>` returning `Err` if `distinct == true` (so callers explicitly fall back). For supported funcs, dispatch on the uppercased name.
- [x] 26. Sanity test: COUNT/SUM/AVG/MIN/MAX each return the expected concrete type via `as_any().downcast_ref()`.

## Phase BA.2.g — GroupByHash (TDD red→green)

- [x] 27. Create `crates/execution/src/group_by_hash.rs` with `GroupByHash { table, keys }`. `pub use` from lib.rs.
- [x] 28. RED: `group_by_hash_assigns_stable_ids` per spec scenario.
- [x] 29. GREEN: implement `get_group_ids` walking row-by-row, building `GroupKey` via the existing `extract_scalar` helper (re-export or move to a shared location).
- [x] 30. RED: `group_by_hash_handles_null_in_group_col`.
- [x] 31. RED: `group_by_hash_two_column_keys`.
- [x] 32. RED: `group_by_hash_keys_preserve_insertion_order`.

## Phase BA.3 — Rewrite HashAggregateExec hot loop (behavior-bearing)

- [x] 33. RED: locking-down test `hash_aggregate_two_groups_sum_count_avg` calling the *operator* (not the accumulators) end-to-end on a `MemoryDataSource`. Run before the rewrite — must already pass.
- [x] 34. Add `fn has_distinct(&self) -> bool` helper on `HashAggregateExec`.
- [x] 35. Add new private method `execute_sync_batch_aware` mirroring `execute_sync` but doing one `get_group_ids` + one `add_input` per aggregate per batch. Build output by iterating `0..gbh.num_groups()`.
- [x] 36. Add new private method `execute_no_grouping_batch_aware` (synthesises `vec![0u32; n_rows]`).
- [x] 37. Update `execute_sync` to call the new batch-aware method when `!has_distinct()`; otherwise call the legacy per-row method (now renamed `execute_sync_legacy`).
- [x] 38. Update `execute_no_grouping` similarly.
- [x] 39. Run `hash_aggregate_two_groups_sum_count_avg` — must stay green.
- [x] 40. Update `build_partial_groups` to use the new path when distinct=false; legacy when distinct=true. Output `(GroupByHash, Vec<Box<dyn GroupedAccumulator>>)` for the batch-aware case.
- [x] 41. Rewrite the merge step in `execute_parallel` to construct global `GroupByHash` + final GroupedAccumulators + call `merge_from(..., &remap)` per partial per aggregate. Keep the legacy merge for the distinct case.
- [x] 42. RED: `parallel_aggregate_overlapping_groups` test exercising the group_remap path with 2 partitions producing different partial group orderings.
- [x] 43. GREEN: bug-fix as needed until parallel test passes.

## Phase BA.4 — Validation milestone

- [x] 44. `cargo test --workspace`.
- [x] 45. `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] 46. `cargo fmt --check`.
- [x] 47. `/trino-diff 1e-9` — **16/16 must remain green**.
- [x] 48. TPC-H benchmark (`cd benchmarks/tpch && cargo run --release -- --engine arneb --port 5432`); capture Q01/Q03/Q06/Q14 times vs Trino. Expected: Q01 wall-clock ≥ 4× faster than Step 3.5 baseline.
- [x] 49. Update memory `project_batch_aware_accumulator.md` with the measured numbers (replace the "expected ~5×" line with actual delta).
- [x] 50. Stop and wait for user — do not auto-commit (per `feedback_commit_cadence.md`).
