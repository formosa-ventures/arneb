# Tasks — exec-typed-hash-keys

## 1. New `GroupKey` module

- [x] 1.1 Create `crates/execution/src/group_key.rs`. Define `pub(crate) struct GroupKey(pub(crate) Vec<ScalarValue>)`. Derive `Debug, Clone`.
- [x] 1.2 Implement `Hash` for `GroupKey` manually. Walk `self.0`; for each `ScalarValue` variant write a 1-byte type tag then the payload (using `to_bits()` for `Float32` / `Float64`).
- [x] 1.3 Implement `PartialEq` and `Eq` for `GroupKey` manually. Length-first, then per-element: equal type, equal payload (using `to_bits()` for floats). NULL == NULL, NULL != non-NULL.
- [x] 1.4 Add `mod group_key;` to `crates/execution/src/lib.rs`.
- [x] 1.5 Unit tests in `group_key.rs`:
  - `int_keys_hash_eq_consistent` — `GroupKey([Int64(1)])` == `GroupKey([Int64(1)])`, hashes equal.
  - `float_nan_same_bits_collapses` — same NaN bit pattern hashes / equals.
  - `float_nan_different_bits_distinct` — different NaN bit patterns are distinct.
  - `cross_type_distinct` — `GroupKey([Int32(1)])` != `GroupKey([Int64(1)])` != `GroupKey([Utf8("1")])`.
  - `null_handling` — `GroupKey([Null])` == `GroupKey([Null])`, != `GroupKey([Int32(0)])`.

## 2. `DistinctAccumulator` migration

- [x] 2.1 In `crates/execution/src/aggregate.rs`, change `DistinctAccumulator.seen` from `FastHashSet<String>` to `FastHashSet<GroupKey>`.
- [x] 2.2 Replace `dedup_key()` with `array_value_to_scalar(arr, index) -> Result<ScalarValue>` (or reuse existing helper if one is in the codebase already; search `array_value_to_scalar` / `scalar_from_array`).
- [x] 2.3 `DistinctAccumulator::update_batch` walks the array, skips nulls (existing behavior), extracts `ScalarValue`, wraps in `GroupKey(vec![scalar])`, inserts into seen.
- [x] 2.4 Delete `dedup_key()` and the `format!()`-based code path entirely.

## 3. `HashAggregateExec` migration

- [x] 3.1 In `crates/execution/src/operator.rs`, change the type of the `groups` map from `FastHashMap<String, GroupState>` to `FastHashMap<GroupKey, GroupState>` (both declarations near lines 545 and 635).
- [x] 3.2 Rewrite `group_key()` helper to return `GroupKey` (build `Vec<ScalarValue>` row-wise from the group-by `cols`, wrap in `GroupKey`). Keep the row-wise extraction loop — vectorization is out of scope for this change.
- [x] 3.3 Rewrite `build_aggregate_output()` to read each group's scalars directly from the stored `GroupKey` instead of splitting the joined String by `'|'` and re-parsing. Each output group-by column SHALL build an Arrow `Array` from the `Vec<ScalarValue>` collected from `GroupKey.0[group_by_index]` across all groups.
- [x] 3.4 Delete the `extract_scalar(...).to_string()` path (the helper may still be useful for the `GroupKey` build — keep `extract_scalar` itself).

## 4. Verification

- [x] 4.1 `cargo fmt -- --check` clean.
- [x] 4.2 `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [x] 4.3 `cargo nextest run --workspace` — all tests green, including existing PB-003 regression tests and the five new `group_key` unit tests.
- [x] 4.4 `cargo deny check` — no advisories or license issues.
- [x] 4.5 `trino-diff` skill at 1e-9: **16/16 values-identical** (no correctness regression). Confirmed 2026-05-12.
- [x] 4.6 TPC-H SF1 benchmark (8 runs / 2 warmup) — see `benchmarks/tpch/results/after-typed-hash-keys.md`:
  - Geomean vs AHash:     **1.090×** (gate of 1.20× missed; see Observations).
  - Geomean vs Baseline:  **1.173×** (cumulative two-change speedup).
  - Regressions (<0.95×): **0**.
  - Wins ≥1.10×:          **6** (Q01 1.27×, Q05 1.11×, Q07 1.26×, Q08 1.17×, Q13 1.14×, Q16 1.11×).
  - Q01 (headline): 2879→2266 ms, a 21% wall-clock reduction on a single big GROUP BY.
  - Saved to `benchmarks/tpch/results/after-typed-hash-keys-arneb.json` and `after-typed-hash-keys.md`.

## 5. PR

- [ ] 5.1 `commit` skill. Suggested title: `perf(execution): typed hash keys for HashAggregate and DISTINCT`.
- [ ] 5.2 `pr` skill. Body includes the before/after table and the NaN scenarios from the spec.
- [ ] 5.3 After merge, run `openspec-archive-change`.
