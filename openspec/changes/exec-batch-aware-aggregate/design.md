## Context

Memory `project_batch_aware_accumulator.md` and
`reference_trino_internals.md` document the finding: Trino's
hash-aggregate is **batch-aware**, not SIMD. The `GroupedAccumulator`
takes the entire Page + a pre-computed `int[] groupIds` and iterates
positions inside one method call. Arneb's current per-row dispatch
allocates ~30M `ArrayRef` slices for TPC-H Q01.

This design captures the Arneb mapping and the trade-offs that came
up when sketching the API.

## Decision 1 — Two-trait coexistence (don't remove `Accumulator`)

Keep the existing `Accumulator` trait (per-instance, batch-input)
in `crates/execution/src/aggregate.rs`. It is still used by the
**window** operator (`crates/execution/src/window.rs`) which feeds
a fresh accumulator per window frame; rewriting Window is out of
scope for this change.

Add a sibling trait `GroupedAccumulator` (one instance per aggregate,
internal state indexed by `group_id: u32`). HashAggregateExec uses
the new trait. Both traits live in the same module and share helpers
where possible.

**Why:** Surgically scoped change. Migrating Window can be a follow-up
once batch-aware aggregates are proven by a TPC-H bench delta.

## Decision 2 — GroupedAccumulator trait shape

```rust
pub trait GroupedAccumulator: Send + Sync {
    /// Grow internal `Vec<State>` to at least `num_groups` entries.
    /// Called by HashAggregateExec after `GroupByHash::get_group_ids`
    /// once per batch with `num_groups = group_by_hash.num_groups()`.
    fn ensure_capacity(&mut self, num_groups: usize);

    /// Update state for every position `i` in `values`:
    /// `state[group_ids[i]] ⊕= values[i]` (skipping nulls).
    /// `group_ids.len() == values.len()` is required.
    fn add_input(
        &mut self,
        group_ids: &[u32],
        values: &ArrayRef,
    ) -> Result<(), ExecutionError>;

    /// Materialise the aggregate result for a single group.
    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError>;

    fn num_groups(&self) -> usize;

    /// Merge `other`'s state into self, mapping its group IDs through
    /// `group_remap` (i.e. `self.state[group_remap[g]] ⊕= other.state[g]`).
    /// Used by the parallel partial-merge step.
    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError>;

    fn as_any(&self) -> &dyn std::any::Any;
}
```

**No `AggregationMask` parameter.** Arneb's input is Arrow `ArrayRef`,
which already carries a null bitmap (`is_null(i)`). We use that
directly instead of introducing a separate mask type. If a future
filter-pushdown pass wants to skip rows based on an external predicate,
we can layer a `mask: &BooleanArray` arg later.

## Decision 3 — GroupByHash batch interface

```rust
pub struct GroupByHash {
    table: FastHashMap<GroupKey, u32>,
    keys: Vec<GroupKey>,
}

impl GroupByHash {
    pub fn new() -> Self;

    /// For each row, assign a stable group ID (inserting on miss).
    /// Returns a Vec aligned with the batch (`result[i]` = group_id
    /// for row i).
    pub fn get_group_ids(
        &mut self,
        group_cols: &[ArrayRef],
    ) -> Result<Vec<u32>, ExecutionError>;

    pub fn num_groups(&self) -> usize { self.keys.len() }
    pub fn keys(&self) -> &[GroupKey] { &self.keys }
}
```

`get_group_ids` still walks `for i in 0..n_rows` to build the
`GroupKey`. We did **not** rebuild the typed group-key path. This is
intentional:

- `GroupKey` already exists, is well-tested under `crates/execution/src/group_key.rs`, and serves both this path and the existing DistinctAccumulator. Reusing it bounds the rewrite scope to the per-aggregate work, which is where the slice-alloc dominates.
- The marginal gain from a typed-block-direct group hash (à la Trino's `FlatGroupByHash`) is on the order of 1.2–1.5×; the slice-alloc removal is the 5× win.
- A follow-up change (`exec-typed-group-by-hash`) can introduce the typed open-addressing table later without touching the accumulator trait.

## Decision 4 — DISTINCT stays on the legacy path in v1

`DistinctAccumulator` wraps another `Accumulator` and tracks a
per-aggregate `FastHashSet<GroupKey>`. Porting this to the new trait
requires a per-group hash set (`Vec<FastHashSet<GroupKey>>` indexed
by group_id), which adds complexity and is not on the Q01 critical
path (Q01 has no DISTINCT).

**Behaviour:** when any aggregate in a `HashAggregateExec` has
`distinct = true`, the operator falls back to the existing per-row
loop. trino-diff PB-003 (COUNT DISTINCT) keeps passing because the
legacy path is unchanged.

Plan to revisit when running TPC-H queries that exercise DISTINCT in
hot loops; current 16 TPC-H queries do not.

## Decision 5 — Parallel partial merge becomes group-remapped

In the parallel two-phase path (`execute_parallel`), each tokio task
builds its own `GroupByHash` + GroupedAccumulators. After collecting
all partials, the outer task:

1. Builds the **global** `GroupByHash` by inserting every partial's
   group keys (creates a `group_remap[partial_group_id] -> global_group_id`
   mapping per partial).
2. Allocates the **final** GroupedAccumulators sized for the global
   group count.
3. Calls `final_acc.merge_from(&partial_acc, &group_remap)` once per
   partial per aggregate.

This is one merge call per (partial × aggregate), compared to the
current sequential `merge` loop that iterates per group key and per
accumulator. Big-O is the same; allocation profile is much better.

## Decision 6 — No-grouping degenerate case

`execute_no_grouping` becomes: every row maps to `group_id = 0`. We
do NOT need to call `GroupByHash::get_group_ids` — we just synthesize
a `vec![0u32; batch.num_rows()]` once per batch and call
`add_input(&group_ids, &col)`.

Even cheaper alternative considered and rejected: a separate
`add_input_no_groups(&mut self, values: &ArrayRef)` trait method.
Rejected because the inner loop is identical to "all group_ids are 0";
the extra method buys at most a couple of % and doubles the trait
surface.

## Tests we will run

- Per-accumulator RED→GREEN: `count_group_aware`, `sum_group_aware`,
  `avg_group_aware`, `min_group_aware`, `max_group_aware`. Each test
  sets up a 2-group / 3-row input and checks correct per-group
  output.
- `group_by_hash_assigns_stable_ids` — same key → same id, different
  key → different id.
- `group_by_hash_handles_null_in_group_col` — NULL must be a distinct
  group, not a hash collision.
- Operator-level: `hash_aggregate_two_groups_sum` (existing tests
  should keep passing).
- Parallel: `parallel_aggregate_with_disjoint_groups` and
  `parallel_aggregate_with_overlapping_groups` (latter exercises the
  group_remap path).
- `/trino-diff` 16/16 at 1e-9.
- TPC-H bench: Q01 expected wall-clock drop ≥ 4×.

## Risks

- **Hidden corners with NULL in group columns.** Arrow's null bitmap
  on a primitive array represents "no value here" — but our existing
  `extract_scalar` returns `ScalarValue::Null`, and `GroupKey` already
  supports a Null variant. Verify in
  `group_by_hash_handles_null_in_group_col`.
- **Decimal precision/scale propagation.** SumAccumulator already
  widens to precision=38; the new GroupedSumAccumulator must do the
  same. Add a regression test based on `sum_decimal128`.
- **Window operator coupling.** Window uses the per-instance trait.
  Verify nothing imports `Accumulator` expecting it to have changed.
