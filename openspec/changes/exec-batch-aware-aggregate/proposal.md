## Why

Arneb's hash aggregate hot loop is per-row, not per-batch. For each
row in the input the executor:

1. allocates a `GroupKey` (`Vec<ScalarValue>` of the group columns),
2. probes a `HashMap<GroupKey, Vec<Box<dyn Accumulator>>>`,
3. for each aggregate, takes an Arrow `slice(row, 1)` of the input
   column, and
4. dynamically dispatches `Accumulator::update_batch` on a 1-row array.

For TPC-H Q01 on SF1 — `lineitem` (6M rows) × 5 aggregates × 4 groups
— that is **~30M dyn dispatches plus ~30M one-row `ArrayRef` slice
allocations** per query. Profiling and the Step 3.5 parallel-aggregate
finding (parallel hash aggregate added correctness but moved Q01
wall-clock by < 1%) both point to this loop as the bottleneck.

Trino's hash aggregate avoids this entirely. Verified by an
Explore-agent diff against
`/Users/bochengyang/formosa-ventures/repos/trino` (2026-05-14):

> `core/trino-main/.../aggregation/GroupedAccumulator.java:26`
> ```java
> void addInput(int[] groupIds, Page page, AggregationMask mask);
> ```
> Takes the **whole page** + a pre-computed group-id array. Iterates
> positions inside JIT-compiled bytecode (`AggregationLoopBuilder`),
> with direct block access — no per-row allocation, no per-row dyn
> dispatch.

This change scopes the rewrite that closes most of the **~5× Q01 gap
vs Trino** on the same hardware reading the same Parquet files. It is
orthogonal to parallelism: the existing parallel-aggregate path
(`HashAggregateExec::execute_parallel`) and per-partition merge stay,
and start scaling for the first time once the per-row CPU work is
removed.

## What Changes

1. **New `GroupedAccumulator` trait** (lives alongside the existing
   per-instance `Accumulator`). One instance covers all groups for
   one aggregate; internal state is `Vec<S>` indexed by `group_id: u32`.
   - `fn ensure_capacity(&mut self, num_groups: usize)`
   - `fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<()>`
   - `fn evaluate(&self, group_id: u32) -> Result<ScalarValue>`
   - `fn merge_from(&mut self, other: &dyn GroupedAccumulator, group_remap: &[u32]) -> Result<()>` for parallel partials
2. **New `GroupByHash` struct** in `crates/execution/src/group_by_hash.rs`:
   - `get_group_ids(&mut self, group_cols: &[ArrayRef]) -> Vec<u32>` —
     one call per batch, returns the group_id assigned to each row
     (inserts new keys on miss).
   - `keys() -> &[GroupKey]` — group-id → key mapping for output
     materialisation.
3. **Rewrite `HashAggregateExec::execute_sync` and
   `execute_no_grouping`** to do one `get_group_ids` + one
   `add_input` call per batch per aggregate. No-grouping is the
   degenerate case where every row maps to `group_id = 0`.
4. **Concrete `GroupedAccumulator` impls** for `COUNT`, `SUM`, `AVG`,
   `MIN`, `MAX`. Each owns its own `Vec<State>` indexed by group_id.
5. **DISTINCT v1 fallback.** `DistinctAccumulator` is not ported in
   v1 — `HashAggregateExec` continues to use the legacy per-group
   path when any aggregate has `distinct = true`. This keeps PB-003
   correctness and bounds the rewrite scope.
6. **Parallel partial-merge path updated.** `build_partial_groups`
   constructs the new GroupedAccumulators per partition; the outer
   merge step calls `merge_from` with a remapped group-id table.

## Impact

**Affected files:**
- `crates/execution/src/aggregate.rs` — add `GroupedAccumulator` trait + 5 concrete impls. Keep existing per-instance `Accumulator` (used by Window).
- `crates/execution/src/group_by_hash.rs` (new) — batch group-id assignment.
- `crates/execution/src/operator.rs:566-825` — `HashAggregateExec::execute_sync` / `execute_no_grouping` / `build_partial_groups` rewritten.
- `crates/execution/src/window.rs` — unchanged (still uses per-instance Accumulator).

**No change to:**
- The Window operator's per-frame aggregator path.
- The pgwire/protocol layer, output Arrow schema, or any LogicalPlan node.
- The parallel-aggregate two-phase plan shape — only the per-partition inner loop is rewritten.

**Expected perf (TPC-H SF1, target_partitions = 14):**
- Q01: 1584ms → ~300ms (≈5× speedup) — closes most of the gap to Trino's 287ms.
- Q03/Q06/Q14: similar percentage gains where aggregate work dominates.
- Non-aggregate-heavy queries (Q02/Q04/Q11/Q16/Q19): no significant change.
- `/trino-diff` 16/16 at 1e-9 — correctness MUST hold.
