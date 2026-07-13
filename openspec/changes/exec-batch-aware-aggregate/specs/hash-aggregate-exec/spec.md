## MODIFIED Requirements

### Requirement: Per-batch aggregate dispatch

`HashAggregateExec::execute_sync` and `execute_no_grouping` SHALL
dispatch aggregate updates **once per batch per aggregate**, not
once per row. The per-row inner loop SHALL be moved inside the
concrete `GroupedAccumulator::add_input` implementation, with
direct typed-array access (no `Box<dyn Accumulator>` per row, no
`ArrayRef::slice(row, 1)`).

Concretely, for each batch:

1. Evaluate each `group_by` expression → `Vec<ArrayRef>`.
2. Evaluate each aggregate's input expression → `Vec<ArrayRef>`.
3. Call `group_by_hash.get_group_ids(&group_cols)` once → `Vec<u32>`.
4. For each aggregate `acc_i`: `acc_i.ensure_capacity(group_by_hash.num_groups()); acc_i.add_input(&group_ids, &aggr_input_cols[acc_i])`.

When all batches are consumed, the output is built by iterating
`0..group_by_hash.num_groups()` and calling
`acc.evaluate(group_id)` per aggregate per group, plus emitting the
materialised group keys from `group_by_hash.keys()`.

#### Scenario: aggregate result is identical to legacy per-row path

- **GIVEN** a query `SELECT col_a, SUM(col_b), COUNT(*), AVG(col_c), MIN(col_d), MAX(col_e) FROM t GROUP BY col_a` over an arbitrary `RecordBatch`
- **WHEN** the new per-batch dispatch path runs to completion
- **THEN** the resulting `RecordBatch` has the same rows (after sorting by `col_a`) and the same aggregate values as the legacy per-row path on the same input (`/trino-diff` 16/16 must remain green)

#### Scenario: no per-row slice allocation in the hot path

- **GIVEN** the per-batch dispatch path
- **WHEN** profiling Q01 over `lineitem`
- **THEN** `ArrayRef::slice` is **NOT** called from within `HashAggregateExec::execute_sync` (verified by grep on the rewritten code: the literal `.slice(row, 1)` and `extract_scalar(col, row)` calls inside the group/aggregate loops have been removed)

### Requirement: DISTINCT aggregates use the legacy per-row path

The system SHALL use the legacy per-row `Accumulator` path whenever any aggregate inside a `HashAggregateExec` has `distinct = true`. The new batch-aware `GroupedAccumulator` path MUST NOT be selected for that operator instance. This bounds the v1 scope and preserves PB-003 correctness for `COUNT(DISTINCT ...)`.

#### Scenario: COUNT(DISTINCT col) keeps the legacy code path

- **GIVEN** a query `SELECT col_a, COUNT(DISTINCT col_b) FROM t GROUP BY col_a`
- **WHEN** `HashAggregateExec::execute_sync` runs
- **THEN** the legacy per-row dispatch is used; PB-003 trino-diff scenario for COUNT(DISTINCT) remains green at 1e-9

### Requirement: Parallel partial merge uses GroupedAccumulator

`build_partial_groups` SHALL construct `GroupByHash` +
`GroupedAccumulator`s per partition. The outer merge step in
`execute_parallel` SHALL:

1. Build the global `GroupByHash` by inserting each partial's keys
   in order, producing a `Vec<u32>` group_remap per partial.
2. Construct the final `GroupedAccumulator`s sized for the global
   group count.
3. Call `final_acc[i].merge_from(&partial.accs[i], &remap)` per
   partial per aggregate.

#### Scenario: parallel result matches sequential

- **GIVEN** an aggregate over an input that is splittable into N partitions, with `target_partitions = 4`
- **WHEN** executed end-to-end through `execute_parallel`
- **THEN** the output rows and values are identical (after `ORDER BY` on group keys) to running the same plan with `target_partitions = 1`
