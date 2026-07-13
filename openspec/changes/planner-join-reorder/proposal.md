## Why

The TPC-H SF1 benchmark after the `exec-ahash-hashtables` +
`exec-typed-hash-keys` quick-win bundle puts Arneb at 0.165× of Trino
on geometric mean. The worst single query, Q08 (35× slower), is a
6-way join with a self-join and a CASE — the planner builds the join
tree left-to-right in SQL text order:

```rust
// crates/planner/src/planner.rs::plan_from, ~line 417-427
let (mut plan, mut ctx) = self.plan_table_with_joins(&from[0]).await?;
for table in &from[1..] {
    let (right, right_ctx) = self.plan_table_with_joins(table).await?;
    plan = LogicalPlan::Join { left: Box::new(plan), right: ... };
    // ctx merged ...
}
```

There is no cardinality estimation, no cost model, no statistics —
the planner is operating blind. `grep -r "reorder\|cost\|ndv\|cardinality\|Statistics"
crates/planner/ crates/catalog/` returns zero hits.

Trino runs a sophisticated cost-based join reorderer that always
places the smallest table on the build side and picks the
cheapest-first join order using catalog statistics + ndv estimates +
selectivity. Our diagnosis after explore phase: this single gap is
worth **5-10× on multi-join TPC-H queries** (Q05 / Q07 / Q08 / Q09 /
Q21) — the largest blast-radius lever remaining.

This change is the first half of "the big lever": establish the
statistics + cost-model infrastructure and ship a join reorderer on
top of it.

## What Changes

1. **Statistics infrastructure.**
   - Extend `crates/catalog/src/` with `TableStatistics` (row count,
     per-column NDV approximation, per-column null fraction, per-column
     min/max where cheap). Add to `TableProvider`.
   - HMS catalog reads `Parameters.numRows`, `numFiles`, `totalSize`
     from `Table.parameters` (Hive auto-populates these on CTAS) and
     column-level `numDistincts`, `lowValue`, `highValue` from
     `Table.columns[i].columnStats`. Fall back to row-count-only when
     column stats are absent.
   - File connectors compute `numRows = sum(row_group.num_rows)` from
     Parquet metadata at register-time (cheap; metadata is footer-only).
2. **Cost model.**
   - New module `crates/planner/src/cost.rs`. Cost := `f64` (output
     row count is the primary metric; CPU cost is a tunable but
     defaulted to identity on rows for v1).
   - `LogicalPlan::estimated_cardinality(stats: &CatalogStats) -> u64`
     walks the plan and propagates row counts through Scan / Filter
     (selectivity × parent) / Join (build × probe / max(ndv_l,
     ndv_r)) / Aggregate (group-by ndv product) / Limit (min).
3. **Selectivity helpers.**
   - `crates/planner/src/selectivity.rs`: per-predicate-shape estimator
     for `col = lit` (1/ndv), `col < lit` (range fraction from
     min/max), `col IN (...)` (k/ndv), `col LIKE 'foo%'` (0.1
     heuristic), `AND`/`OR` (independence assumption). Conservative
     defaults when stats missing (0.1 for equality, 0.33 for range).
4. **Analyzer pass: `JoinReorder`.**
   - New pass after `TypeCoercion` in the analyzer pipeline. Runs
     dynamic programming (Selinger-style) over join sub-trees up to
     N=8 tables (TPC-H Q08 has 6 — well within bounds). For larger
     trees, fall back to a greedy "smallest build side, most selective
     first" heuristic.
   - Respects user-provided join hints (`/*+ NO_REORDER */` syntax —
     parser extension piggybacked here) so query authors can pin
     ordering when needed.
   - Outputs the reordered plan with `[reordered=true]` annotation
     visible in `EXPLAIN ANALYZE`.
5. **EXPLAIN extension.**
   - `EXPLAIN ANALYZE` prints estimated cardinality, actual row count,
     and the reorder decision for each join. Mirrors Trino's
     `Estimates: ...` lines.

## Capabilities

### New Capabilities
- `catalog-statistics`: row counts, NDV, null fraction, min/max per
  column, exposed via `TableProvider::statistics() -> Option<TableStatistics>`.
- `cost-model`: row-count-based cardinality propagation for every
  `LogicalPlan` node.
- `selectivity-estimator`: per-predicate selectivity heuristics with
  conservative fallbacks.
- `join-reorder`: analyzer pass that reorders inner joins by cost.

### Modified Capabilities
- `query-planner`: `plan_from` no longer fixes join order from SQL
  text. After the analyzer phase, joins are ordered by the
  `JoinReorder` pass's output.
- `analyzer-phase`: gains a new ordered pass between `TypeCoercion`
  and `LogicalOptimizer`.
- `catalog-traits` / `catalog-memory` / `hive-data-source`: extended
  to expose statistics.

## Impact

- **Behavior**: identical output rows (set equality preserved); SQL
  text ordering of joins is no longer authoritative — `EXPLAIN`
  output will differ for every multi-join query. Document in
  release notes.
- **Tests**: extensive new unit suite for cost / selectivity / DP
  reorderer + correctness regression via `trino-diff` 16/16 at 1e-9.
- **Performance**: target **≥ 3× geomean speedup** on the
  multi-join subset (Q05 / Q07 / Q08 / Q09). Q05 currently 6.8 s
  vs Trino 0.49 s; a 3× win brings it to 2.3 s (still 4.7× slower
  but moves the needle into Q03's neighborhood).
- **Dependencies**: zero new crates. HMS stats fetch reuses existing
  Thrift client.
- **Effort**: estimated 4–6 weeks for a single engineer. Largest
  cost is robustness around missing stats — must degrade gracefully
  to "best-effort textual order" without panics.
- **Out of scope**:
  - Histograms or quantile sketches (defer to a future stats-v2 change).
  - Cost-based aggregate placement (decide group-then-join vs
    join-then-group). Tracks separately.
  - Cross-source statistics (e.g., when joining Hive against a file
    connector). Each source contributes its own stats; merged
    estimation per-side.
  - Dynamic-filter integration (separate `dynamic-filtering` change).
  - Adaptive query execution (re-plan mid-query based on actual
    row counts). Future stage 3 work.

## Open questions for design.md

- Should we fetch HMS column statistics eagerly at `plan_query` time
  (one Thrift round-trip per table) or lazily (only when the
  reorderer wants them)? Latency vs simplicity trade-off.
- DP cutoff value (currently proposed N=8). At N=12 the search space
  is 12!^2 ≈ 220B which is too large; greedy must kick in.
- How to surface `NO_REORDER` hint in SQL? Trino uses `/*+ ... */`
  comments; this would extend `sqlparser-rs` consumption logic.
- Stale-stats handling: HMS stats may not auto-refresh after writes.
  Do we trust them as-is, age them, or schedule background refresh?

## Status

**DRAFT** — proposal only. No implementation in this change yet.
Use this proposal to scope the project, surface design questions,
and prioritize against `executor-multi-core` (the other gap with
similar 2–8× blast radius).
