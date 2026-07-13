## Context

The TPC-H value-equivalence work landed in `tpch-correctness-fixes`
and the `exec-ahash-hashtables` + `exec-typed-hash-keys` quick-win
bundle has Arneb at 0.165× of Trino on SF1 geometric mean. The
breakdown shows multi-join queries (Q05 6-way, Q07 5-way + self-join,
Q08 6-way + self-join + CASE, Q09 6-way) as the worst gap: 14–28×
slower than Trino each.

The Phase-1 explore agent confirmed:

> No join reordering in planner. `crates/planner/src/planner.rs` lines
> 417–427 builds join tree left-to-right from SQL textual order. ...
> Statistics-driven planning is absent. Grep for "statistics",
> "cardinality", "cost", "ndv" in `crates/planner/`, `crates/catalog/`
> returns no matches. Planner is **completely blind** to table size
> or selectivity.

TPC-H queries are pessimally ordered for textual-order execution:
`Q08`'s six-table FROM lists the smallest tables (region, nation)
last. Building the hash table on the largest table first is the
worst possible choice. A correct reorderer drops Q08 by ~5×
immediately, even before any other change.

This design draft sketches the infrastructure needed before the
reorderer pass can ship. It is not a complete implementation plan —
each section ends with open questions and decisions deferred.

## Goals / Non-Goals

**Goals:**
- Make join order a function of cost, not SQL text.
- Source row counts and NDV from HMS where available; fall back
  gracefully when columns lack stats.
- Output an `EXPLAIN ANALYZE`-style plan trace that shows estimates
  vs actual per node — Trino-compatible mental model.
- Conservative correctness: a missing stat must degrade selectivity
  to a pessimistic default, never to "ignore predicate" semantics.

**Non-Goals:**
- Histograms / equi-depth buckets / KLL sketches. Single-value NDV
  is the v1 budget; richer distributions are stats-v2.
- Cross-engine cost calibration. We optimize for Arneb's cost
  function only — not for Trino's.
- Adaptive (mid-query) re-planning. The reorderer runs once at
  analyzer time.
- Cost-based aggregate placement (push group-by below join when
  selective). Separate change.
- Multi-stage cost model with CPU + IO components. Single
  `f64`-row-cost is sufficient to rank join orderings; CPU cost
  becomes meaningful only after pipelined execution lands.

## Decisions

### Decision 1: Statistics carrier — `TableStatistics`

Add to `crates/catalog/src/`:

```rust
#[derive(Debug, Clone, Default)]
pub struct TableStatistics {
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub columns: HashMap<String, ColumnStatistics>,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    pub ndv: Option<u64>,            // approximate distinct count
    pub null_fraction: Option<f64>,  // 0.0..=1.0
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
}
```

`TableProvider::statistics(&self) -> Option<TableStatistics>` —
nullable by trait so a connector without stats just returns `None`.

**Why nullable end-to-end:** the cost model must work without stats.
HMS often has row counts but rarely complete column stats. The
selectivity helpers fall back to defaults when fields are `None`.

### Decision 2: Cost = expected output rows

Single `f64`. Justification: in a row-based engine without
spillable operators or vectorized scan throughput modeling, output
cardinality dominates wall-clock cost for hash joins (build side
size = memory, probe side size = work). Adding a CPU coefficient
helps marginal cases but complicates the API. Defer until
post-pipelined-execution.

Cost propagation:

| Node           | Output cardinality                                                |
|----------------|-------------------------------------------------------------------|
| `TableScan`    | `stats.row_count.unwrap_or(default_table_size)` = 10K guess       |
| `Filter`       | `child * selectivity(predicate)`                                  |
| `Projection`   | `child` (no row change)                                           |
| `Limit n`      | `min(child, n)`                                                   |
| `Sort`         | `child`                                                           |
| `InnerJoin`    | `(left * right) / max(ndv_l, ndv_r)`  (uniform key distribution)  |
| `LeftJoin`     | `max(left, inner_estimate)`                                       |
| `Aggregate`    | `min(child, product(group_ndv))`                                  |
| `Distinct`     | `min(child, product(col_ndv))`                                    |

### Decision 3: Selectivity heuristics

```
col = literal              →  1 / ndv (default 0.1)
col != literal             →  1 - 1/ndv
col < literal              →  (literal - min) / (max - min)  if min/max known
col BETWEEN a AND b        →  (b - a) / (max - min)
col IN (k items)           →  min(k / ndv, 1.0)
col LIKE 'prefix%'         →  0.1
col IS NULL                →  null_fraction (default 0.05)
col IS NOT NULL            →  1 - null_fraction
A AND B                    →  sel(A) * sel(B)               (independence)
A OR B                     →  sel(A) + sel(B) - sel(A)*sel(B)
NOT A                      →  1 - sel(A)
```

Defaults err on the **selective** side (0.1 for equality, 0.33 for
range) so unknown predicates don't fool the reorderer into picking a
giant build side. Trino uses similar heuristics.

### Decision 4: Reorder algorithm — Selinger-style DP, greedy fallback

For `N ≤ 8` tables (covers every TPC-H query), run dynamic
programming over all join sub-trees:

```
best_plan(S) = min over (L, R) partitions of S of
               (best_plan(L), best_plan(R), join_cost(L, R))
```

Memoized in a `HashMap<BTreeSet<TableId>, (Plan, Cost)>`. Time
complexity O(3^N) — fine up to N≈12.

For `N > 8`, switch to a greedy heuristic:

1. Start with the single table that has the smallest filtered
   cardinality after applying its WHERE predicates.
2. At each step, pick the table whose join with the current sub-tree
   has the smallest estimated output cardinality.
3. Break ties by NDV product on the join key (lower → better build).

The DP path always runs first if N≤8, regardless of how many
tables — the greedy is just the fallback for unusually wide joins.

### Decision 5: Hint syntax

`/*+ NO_REORDER */` comment at the start of a `SELECT` disables
the pass for that statement. Comment parsing is already in
sqlparser-rs (preserved verbatim); we tap into the comments
attached to the AST to detect the marker.

Future hint vocabulary (out of scope here):

- `/*+ BROADCAST(t) */` — force broadcast join with `t` as build
- `/*+ JOIN_ORDER(a, b, c) */` — pin explicit order

### Decision 6: When to fetch HMS stats

**Eager at planning time.** One Thrift round-trip per table in the
FROM clause, batched (HMS supports `get_table_statistics_req` for
multiple tables in one call where available). The round-trip cost
(~10 ms LAN, ~50 ms WAN) is amortized against the seconds-level
query latency this change is trying to reduce.

Lazy fetch would let the reorderer probe stats only when comparing
two plans — but that's harder to reason about (stats cache hits
become a timing variable) and gains little for the TPC-H workload
where every table is in stats.

### Decision 7: EXPLAIN ANALYZE output

```
Projection ...
  HashJoin [ps.partkey = p.partkey]
    Estimates: rows=200000 (actual: 200000)
    HashJoin [s.suppkey = ps.suppkey]
      Estimates: rows=800000 (actual: 800000)
      Scan partsupp                       rows=800000 (actual: 800000)
      Scan supplier WHERE s_nationkey=2   rows=400  (actual: 412)
    Scan part WHERE p_size IN (...)       rows=27000 (actual: 26943)
  Reorder: applied, original SQL had part as outer
```

Each node gets an `Estimates: rows=N (actual: M)` line. The
"actual" only appears under `EXPLAIN ANALYZE` (which runs the
query); plain `EXPLAIN` shows estimates only.

## Test Strategy

- Unit: cost-model propagation through every operator; selectivity
  heuristics for each predicate shape; DP reorderer correctness on
  hand-built 3/4/5/6-table fixtures.
- Property: random join graphs → reorderer always yields a plan
  with cost ≤ textual order.
- Integration: `trino-diff` 16/16 unchanged at 1e-9 (no value
  regression).
- Benchmark: TPC-H SF1 multi-join queries (Q05, Q07, Q08, Q09)
  hit ≥ 3× speedup vs the post-quick-wins baseline. Simple-join
  queries (Q01, Q04, Q06) within ±5%.

## Risks

- **Stats can be wrong.** HMS stats are not auto-refreshed; a table
  written by an external system may have stale `numRows`. Mitigation:
  detect "no stats present" vs "stats look way off vs file scan",
  surface a warning, and degrade to greedy. Don't let bad stats
  silently degrade performance.
- **Plan instability.** Adding stats means tiny config changes
  (refresh HMS, add a row) flip join ordering and benchmark numbers.
  Document the deterministic-with-given-stats contract; provide a
  `BENCHMARK_PIN_PLAN` env var that prints the chosen plan for
  diff-testing.
- **DP cost explosion.** N=8 is safe; N=12 is the edge. Cap the
  DP at N=10, fall back to greedy with a warning above that.
- **Correctness regressions in unusual subquery shapes.** Subqueries
  (especially correlated) are not reorderable today. The pass MUST
  detect subquery membership and leave those trees untouched.

## Rollback

The reorderer is a single analyzer pass. Disabling it means removing
it from the pipeline order in `analyzer/mod.rs::default_pipeline()`.
Statistics infrastructure (the larger code surface) is harmless
without a consumer.
