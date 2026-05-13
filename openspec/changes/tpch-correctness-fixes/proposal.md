## Why

The `planner-type-coercion` change shipped task #53 with a values-equivalence
follow-up: each TPC-H query was run through both Arneb and Trino on SF1, and
the CSV output diffed cell-by-cell with relative float tolerance. The result
was **12/16 values-identical**. The four discrepancies (Q07, Q08, Q14, Q16)
were diagnosed as **pre-existing planner / execution defects unrelated to type
coercion** and explicitly filed for a separate change. They are documented in
the repo root `TODO.md` as PB-001 / PB-002 / PB-003 — with reproductions,
suspected root causes, and suggested investigations — but **no OpenSpec
change has ever been opened to track them**.

These three bugs are small, surgical fixes individually; together they unblock
the "TPC-H 16/16 values-identical with Trino" milestone, which is the
honest correctness bar for the Trino-alternative positioning. They share a
single end-to-end verification (re-run the values-equivalence diff), share a
test discovery workflow, and live exclusively in `crates/planner` and
`crates/execution`. Folding them into one umbrella change avoids three
near-empty proposals and lets reviewers see the full corrective scope in
one PR.

## What Changes

- **PB-001 — Self-join alias collapse.** When the FROM clause aliases the
  same table twice (e.g. `nation n1 ... JOIN nation n2`) and the SELECT list
  projects both aliases' shared column after GROUP BY, the post-aggregate
  context drops the qualifier and both projected columns resolve to the
  first alias's value. Fix: preserve the originating qualifier through the
  Aggregate output context in `plan_aggregate_projection`. Affects
  TPC-H Q07, Q08.
- **PB-002 — Duplicate aggregate index collision.** Two failure modes
  combined into one symptom: (a) `find_aggregate_index` in
  `crates/planner/src/planner.rs` had a name-prefix fallback that, when
  two `SUM(...)` outputs share the function name, returned the *first*
  one for every `SUM` projection; (b) the exact-match arm formatted the
  AST expression with `format!("{expr}")`, which preserves both column
  qualifiers (`SUM(t.col)`) and parenthesized `Nested(...)` nodes
  (`SUM(x * (1 - y))`), neither of which appear in the stored aggregate
  column name (built from `PlanExpr` Display, which is unqualified and
  flattened) — so exact-match missed and the prefix fallback collapsed
  everything onto slot 0. Fix: delete the prefix fallback and add a
  qualifier-stripping + Nested-unwrapping AST formatter
  (`format_ast_unqualified`) so the exact-match loop succeeds.
  Affects TPC-H Q08, Q14.
- **PB-003 — COUNT(DISTINCT) over-counts.** The `distinct` flag survives
  parsing into `PlanExpr::Function { distinct, .. }` but
  `HashAggregateExec` builds its `AggrInfo` struct without carrying the
  flag, and `aggregate::create_accumulator` has no `distinct` parameter
  at all. Result: `COUNT(DISTINCT v)` silently degrades to `COUNT(v)`,
  over-counting whenever a group sees the same value more than once.
  In TPC-H Q16 this typically inflates `supplier_cnt` by one per
  group, which then cascades through `ORDER BY supplier_cnt DESC`.
  Fix: thread `distinct` through `AggrInfo` → `create_accumulator`, and
  add a `DistinctAccumulator` wrapper that deduplicates by a
  type-prefixed hashable key before forwarding to the wrapped
  accumulator. NULLs are skipped (SQL semantics). Affects TPC-H Q16.
- **No public API changes.** All three fixes are internal to the planner /
  execution crates. No connector, protocol, or spec-surface breakage.
- **`TODO.md` cleanup.** PB-001 / PB-002 / PB-003 entries are removed once
  the corresponding fixes land. The Discovery workflow block stays as the
  reference recipe for running values-equivalence diffs.

## Capabilities

### New Capabilities

- None. This change adds scenarios to existing capabilities.

### Modified Capabilities

- `query-planner`: today's spec covers qualified-column resolution after
  GROUP BY with a *single* table alias (Scenario "Table alias in GROUP BY
  and SELECT") and ORDER BY on aggregate expressions. The spec is extended
  with two scenarios that pin down the previously-undefined behaviour for
  (a) self-joins where two aliases share a column, and (b) two
  same-function aggregates in one SELECT list that differ only in
  arguments.
- `accumulators`: today's spec covers reset / unknown-function semantics
  but does not pin `COUNT(DISTINCT)` correctness. The spec is extended
  with a requirement that `COUNT(DISTINCT col)` returns exactly the cardinality of the
  non-null distinct value set per group.

## Impact

- **Behaviour change (correctness).** Four previously-wrong TPC-H query
  results become byte-identical to Trino. No query that was correct
  changes behaviour. No error surface changes.
- **Code.**
  - `crates/planner/src/planner.rs` — post-aggregate context build
    in `plan_query_body` carries the GROUP BY expression's source
    qualifier forward (`group_by_qualifier` helper);
    `find_aggregate_index` matches via `format_ast_unqualified` and
    its name-prefix fallback is deleted.
  - `crates/execution/src/aggregate.rs` — new `DistinctAccumulator`
    + `dedup_key` helper; `create_accumulator` gains `distinct: bool`.
  - `crates/execution/src/operator.rs` — `AggrInfo` carries `distinct`;
    `HashAggregateExec` threads it to `create_accumulator`.
- **Tests.**
  - Unit: one reproduction test per PB in the owning crate (planner or
    execution). Each is a strict failing-test-first → fix cycle.
  - End-to-end: re-run the SF1 values-equivalence diff (`trino-diff`
    skill) and confirm 16/16 queries identical at relative tolerance
    1e-9.
- **Dependencies.** None added.
- **Out of scope.**
  - The remaining TPC-H queries Q15, Q17, Q18, Q20, Q21, Q22 are not run
    by the existing benchmark suite (only 16 are selected). Bringing the
    full 22 into the verification loop is a separate change.
  - Q07 and Q08 also need PB-002's fix to fully match (they have both
    self-joins *and* multi-SUM SELECTs); that's covered by the umbrella
    naturally.
  - No optimizer changes, no analyzer changes — these are pure
    resolution / accumulation fixes.
