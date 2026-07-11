## Why

The same-fragment dynamic-filter optimization (`inject_inlist_dynamic_filters` in
`crates/execution/src/hash_join.rs`) pushes a hash join's build-side key `IN (distinct values)`
into the probe subtree to prune the probe scan. It propagates the filter **by column name**: the
filter carries a `Column { name }`, recurses into both children, and every descendant `ScanExec`
whose schema has a column of that name accepts it (`rewrite_filter_indices`,
`crates/execution/src/operator.rs`).

**This is a latent correctness bug for self-joins.** When the probe subtree contains a column with
the *same name* as the join key but from a *different table instance*, the filter lands on the
wrong column. Measured on q08 (2026-06-08): with the `nation n1`/`nation n2` self-join reordered so
both nation scans sit on the probe spine, the `+region` join's key `n1.n_regionkey = r_regionkey`
injects `n_regionkey IN (region/AMERICA)` and it hits **both** `n1.n_regionkey` AND `n2.n_regionkey`
— pruning the supplier-nation side (`n2`) to only AMERICA suppliers. Result: q08 mkt_share = 0.197
instead of 0.0388 (~5× wrong). Disabling all injection → q08 correct (0.0388), confirming the root
cause. (This is the same defect class that broke the earlier F-Perf-RN q18 attempt via a
"RowConverter index mismatch at runtime".)

Today the bug is masked: `JoinReorder` bails on self-joins (`has_duplicate_leaf_column_names`), so
the corrupting order never arises, and in SQL order the build side happens to carry the full key
set (harmless). The companion change **planner-build-side-selection** (Front-2-v2, leaf-origin
remap — logically verified correct) removes that bail to win q08's memory (3.4×→~1×). Doing so
**exposes** this dynamic-filter bug. So provenance-correct injection is the hard prerequisite for
shipping build-side self-join reorder.

**The injection cannot simply be skipped on ambiguity:** measured at SF1, the cross-table "sibling"
injection collapses q18's build from **59,986,052 rows / 973 MB → 4,368 rows / 87 KB** (q18 latency
1.87 s vs 3.80 s). At SF30 that ~1 GB build scales toward OOM. The sibling injection is
load-bearing for q18 memory and must be preserved.

## What Changes

- Replace **name-based** dynamic-filter propagation with **provenance (index) targeting**: a
  build-key filter is injected only onto probe-side columns that are genuinely **join-equal** to the
  build key — never onto a coincidentally same-named column from a different table.
- The valid target set is exactly the **equivalence class of the probe-side join key within the
  probe subtree** (`properties::derive_equivalences` / `ActualProperties::equivalent_columns`). This
  set naturally:
  - includes the direct probe key (q08's `n1.n_regionkey`),
  - excludes the non-equal self-join twin (q08's `n2.n_regionkey`),
  - includes the transitively-equal cross-table sibling (q18's main `lineitem.l_orderkey`),
  so it unifies and replaces both the first (left-key) and the second (right-key "dual") injections.
- Compute the target column indices at plan/physical-planning time (equivalence is a plan-level
  fact); thread them onto `HashJoinExec`; at runtime inject each target by **index descent** down
  the physical probe subtree (remap the index through each operator, apply at the owning scan by
  index — never by name).
- Remove the name-based second ("sibling") injection (subsumed correctly by the equivalence class).
- The previously-silent wrong-column path becomes impossible by construction; keep a `debug_assert`
  that an applied target's scan column name matches the build-key name (sanity, not routing).

## Impact

- Affected: `crates/execution/src/hash_join.rs` (injection), `crates/execution/src/operator.rs`
  (per-operator index-descent inject), `crates/execution/src/planner.rs` (compute + thread targets),
  `crates/planner/src/properties.rs` (expose a logical-plan equivalence helper).
- Unblocks **planner-build-side-selection** (q07/q08/q18 self-join reorder) without corrupting q08.
- Correctness gate: SF1 trino-diff (q08 0.197→0.0388, all 17 match). Perf gate: q18 build stays
  ~87 KB (sibling collapse preserved), no latency/memory regression on the queries that currently
  rely on same-fragment dynamic filters (q05/q12/q18…).
- No SQL-surface or wire-protocol change.
