## Why

At SF30, q08 uses **3.4× Trino's memory** — the worst memory result in the suite. Per-operator
profiling (`arneb::profile` worker logs, 2026-06-08) pinned the cause: q08 builds the **90M-row
lineitem side** of a join and probes it with only **20K filtered-part rows**, materialising a
~3.6 GB hash table where ~KB would do.

The build side of a `HashJoinExec` is always the join's **right child**. The only mechanism that
puts the smaller table on the right (build) is `JoinReorder` (partition-aware Selinger). But
`JoinReorder` **bails on q08**: the `nation n1 / nation n2` self-join produces duplicate leaf
column names, so `has_duplicate_leaf_column_names` returns true and `try_reorder_chain` returns
`None` (`crates/planner/src/analyzer/join_reorder.rs:375`). q08 then keeps the **original SQL join
order**, whose left-deep tree builds the large side. `JoinReorder` tracks columns **by name**,
which cannot disambiguate self-join columns, so it bails for safety; the same name-collision also
makes `rewrite_against` bail (`join_reorder.rs:1301`).

This is not q08-specific: every self-join query (q07, q08, and the previously-blocked q18 reorder)
loses cost-based build-side selection. Closing it is the most concrete, highest-leverage memory win
in the SF30 root-cause map, and — unlike the other heavy-query bottlenecks (q09 probe throughput,
q17 data movement, q21 anti-join build) — it is a single, well-localised planner defect.

A prior attempt at the underlying fix (the "F-Perf-RN" leaf-origin work) was **reverted** because
it broke q18 at runtime via a RowConverter index mismatch. So this is a correctness-sensitive
optimizer change that needs the oracle/gate-first discipline — hence an OpenSpec change rather than
an ad-hoc patch.

## What Changes

- Make build-side selection **robust to self-joins** so the smaller estimated side is always built,
  even when two join leaves share column names.
- The design will choose between two approaches (and may combine them):
  - **(a) Leaf-origin column tracking** — disambiguate columns by `(leaf, position)` instead of by
    name in `has_duplicate_leaf_column_names` + `rebuild_plan_indices` (+ `rewrite_against`), so
    `JoinReorder` no longer bails on self-joins. This is the reverted F-Perf-RN direction; it must
    be re-landed incrementally with the q18 RowConverter regression as a guard.
  - **(b) Local INNER-only build-side swap** — a separate, narrow pass that swaps a join's children
    when the right/build side is estimated larger than the left/probe, independent of full
    reordering (also requires join-key index + output-schema rewrites; INNER-only for semantic
    safety).
- No change to the cost model itself (Selinger already prefers small builds); the fix is about
  letting that decision actually apply to self-join chains.

## Capabilities

### New Capabilities
- `build-side-selection`: the planner guarantee that, for reorderable INNER-join chains, the
  hash-join build (right) side is the smaller estimated input — robustly, including chains that
  contain self-joins (duplicate leaf column names), without producing an incorrect plan.

### Modified Capabilities
<!-- None: query-planner / analyzer-phase behavior is extended via the new capability's requirements; no existing requirement is removed. -->

## Impact

- **Code:** `crates/planner/src/analyzer/join_reorder.rs` (the bail predicates + index rebuild, or a
  new local-swap pass), supporting helpers in `crates/planner/src/cost.rs` /
  `crates/planner/src/plan.rs`. Physical `HashJoinExec` build/probe mapping
  (`crates/execution/src/planner.rs`, `crates/execution/src/hash_join.rs`) is read-only unless
  approach (b) needs a swap annotation.
- **Risk:** previously reverted (q18 RowConverter index mismatch). Mitigated by the validation gate.
- **Validation gate:** existing self-join + index-rebuild tests stay green
  (`join_reorder.rs` ~1568-1936, the q07/q08 nation self-join paths); `trino-diff` cell-parity at
  SF1; SF30 remote q08 total-cluster peak memory drops from 3.4× toward ~1× Trino with **no latency
  or correctness regression** on the 22q bench.
- **Out of scope:** q09 probe throughput, q17 data movement, q21 anti-join build — separate root
  causes recorded in the SF30 heavy-query map.
