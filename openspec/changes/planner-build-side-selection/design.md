## Context

Build side of a `HashJoinExec` = the join's **right child** (`hash_join.rs` builds right). The
partition-aware Selinger cost (`cost.rs:357` `build_cost = estimated_cardinality(right)`) already
prefers the ordering with the smallest sum of build cardinalities, and `JoinReorder` rewrites the
left-deep tree to realise it — including swapping a join's children and rebuilding all column
indices via `rebuild_plan_indices`. The reorder tests (`join_reorder.rs` ~1568-1936:
`reorders_two_way_to_put_smaller_on_build`, `filter_above_swapped_join_has_correct_column_indices`,
etc.) confirm the swap + index-rebuild machinery is correct **for non-self-join chains**.

The defect: `JoinReorder` tracks columns **by name**. When two leaves share names (self-join, e.g.
q08's `nation n1 / nation n2`), it cannot tell which leaf a column reference belongs to, so it
bails for safety: `has_duplicate_leaf_column_names` → `try_reorder_chain` returns `None`
(`join_reorder.rs:375`), and `rewrite_against` returns the expression unchanged on
`schema_has_duplicate_names` (`join_reorder.rs:1301`). The chain then keeps the original SQL order,
which builds the large side (q08 builds 90M lineitem, probes 20K part → ~3.6 GB hash table = 3.4×
Trino memory).

A prior fix (F-Perf-RN) tried to make the reorder handle self-joins but was **reverted**: it broke
q18 at runtime via a `RowConverter` index mismatch (the rewritten indices pointed at the wrong
leaf's columns).

## Goals / Non-Goals

**Goals:**
- For reorderable INNER-join chains that contain self-joins, the build (right) side is the smaller
  estimated input — the same guarantee already held for non-self-join chains.
- Zero correctness regression: every existing reorder/index-rebuild test stays green; SF1 trino-diff
  cell-parity; SF30 q07/q08 cell-correct.
- Measurable memory win: SF30 q08 total-cluster peak drops from 3.4× toward ~1× Trino, no latency or
  correctness regression on the 22q bench.

**Non-Goals:**
- The cost model (Selinger) — unchanged; it already prefers small builds.
- Non-INNER build-side flipping (LEFT/RIGHT/SEMI/ANTI can't freely swap) — out of scope.
- q09 probe throughput, q17 data movement, q21 anti-join build — separate root causes.

## Decisions

**Decision 1 — Approach (a) leaf-origin column tracking is the primary direction; (b) local swap is
the documented fallback.**

- **(a) Leaf-origin tracking (CHOSEN):** disambiguate columns by `(leaf_index, position)` instead of
  by name throughout the bail/rewrite path — `has_duplicate_leaf_column_names`,
  `rewrite_against`/`schema_has_duplicate_names`, and `rebuild_plan_indices`. This reuses the
  **already-proven** swap + index-rebuild machinery (the reorder tests pass for non-self-join
  chains); the only change is teaching it to resolve column references positionally when names
  collide. Root-cause fix → unblocks q07, q08, and the previously-blocked q18 reorder at once.
- **(b) Local INNER-only build-side swap (FALLBACK):** a narrow pass that, per INNER join, swaps
  children when `est_card(right) > est_card(left)`, then swaps the join-key index references and
  restores output column order (left.cols ++ right.cols) with a projection so downstream indices are
  unchanged. Considered as a fallback because it reimplements swap+index logic that (a) already has;
  prefer (a) unless its index rewrite proves intractable to make self-join-safe.

**Decision 2 — Oracle/gate-first, incremental, revert-on-regression (the F-Perf-RN lesson).**
- Land the leaf-origin change behind the existing test suite first: extend the reorder tests with a
  self-join case (q08-shaped: a chain with two same-schema leaves) that asserts the smaller side is
  built AND the rebuilt indices resolve to the correct leaf (the exact failure mode of the revert).
- Re-create the q18 RowConverter regression as a guard test before changing the rewrite, so a
  re-introduction of the index mismatch fails loudly at unit-test time, not at SF30.
- Only after green unit tests: SF1 trino-diff, then SF30 remote q08 memory + 22q no-regression.

**Decision 3 — Track columns positionally where names are ambiguous, not globally.**
Keep name-based resolution where it works (no duplicates) and switch to `(leaf, position)` only for
the ambiguous leaves, minimising the blast radius vs a wholesale rewrite of the reorder's column
model.

## Risks / Trade-offs

- **Re-introducing the q18 RowConverter mismatch.** Highest risk; the prior revert. Mitigation: the
  guard test in Decision 2 + SF1 trino-diff before any SF30 run.
- **Subtle index errors in self-join chains** that pass unit tests but corrupt results at scale.
  Mitigation: SF30 q07 AND q08 cell-diff vs Trino (row-count ≠ cell-correct), not just q08 memory.
- **(a) proves intractable.** If leaf-origin tracking can't be made safe within a reasonable bound,
  fall back to (b) — narrower scope (q08 INNER only) but self-contained; documented above.
- **Scope creep into general JoinReorder rework.** Hold the line: only the self-join column-tracking
  path changes; the cost model and the non-self-join behavior are untouched.
