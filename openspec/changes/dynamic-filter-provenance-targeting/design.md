# Design — dynamic-filter provenance targeting

## Root cause (measured, 2026-06-08)

`inject_inlist_dynamic_filters` (hash_join.rs) builds `Column { name } IN (build distinct values)`
and calls `left.inject_dynamic_filter(filter)`. The default descent recurses into BOTH children;
`ScanExec::inject_dynamic_filter` accepts iff `rewrite_filter_indices` finds the name in its schema
(`local_schema.iter().position(|c| &c.name == name)`). For a self-join, two scans share the name →
both accept → the non-join-equal twin is pruned → silent value corruption.

Evidence:
- q08 EXPLAIN/trace: planner output is correct (Front-2-v2 maps `nation`=`Column{57}`→`Column{50}`
  = n2.n_name; join conditions correct). Yet execution returns 0.197 vs Trino 0.0388.
- Disabling ALL injection → q08 = 0.0388 (correct). Disabling only the second injection → still
  0.197 (the FIRST injection alone corrupts via `n_regionkey` onto n2). → both injections are
  name-unsafe.
- q18 SF1, sibling injection ON vs OFF: build `4,368 rows / 87 KB` vs `59,986,052 rows / 973 MB`;
  latency 1.87 s vs 3.80 s. → sibling injection is load-bearing; cannot be dropped.

## Key idea: the valid target set = the probe key's equivalence class (within the probe subtree)

A filter `build_key IN (V)` is sound on a probe-side column `C` **iff** `C = build_key` is implied,
i.e. `C` is in the equivalence class of the probe-side join key (since the join asserts
`probe_key = build_key`, and equi-joins below propagate equalities). That class:
- contains the direct probe key → first injection covered;
- contains transitively-equal columns (cross-table siblings) → q18 covered;
- does NOT contain a coincidentally same-named column from an unrelated table → q08's n2 excluded.

So one equivalence-derived target set replaces BOTH the current name-based injections.

`crates/planner/src/properties.rs` already computes this: `derive_equivalences(plan, …)` walks a
`LogicalPlan` and yields equivalence pairs in the plan's **output-schema index space**;
`ActualProperties::equivalent_columns(col)` returns the class. We expose a thin logical-plan helper
and reuse it.

## Architecture

1. **Planner helper (`properties.rs`)** — add
   `pub fn equivalent_output_columns(plan: &LogicalPlan, col: usize) -> Vec<usize>`: run
   `derive_equivalences` with an empty `source_props` (same-fragment; an `ExchangeNode` in the
   subtree just truncates the class — conservative, still correct), build `ActualProperties`, return
   `equivalent_columns(col)`. Indices are in `plan`'s output schema.

2. **Physical planning (`execution/planner.rs`)** — when building a `HashJoinExec` for an inner
   join with pure `Column = Column` equi-keys, for each `(left_key_idx, right_key_idx)` compute
   `targets = equivalent_output_columns(left_logical_child, left_key_idx)` (in the probe child's
   output-schema space). Store `Vec<Vec<usize>>` (one target list per equi-key) on `HashJoinExec`.
   Non-inner joins / expression keys → empty (no injection), matching today's eligibility.

3. **Runtime injection (`hash_join.rs`)** — replace `inject_inlist_dynamic_filters`'s name build +
   the two name-based injections with: for each equi-key slot, for each `target_idx` in that slot's
   target list, build `Column { index: target_idx, name } IN (V)` and call
   `left.inject_dynamic_filter_at(filter, target_idx)` (index-authoritative). The first/second
   injection distinction disappears.

4. **Index-descent (`operator.rs` + others)** — `inject_dynamic_filter` becomes index-aware. The
   carried target index is in the CURRENT node's output schema; each operator remaps it into the
   owning child and descends into ONLY that child:
   - `HashJoinExec`: `idx < left.width` → recurse left with `idx`; else recurse right with
     `idx - left.width`.
   - `ProjectionExec`: `expr[idx]` must be `Column { index: child_idx }` → recurse input with
     `child_idx`; otherwise (computed) drop.
   - `FilterExec` / `CoalescePartitionsExec` / `RepartitionExec`: schema-preserving → recurse with
     same `idx`.
   - `ScanExec`: `idx` is the local column → apply the filter at exactly that index;
     `debug_assert_eq!(schema[idx].name, filter_name)` (sanity, NOT routing).
   This mirrors the plan-time `tag_consumer` tracer but runs on physical operators and threads the
   index through joins (which `tag_consumer` does not).

### Signature note
Keep `inject_dynamic_filter(&self, filter)` for any remaining callers, but route the dynamic-filter
flow through a new index-authoritative entry. The filter's `Column.index` is the authoritative
target at each level; the `name` is retained only for the scan-side sanity assert and EXPLAIN.

## Decisions / trade-offs

- **Equivalence at plan time, descent at runtime.** Equivalence is a plan fact (transitive join
  equalities); the owning-scan resolution is cleanest as a runtime physical descent (handles
  partition/coalesce/repartition shapes that only exist physically). Splitting it this way avoids
  extending the plan-time `tag_consumer` to trace through joins.
- **Why not skip-on-ambiguity (the rejected option B).** It cannot distinguish q18's safe sibling
  (join-equal) from q08's unsafe twin (not equal) — both are "a same-named probe column" — so it
  would either corrupt q08 or kill q18's 11,000× memory win. Only equivalence separates them.
- **Empty `source_props`.** For same-fragment injection the probe subtree is within one fragment;
  no `ExchangeNode`. If one appears, equivalence truncates there → fewer targets → still correct
  (we never inject onto a non-equal column; we may just miss a pruning opportunity).
- **Multi-key joins.** Each equi-key gets its own target list and its own filter; independent.

## Gates (gate-first; planner work is revert-prone)

1. **RED first**: a value-based test reproducing the q08 misroute (self-join + dynamic filter +
   build-key filter routed to the wrong twin) that FAILS on the current name-based injection and
   passes after provenance targeting. Prefer an end-to-end execution test with distinguishable
   values, OR rely on SF1 trino-diff as the authoritative gate (242 green unit tests shipped the
   original q08 corruption — unit structural tests are insufficient here).
2. **SF1 trino-diff** (the authoritative correctness gate): q08 0.197→0.0388, q07 + all 17 match.
   Run on the companion branch (Front-2-v2 active) so the corrupting reorder is in play.
3. **q18 perf guard**: with `--profile`, q18's hash-join build stays ~4,368 rows / ~87 KB (sibling
   collapse preserved); not the 60M / 973 MB unguarded build.
4. Full planner+execution unit/doc tests + `clippy --all-targets -D warnings`; no regression on
   queries currently using same-fragment dynamic filters.
5. Then enable build-side: land planner-build-side-selection together; verify SF30 q08 memory
   3.4×→~1× and 22q no-regression.
