## Status (2026-06-08)
SF1 milestone DONE: q08 0.197→0.0388 (matches Trino), q07 + all 17 trino-diff match, no regression;
891 workspace tests + clippy green. Companion planner-build-side-selection (Front-2-v2) active (bail
removed, 1.1 reorder test green). REMAINING: §6 SF30 remote validation + §7 ship. Uncommitted WIP
(not in Taiwan commit window).

## 1. Gate first (anti-revert; the q08 corruption only trino-diff caught)

- [x] 1.1 RED reproduction = SF1 trino-diff on the companion branch (Front-2-v2 active): q08 was
  0.197 (confirmed); fixed → 0.0388201425143322 / 0.03948968749183993 (matches Trino).
- [x] 1.2 q18 perf baseline measured: sibling injection ON = build ~4,368 rows / 87 KB; OFF =
  ~59,986,052 rows / 973 MB (the regression to avoid). Drove the decision to do the full equivalence
  version (not skip-on-ambiguity).

## 2. Planner equivalence helper

- [x] 2.1 `properties::equivalent_output_columns(plan, col)` (pub) — `derive_equivalences` (empty
  source_props) → `equivalent_columns`. Test `equivalent_output_columns_excludes_samename_twin_
  includes_transitive_sibling` GREEN (twin excluded, transitive sibling included).

## 3. Thread targets onto the physical join

- [x] 3.1 `HashJoinExec.df_targets: Vec<Vec<usize>>` (one target index list per equi-key, in the
  probe/left child's output-schema space). Default empty.
- [x] 3.2 `execution/planner.rs` Join case: `df_targets[k] = equivalent_output_columns(left_logical,
  left_key_idx[k])`. SemiJoinExec gets `with_df_targets` from its `left_key` index too. Non-inner /
  expression keys → empty.

## 4. Index-descent injection (replace name-based)

- [x] 4.1 `inject_dynamic_filter(&self, filter, target_index)` — index is in THIS operator's output
  schema; default no-op (drop).
- [x] 4.2 Per-operator descent: `HashJoinExec` (split by left width), `ProjectionExec` (map through
  `Column` exprs, drop on computed), `FilterExec`/`CoalescePartitionsExec`/`RepartitionExec`/
  `AssignUniqueIdExec` (passthrough), `ScanExec` (map target through `ScanContext.projection` →
  source index, apply; `rewrite_filter_at_index` `debug_assert`s the name matches — loud, not silent).
- [x] 4.3 `inject_inlist_dynamic_filters` + `inject_grace_dynamic_filters` + the SemiJoin injection
  rewritten to inject per `df_target` index; the two name-based "sibling" injections deleted
  (subsumed by the equivalence class). Dead `rewrite_filter_indices` removed.

## 5. Validate correctness (SF1)

- [x] 5.1 891 workspace tests + `clippy --all-targets -D warnings` green; `cargo fmt`.
- [x] 5.2 SF1 trino-diff (companion branch): q08 0.197→0.0388; q07 + all 17 match (q11 = both
  engines 0 rows = match). No regression.
- [x] 5.3 q18 correct vs Trino + fast (1.84 s); build 51 MB (NOT the 973 MB unguarded blow-up —
  dynamic filters still effective). NOTE: q18 now reorders (Front-2-v2), so its build profile
  differs from the un-reordered 87 KB baseline; revisit exact SF30 memory in §6.

## 6. Enable build-side + SF30 validation

- [x] 6.1 Build-side enabled: `try_reorder_chain` self-join bail already removed (Front-2-v2);
  `reorders_self_join_chain_puts_fact_on_probe_spine` green.
- [ ] 6.2 SF30 remote: q08 total-cluster peak memory 3.4×→~1× Trino; q07/q08/q18 cell-diff vs Trino.
- [ ] 6.3 Full 22q SF30: no latency/memory regression (watch self-join queries + any query relying
  on same-fragment dynamic filters; confirm q18 SF30 memory acceptable after the reorder shift).

## 7. Ship

- [ ] 7.1 Update SF30 heavy-query root-cause map + resumption memory with outcome.
- [ ] 7.2 Commit in the Taiwan window (milestone + user decides); archive this change +
  planner-build-side-selection when validated.
