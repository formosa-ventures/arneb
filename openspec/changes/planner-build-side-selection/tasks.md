## 1. Oracle & guard tests first (anti-revert — F-Perf-RN lesson)

- [x] 1.1 RED test `reorders_self_join_chain_puts_fact_on_probe_spine` — q08-shape (nation self-join
  + 6M lineitem fact); asserts lineitem lands on the probe spine. FAILS today
  (`["nation","nation","lineitem"]` — bail keeps SQL order, lineitem built). Confirmed RED.
- [x] 1.2 q18 RowConverter guard `self_join_reorder_keeps_column_indices_consistent` +
  `assert_join_column_indices_consistent` helper — every join-condition `Column{index,name}` must
  resolve to a same-named column in that join's combined left++right schema (catches the cross-leaf
  index mismatch that caused the F-Perf-RN revert). GREEN today (bail = indices unchanged); MUST stay
  green through §2.
- [x] 1.3 Baseline: planner lib suite 241 pass / 1 fail (only the intended RED 1.1).

## 2. Leaf-origin column tracking (approach a — primary)

**Scope confirmed by reading the code (2026-06-08): TWO name-based resolution fronts must become
index-based leaf-origin.** Each edge/expr `Column { index, name }` carries the ORIGINAL combined
position, which maps unambiguously to `(leaf, pos_in_leaf)` via cumulative original leaf widths —
names are not needed. Precompute `orig_offset[leaf]` (original order) + `leaf_of_orig_index(oi)`
and `new_offset[leaf]` (from the chosen order); remap any original index `oi` to
`new_offset[leaf_of(oi).leaf] + leaf_of(oi).pos`.

- [x] 2.1 Leaf-layout helpers added: `leaf_widths`, `cumulative_offsets`, `leaf_of_orig_index(oi)
  -> (leaf,pos)`, `new_offsets(order)`, `leaves_referenced_by_index`, `visit_column_indices`.
- [x] 2.2 **Front 1 — `emit_left_deep` DONE (committed):** refactored `rewrite_expr` to take an
  index-remap CLOSURE (shared by name + leaf-origin); replaced name-based `compute_leaf_owners` /
  `leaves_referenced` / `rebuild_column_indices` with index-based leaf-origin remap; removed the 3
  dead name-based helpers. **Behavior-preserving (the 375 bail is still active → self-joins still
  bail → no behavior change): 241 planner tests pass / 1 ignored, clippy clean.** Pure structural
  (Tidy-First).
- [ ] 2.3 **Front 2 — `rebuild_plan_indices` / `rewrite_against` (`join_reorder.rs:~967,1385`) — the
  reverted-once core.** It re-resolves the WHOLE post-reorder plan by NAME bottom-up and bails on any
  duplicate-name schema (so it runs even on the bare-join test and bails → analyze falls back to the
  original). Front 1 alone can't make 1.1 green because of this. The fix needs to thread each
  reordered chain's **old→new column permutation** (the same `orig_offset`/`new_offset` mapping)
  through the bottom-up rebuild so parent Projection/Filter/Sort refs AND the reordered join
  conditions remap by leaf-origin instead of by name. (Considered + rejected: a restoring-projection
  that preserves output order — it changes the plan shape for ALL reorders and `rebuild_plan_indices`
  still re-resolves the self-join condition; and "trust existing index on dup name" alone is the
  reverted unsafe behavior for stale parent refs.) **Design carefully before coding; SF1 trino-diff
  is the gate.**
  - **ATTEMPT 1 (2026-06-08) — restoring-projection + trust-existing — REVERTED on q08 corruption.**
    Tried: remove the 375 bail; wrap self-join chains in `wrap_restoring_order` (a Projection that
    re-emits columns in the ORIGINAL order so parent refs stay valid); relax `rewrite_against` to
    TRUST existing indices on duplicate-name schemas (safe *only* with the order-preserving wrap).
    **Unit-level: PASSED** — 1.1 target green (q08-shape builds the small side), 1.2 guard green, 242
    planner + 853 workspace tests + clippy --all-targets all green. **BUT SF1 trino-diff CAUGHT A
    CORRECTNESS REGRESSION the 242 unit tests MISSED: q08 DIFF (values ~5× wrong: 0.197 vs 0.039);
    q07 (also a nation self-join) MATCHed; q01-q06/q09 MATCHed.** So the design is close but has a
    STRUCTURAL bug specific to q08's plan shape (q07 works). Reverted (uncommitted → discarded).
    **Hypothesis to diagnose FIRST next time: `wrap_restoring_order` assumes the chain's original
    output = leaves concatenated in flatten (left-deep) order; q08's original tree / parent refs
    likely don't match that assumption (non-left-deep, or a parent op the wrap doesn't cover).
    EXPLAIN q08 vs q07 to find the structural difference before re-attempting.** LESSON (re-confirmed):
    for this reverted-once class, **trino-diff is the gate, not unit tests** — the diff discipline
    the user insisted on caught exactly the corruption unit tests can't.
- [ ] 2.4 Remove the duplicate-name bail in `try_reorder_chain` (`join_reorder.rs:375`) once 2.3 is
  index-based.
- [ ] 2.5 Make 1.1 GREEN; keep 1.2 GREEN (no RowConverter mismatch) + all existing reorder tests
  green. If 2.3 cannot be made safe within a bounded effort, STOP and switch to the (b) fallback (§5).

## 3. Validate correctness (SF1, no remote yet)

- [ ] 3.1 Full planner + execution + analyzer unit/doc tests + `clippy --all-targets` green.
- [ ] 3.2 `trino-diff` full suite at SF1 — cell-parity (1e-9), no correctness regression
  (especially the self-join queries q07/q08).

## 4. Validate memory + no-regression (SF30 remote)

- [ ] 4.1 On the remote bench host (plain image, bench+sf30 config): confirm q08 now builds the
  smaller side — re-capture the `arneb::profile` join events (`build_rows` should be the small side,
  not 90M).
- [ ] 4.2 SF30 q08 total-cluster peak memory drops from ~3.4× toward ~1× Trino; q07 + q08 cell-diff
  vs Trino (row-count ≠ cell-correct).
- [ ] 4.3 Full 22q SF30 bench: no latency or memory regression on any other query (watch the
  self-join queries and anything whose build side could shift). Record before/after.

## 5. Fallback — local INNER-only build-side swap (approach b, only if §2 is abandoned)

- [ ] 5.1 Add a narrow pass that swaps an INNER join's children when `est_card(right) > est_card(left)`,
  swaps the join-key index references, and restores output column order with a projection so
  downstream indices are unchanged.
- [ ] 5.2 Same gates as §3-4 (SF1 trino-diff, SF30 q08 memory + 22q no-regression).

## 6. Ship

- [ ] 6.1 Update the SF30 heavy-query root-cause map memory + the resumption note with the outcome
  (q08 memory before/after, which approach landed, any new build-side-shift findings).
- [ ] 6.2 Commit in the Taiwan window (milestone + user decides); archive the change when validated.
