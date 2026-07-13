## 1. Oracle first (the safety belt — build before touching the exchange)

- [x] 1.1 Pick a set of nested multi-way join queries that exercise different key chains (q09 6-way is the anchor; add ≥ 2 of q05/q07/q08). Confirm each is a multi-level hash join in arneb's fragment tree
- [x] 1.2 Build a fast forced-N>2 cell-diff harness: set `ARNEB_HASH_PARTITION_TARGET_ROWS` low on the local SF1 distributed stack so the chosen queries fan to N>2, run each through arneb and Trino, cell-diff at 1e-9. (Technique already verified manually: q09 N=13 reproduces the undercount.)
- [x] 1.3 Encode invariants as explicit checks: (a) row count matches Trino (no undercount/duplication); (b) the same query at N=2 vs N>2 is cell-identical. Make the harness print a clear PASS/FAIL per query
- [x] 1.4 Capture the BASELINE with the harness on current HEAD: confirm it FAILS at N>2 (q09 ~0.148×) and PASSES at N=2 — the harness must catch the known bug before it can certify a fix

## 2. Confirm the exact drop point (close the last mile before any fix)

- [x] 2.1 Instrument a forced-N>2 run (env-gated `ARNEB_TRACE_FRAGMENTS` → `eprintln`:
  fragment-tree dump + per-fragment `(task_count,output_partitions)` classification in
  `coordinator.rs`; `do_get` partition-pull log in `flight_service.rs`). **TRACED — the
  inferred mechanism in the proposal/design was STALE/WRONG.** Real root cause:
  **`partition_count` NON-UNIFORMITY across a join→join boundary.** The adaptive rule
  (`choose_partition_count`) picks each join's count independently from its LOCAL children
  estimate; the fragmenter writes the parent's count onto a child's `output_partitioning`
  only ONE level deep, so a child join's OUTPUT count (set by its parent) diverges from the
  INPUT count its own children were given (set by itself). The coordinator uses the single
  `output_partitioning.partition_count` field for BOTH `task_count` (how many buckets it
  consumes) and `output_partitions` (how many it produces) → an M×N exchange where the
  runtime assumes M==N. Evidence (forced N>2, SF1):
  - **q07 (out-of-range → hard error):** stage 2 `s⋈l` emits `n=4` buckets; its parent
    stage 4 has own `n=64` → coord launches 64 consumer tasks, each pulls
    `partition_id=consumer_k`. `do_get` log: stage-2 task buffer serves partitions 0–3 = OK,
    **4–63 = ALREADY_CONSUMED** (really "never created"). Hence
    `partition 58 already consumed for task '...2.0'`.
  - **q09 (in-range → silent undercount = the 2/13):** stage 4 emits `n=13` buckets; its
    parent stage 6 has own `n=2` → coord launches only 2 consumer tasks pulling partitions
    0,1. **Stage-4 buckets 2–12 are never consumed → silently dropped → 2/13 ≈ 0.154**
    (observed ALGERIA 45.7M / 308.8M = 0.148). Counts diverge because lineitem-side joins
    estimate `ceil(6M/500k)=13` while part/supplier/nation-side joins floor at 2.
  - Direction decides the symptom: **parent>child → out-of-range hard error**;
    **parent<child → silent undercount**. At the historical fixed N=2 every stage is 2 →
    M==N trivially → no bug.
- [x] 2.2 **Resolved (D4 / Open Question 3): the `(N,N)` α-producer-and-consumer path is NOT
  inherently broken at N>2 — it is correct whenever producer M == consumer N. Intermediate
  joins ALSO already carry non-empty keyed columns (the proposal's "empty columns → (N,1)"
  theory is obsolete for current HEAD). The ONLY defect is non-uniform `partition_count`
  across an exchange boundary.** Minimal correct fix shape (two candidates — DECIDE in 3.1):
  - **(A) Uniform-N per connected hash chain (recommended, simplest, lowest revert risk):**
    pick ONE `partition_count` for an entire connected chain of hash-repartitioning join
    fragments (size it to the chain's max local estimate, clamped), thread it to every
    fragment + source in the chain. Restores the M==N invariant the fixed-N=2 path relied on
    while staying adaptive per chain. Matches Trino/DataFusion "one count per partitioning
    group." Smallest diff to the existing `(N,N)` runtime (no coord change).
  - **(B) Split the conflated field into input-fan-in vs output-fan-out + a real repartition
    at every boundary:** `task_count` = children's count, `output_partitions` = own count,
    worker re-hashes M→N. More flexible (true heterogeneous M×N) but this is exactly the
    A.4-style surface that broke 7 queries — higher risk, larger diff. Defer unless (A) is
    insufficient.

## 3. Fix — uniform `partition_count` across each connected hash chain (option A)

**Decision (3.1, chosen by user 2026-06-05): option A — uniform N per connected hash
chain, implemented as a fragmenter POST-PASS (not a `split()` rewrite).** Rationale:
- The `split()` recursion is the A.4-revert surface — leave it untouched. A post-pass
  mirrors the existing `prune_fragment_tree` pattern (coord already runs one at
  `coordinator.rs:114`), is trivially gated/tested, and reverts cleanly.
- The fragmenter probe (T2) proved the exact mechanism: each join writes its OWN
  `o_i` to its children's output count, while its own output is overwritten by its
  parent to `o_parent`; the coord uses `output.partition_count` for BOTH `task_count`
  (buckets read) and `output_partitions` (buckets produced), so a chain boundary with
  `o_i ≠ o_parent` mis-sizes the pull. Forcing one count per chain makes M==N hold.
- **Sizing = MAX of the chain's per-join `o_i`** (q09: 13). Sizes to the biggest
  intermediate (the lineitem joins) for best parallelism; over-partitioning the tiny
  joins (nation/supplier) is correct (empty buckets are free) and only a minor perf
  cost, measured at the §5.4 SF30 gate. (Min/top-of-chain would under-partition the
  big joins — worse perf, still correct; max is the right default.)

- [x] 3.1 Decide the fix shape + implementation mechanism. **DONE: option A, post-pass,
  size = chain-max (above).**
- [x] 3.2 Implemented `normalize_chain_partition_counts(&mut root)` in `fragment.rs`
  (+ `is_hash_output`/`chain_max_count`/`apply_chain_count` helpers), called at the end
  of `PlanFragmenter::fragment()`. For each maximal connected component of hash-output
  fragments, sets every member's `partition_count` to the component's max; `Broadcast`/
  `Single`/non-hash children are boundaries (recursed for independent nested chains).
  Columns + schemes untouched; idempotent. Verified live: q09 chain now uniform at N=13
  (was 13,13,13,2,2,2 → all 13).
- [x] 3.3 Unit tests (planner, all green): `normalize_makes_nested_join_chain_uniform_at_max`
  (q09-like 13→2 boundary → all 13), `normalize_exempts_broadcast_build` (build stays
  Broadcast), `normalize_is_idempotent_on_uniform_chain` (N=2 unchanged),
  `normalize_treats_independent_chains_separately` (two chains keep own max). 240 planner
  tests pass.

## 4. Harden the gate (the A.4 fix done right)

**Satisfied by construction by the uniform-N fix (option A), not a coord change.** With
every chain uniform, a consumer's `task_count` (= its own `partition_count`) equals the
`output_partitions` its upstream produced (= the same chain count), so the existing gate
(`coordinator.rs:376-411`, pull `partition_id = consumer_k` iff upstream non-empty columns)
can never ask for a bucket k ≥ `output_partitions`. Empirically confirmed: the forced-N>2
22-query distributed run produced ZERO "already consumed" errors (was q07/q08 before).

- [x] 4.1 Per-partition-pull gate holds — no out-of-range pull at forced N>2 (22/22 clean).
- [x] 4.2 `(N,N)`/`(N,1)` classification + emitted `output_partitions` line up (uniform N).
- [x] 4.3 The `(N,N)` vs `(N,1)` pull is exercised by every forced-N>2 query; the unit
  coverage lives in the planner normalize tests (§3.3) since the fix is upstream of the gate.

## 5. Correctness gate (non-negotiable — this is the A.4 revert area)

- [x] 5.1 `arneb-planner`/`arneb-server`/`arneb-rpc` unit + doc tests green; `cargo clippy
  -p arneb-planner -p arneb-server -p arneb-rpc --all-targets -- -D warnings` clean.
- [x] 5.2 Oracle PASSES at forced N>2 (`MAX=64`, `TARGET=500000`): q05/q07/q08/q09 + the
  FULL 22-query distributed cell-diff (1e-9) ALL PASS. q07/q08 "already consumed" and
  q05/q09 undercount both gone. N>2 and N=2 both match Trino → cell-identical to each other.
- [x] 5.3 No regression at N=2: full 22-query distributed cell-diff at `MAX=2` (historical
  fixed fan-out) ALL PASS (1e-9). The fix is idempotent there (unit-proven + measured).
- [x] 5.4 Remote SF30 (a constrained bench host — 8-core / 31 GB / x86_64; access details
  in memory, not in-repo — SF30 seeded = 180 M lineitem) at REAL N>2 (`ARNEB_MAX_HASH_PARTITIONS=64` → chains fan to
  **N=45** on the 180 M intermediate). **Correctness gate PASS:** q05/q07/q08/**q09** all
  cell-identical to Trino at N=45 (the four formerly-broken nested joins; q09 25 rows, was
  0.148×). q18 cell-correct at N=2 (100 rows == Trino). **No row loss / duplication / value
  drift from the fix at real scale.** Per-task peak RSS ~3.3–4.2 GB (worker `arneb::mem`
  tracked_peak); q09 latency 563 s at N=45.
  - **Out-of-scope finding — pre-existing high-N gather robustness bug (EXPOSED, not caused,
    by this fix; blocks `dist-adaptive-partition` from flipping the default until fixed):**
    q18 ERRORS with an `h2 broken pipe` at any N>2 (tested N=8 AND N=45; PASSES at N=2).
    RCA (fragment-tree + dose-response + q09 contrast): q18's IN-subquery becomes a FIXED
    single-task Join (stage 7) that BUILDs from a FIXED side (stage 6, the
    `lineitem GROUP BY l_orderkey` aggregate) while PROBING an N-way partitioned side
    (stage 4). While it reads the build side, the N stage-4 producers block on their bounded
    OutputBuffers (cap 64); at N>2 this stalls long enough that a producer connection resets
    → broken pipe on the gather's read. A 45→1 gather into a FINAL AGGREGATE is fine (q09
    stage 10→11 at N=45 PASSES) — the bug is specific to a FIXED Join/SemiJoin gathering
    >2 partitioned producers. **`dist-adaptive-partition`'s smaller-N sizing does NOT fix
    this (N=8 fails too)** — the gather path itself must be fixed first (distribute the
    semi-join / let a FIXED join consume a partitioned child in parallel / bounded+spill
    exchange, i.e. `exec-exchange-backpressure`). This change ships safe regardless: the
    DEFAULT stays parked at N=2 (q18 works there) and the fix is idempotent.
  - (b) q09 = 563 s at N=45 — uniform-MAX over-partitions the small joins (45 tasks each)
    on a 4-cpu (2×2-worker) cluster. Perf, not correctness; `dist-adaptive-partition` should
    size N from SCAN inputs (not join-output, which explodes) so q09 picks ~13 not 45.
- [x] 5.5 No regression observed at any step (22/22 at both N=2 and forced N>2). Nothing to revert.

## 6. Ship + unblock the dependent change

- [ ] 6.1 `cargo fmt`; commit in the Taiwan time window with `GIT_AUTHOR_DATE` + `GIT_COMMITTER_DATE` set (only touched files); message describes the nested-join M×N fix + the gate + the forced-N>2 evidence
- [ ] 6.2 Update memory + roadmap: nested-join N>2 now correct; this unblocks `dist-adaptive-partition` (resume its task 5.2/5.3/6.x). Record the exact drop point found in T2 and the oracle as a reusable pattern
- [ ] 6.3 Note for the dependent change: `dist-adaptive-partition` can now re-run its forced-N>2 + SF30 gates and ship
