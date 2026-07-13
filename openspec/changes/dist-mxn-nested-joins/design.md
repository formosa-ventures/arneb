## Context

arneb's distributed hash exchange drops rows from nested multi-way joins when
the hash fan-out `N > 2`. Diagnosed empirically (SF1, forced
`ARNEB_HASH_PARTITION_TARGET_ROWS=500000` → q09 fans to N=13):

- q09 (6-way join): arneb `ALGERIA = 45,755,156` vs Trino `308,811,555`
  (~0.148 ≈ 2/13 — an undercount).
- single 2-way join `lineitem ⋈ orders COUNT(*)` at N=13 = `6,001,215` = Trino
  (single joins fine at N>2).
- nested join at N=2 = correct.

**Mechanism (from code + the boundary + the 2/13 ratio):**
The default hash-join fragmenter path sets the join fragment's
`output_partitioning` to `Hash { columns: vec![], partition_count }`
(`fragment.rs:726`) — **empty** hash columns. The coordinator's M×N match arm
(`coordinator.rs:318-340`) therefore classifies the join fragment as
"α consumer only" `(task_count = N, output_partitions = 1)` (`:338`), never
"α producer-and-consumer" `(N, N)` (`:332`, reserved for fragments with
non-empty hash columns — "today only Source fragments get non-empty hash
columns"). So an intermediate join emits one un-keyed stream; the next join
cannot colocate its inputs on its own equi-keys at N>2, and rows that should
have met in the same partition do not.

This is the **A.4-reverted work**: enabling the M×N producer path for
non-Source fragments. A.4 attempted it and broke 7 queries (memory
`project_2026-05-20_a4_revert_root_cause`: "α-producer feeding α-consumer is
silently wrong without M×N exchange"). The lesson: the per-partition pull must
be gated on a genuine M×N producer.

## Goals / Non-Goals

**Goals:**
- Nested multi-way joins produce cell-identical results at any `N ≥ 2`.
- Re-partition an intermediate join's output onto the downstream join's keys
  (non-empty `output_partitioning` columns → M×N `(N,N)` path).
- Gate per-partition pull on a real M×N producer (the A.4 fix done right).
- Zero regression at `N = 2` (the historical fixed value).
- Build a fast forced-N>2 cell-diff oracle FIRST, as the safety belt.

**Non-Goals:**
- Adaptive partition count (`dist-adaptive-partition`, blocked on this).
- Broadcast joins, `properties.rs` EnsureRequirements consolidation, memory work.
- A general cross-cutting validation harness (this change carries only the
  focused nested-join oracle).

## Decisions

### D1 — Oracle first (non-negotiable for this revert-prone area)

Before touching the exchange, build a fast harness: force SF1 nested joins
through N>2 via `ARNEB_HASH_PARTITION_TARGET_ROWS`, run a small set of
nested-join queries (q09 + a couple more multi-way joins) through arneb and
Trino, and cell-diff (1e-9). This reproduces the bug in minutes (already
verified manually). It also encodes invariant checks (no row loss / no
duplication). Every subsequent step is gated on this oracle.
- _Alternative_: rely on SF30 remote runs. Rejected — 90-minute loop; the whole
  point is that forced-N>2 SF1 reproduces the defect faithfully and fast.

### D2 — Set the downstream join's keys on the intermediate join's output

In the fragmenter's Join arm, when the join's result will feed a downstream
join requiring partitioning on keys `K`, set the join fragment's
`output_partitioning = Hash { columns: K, partition_count }`. This moves it onto
the `(N, N)` producer-and-consumer path so it re-hashes its output onto `K`.
- _Open_: how the fragmenter learns the *downstream* join's keys at the point it
  builds the *current* fragment. The fragmenter is bottom-up; the parent join's
  keys are known one level up. Options: thread the parent's required
  partitioning down, or do a post-pass that rewrites a child join fragment's
  output columns once the parent's keys are known. Resolve during the
  execution-trace task (T2) before coding the fix.

### D3 — Gate the per-partition pull (the A.4 fix)

The consumer's source-exchange builder (`coordinator.rs:376-411`) already pulls
`partition_id = consumer_k` only when the upstream's `output_partitioning` has
non-empty columns, else `0` (`:384`). With D2 making intermediate joins
non-empty, this gate must hold: a consumer pulls per-partition **iff** the
upstream genuinely produced N buckets (output_partitions = N), never against a
single-stream producer. Confirm the `(N,N)` vs `(N,1)` classification and the
`output_partitions` actually emitted line up so a consumer never asks for a
bucket the producer did not create.

### D4 — Confirm the exact drop point before the fix (close the last mile)

The mechanism above is inferred from code + the boundary + the 2/13 ratio, not
yet execution-traced. Task T2 instruments/traces a forced-N>2 q09 run to prove
*where* rows are dropped (e.g. a consumer pulling an empty bucket, or a join
task missing colocated rows), so the fix targets the real point, not a plausible
one. This is the "understand before acting" guard for the revert-prone area.

**RESOLVED (T2, 2026-06-05) — the inferred mechanism above (D2/Context) was
WRONG for current HEAD.** Env-gated trace (`ARNEB_TRACE_FRAGMENTS`) of forced-N>2
q07 + q09 proves intermediate join fragments DO carry non-empty keyed columns
(parent-overwrite at `fragment.rs:779` works) and DO take the `(N,N)` path — that
path is correct whenever producer M == consumer N. **The real defect is
`partition_count` non-uniformity across a join→join boundary:** `choose_partition_count`
sizes each join independently from its LOCAL children estimate, and the fragmenter
propagates the parent's count onto a child's `output_partitioning` only one level
deep, so a child join's OUTPUT count (set by its parent) diverges from the INPUT
count its own children received (set by itself). The coordinator uses the single
`output_partitioning.partition_count` for BOTH `task_count` and `output_partitions`,
so an M≠N boundary either pulls out-of-range buckets (parent>child → q07 "already
consumed" hard error) or drops the high buckets (parent<child → q09 silent 2/13
undercount). At the historical fixed N=2 every stage is 2 → M==N → no bug. Full
evidence in `tasks.md` §2.1. **Consequence: D2 (set the downstream keys on the
intermediate join) is OBSOLETE — those keys are already set. The fix is uniform
`partition_count` per connected hash chain (tasks.md §2.2 option A), not a
fragmenter key change.** D3's per-partition-pull gate is still relevant but secondary.

## Risks / Trade-offs

- **[Repeat of A.4 — break N queries]** This is the exact change A.4 reverted. →
  Mitigation: D1 oracle first; gate (D3); land incrementally; every step gated by
  forced-N>2 cell-diff AND the existing N=2 suite (no regression) AND remote SF30
  nested-join cell-diff. Revert immediately if any query regresses.
- **[Inferred mechanism is wrong]** The fix could target the wrong point. →
  Mitigation: D4 execution-trace before the fix.
- **[More exchange traffic]** Re-partitioning intermediates adds shuffle. →
  Mitigation: correctness first; measure SF30 latency/memory after; this change
  is about correctness, not the perf win (that follows once N>2 is safe).
- **[Silent corruption is invisible to row-count gates]** The earlier SF30 bench
  showed q09 "OK, 25 rows" — but that was N=2, and the bench checks row count,
  not cell values. At N>2 it would show "OK" with wrong sums. → Mitigation: the
  oracle is cell-diff, not row-count.

## Migration Plan

Internal planner + coordinator change; no wire/API/schema change. Until landed,
the safe state is N=2 everywhere (the historical fixed value), which is correct.
Rollback = revert the fragmenter change (intermediate joins go back to empty
output columns → N=2-only safe behaviour). Land only when the full gate is green.

## Open Questions

- D2: the cleanest way for the bottom-up fragmenter to apply the *parent* join's
  keys to a *child* join fragment's output partitioning (thread-down vs post-pass).
- Which nested-join queries beyond q09 best exercise the oracle (need ≥ 3-way
  joins with different key chains; candidates: q05, q07, q08, q09).
- Does the `(N,N)` α-producer-and-consumer path itself have latent issues at N>2
  (it has been inert), or only the fragmenter gate? Resolve in T2.
