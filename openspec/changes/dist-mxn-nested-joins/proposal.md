## Why

arneb's distributed hash exchange silently **drops rows from nested
multi-way joins whenever the hash fan-out N is greater than 2**. The
intermediate join fragment's `output_partitioning` carries empty hash columns
(`crates/planner/src/fragment.rs:726`, `columns: Vec::new()`), so the
coordinator schedules it as an "α consumer only" stage `(N, 1)`
(`crates/server/src/coordinator.rs:338`) and **never re-partitions the join's
output onto the next join's keys**. The M×N `(N, N)` producer-and-consumer path
(`coordinator.rs:332`) is inert for join fragments ("today only Source fragments
get non-empty hash columns"). At N = 2 nested joins survive; at N > 2 they lose
colocation.

Measured (SF1, forced N = 13): q09 (a 6-way join) returns
`ALGERIA = 45,755,156` vs Trino's `308,811,555` — an undercount of ~0.148
(≈ 2/13). A single 2-way join (`lineitem ⋈ orders COUNT(*)`) at N = 13 is
correct (`6,001,215` = Trino), pinning the bug to nested joins. This is the same
defect the reverted **A.4** work hit ("α-producer feeding α-consumer is silently
wrong without M×N exchange"), which broke 7 queries when attempted without the
right gate. It is the prerequisite that unblocks `dist-adaptive-partition` (and
every later distribution change) — none can fan out past N = 2 until nested-join
colocation is correct.

## What Changes

- The fragmenter SHALL set an intermediate join fragment's `output_partitioning`
  hash columns to the **next consuming join's equi-keys**, so the fragment
  enters the M×N `(N, N)` producer-and-consumer path and its output is
  re-hashed onto the keys the downstream join colocates on.
- The coordinator's M×N scheduling SHALL drive correct partition fan-out/pull
  for these join-fragment producers (each downstream task K pulls partition K
  from every upstream task), gated so a consumer only pulls per-partition when
  the upstream genuinely fanned out M×N — otherwise it falls back to the safe
  (single-partition pull) behaviour. This reinstates the A.4 capability **with**
  the gate that prevents the 7-query regression.
- A **forced-N>2 correctness oracle**: a fast harness that drives SF1 nested
  joins through N > 2 (via the `ARNEB_HASH_PARTITION_TARGET_ROWS` knob) and
  cell-diffs against Trino, plus invariant assertions (no row loss / no
  duplication / colocation holds). Built first, as the safety belt for this
  high-revert-risk change.

## Capabilities

### New Capabilities
- `nested-join-repartition`: the contract that an intermediate join fragment is
  re-partitioned onto the next join's keys, so nested multi-way joins produce
  results identical at any hash fan-out `N ≥ 2` (no row loss or duplication),
  with the gate that a consumer pulls per-partition only when its upstream
  performed the M×N fan-out.

### Modified Capabilities
<!-- None. The `partitioning` spec (the enum + compatibility) is unchanged.
     This change adds the fragmenter's distribution *decision* for intermediate
     joins + the coordinator's M×N gate — new behaviour, not a modification of
     the existing enum spec. -->

## Impact

- **Code**: `crates/planner/src/fragment.rs` (set next-level keys on the join
  fragment's `output_partitioning`); `crates/server/src/coordinator.rs` (the
  M×N `(N,N)` path + the per-partition-pull gate at :332/:338/:376-411).
- **Correctness**: this is arneb's highest-revert-risk area (A.4 broke 7
  queries). Every step gated by the forced-N>2 cell-diff + existing SF1
  trino-diff (N = 2 must not regress) + remote SF30 nested-join cell-diff.
- **Unblocks**: `dist-adaptive-partition` (currently blocked); every later
  distribution change that needs N > 2.
- **Out of scope (separate changes)**: adaptive partition count
  (`dist-adaptive-partition`), broadcast joins, `properties.rs` EnsureRequirements
  consolidation, memory accounting; and the general cross-cutting
  validation-harness (this change carries only the focused nested-join oracle).
