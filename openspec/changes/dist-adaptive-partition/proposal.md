## Why

The fragmenter hard-codes the hash-repartition fan-out to **2** partitions
(`crates/planner/src/fragment.rs:615` — `let partition_count = 2usize;`),
regardless of cluster size or data volume. At SF30 the wide
`orders ⋈ lineitem` intermediate (~90M rows / 5.2 GB) is therefore split into
just two partitions, so each worker partition must hold ~45M rows — a direct
contributor to the per-partition memory pressure that OOM-kills workers on q09
and q18. Raising and adapting this fan-out is the cheapest, lowest-risk first
step of the "distributed brain" roadmap (shrink the per-partition working set
before reaching for exchange elision or broadcast).

## What Changes

- Replace the fixed `partition_count = 2` in the fragmenter's hash-exchange
  insertion with an **adaptive** count derived from the worker count and an
  estimated-cardinality heuristic (e.g.
  `clamp(max(worker_count, ceil(estimated_rows / target_rows_per_partition)))`).
- Add an `ARNEB_*` runtime knob (default in source, env override, log the
  effective value) per the build-time-vs-runtime convention in
  `docs/guide/configuration.md`. The knob bounds the adaptive count
  (target rows-per-partition and/or a hard max) without an image rebuild.
- No new exchange machinery: the M×N scheduling already reads
  `*partition_count` (`crates/server/src/coordinator.rs:312-325`) and the
  W3-Hash partitioned probe (deterministic hash seed) already executes N-way
  fan-out. This change only computes a better N and flows it through the
  existing plumbing.

## Capabilities

### New Capabilities
- `adaptive-partition-count`: the rule by which the fragmenter chooses the
  number of hash partitions for a repartition exchange — adaptive on worker
  count and estimated cardinality, runtime-configurable, with a deterministic
  default. Owns the correctness contract that an N-way fan-out (N ≥ 2)
  produces results identical to the previous 2-way fan-out.

### Modified Capabilities
<!-- None. The `partitioning` spec describes the Partitioning enum + compatibility
     (the data structure); it is unchanged. This change adds the *decision rule*
     for the count, which is new behaviour, not a modification of the enum. -->

## Impact

- **Code**: `crates/planner/src/fragment.rs` (the hard-coded `2`); a small
  cardinality/worker-count input threaded into the fragmenter; config plumbing
  in `crates/server/src/config.rs` + the `ARNEB_*` read site.
- **Execution**: exercises the existing M×N coordinator scheduling
  (`coordinator.rs`) and partitioned-probe path at N > 2 — previously only
  routinely run at N = 2.
- **No API/wire changes.** No change to the `Partitioning` enum, RepartitionExec,
  or the memory pool.
- **Out of scope (separate changes)**: `properties.rs` consolidation / exchange
  elision (`dist-ensure-requirements`), broadcast joins (`dist-broadcast-join`),
  and memory accounting / spill (`exec-memory-accounting`, in progress).
