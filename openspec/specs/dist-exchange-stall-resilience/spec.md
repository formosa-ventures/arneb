# dist-exchange-stall-resilience Specification

## Purpose
Guarantee that a distributed query never returns SILENTLY truncated results when a
downstream FIXED consumer (a single-task semi/anti-join, or any operator that reads
a large build side to completion before draining its probe) defers reading its
probe input. Probe-side producers must deliver their complete output; if a stall
ever does truncate a partition, the engine must fail loud (the Layer-1 `must_drain`
guard) rather than succeed with missing rows. Establishes the SF30 q21/q02
cell-correctness + determinism bar this resilience is measured against.

## Requirements
### Requirement: Probe producers SHALL NOT be silently truncated by a downstream build stall

When a downstream FIXED consumer (a single-task semi/anti-join, or any consumer that reads a large build side to completion before draining its probe) defers reading its probe input, the probe-side producers SHALL deliver their COMPLETE output. The exchange SHALL NOT drop a producer's connection and let the producer silently end with missing rows.

#### Scenario: FIXED semi-join builds a large side while the partitioned probe waits

- **WHEN** q21 runs at SF30 N=2 (the FIXED EXISTS-semi at stage 8 builds from `l2` = 179,998,372 rows for minutes before pulling its probe = the partitioned `supplier⋈l1⋈orders⋈nation` chain)
- **THEN** every probe producer (stage 2/4/6 tasks) delivers all its rows to its consumer, the per-stage operator output equals its expected cardinality across runs (e.g. stage 2 total == |l1| = 113,797,647), and no `consumer dropped receiver mid-stream on a must-drain exchange` failure is raised

#### Scenario: Idle probe connection survives a multi-minute build

- **WHEN** a probe producer has filled its bounded `OutputBuffer`, spilled its overflow, and its consumer has not begun draining (because the consumer is still building its other side)
- **THEN** the producer's connection to its consumer stays alive for the full build duration (no idle-driven reset), and the producer resumes delivery when the consumer drains

### Requirement: q21 and q02 SHALL be cell-correct at SF30 and deterministic

The queries that the Layer-1 safety net currently fails loud (q21, q02) SHALL complete successfully with results cell-identical to Trino at SF30 N=2, and SHALL be deterministic run-to-run.

#### Scenario: Oracle gate passes for the previously-failing queries

- **WHEN** `benchmarks/tpch/scripts/blast_radius_oracle.py` runs all 22 queries at SF30 N=2 with at least 2 runs each
- **THEN** q21 and q02 report PASS (deterministic AND cell-identical to Trino), and the other 19 queries remain PASS (no new ERROR and no new cell-diff introduced by this change)

#### Scenario: Determinism under sustained load

- **WHEN** q21 (and q02) is run ≥3 times back-to-back on the loaded SF30 stack
- **THEN** every run returns the identical result set (no run-to-run row churn), and that set matches Trino

### Requirement: The stall fix SHALL keep memory bounded

Removing the stall SHALL NOT trade a stall-error for an out-of-memory error. Per-node memory SHALL stay within the configured cap, and operators that need to spill SHALL spill rather than exhaust the pool.

#### Scenario: q02 completes without OOM

- **WHEN** q02 runs at SF30 N=2 (where it currently also hits `HashAggregateExec: pool exhausted`)
- **THEN** q02 completes without a `resource exhausted: memory pool exhausted` error and the per-node peak stays within the configured `mem_limit`

### Requirement: The Layer-1 silent-truncation guard SHALL remain intact as a backstop

This change SHALL make the stall not occur; it SHALL NOT remove or weaken the Layer-1 `must_drain` guard (commit `1704661`). If the stall ever recurs (e.g. on a future query or higher scale), the engine SHALL still fail loud rather than return silently-wrong results.

#### Scenario: A future build-stall still fails loud, never silent

- **WHEN** a must-drain consumer's probe producer is truncated mid-stream by any future stall/reset not covered by this fix
- **THEN** the query raises a clear error (the `must_drain` guard), and never returns a truncated result as success

