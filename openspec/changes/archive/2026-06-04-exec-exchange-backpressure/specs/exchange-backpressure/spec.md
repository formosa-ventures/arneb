## ADDED Requirements

### Requirement: Inter-stage exchange has no hard wall-clock registration deadline

The distributed inter-stage exchange (worker-to-worker Arrow Flight output buffer + coordinator `do_get`) SHALL NOT fail a query because a producer took longer than a fixed wall-clock interval to register or populate its OutputBuffer. The current hard 5-minute `"worker never registered OutputBuffer within N min"` failure SHALL be removed. A genuinely slow stage (heavier scale, fewer cores) MUST degrade to *slow*, never to a hard query failure.

Liveness SHALL be detected by producer **progress/heartbeat**, not elapsed time: the consumer SHALL fail only if the producer task has died or has made no progress for a heartbeat interval. An optional, configurable large safety ceiling MAY exist (default unbounded or very large) to bound truly hung queries, but it SHALL NOT be the primary mechanism and SHALL NOT trip on merely-slow stages.

#### Scenario: Slow stage degrades to slow, not failure

- **WHEN** a producer worker's stage at SF10/SF100 takes longer than the old 5-minute deadline to begin producing output
- **THEN** the query continues and completes (slow), and the coordinator's `do_get` does not fail with an OutputBuffer-registration timeout

#### Scenario: Dead producer is still detected

- **WHEN** a producer worker task dies (panic / process exit) without producing output
- **THEN** the consumer detects the loss of liveness (producer death / no-progress heartbeat) and the query fails promptly with a clear error, rather than hanging indefinitely

#### Scenario: q09/q21 reliable at SF10 on a constrained host

- **GIVEN** an 8-core host with SF10 data seeded
- **WHEN** q09 and q21 run repeatedly (warm)
- **THEN** they complete every time with correct results (no intermittent OutputBuffer hard-fail), matching Trino's reliability on the same host

### Requirement: Inter-stage exchange is bounded

The inter-stage exchange SHALL bound its in-memory buffering (a configurable per-partition channel capacity), so that a slow consumer applies smooth back-pressure to the producer rather than causing unbounded memory growth or a stall. The bound SHALL be the back-pressure mechanism — a producer that outruns its consumer parks on the full channel instead of accumulating without limit.

> **Spill-to-disk overflow descoped.** The empirical resolution of this change's Open Question (after D1 landed) is that the hard-fail was caused by the wall-clock deadline, not by unbounded exchange memory — D1 (liveness) plus the bounded in-memory buffer removed the SF10 reliability gap without disk spill. Spilling the exchange overflow to disk for large intermediates (SF30/SF100) is genuinely useful, but it MUST coordinate with the global `MemoryPool` (spill under pool pressure, not at a fixed per-channel cap) — doing it here in isolation would have to be retrofitted when allocation-level memory accounting lands. It is therefore deferred to the upcoming memory-accounting change ("Killer 3"), which owns both tracking the exchange channel in the global pool **and** spilling it to disk.

#### Scenario: Slow consumer back-pressures producer within the bound

- **WHEN** the consumer drains slower than the producer produces
- **THEN** the producer parks on the full bounded channel (bounded memory, no unbounded growth) and resumes when the consumer frees capacity, with no spill files on this path

### Requirement: Exchange back-pressure is decoupled from task admission

A producer parked on a full exchange buffer SHALL NOT hold any resource (admission permit, lock, memory-pool reservation) required for its downstream consumer to be admitted or to execute. This guarantees the 2026-05-23 streaming-refactor deadlock class (producer-blocked-while-holding-admission) cannot recur.

#### Scenario: Deadlock-regression guard

- **GIVEN** a producer stage whose exchange buffer is full and a dependent downstream consumer stage awaiting admission
- **WHEN** the producer remains parked on the full buffer
- **THEN** the downstream consumer is still admitted and executes, and a regression test asserts this exact configuration does not deadlock
