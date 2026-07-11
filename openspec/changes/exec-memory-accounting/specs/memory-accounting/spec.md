## ADDED Requirements

### Requirement: Large worker allocations are accounted in a single global pool

Every large Arrow allocation on a worker SHALL be registered against a single process-global `MemoryPool` via a `MemoryReservation`, so that the worker's tracked memory reflects its true peak. The accounted sites SHALL include, at minimum: the inter-stage exchange channel (`RepartitionExec`), the rpc `OutputBuffer`, Parquet scan decode buffers, and intermediate operator output batches (`FilterExec`/`ProjectionExec`/`RepartitionExec`) — in addition to the already-tracked join/aggregate build state. A consumer SHALL reserve before (or at) allocation and SHALL release (via `shrink`/`free`, including RAII `Drop`) when the memory is no longer held.

The fast path (pool not under pressure) SHALL add no spill and no measurable per-row overhead — accounting SHALL be per-batch (one reservation adjustment per batch), not per-row.

#### Scenario: Tracked memory approximates true RSS at scale

- **GIVEN** a worker running a heavy distributed query (e.g. SF30 q09) whose dominant memory is exchange/intermediate buffers
- **WHEN** the worker reaches its memory peak
- **THEN** the pool's reported reserved bytes account for the exchange channels, OutputBuffer, scan decode buffers, and intermediate batches (not only the join/aggregate build), so tracked bytes approximate the worker's true peak rather than ~3 % of it

#### Scenario: Fast path is reservation-only

- **GIVEN** a query whose working set fits under the pool limit (no pressure)
- **WHEN** it runs to completion
- **THEN** no consumer spills, and the per-batch reservation adds no measurable overhead versus the untracked baseline

### Requirement: Spillable consumers degrade to disk under pool pressure

When a `try_grow` would exceed the global pool limit, a spillable consumer (exchange-channel overflow, join build, aggregate state) SHALL spill to disk (Arrow IPC, reusing the Grace spill machinery) and release its reservation, rather than allowing the process to OOM. The spill trigger SHALL be **global pool pressure**, not a fixed per-consumer cap, so the pool can balance which consumer spills. q21 at SF30 SHALL complete on a constrained host within the configured bound.

#### Scenario: q21 SF30 completes instead of OOMing

- **GIVEN** a constrained 8-core / 31 GB host with SF30 data and a pool limit below the untracked peak
- **WHEN** q21 runs (its intermediates exceed the limit)
- **THEN** spillable consumers spill to disk under pool pressure and the query completes with correct results, with worker tracked memory staying within the configured bound (no OOM, no host-death thrash)

#### Scenario: Spilling holds no cross-task resource

- **GIVEN** a consumer spilling under pool pressure
- **WHEN** it parks or performs spill I/O
- **THEN** it holds no admission permit, lock, or pool reservation that its downstream consumer needs to make progress (reusing the exchange-backpressure deadlock invariant)

### Requirement: OOM names the largest consumers

When a `try_grow` ultimately fails (spill cannot free enough), the resulting `ResourceExhausted` error SHALL identify the top memory consumers by reserved bytes, so the dominant allocation site is attributable from the error alone.

#### Scenario: Exhaustion error is attributable

- **WHEN** the pool cannot satisfy a reservation even after spilling
- **THEN** the error reports the largest consumers (by name and reserved bytes), rather than a bare "resources exhausted"
