## MODIFIED Requirements

### Requirement: Inter-stage exchange is bounded

The inter-stage exchange SHALL bound its in-memory buffering (a configurable per-partition channel capacity), so that a slow consumer applies smooth back-pressure to the producer rather than causing unbounded memory growth or a stall. The bound SHALL be the back-pressure mechanism — a producer that outruns its consumer parks on the full channel instead of accumulating without limit.

The exchange's in-memory buffers (the `RepartitionExec` channel and the rpc `OutputBuffer`) SHALL be **registered against the global `MemoryPool`** so their bytes count toward the worker limit. When the **global pool** is under pressure (not at a fixed per-channel cap), the exchange SHALL **spill overflow batches to disk** (Arrow IPC, reusing the Grace spill machinery) and drain them in order, so a large intermediate (SF30/SF100) degrades to disk and the worker stays within its bound rather than OOMing. This delivers the disk-spill that the `exec-exchange-backpressure` change explicitly deferred to memory accounting, now correctly triggered by shared pool pressure.

#### Scenario: Slow consumer back-pressures producer within the bound

- **WHEN** the consumer drains slower than the producer produces and the pool is not under pressure
- **THEN** the producer parks on the full bounded channel (bounded memory, no unbounded growth) and resumes when the consumer frees capacity, with no spill files on this path

#### Scenario: Large intermediate spills under pool pressure instead of OOMing

- **WHEN** an upstream stage produces an intermediate larger than the pool can hold while the consumer drains slowly
- **THEN** the pool-tracked exchange buffers spill overflow to disk under pool pressure and the query completes, with exchange memory staying within the configured bound (no OOM)
