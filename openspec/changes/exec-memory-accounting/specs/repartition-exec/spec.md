## MODIFIED Requirements

### Requirement: Backpressure via bounded mpsc channels

The system SHALL implement `RepartitionExec` using `tokio::sync::mpsc::channel` with a configurable bounded capacity (default 4 batches per channel). When any output partition is slow to consume, the channel SHALL apply bounded back-pressure to the producer **without deadlocking and without holding any cross-task resource while parked**: a producer that parks on a full channel MUST NOT hold an admission permit, a lock, or a memory-pool reservation that its downstream consumer (or any peer task) needs in order to make progress.

The channel's buffered batches SHALL be **registered against the global `MemoryPool`** (via a `MemoryReservation`: `try_grow` on enqueue, release on dequeue/drop), so the exchange's in-flight bytes are visible to the pool. When the **global pool** — not a fixed per-channel cap — is under pressure, the producer SHALL **spill overflow batches to disk** (Arrow IPC, reusing the existing Grace spill machinery) and drain them in FIFO order, releasing the reservation on spill and re-reserving on read-back, so a slow consumer or large intermediate degrades to *slow/spill* and never OOMs the process.

The in-memory channel capacity SHALL be configurable via `[execution] channel_capacity` in `arneb.toml` (default 4).

#### Scenario: Channel bytes are pool-tracked

- **GIVEN** a `RepartitionExec` whose channels hold buffered batches
- **WHEN** the worker's memory is measured
- **THEN** the buffered channel bytes are counted in the global pool's reserved total (not invisible to it)

#### Scenario: Slow consumer back-pressures producer, spills under pool pressure

- **GIVEN** a 4-partition `RepartitionExec` where one output stream is consumed slowly
- **WHEN** the producer fills the channel and the global pool is under pressure
- **THEN** the producer parks on the bounded channel and overflow batches spill to disk under pool pressure (released from the reservation), draining in FIFO order
- **AND** the producer holds no admission permit / lock / pool reservation while parked, so its downstream consumer is never prevented from being admitted or making progress

#### Scenario: All producers shut down on error

- **GIVEN** a 4-partition `RepartitionExec` where one consumer drops its receiver (panic or cancellation)
- **WHEN** the producer next attempts to send to that channel
- **THEN** the send returns an `Err` and the producer cleanly shuts down all remaining channels and releases any spill files and reservations
