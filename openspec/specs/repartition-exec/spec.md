## ADDED Requirements

### Requirement: RepartitionExec operator

The system SHALL provide a `RepartitionExec` operator in `crates/execution/src/repartition.rs` that takes a single child producing `M` input partitions and produces `N` output partitions according to a configured `Partitioning` mode.

```rust
pub struct RepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
}

impl RepartitionExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self;
}
```

`RepartitionExec::output_partitioning()` SHALL return its configured `partitioning`. `RepartitionExec::required_input_partitioning()` SHALL return `vec![UnknownPartitioning(M)]` where `M` is the child's current partition count (no requirement on shape).

#### Scenario: RoundRobin fan-out is non-empty

- **GIVEN** a `RepartitionExec(input, RoundRobinBatch(4))` whose input emits 100 batches
- **WHEN** all 4 output partitions are drained
- **THEN** the union of their batches contains exactly the same rows as the input (set equality)
- **AND** every output partition receives at least one batch when the input has more than 4 batches

#### Scenario: Round-robin is balanced

- **GIVEN** a `RepartitionExec(input, RoundRobinBatch(4))` whose input emits 100 batches of equal size
- **WHEN** all 4 output partitions are drained
- **THEN** each output partition's batch count differs from the others by at most 1

#### Scenario: Hash partitioning routes same-key rows to same partition

- **GIVEN** a `RepartitionExec(input, Hash(vec![col_0], 4))` whose input emits rows with `col_0 ∈ {A, B, C, A, B, C, A, ...}`
- **WHEN** all 4 output partitions are drained
- **THEN** every row with `col_0 = A` resides in the same single output partition (likewise B, C)

#### Scenario: Hash partitioning preserves total row count

- **GIVEN** any input and `Hash(_, n)` partitioning
- **WHEN** all output partitions are drained
- **THEN** the sum of output row counts equals the input row count

### Requirement: Backpressure via bounded mpsc channels

The system SHALL implement `RepartitionExec` using `tokio::sync::mpsc::channel` with a configurable bounded capacity (default 4 batches per channel). When any output partition is slow to consume, the channel SHALL apply bounded back-pressure to the producer **without deadlocking and without holding any cross-task resource while parked**: a producer that parks on a full channel MUST NOT hold an admission permit, a lock, or a memory-pool reservation that its downstream consumer (or any peer task) needs in order to make progress.

The in-memory channel capacity SHALL be configurable via `[execution] channel_capacity` in `arneb.toml` (default 4). The bounded channel is the back-pressure mechanism: a persistently slow consumer parks the producer on the full channel (bounded memory) rather than stalling the whole query, because D1 liveness removed the wall-clock hard-fail that previously turned such a park into a failure.

> **Disk-spill of the channel overflow descoped to the memory-accounting change ("Killer 3").** Spilling a large at-scale intermediate out of the exchange must coordinate with the global `MemoryPool` (spill under pool pressure, not at a fixed per-channel cap); doing it here in isolation would be retrofitted when allocation-level accounting lands. This change ships the bounded (back-pressure) half only.

#### Scenario: Slow consumer back-pressures producer without deadlock

- **GIVEN** a 4-partition `RepartitionExec` where one output stream is consumed slowly
- **WHEN** the producer task fills the channel beyond capacity
- **THEN** the producer yields (does not busy-loop, does not allocate unbounded memory) and parks on the full bounded channel until the consumer frees capacity
- **AND** the producer holds no admission permit / lock / pool reservation while parked, so its downstream consumer is never prevented from being admitted or making progress

#### Scenario: All producers shut down on error

- **GIVEN** a 4-partition `RepartitionExec` where one consumer drops its receiver (panic or cancellation)
- **WHEN** the producer next attempts to send to that channel
- **THEN** the send returns an `Err` and the producer cleanly shuts down all remaining channels

#### Scenario: Producer parked on a full channel cannot deadlock a dependent consumer

- **GIVEN** a producer stage whose output channel is full and a downstream consumer stage awaiting admission/execution
- **WHEN** the producer is parked on the full channel for an extended period
- **THEN** the downstream consumer is still admitted and executes (no resource held by the parked producer gates it), reproducing-and-preventing the 2026-05-23 streaming-refactor deadlock class

### Requirement: RepartitionExec is acyclic

The physical-plan construction SHALL guarantee that `RepartitionExec` instances form a directed acyclic graph (DAG). A debug assertion SHALL fire if a cycle is detected during construction.

#### Scenario: DAG construction

- **WHEN** the physical planner inserts `RepartitionExec` nodes
- **THEN** the resulting plan tree is acyclic by construction (tree-shaped, never a graph)
