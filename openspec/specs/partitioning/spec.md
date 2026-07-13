## ADDED Requirements

### Requirement: Partitioning enum

The system SHALL provide a `Partitioning` enum in `crates/execution/src/lib.rs` (or `partitioning.rs`) describing how a physical operator's output rows are distributed across N independent streams.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Partitioning {
    /// `n` partitions, no order or content guarantee within or across them.
    UnknownPartitioning(usize),
    /// `n` partitions filled round-robin from upstream batches.
    /// Useful as a load-balancer.
    RoundRobinBatch(usize),
    /// `n` partitions where rows with the same hash of the given
    /// expressions land in the same partition. Required for shuffle
    /// joins and partitioned aggregates.
    Hash(Vec<PlanExpr>, usize),
}
```

Every `Partitioning` variant SHALL carry a partition count `n >= 1`. Constructing `UnknownPartitioning(0)` or `RoundRobinBatch(0)` or `Hash(_, 0)` is a programmer error and SHALL be rejected at construction time via a debug assert.

#### Scenario: Partition count is positive

- **WHEN** a `Partitioning` variant is constructed with `n = 0`
- **THEN** a debug assertion fires in debug builds; release builds clamp to 1

#### Scenario: Hash partitioning carries key expressions

- **GIVEN** a hash partitioning over `(o_orderkey, o_custkey)` into 14 partitions
- **WHEN** `Partitioning::Hash(vec![PlanExpr::Column(0), PlanExpr::Column(1)], 14)` is constructed
- **THEN** it equals (via `PartialEq`) the same variant constructed elsewhere with the same arguments

### Requirement: Partitioning compatibility check

The system SHALL provide a helper `Partitioning::satisfies(&self, required: &Partitioning) -> bool` returning `true` when the current partitioning satisfies the requirement.

The compatibility rules:
- `RoundRobinBatch(n)` satisfies `UnknownPartitioning(n)`.
- `Hash(exprs, n)` satisfies `UnknownPartitioning(n)`.
- `Hash(exprs_a, n)` satisfies `Hash(exprs_b, n)` iff `exprs_a == exprs_b`.
- Any partitioning satisfies a requirement with matching partition count if the requirement is `UnknownPartitioning`.
- A partitioning does NOT satisfy a requirement with a different partition count.

#### Scenario: RoundRobin satisfies Unknown

- **WHEN** `Partitioning::RoundRobinBatch(14).satisfies(&Partitioning::UnknownPartitioning(14))` is called
- **THEN** the result is `true`

#### Scenario: Hash with mismatched keys does not satisfy

- **WHEN** `Partitioning::Hash(vec![ColA], 14).satisfies(&Partitioning::Hash(vec![ColB], 14))` is called
- **THEN** the result is `false`

#### Scenario: Mismatched partition count fails

- **WHEN** `Partitioning::RoundRobinBatch(4).satisfies(&Partitioning::UnknownPartitioning(14))` is called
- **THEN** the result is `false`

### Requirement: Helper accessors

The system SHALL provide helpers `Partitioning::partition_count(&self) -> usize` returning the partition count from any variant.

#### Scenario: Partition count accessor

- **WHEN** `Partitioning::Hash(_, 14).partition_count()` is called
- **THEN** the result is `14`
