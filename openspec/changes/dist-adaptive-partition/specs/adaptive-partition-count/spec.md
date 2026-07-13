## ADDED Requirements

### Requirement: Adaptive hash-partition count

The fragmenter SHALL choose the number of hash partitions for a repartition
exchange adaptively rather than using a fixed constant. The chosen count SHALL
be a function of at least the cluster worker count and an estimated input
cardinality, such that larger intermediates and larger clusters yield more
partitions (smaller per-partition working sets).

#### Scenario: More workers yield at least as many partitions

- **GIVEN** a repartition exchange whose estimated input cardinality is held constant
- **WHEN** the fragmenter runs against a cluster of `W` workers
- **THEN** the chosen partition count SHALL be at least `min(W, configured_max)` and SHALL be monotonically non-decreasing in `W`

#### Scenario: Larger cardinality yields at least as many partitions

- **GIVEN** a fixed worker count
- **WHEN** the estimated input cardinality of the repartition's child increases
- **THEN** the chosen partition count SHALL be monotonically non-decreasing in the estimated cardinality, up to the configured maximum

#### Scenario: Replaces the previous fixed count

- **WHEN** a hash-repartition exchange is inserted by the fragmenter
- **THEN** the partition count SHALL NOT be the hard-coded literal `2`; it SHALL be the value returned by the adaptive rule

### Requirement: Partition count is bounded and at least two

The adaptive partition count SHALL always be at least `2` and SHALL never
exceed the configured maximum. A degenerate estimate (zero/unknown cardinality)
SHALL fall back to a deterministic default count, never to zero or one.

#### Scenario: Floor of two

- **WHEN** the adaptive rule would compute a value below `2` (e.g. a single worker and a tiny estimate)
- **THEN** the chosen partition count SHALL be exactly `2`

#### Scenario: Capped at the configured maximum

- **GIVEN** a configured maximum partition count `M`
- **WHEN** the adaptive rule would compute a value above `M`
- **THEN** the chosen partition count SHALL be exactly `M`

#### Scenario: Unknown cardinality falls back deterministically

- **GIVEN** the child's estimated cardinality is unavailable
- **WHEN** the fragmenter chooses a partition count
- **THEN** it SHALL use a deterministic default (a function of worker count only) and SHALL log the effective value

### Requirement: Runtime-configurable with a defaulted knob

The partition-count policy SHALL be tunable at runtime via an `ARNEB_*`
environment variable per the build-time-vs-runtime convention, with a sensible
in-source default. The effective resolved value SHALL be logged at startup or
at plan time so an operator can confirm what was applied.

#### Scenario: Environment override takes effect

- **GIVEN** the policy knob is set via its `ARNEB_*` environment variable to a value different from the default
- **WHEN** the server resolves the policy
- **THEN** the resolved value SHALL be the environment value, and the effective value SHALL be logged

#### Scenario: Default applies when unset

- **WHEN** the `ARNEB_*` knob is unset
- **THEN** the in-source default SHALL apply and the resolved value SHALL be logged

### Requirement: N-way fan-out preserves results

Distributed execution SHALL produce results cell-identical (within the engine's
float tolerance) for any adaptive partition count `N` of two or more, compared
to the previous fixed count of two. The deterministic hash assignment and the
M×N exchange fan-out MUST route every input row to exactly one consumer
partition, with no rows dropped or duplicated.

#### Scenario: Cell-parity across partition counts

- **GIVEN** a query that inserts a hash-repartition exchange
- **WHEN** it is executed at partition count `2` and again at an `N > 2`
- **THEN** the two result sets SHALL be cell-identical (sorted, within 1e-9 float tolerance)

#### Scenario: No row loss or duplication under N-way fan-out

- **WHEN** the repartition routes its input across `N` consumer partitions
- **THEN** the union of all consumer partitions SHALL contain exactly the repartition's input rows (no drops, no duplicates)
