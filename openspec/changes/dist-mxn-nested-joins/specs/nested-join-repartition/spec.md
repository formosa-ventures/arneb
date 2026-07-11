## ADDED Requirements

### Requirement: Nested joins preserve results at any fan-out

Distributed execution MUST produce results cell-identical (within the engine's
float tolerance) for a nested multi-way join at any hash fan-out `N` of two or
more, compared to the `N = 2` result and to the reference engine. No input row
may be dropped or duplicated as it flows from one join level to the next.

#### Scenario: Nested join cell-parity across fan-out

- **GIVEN** a multi-way join (e.g. TPC-H q09, a 6-way join)
- **WHEN** it is executed at fan-out `N = 2` and again at an `N > 2`
- **THEN** the two result sets SHALL be cell-identical (sorted, within 1e-9 float tolerance)

#### Scenario: Nested join matches the reference engine at N > 2

- **GIVEN** a nested multi-way join run at `N > 2`
- **WHEN** its output is compared cell-by-cell to Trino over the same data
- **THEN** every cell SHALL match within 1e-9, with the same row count (no undercount, no duplication)

#### Scenario: Single join unaffected

- **WHEN** a single two-way hash join is executed at any `N ≥ 2`
- **THEN** its result SHALL remain cell-identical to the reference engine (single joins already colocate correctly; this change must not regress them)

### Requirement: Intermediate join output is re-partitioned on the next join's keys

The fragmenter SHALL set an intermediate join fragment's `output_partitioning`
hash columns to the downstream join's equi-keys (a non-empty column set) when
that join's result feeds a downstream join requiring partitioning on those keys,
so the fragment re-hashes its output onto those keys rather than emitting a
single un-partitioned stream.

#### Scenario: Non-empty output columns on a feeding intermediate join

- **GIVEN** an intermediate join fragment whose result feeds another join on keys `K`
- **WHEN** the fragmenter assigns the fragment's `output_partitioning`
- **THEN** the hash columns SHALL be the downstream join's equi-keys `K` (non-empty), placing the fragment on the M×N producer path rather than the "α consumer only" path

#### Scenario: Top-level / non-feeding join unchanged

- **WHEN** a join fragment's output is gathered (it feeds no key-partitioned downstream join)
- **THEN** its `output_partitioning` MAY remain un-keyed (single-stream gather), unchanged from today

### Requirement: Per-partition pull is gated on a real M×N producer

A consumer task SHALL pull a specific upstream partition (`partition_id = k`)
only when its upstream fragment actually performed an M×N fan-out (its
`output_partitioning` carries non-empty hash columns). Otherwise the consumer
SHALL fall back to the safe single-partition pull (`partition_id = 0`). This
gate is what prevents an α-producer feeding an α-consumer without a real M×N
exchange — the failure mode that previously broke multiple queries.

#### Scenario: Per-partition pull only behind an M×N producer

- **GIVEN** a consumer whose upstream fragment has non-empty hash output columns (a real M×N producer)
- **WHEN** the coordinator builds the consumer's source exchanges
- **THEN** consumer task `k` SHALL pull `partition_id = k` from every upstream task

#### Scenario: Safe fallback when upstream did not fan out

- **GIVEN** a consumer whose upstream fragment has empty hash output columns (single-stream producer)
- **WHEN** the coordinator builds the consumer's source exchanges
- **THEN** the consumer SHALL pull `partition_id = 0` (no per-partition pull), preserving correct behaviour

### Requirement: N = 2 distributed results do not regress

Activating the M×N producer path for intermediate joins MUST NOT change any
result that is correct today at the historical fixed fan-out of two. The
existing single-scale-factor cell-parity suite SHALL remain green.

#### Scenario: Existing suite stays green at N = 2

- **WHEN** the full TPC-H cell-parity suite runs at `N = 2` after this change
- **THEN** every query SHALL match the reference engine exactly as before (no regression)
