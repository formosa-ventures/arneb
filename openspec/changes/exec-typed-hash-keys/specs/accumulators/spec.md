# Spec: Accumulators (typed hash keys)

## MODIFIED Requirements

### Requirement: DistinctAccumulator
The system SHALL provide a `DistinctAccumulator` that wraps another `Accumulator` and deduplicates inputs before forwarding them. Deduplication SHALL use a typed `GroupKey` (single-element wrapper around `ScalarValue`) such that distinct types — e.g. `Int32(1)`, `Int64(1)`, `Utf8("1")` — never collide. `Float32` and `Float64` values SHALL hash and compare equal via their raw IEEE 754 bit pattern (`to_bits()`); two NaN inputs with the same bit pattern SHALL collapse into a single distinct value, two NaN inputs with different bit patterns SHALL remain distinct. NULL values SHALL be skipped — they never count, never reach the wrapped accumulator. Calling `reset()` SHALL clear both the wrapped accumulator and the dedup set. The accumulator SHALL accept Arrow scalars of at least `Int32`, `Int64`, `Float32`, `Float64`, `Utf8`, `LargeUtf8`, `Boolean`, `Date32`, `Date64`, `Decimal128`, and `Timestamp`; unsupported types SHALL return `ExecutionError::InvalidOperation`.

#### Scenario: COUNT(DISTINCT) drops duplicates and nulls across batches
- **WHEN** a `DistinctAccumulator` wrapping `CountAccumulator` receives batch `[1, 1, 2, NULL, 3]` followed by batch `[2, 4, NULL]`
- **THEN** `evaluate()` returns `ScalarValue::Int64(4)` (distinct non-null set `{1, 2, 3, 4}`)

#### Scenario: COUNT(DISTINCT) resets per group
- **WHEN** a `DistinctAccumulator` is updated with `[1, 1, 2]`, then `reset()`, then updated with `[5, 5]`
- **THEN** the first `evaluate()` returns `Int64(2)`; the post-reset `evaluate()` returns `Int64(1)`

#### Scenario: DISTINCT on unsupported scalar type returns an error
- **WHEN** a `DistinctAccumulator` is updated with an array whose Arrow type is outside the supported set
- **THEN** `update_batch` returns `Err(ExecutionError::InvalidOperation(...))` naming the type, rather than silently accepting duplicates

#### Scenario: NaN with identical bit pattern collapses
- **WHEN** a `DistinctAccumulator` over `Float64` is updated with the same NaN value (constructed via `f64::NAN` or `f64::from_bits(0x7ff8_0000_0000_0000)`) twice
- **THEN** the dedup set contains exactly one entry; subsequent COUNT(DISTINCT) reports the NaN as one distinct value

#### Scenario: NaN with different bit pattern stays distinct
- **WHEN** a `DistinctAccumulator` over `Float64` is updated with two NaN values whose bit patterns differ (e.g. `f64::from_bits(0x7ff8_0000_0000_0000)` and `f64::from_bits(0x7ff8_0000_0000_0001)`)
- **THEN** the dedup set contains two entries; COUNT(DISTINCT) reports two distinct NaN values
