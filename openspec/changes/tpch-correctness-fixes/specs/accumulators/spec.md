# Spec: Accumulators (TPC-H Correctness)

## MODIFIED Requirements

### Requirement: Accumulator factory
The system SHALL provide a `create_accumulator(func_name, is_count_star, distinct)` function that returns the appropriate accumulator for the given aggregate function name (case-insensitive). When `distinct` is `true`, the returned accumulator SHALL deduplicate its non-null inputs (by value, type-prefixed) before forwarding them to the wrapped function-specific accumulator. The `distinct` wrapper SHALL be a no-op for `COUNT(*)`, `MIN`, and `MAX` (those functions are insensitive to duplicates). It SHALL return `ExecutionError::InvalidOperation` for unknown function names.

#### Scenario: Creating a SUM accumulator
- **WHEN** `create_accumulator("SUM", false, false)` is called
- **THEN** it returns `Ok(Box<dyn Accumulator>)` that is a `SumAccumulator`

#### Scenario: Unknown aggregate function
- **WHEN** `create_accumulator("MEDIAN", false, false)` is called
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))`

#### Scenario: DISTINCT wraps the base accumulator
- **WHEN** `create_accumulator("COUNT", false, true)` is called
- **THEN** it returns an accumulator that on input `[1, 1, 2, NULL, 3, 2]` evaluates to `ScalarValue::Int64(3)` (three distinct non-null values)

#### Scenario: DISTINCT is a no-op on COUNT(*)
- **WHEN** `create_accumulator("COUNT", true, true)` is called
- **THEN** the returned accumulator behaves identically to `create_accumulator("COUNT", true, false)` — it counts every row regardless of value

## ADDED Requirements

### Requirement: DistinctAccumulator
The system SHALL provide a `DistinctAccumulator` that wraps another `Accumulator` and deduplicates inputs before forwarding them. Deduplication SHALL use a type-prefixed key (so logically distinct types — e.g. `Int32(1)` vs `Int64(1)` vs `Utf8("1")` — never collide) over Arrow scalars supporting at least `Int32`, `Int64`, `Float32`, `Float64`, `Utf8`, `LargeUtf8`, `Boolean`, `Date32`, `Date64`, `Decimal128`, and `Timestamp`. `Float32`/`Float64` keys SHALL use bit-pattern equality (`to_bits()`). NULL values SHALL be skipped — they never count, never reach the wrapped accumulator. Calling `reset()` SHALL clear both the wrapped accumulator and the dedup set.

#### Scenario: COUNT(DISTINCT) drops duplicates and nulls across batches
- **WHEN** a `DistinctAccumulator` wrapping `CountAccumulator` receives batch `[1, 1, 2, NULL, 3]` followed by batch `[2, 4, NULL]`
- **THEN** `evaluate()` returns `ScalarValue::Int64(4)` (distinct non-null set `{1, 2, 3, 4}`)

#### Scenario: COUNT(DISTINCT) resets per group
- **WHEN** a `DistinctAccumulator` is updated with `[1, 1, 2]`, then `reset()`, then updated with `[5, 5]`
- **THEN** the first `evaluate()` returns `Int64(2)`; the post-reset `evaluate()` returns `Int64(1)`

#### Scenario: DISTINCT on unsupported scalar type returns an error
- **WHEN** a `DistinctAccumulator` is updated with an array whose Arrow type is outside the supported set
- **THEN** `update_batch` returns `Err(ExecutionError::InvalidOperation(...))` naming the type, rather than silently accepting duplicates
