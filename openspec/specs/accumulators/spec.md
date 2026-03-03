## ADDED Requirements

### Requirement: Accumulator trait
The system SHALL define an `Accumulator` trait with methods `update_batch(&mut self, values: &ArrayRef)`, `evaluate(&self) -> ScalarValue`, and `reset(&mut self)`. The trait SHALL require `Send + Sync` bounds.

#### Scenario: Accumulator lifecycle
- **WHEN** an accumulator is created, updated with batches, evaluated, reset, and evaluated again
- **THEN** the first evaluation returns the aggregate result; after reset, the second evaluation returns the initial state

### Requirement: CountAccumulator
The system SHALL implement a `CountAccumulator` that supports two modes: `COUNT(expr)` counting non-null values, and `COUNT(*)` counting all rows.

#### Scenario: COUNT non-null values
- **WHEN** a CountAccumulator (non-star mode) receives `[1, NULL, 3]`
- **THEN** `evaluate()` returns `ScalarValue::Int64(2)`

#### Scenario: COUNT star
- **WHEN** a CountAccumulator (star mode) receives `[1, NULL, 3]`
- **THEN** `evaluate()` returns `ScalarValue::Int64(3)`

### Requirement: SumAccumulator
The system SHALL implement a `SumAccumulator` that sums numeric values (Int32, Int64, Float32, Float64). It SHALL return Int64 for integer inputs and Float64 for floating-point inputs. It SHALL return `ScalarValue::Null` when no values have been accumulated.

#### Scenario: SUM of integers
- **WHEN** a SumAccumulator receives `[10, 20, 30]` (Int64)
- **THEN** `evaluate()` returns `ScalarValue::Int64(60)`

#### Scenario: SUM of empty input
- **WHEN** a SumAccumulator receives no values
- **THEN** `evaluate()` returns `ScalarValue::Null`

### Requirement: AvgAccumulator
The system SHALL implement an `AvgAccumulator` that computes the arithmetic mean, always returning `ScalarValue::Float64`. It SHALL skip null values and return `ScalarValue::Null` for empty input.

#### Scenario: AVG of integers
- **WHEN** an AvgAccumulator receives `[10, 20, 30]` (Int64)
- **THEN** `evaluate()` returns `ScalarValue::Float64(20.0)`

### Requirement: MinAccumulator and MaxAccumulator
The system SHALL implement `MinAccumulator` and `MaxAccumulator` that track the minimum/maximum value respectively. They SHALL support Int32, Int64, Float32, Float64, and Utf8 types. They SHALL skip null values and return `ScalarValue::Null` for empty input.

#### Scenario: MIN of integers
- **WHEN** a MinAccumulator receives `[3, 1, 2]`
- **THEN** `evaluate()` returns `ScalarValue::Int32(1)`

#### Scenario: MAX of strings
- **WHEN** a MaxAccumulator receives `["banana", "apple", "cherry"]`
- **THEN** `evaluate()` returns `ScalarValue::Utf8("cherry")`

### Requirement: Accumulator factory
The system SHALL provide a `create_accumulator(func_name, is_count_star)` function that returns the appropriate accumulator for the given aggregate function name (case-insensitive). It SHALL return `ExecutionError::InvalidOperation` for unknown function names.

#### Scenario: Creating a SUM accumulator
- **WHEN** `create_accumulator("SUM", false)` is called
- **THEN** it returns `Ok(Box<dyn Accumulator>)` that is a `SumAccumulator`

#### Scenario: Unknown aggregate function
- **WHEN** `create_accumulator("MEDIAN", false)` is called
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))`
