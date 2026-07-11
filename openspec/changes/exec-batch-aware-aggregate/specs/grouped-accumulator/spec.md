## ADDED Requirements

### Requirement: GroupedAccumulator trait

The system SHALL define a `GroupedAccumulator` trait in
`crates/execution/src/aggregate.rs` with the following shape:

```rust
pub trait GroupedAccumulator: Send + Sync {
    fn ensure_capacity(&mut self, num_groups: usize);
    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError>;
    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError>;
    fn num_groups(&self) -> usize;
    fn merge_from(&mut self, other: &dyn GroupedAccumulator, group_remap: &[u32]) -> Result<(), ExecutionError>;
    fn as_any(&self) -> &dyn std::any::Any;
}
```

The trait SHALL coexist with the existing `Accumulator` trait. The
existing trait remains in use by the Window operator and by
`DistinctAccumulator`.

#### Scenario: ensure_capacity grows internal Vec to at least N

- **GIVEN** a `GroupedSumAccumulator` constructed with default capacity 0
- **WHEN** `ensure_capacity(5)` is called
- **THEN** the accumulator's internal sum Vec has length ≥ 5 and `evaluate(4)` returns `ScalarValue::Null`

#### Scenario: add_input updates one row per group_id

- **GIVEN** a `GroupedSumAccumulator` over `Int64`, capacity 2, with `group_ids = [0, 1, 0, 1]` and `values = [10, 20, 30, 40]`
- **WHEN** `add_input` is called
- **THEN** `evaluate(0)` returns `ScalarValue::Int64(40)` and `evaluate(1)` returns `ScalarValue::Int64(60)`

#### Scenario: add_input skips nulls per Arrow null bitmap

- **GIVEN** a `GroupedSumAccumulator` over `Int64` with `group_ids = [0, 0, 1]` and `values = [10, NULL, 20]`
- **WHEN** `add_input` is called
- **THEN** `evaluate(0)` returns `ScalarValue::Int64(10)` and `evaluate(1)` returns `ScalarValue::Int64(20)`

#### Scenario: evaluate on an unused group returns Null

- **GIVEN** a `GroupedSumAccumulator` with capacity 3 and no `add_input` calls
- **WHEN** `evaluate(2)` is called
- **THEN** the result is `ScalarValue::Null`

### Requirement: COUNT GroupedAccumulator

The system SHALL provide `GroupedCountAccumulator` with two modes:

- `count_star = true`: every row in `group_ids` increments
  `state[group_ids[i]]`.
- `count_star = false`: only non-null rows in `values` increment.

The result type SHALL be `ScalarValue::Int64`.

#### Scenario: COUNT(*) counts every row including those over null values

- **GIVEN** group_ids `[0, 0, 1]` and values `[10, NULL, 30]` over `count_star = true`
- **WHEN** `add_input` then `evaluate(0)` and `evaluate(1)`
- **THEN** `evaluate(0) == ScalarValue::Int64(2)` and `evaluate(1) == ScalarValue::Int64(1)`

#### Scenario: COUNT(col) skips nulls

- **GIVEN** the same input with `count_star = false`
- **WHEN** `add_input` then `evaluate(0)` and `evaluate(1)`
- **THEN** `evaluate(0) == ScalarValue::Int64(1)` (only the non-null `10`) and `evaluate(1) == ScalarValue::Int64(1)`

### Requirement: SUM GroupedAccumulator

The system SHALL provide `GroupedSumAccumulator` covering `Int32`,
`Int64`, `Float32`, `Float64`, `Decimal128(p, s)`. The result type
SHALL match the existing single-instance `SumAccumulator`: `Int64`
for integer inputs, `Float64` for float, `Decimal128(38, s)` for
decimal.

#### Scenario: SUM widens decimal precision to 38

- **GIVEN** group_ids `[0, 0, 1]` and a `Decimal128(10, 2)` array `[1000, 2000, 3000]`
- **WHEN** `add_input` then `evaluate(0)` and `evaluate(1)`
- **THEN** `evaluate(0) == ScalarValue::Decimal128 { value: 3000, precision: 38, scale: 2 }` and `evaluate(1) == ScalarValue::Decimal128 { value: 3000, precision: 38, scale: 2 }`

### Requirement: AVG GroupedAccumulator

The system SHALL provide `GroupedAvgAccumulator` that tracks
`(sum_f64, count_i64)` per group. The result type is `Float64`.
`evaluate(g)` SHALL return `ScalarValue::Null` if `count == 0`.

#### Scenario: AVG over two groups returns per-group mean

- **GIVEN** group_ids `[0, 0, 1, 1]` and `Int64` values `[10, 20, 30, 50]`
- **WHEN** `add_input` then `evaluate(0)` and `evaluate(1)`
- **THEN** `evaluate(0) == ScalarValue::Float64(15.0)` and `evaluate(1) == ScalarValue::Float64(40.0)`

#### Scenario: AVG on an empty group returns Null

- **GIVEN** a `GroupedAvgAccumulator` with `ensure_capacity(2)` and no `add_input` calls touching group 1
- **WHEN** `evaluate(1)` is called
- **THEN** the result is `ScalarValue::Null`

### Requirement: MIN / MAX GroupedAccumulator

The system SHALL provide `GroupedMinAccumulator` and
`GroupedMaxAccumulator` covering the same types as the existing
single-instance Min/Max (`Int32/64`, `Float32/64`, `Utf8`, `Date32`,
`Decimal128`, `Timestamp`).

#### Scenario: MIN over two groups picks per-group minimum

- **GIVEN** group_ids `[0, 0, 1, 1]` and `Int32` values `[3, 1, 5, 2]`
- **WHEN** `add_input` then `evaluate(0)` and `evaluate(1)`
- **THEN** `evaluate(0) == ScalarValue::Int32(1)` and `evaluate(1) == ScalarValue::Int32(2)`

#### Scenario: MAX over Utf8 picks per-group lexicographic max

- **GIVEN** group_ids `[0, 0, 1]` and `Utf8` values `["banana", "apple", "cherry"]`
- **WHEN** `add_input` then `evaluate(0)` and `evaluate(1)`
- **THEN** `evaluate(0) == ScalarValue::Utf8("banana".into())` and `evaluate(1) == ScalarValue::Utf8("cherry".into())`

### Requirement: merge_from for parallel partial merge

The system SHALL provide `merge_from(&mut self, other, group_remap)`
that, for each `g` in `0..other.num_groups()`, applies
`self.state[group_remap[g]] ⊕= other.state[g]` where `⊕` is the
aggregate's combine operation.

#### Scenario: merge_from with disjoint partial group sets

- **GIVEN** partial A has groups `{0,1}` and partial B has groups `{0,1}` representing the same logical keys but with different partial-local IDs
- **WHEN** the outer task remaps partial B's IDs through `group_remap = [1, 0]` (B's group 0 is A's group 1, B's group 1 is A's group 0)
- **THEN** after `final.merge_from(A, [0,1])` and `final.merge_from(B, [1,0])`, `evaluate(g)` returns the union sum for each global g

#### Scenario: merge_from rejects type mismatch

- **GIVEN** `final: GroupedSumAccumulator` and `other: GroupedCountAccumulator`
- **WHEN** `merge_from` is called
- **THEN** it returns `Err(ExecutionError::InvalidOperation(_))`
