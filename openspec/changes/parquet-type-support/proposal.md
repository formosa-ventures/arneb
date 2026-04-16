## Why

Arneb can parse Decimal128 and Timestamp types from Parquet schemas but cannot use them in expressions, comparisons, or aggregate functions. Queries like `SELECT SUM(l_extendedprice * (1 - l_discount))` fail when columns use Decimal type (common in Hive/Trino-produced Parquet). Similarly, Timestamp columns cannot be compared or aggregated. Nested types (List, Map, Struct) are completely unsupported, blocking real-world data lake files.

These gaps affect TPC-H queries when reading from Hive (which preserves Decimal types) and will block most TPC-DS queries.

## What Changes

- Implement Decimal128 support in expressions (literals, comparisons, arithmetic) and aggregates (SUM, AVG, MIN, MAX)
- Implement Timestamp support in expressions (literals, comparisons) and aggregates (MIN, MAX)
- Implement Binary literal array conversion
- Add framework for nested type support (List, Map, Struct) in DataType and schema conversion

## Capabilities

### New Capabilities

- `decimal-expressions`: Decimal128 literals, comparisons, arithmetic, and aggregates
- `timestamp-expressions`: Timestamp literals, comparisons, MIN/MAX, and CAST
- `binary-literals`: Binary type literal array conversion
- `nested-types`: List/Map/Struct type recognition in schema (read-only initially)

### Modified Capabilities

- Expression evaluator (`crates/execution/src/expression.rs`): extended type support
- Aggregate accumulators (`crates/execution/src/aggregate.rs`): extended type support
- Type system (`crates/common/src/types.rs`): nested type variants

## Impact

- **crates/execution/src/expression.rs**: Add Decimal128/Timestamp to literal conversion, compare_op, arithmetic_op
- **crates/execution/src/aggregate.rs**: Add Decimal128 to SUM/AVG, Decimal128/Timestamp to MIN/MAX
- **crates/common/src/types.rs**: Add List/Map/Struct variants (future)
- **No new dependencies**: Uses existing Arrow types
