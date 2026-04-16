## 1. Decimal128 Expression Support

- [x] 1.1 Implement Decimal128 literal-to-array conversion in `expression.rs` (replace "not yet supported" at line ~193)
- [x] 1.2 Add Decimal128 to `compare_op` in `expression.rs` (use Arrow compute kernels for eq/ne/lt/le/gt/ge)
- [x] 1.3 Add Decimal128 to `arithmetic_op` in `expression.rs` (use Arrow compute for add/sub/mul/div)
- [x] 1.4 Add Decimal128 to numeric type coercion rules in `wider_numeric_type`
- [x] 1.5 Write unit tests for Decimal128 literals, comparisons, and arithmetic

## 2. Decimal128 Aggregate Support

- [x] 2.1 Add Decimal128 to SUM accumulator in `aggregate.rs` (accumulate as i128 with precision/scale)
- [x] 2.2 Add Decimal128 to AVG accumulator in `aggregate.rs`
- [x] 2.3 Add `Decimal128(i128, u8, i8)` variant to `OrdScalar` enum for MIN/MAX
- [x] 2.4 Add Decimal128 to MIN/MAX accumulators
- [x] 2.5 Write unit tests for SUM, AVG, MIN, MAX with Decimal128 values

## 3. Timestamp Expression Support

- [x] 3.1 Implement Timestamp literal-to-array conversion in `expression.rs` (replace "not yet supported" at line ~200)
- [x] 3.2 Add Timestamp to `compare_op` in `expression.rs` (use Arrow compute kernels)
- [x] 3.3 Add `Timestamp(i64, TimeUnit)` variant to `OrdScalar` for MIN/MAX
- [x] 3.4 Add Timestamp to MIN/MAX accumulators in `aggregate.rs`
- [x] 3.5 Write unit tests for Timestamp comparisons and MIN/MAX

## 4. Binary Literal Support

- [x] 4.1 Implement Binary literal-to-array conversion in `expression.rs` (replace "not yet supported" at line ~190)
- [x] 4.2 Write unit test for Binary literal arrays

## 5. Nested Types Schema Support

- [x] 5.1 Add `List(Box<DataType>)` variant to DataType enum in `types.rs`
- [x] 5.2 Add `Map(Box<DataType>, Box<DataType>)` variant to DataType enum
- [x] 5.3 Add `Struct(Vec<(String, DataType)>)` variant to DataType enum
- [x] 5.4 Implement `TryFrom<ArrowDataType>` for List, Map, Struct types
- [x] 5.5 Implement `Into<ArrowDataType>` for List, Map, Struct types
- [x] 5.6 Ensure file connector and Hive data source can scan files containing nested columns (SELECT primitive columns only)
- [x] 5.7 Add clear error message when nested type columns are used in expressions: "nested type operations not yet supported"
- [x] 5.8 Write integration test: scan a Parquet file with List column, SELECT only primitive columns
