## Context

Arneb's DataType enum includes Decimal128 and Timestamp, and the Parquet/Arrow reader can decode these types. However, the execution layer (expression evaluator and aggregate accumulators) has explicit "not yet supported" error paths for these types. This creates a frustrating experience: data loads successfully, but queries fail on operations that should be basic (SUM of a decimal column, comparing timestamps).

This was discovered during the Parquet compatibility audit. The file connector (`crates/connectors/src/file.rs`) and Hive data source (`crates/hive/src/datasource.rs`) can read these types, but `crates/execution/src/expression.rs` (lines 190-201) and `crates/execution/src/aggregate.rs` (lines 96-139, 250-386) reject them.

## Goals / Non-Goals

**Goals:**

- Decimal128: full expression support (literals, comparisons, arithmetic, SUM, AVG, MIN, MAX)
- Timestamp: expression support (literals, comparisons, MIN, MAX)
- Binary: literal array conversion
- Nested types: schema-level recognition (DataType variants) for future extensibility

**Non-Goals:**

- Nested type query operations (SELECT from arrays, MAP access, STRUCT field access)
- Timestamp arithmetic (INTERVAL, DATE_ADD — separate SQL feature changes)
- Custom Decimal precision/scale inference rules
- Timezone conversion logic

## Decisions

### D1: Decimal128 arithmetic via Arrow compute kernels

**Choice**: Use Arrow's `arrow::compute` kernels for Decimal128 arithmetic and comparisons rather than manual implementation. Arrow handles precision/scale propagation.

**Rationale**: Arrow already has correct Decimal128 math. Reimplementing it would be error-prone (overflow, scale alignment). The execution layer should delegate to Arrow compute wherever possible.

### D2: Decimal128 accumulators use i128 internally

**Choice**: SUM/AVG accumulators for Decimal128 accumulate as `i128` with tracked precision/scale. MIN/MAX add a `Decimal128(i128, u8, i8)` variant to `OrdScalar`.

**Rationale**: Matches Arrow's internal representation. Avoids lossy Float64 conversion for monetary values (TPC-H l_extendedprice, l_discount).

### D3: Timestamp comparisons via Arrow compute

**Choice**: Use Arrow's `cmp::lt`, `cmp::gt`, etc. for Timestamp comparisons. Add `Timestamp(i64, TimeUnit)` variant to `OrdScalar` for MIN/MAX.

**Rationale**: Arrow handles timezone-aware and timezone-naive comparisons correctly.

### D4: Nested types are schema-only initially

**Choice**: Add `List(Box<DataType>)`, `Map(Box<DataType>, Box<DataType>)`, `Struct(Vec<(String, DataType)>)` to the DataType enum. Implement `TryFrom<ArrowDataType>` for these types. Do NOT implement expression or aggregate support — just prevent "unsupported Arrow type" errors when scanning files that contain nested columns (they will be readable but not queryable in expressions).

**Rationale**: Many real-world Parquet files contain nested columns alongside primitive ones. Currently, even reading the schema fails. With schema-level support, users can at least SELECT the primitive columns from these files.

## Risks / Trade-offs

**[Decimal precision overflow]** -> SUM of many Decimal128 values may overflow i128. **Mitigation**: Arrow's Decimal128 supports up to 38 digits of precision, sufficient for financial sums.

**[Timestamp timezone handling]** -> Timezone-aware timestamps add complexity. **Mitigation**: Start with timezone-naive comparisons. Timezone-aware behavior follows Arrow's semantics.

**[Nested types scope creep]** -> Schema-only support may confuse users who try to query nested fields. **Mitigation**: Clear error messages: "nested type operations not yet supported".
