## Overview

Provide a public API for parsing SQL strings into the trino-alt AST. Internally uses `sqlparser-rs` for grammar parsing, then converts to the trino-alt-specific AST representation through a conversion layer.

## Requirements

### R1: Top-level parse function

Provide a `parse(sql: &str) -> Result<Statement, ParseError>` function:
- Accepts any SQL string
- Returns the parsed trino-alt `Statement`
- Returns `ParseError::InvalidSyntax` for syntax errors
- Returns `ParseError::UnsupportedFeature` for unsupported syntax

**Scenarios:**
- `parse("SELECT 1")` → `Ok(Statement::Query(...))`
- `parse("SELCT 1")` → `Err(ParseError::InvalidSyntax(...))`
- `parse("CREATE TABLE t (a INT)")` → `Err(ParseError::UnsupportedFeature(...))`
- `parse("")` → `Err(ParseError::InvalidSyntax(...))`

### R2: sqlparser-rs AST conversion

Implement conversion from `sqlparser::ast` to the trino-alt AST:
- Use `GenericDialect` or a custom dialect
- Correctly convert all AST nodes defined in R1-R5 of the sql-ast spec
- Return `ParseError::UnsupportedFeature` for unsupported sqlparser AST nodes

The conversion must be a total function — every sqlparser output has a defined handling path (either successful conversion or an unsupported error).

**Scenarios:**
- `sqlparser::ast::Statement::Query` → Correctly converts to trino-alt `Statement::Query`
- `sqlparser::ast::Statement::CreateTable` → `Err(ParseError::UnsupportedFeature("CREATE TABLE"))`
- `sqlparser::ast::Expr::Nested(inner)` → Unwraps the nesting and returns the inner expr directly

### R3: Literal conversion

Correctly convert `sqlparser::ast::Value` to `ScalarValue`:
- Numeric literals → Determine `Int64` or `Float64` based on presence of decimal point
- String literals → `ScalarValue::Utf8`
- Boolean literals → `ScalarValue::Boolean`
- NULL → `ScalarValue::Null`
- Unsupported literal types → `ParseError::UnsupportedFeature`

**Scenarios:**
- `42` → `ScalarValue::Int64(42)`
- `3.14` → `ScalarValue::Float64(3.14)`
- `'hello'` → `ScalarValue::Utf8("hello")`
- `TRUE` → `ScalarValue::Boolean(true)`
- `NULL` → `ScalarValue::Null`

### R4: DataType conversion

Convert `sqlparser::ast::DataType` to `trino-common::DataType`:
- `INT`/`INTEGER` → `DataType::Int32`
- `BIGINT` → `DataType::Int64`
- `SMALLINT` → `DataType::Int16`
- `TINYINT` → `DataType::Int8`
- `FLOAT`/`REAL` → `DataType::Float32`
- `DOUBLE` → `DataType::Float64`
- `BOOLEAN` → `DataType::Boolean`
- `VARCHAR`/`TEXT`/`STRING` → `DataType::Utf8`
- `DECIMAL(p,s)` → `DataType::Decimal128 { precision, scale }`
- `DATE` → `DataType::Date32`
- `TIMESTAMP` → `DataType::Timestamp { unit: Microsecond, timezone: None }`
- Unsupported types → `ParseError::UnsupportedFeature`

**Scenarios:**
- `CAST(x AS INTEGER)` → DataType::Int32
- `CAST(x AS DECIMAL(10,2))` → DataType::Decimal128 { precision: 10, scale: 2 }
- `CAST(x AS ARRAY<INT>)` → ParseError::UnsupportedFeature
