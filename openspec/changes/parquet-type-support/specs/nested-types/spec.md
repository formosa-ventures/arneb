## ADDED Requirements

### Requirement: List type in DataType enum
The DataType enum in `crates/common/src/types.rs` SHALL include a `List(Box<DataType>)` variant. The `TryFrom<ArrowDataType>` implementation SHALL convert Arrow's `List` and `LargeList` types to this variant instead of returning `UnsupportedArrowType`.

#### Scenario: Schema with List column
- **WHEN** a Parquet file has a schema with a List column (e.g., `tags: list<string>`)
- **THEN** the schema is parsed successfully with DataType::List(Box::new(DataType::Utf8))

#### Scenario: SELECT primitive columns from file with List
- **WHEN** a file contains both primitive and List columns
- **AND** the query SELECTs only primitive columns
- **THEN** the query succeeds (List columns are not read due to projection pushdown)

### Requirement: Map type in DataType enum
The DataType enum SHALL include a `Map(Box<DataType>, Box<DataType>)` variant for Arrow Map types.

#### Scenario: Schema with Map column
- **WHEN** a Parquet file has a Map column (e.g., `metadata: map<string, string>`)
- **THEN** the schema is parsed successfully

### Requirement: Struct type in DataType enum
The DataType enum SHALL include a `Struct(Vec<(String, DataType)>)` variant for Arrow Struct types.

#### Scenario: Schema with Struct column
- **WHEN** a Parquet file has a Struct column (e.g., `address: struct<city: string, zip: string>`)
- **THEN** the schema is parsed successfully

### Requirement: Clear error on nested type operations
When a nested type column is used in an expression (WHERE, GROUP BY, aggregate), the system SHALL return a clear error message rather than a panic or generic error.

#### Scenario: WHERE on List column
- **WHEN** `SELECT * FROM t WHERE tags = ...` is executed on a List column
- **THEN** an error is returned: "nested type operations not yet supported"

#### Scenario: GROUP BY on nested column
- **WHEN** `SELECT metadata, COUNT(*) FROM t GROUP BY metadata` is executed on a Map column
- **THEN** an error is returned: "nested type operations not yet supported"
