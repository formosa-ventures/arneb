## ADDED Requirements

### Requirement: SQL type system (DataType enum)
The system SHALL define a `DataType` enum representing SQL data types. MVP types SHALL include: `Boolean`, `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64`, `Decimal128 { precision: u8, scale: i8 }`, `Utf8`, `LargeUtf8`, `Binary`, `Date32`, `Timestamp { unit: TimeUnit, timezone: Option<String> }`, and `Null`.

#### Scenario: Representing a SQL DECIMAL column
- **WHEN** a table has a column defined as `DECIMAL(10, 2)`
- **THEN** it is represented as `DataType::Decimal128 { precision: 10, scale: 2 }`

#### Scenario: Representing a SQL TIMESTAMP WITH TIME ZONE
- **WHEN** a column is `TIMESTAMP WITH TIME ZONE`
- **THEN** it is represented as `DataType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".to_string()) }`

### Requirement: Arrow type bidirectional conversion
The system SHALL implement `From<DataType> for arrow::datatypes::DataType` and `TryFrom<arrow::datatypes::DataType> for DataType` to convert between SQL types and Arrow types.

#### Scenario: SQL to Arrow conversion
- **WHEN** `DataType::Int64` is converted to Arrow
- **THEN** it produces `arrow::datatypes::DataType::Int64`

#### Scenario: SQL Decimal to Arrow conversion
- **WHEN** `DataType::Decimal128 { precision: 10, scale: 2 }` is converted to Arrow
- **THEN** it produces `arrow::datatypes::DataType::Decimal128(10, 2)`

#### Scenario: Unsupported Arrow type conversion
- **WHEN** an Arrow type with no SQL equivalent (e.g., `arrow::datatypes::DataType::Union`) is converted
- **THEN** a `TryFrom` error is returned indicating the type is unsupported

### Requirement: TableReference with multi-part naming
The system SHALL define a `TableReference` struct supporting one-part (`table`), two-part (`schema.table`), and three-part (`catalog.schema.table`) naming. Fields: `catalog: Option<String>`, `schema: Option<String>`, `table: String`.

#### Scenario: Parsing a fully qualified name
- **WHEN** `TableReference::parse("my_catalog.my_schema.my_table")` is called
- **THEN** it returns `TableReference { catalog: Some("my_catalog"), schema: Some("my_schema"), table: "my_table" }`

#### Scenario: Parsing a simple table name
- **WHEN** `TableReference::parse("users")` is called
- **THEN** it returns `TableReference { catalog: None, schema: None, table: "users" }`

#### Scenario: Display formatting
- **WHEN** a `TableReference { catalog: Some("c"), schema: Some("s"), table: "t" }` is formatted
- **THEN** it produces the string `"c.s.t"`

### Requirement: ColumnInfo metadata
The system SHALL define a `ColumnInfo` struct with fields: `name: String`, `data_type: DataType`, `nullable: bool`.

#### Scenario: Creating a non-nullable integer column
- **WHEN** a column info is created for a `NOT NULL INT` column named "id"
- **THEN** it is `ColumnInfo { name: "id".to_string(), data_type: DataType::Int32, nullable: false }`

#### Scenario: Converting to Arrow Field
- **WHEN** a `ColumnInfo` is converted to `arrow::datatypes::Field`
- **THEN** the name, data type (via DataType conversion), and nullability are preserved

### Requirement: ScalarValue for constant representation
The system SHALL define a `ScalarValue` enum for representing constant/literal values in query plans. MVP variants: `Null`, `Boolean(bool)`, `Int32(i32)`, `Int64(i64)`, `Float32(f32)`, `Float64(f64)`, `Utf8(String)`, `Binary(Vec<u8>)`, `Decimal128(i128, u8, i8)`, `Date32(i32)`, `Timestamp(i64, TimeUnit, Option<String>)`.

#### Scenario: Representing a string literal
- **WHEN** the SQL literal `'hello'` is processed
- **THEN** it is stored as `ScalarValue::Utf8("hello".to_string())`

#### Scenario: Getting the DataType of a ScalarValue
- **WHEN** `ScalarValue::Int64(42).data_type()` is called
- **THEN** it returns `DataType::Int64`

#### Scenario: Null value with type
- **WHEN** a typed NULL for an Int32 column is needed
- **THEN** `ScalarValue::Null` is used and the type is inferred from context

### Requirement: TimeUnit enum
The system SHALL define a `TimeUnit` enum with variants: `Second`, `Millisecond`, `Microsecond`, `Nanosecond`, matching Arrow's `TimeUnit`.

#### Scenario: Conversion to Arrow TimeUnit
- **WHEN** `TimeUnit::Microsecond` is converted to Arrow
- **THEN** it produces `arrow::datatypes::TimeUnit::Microsecond`
