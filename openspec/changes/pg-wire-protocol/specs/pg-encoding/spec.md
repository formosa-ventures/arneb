## ADDED Requirements

### Requirement: Arrow DataType to PostgreSQL type OID mapping
The system SHALL map each supported Arrow/trino `DataType` to a PostgreSQL type OID for use in RowDescription messages. The mapping SHALL cover: Boolean→BOOL(16), Int8/Int16→INT2(21), Int32→INT4(23), Int64→INT8(20), Float32→FLOAT4(700), Float64→FLOAT8(701), Decimal128→NUMERIC(1700), Utf8/LargeUtf8→VARCHAR(1043), Binary→BYTEA(17), Date32→DATE(1082), Timestamp→TIMESTAMP(1114), Null→TEXT(25).

#### Scenario: Mapping Int32
- **WHEN** `type_to_pg_oid(DataType::Int32)` is called
- **THEN** it returns OID 23 (INT4)

#### Scenario: Mapping Utf8
- **WHEN** `type_to_pg_oid(DataType::Utf8)` is called
- **THEN** it returns OID 1043 (VARCHAR)

#### Scenario: Mapping Boolean
- **WHEN** `type_to_pg_oid(DataType::Boolean)` is called
- **THEN** it returns OID 16 (BOOL)

#### Scenario: Mapping Timestamp
- **WHEN** `type_to_pg_oid(DataType::Timestamp { unit: Microsecond, timezone: None })` is called
- **THEN** it returns OID 1114 (TIMESTAMP)

#### Scenario: Mapping unsupported type
- **WHEN** `type_to_pg_oid` is called with a DataType not in the mapping (e.g., a complex nested type)
- **THEN** it returns OID 25 (TEXT) as a fallback

### Requirement: RecordBatch to DataRow text encoding
The system SHALL encode Arrow `RecordBatch` column values as PostgreSQL text-format strings for inclusion in DataRow messages. Each value SHALL be converted to its text representation. NULL values SHALL be encoded as SQL NULL (length -1 in the wire format).

#### Scenario: Encoding Int32 values
- **WHEN** encoding an Int32 array with values [1, 42, -7]
- **THEN** the text representations are ["1", "42", "-7"]

#### Scenario: Encoding Float64 values
- **WHEN** encoding a Float64 array with values [3.14, -0.5]
- **THEN** the text representations are ["3.14", "-0.5"]

#### Scenario: Encoding Boolean values
- **WHEN** encoding a Boolean array with values [true, false]
- **THEN** the text representations are ["t", "f"]

#### Scenario: Encoding Utf8 values
- **WHEN** encoding a Utf8 array with values ["hello", "world"]
- **THEN** the text representations are ["hello", "world"] (passed through unchanged)

#### Scenario: Encoding NULL values
- **WHEN** encoding an Int32 array with values [1, NULL, 3]
- **THEN** the first and third values encode as "1" and "3", the second encodes as NULL (length -1)

#### Scenario: Encoding Date32 values
- **WHEN** encoding a Date32 array with the value representing 2024-01-15
- **THEN** the text representation is "2024-01-15"

#### Scenario: Encoding Timestamp values
- **WHEN** encoding a Timestamp array with a value representing 2024-01-15 10:30:00
- **THEN** the text representation is "2024-01-15 10:30:00"

#### Scenario: Encoding Decimal128 values
- **WHEN** encoding a Decimal128 array with precision 10, scale 2, and value 12345 (representing 123.45)
- **THEN** the text representation is "123.45"

### Requirement: ColumnInfo to FieldInfo conversion
The system SHALL convert a `Vec<ColumnInfo>` (from ExecutionPlan::schema()) into PostgreSQL FieldInfo entries for RowDescription messages. Each FieldInfo SHALL include the column name, PostgreSQL type OID (from the type mapping), and text format code (0).

#### Scenario: Converting a two-column schema
- **WHEN** converting `[ColumnInfo { name: "id", data_type: Int32 }, ColumnInfo { name: "name", data_type: Utf8 }]`
- **THEN** the result is two FieldInfo entries: ("id", OID 23, format 0) and ("name", OID 1043, format 0)

### Requirement: TrinoError to ErrorResponse mapping
The system SHALL map `TrinoError` variants to PostgreSQL ErrorResponse messages with appropriate SQLSTATE codes and severity. The mapping SHALL be: ParseError→42601, PlanError→42P01 (or 42703 for column errors), ExecutionError→XX000, ConnectorError→58030, CatalogError→3D000, ConfigError→F0000. All errors SHALL use severity ERROR.

#### Scenario: Mapping a parse error
- **WHEN** a `TrinoError::Parse(ParseError { message: "unexpected token" })` is converted
- **THEN** the ErrorResponse has severity "ERROR", SQLSTATE "42601", and message containing "unexpected token"

#### Scenario: Mapping a catalog error
- **WHEN** a `TrinoError::Catalog(CatalogError::TableNotFound("users"))` is converted
- **THEN** the ErrorResponse has severity "ERROR", SQLSTATE "3D000", and message containing "users"

#### Scenario: Mapping an execution error
- **WHEN** a `TrinoError::Execution(ExecutionError::TypeError(..))` is converted
- **THEN** the ErrorResponse has severity "ERROR", SQLSTATE "XX000", and a descriptive message

### Requirement: CommandComplete tag generation
The system SHALL generate appropriate CommandComplete tag strings based on the query type and result. For SELECT queries, the tag SHALL be `SELECT <row_count>`. For unrecognized or non-SELECT statements, the tag SHALL be `OK`.

#### Scenario: SELECT returning rows
- **WHEN** a SELECT query returns 42 rows across multiple RecordBatches
- **THEN** the CommandComplete tag is "SELECT 42"

#### Scenario: SELECT returning zero rows
- **WHEN** a SELECT query returns no rows
- **THEN** the CommandComplete tag is "SELECT 0"
