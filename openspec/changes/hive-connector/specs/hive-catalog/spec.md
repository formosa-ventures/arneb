## ADDED Requirements

### Requirement: Connect to Hive Metastore
The system SHALL connect to a Hive Metastore instance via Thrift protocol using the `hive_metastore` crate.

#### Scenario: Successful HMS connection
- **WHEN** the server starts with a Hive catalog configured with `metastore_uri = "thrift://host:9083"`
- **THEN** the system SHALL establish a Thrift connection to the specified HMS instance

#### Scenario: HMS connection failure
- **WHEN** the HMS instance is unreachable
- **THEN** the system SHALL log an error and fail the catalog registration with a descriptive message

### Requirement: Map HMS databases to Arneb schemas
The `HiveCatalogProvider` SHALL map Hive Metastore databases to Arneb `SchemaProvider` instances.

#### Scenario: List available schemas
- **WHEN** the system calls `schema_names().await` on a HiveCatalogProvider
- **THEN** the system SHALL query HMS `get_all_databases()` and return the database names

#### Scenario: Resolve a specific schema
- **WHEN** the system calls `schema("analytics").await`
- **THEN** the system SHALL return a `HiveSchemaProvider` for the `analytics` database

### Requirement: Map HMS tables to Arneb TableProviders
The `HiveSchemaProvider` SHALL map Hive Metastore tables to Arneb `TableProvider` instances.

#### Scenario: List tables in a database
- **WHEN** the system calls `table_names().await` on a HiveSchemaProvider
- **THEN** the system SHALL query HMS `get_all_tables(database)` and return the table names

#### Scenario: Resolve a specific table
- **WHEN** the system calls `table("events").await`
- **THEN** the system SHALL query HMS `get_table(database, "events")` and return a TableProvider with the table's Arrow schema derived from HMS column metadata

### Requirement: HMS type mapping to Arrow
The system SHALL map Hive column types to Arrow data types for schema construction.

#### Scenario: Map common Hive types
- **WHEN** a HMS table has columns with types INT, BIGINT, STRING, DOUBLE, BOOLEAN, DATE, TIMESTAMP
- **THEN** the system SHALL map them to Arrow Int32, Int64, Utf8, Float64, Boolean, Date32, TimestampMicrosecond respectively

#### Scenario: Unsupported Hive type
- **WHEN** a HMS table has a column with an unsupported type (e.g., MAP, ARRAY, STRUCT)
- **THEN** the system SHALL return an error indicating the unsupported type and column name
