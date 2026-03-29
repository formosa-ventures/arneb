# Spec: Connector Traits (DDL Extension)

## MODIFIED Requirements

### Requirement: Add optional DDLProvider to ConnectorFactory
The `ConnectorFactory` trait SHALL provide an optional method to obtain a `DDLProvider` reference.

#### Scenario: Connector with DDL support
- **WHEN** a connector factory implements DDL support
- **THEN** `ddl_provider()` returns `Some(&dyn DDLProvider)`.

#### Scenario: Connector without DDL support
- **WHEN** a connector factory does not implement DDL support
- **THEN** `ddl_provider()` returns `None`.

### Requirement: DDLProvider discovery at execution time
The execution engine SHALL look up the DDLProvider for the target connector when executing DDL/DML statements.

#### Scenario: DDL statement routing
- **WHEN** `CREATE TABLE memory.default.test (...)` is executed
- **THEN** the engine resolves the "memory" connector, calls `ddl_provider()`, and delegates to the returned provider.

#### Scenario: DDL on connector without provider
- **WHEN** a DDL statement targets a connector where `ddl_provider()` returns `None`
- **THEN** the engine returns an error without attempting the operation.

### Requirement: Preserve existing connector interface
All existing ConnectorFactory and DataSource trait methods MUST remain unchanged. DDLProvider is purely additive.

#### Scenario: Existing read-only queries
- **WHEN** a SELECT query is executed against any connector
- **THEN** behavior is identical to before this change.
