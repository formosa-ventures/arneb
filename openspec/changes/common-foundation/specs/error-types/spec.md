## ADDED Requirements

### Requirement: Layered error type hierarchy
The system SHALL provide a layered error type hierarchy where each domain (parsing, planning, execution, connector, catalog, config) has its own error enum defined with `thiserror::Error`. A top-level `TrinoError` enum SHALL compose all domain errors via `#[from]` attributes.

#### Scenario: Domain-specific error creation
- **WHEN** a parse failure occurs in the sql-parser crate
- **THEN** a `ParseError` variant is returned without requiring knowledge of other error domains

#### Scenario: Error composition at top level
- **WHEN** a `ParseError` is propagated to the server binary
- **THEN** it is automatically convertible to `TrinoError::Parse(ParseError)` via the `From` trait

### Requirement: Error context preservation
Each domain error enum SHALL implement `std::error::Error` with proper `source()` chaining, so that the full causal chain is available for debugging and logging.

#### Scenario: Error chain traversal
- **WHEN** an `ExecutionError::ArrowError` wraps an underlying `arrow::error::ArrowError`
- **THEN** calling `.source()` on the error returns the original Arrow error

#### Scenario: Display formatting
- **WHEN** an error is formatted with `Display`
- **THEN** it produces a human-readable message describing the error without exposing the full chain (chain is accessible via `source()`)

### Requirement: Non-exhaustive error enums
All domain error enums and the top-level `TrinoError` SHALL be annotated with `#[non_exhaustive]` to allow adding new variants in future crate versions without breaking downstream matches.

#### Scenario: Adding a new error variant
- **WHEN** a new error variant is added to `ExecutionError` in a minor version
- **THEN** downstream crates that use a wildcard match arm (`_`) continue to compile without changes

### Requirement: Domain error enum variants (MVP)
The system SHALL define the following domain error enums with at minimum these variants:

- `ParseError`: `InvalidSyntax(String)`, `UnsupportedFeature(String)`
- `PlanError`: `TableNotFound(String)`, `ColumnNotFound(String)`, `TypeMismatch { expected, found }`, `InvalidExpression(String)`
- `ExecutionError`: `ArrowError(arrow::error::ArrowError)`, `InvalidOperation(String)`, `ResourceExhausted(String)`
- `ConnectorError`: `ConnectionFailed(String)`, `TableNotFound(String)`, `ReadError(String)`, `UnsupportedOperation(String)`
- `CatalogError`: `CatalogNotFound(String)`, `SchemaNotFound(String)`, `TableAlreadyExists(String)`
- `ConfigError`: `FileNotFound(String)`, `ParseError(String)`, `InvalidValue { key, value, reason }`

#### Scenario: Parse error with syntax detail
- **WHEN** the parser encounters invalid SQL `"SELCT * FROM t"`
- **THEN** a `ParseError::InvalidSyntax("...".to_string())` is returned with a message describing the syntax error

#### Scenario: Execution resource exhaustion
- **WHEN** execution exceeds a memory limit
- **THEN** an `ExecutionError::ResourceExhausted("memory limit exceeded".to_string())` is returned

#### Scenario: Config invalid value
- **WHEN** a config file contains `port = "not_a_number"`
- **THEN** a `ConfigError::InvalidValue { key: "port", value: "not_a_number", reason: "expected u16" }` is returned
