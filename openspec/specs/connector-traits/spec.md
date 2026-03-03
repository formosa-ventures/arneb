## ADDED Requirements

### Requirement: ConnectorFactory trait
The system SHALL define a `ConnectorFactory` trait with a method `create_data_source(&self, table: &TableReference, schema: &[ColumnInfo]) -> Result<Arc<dyn DataSource>, ConnectorError>` that creates a DataSource from table metadata. The trait SHALL require `Send + Sync + Debug` bounds.

#### Scenario: Creating a data source for a registered table
- **WHEN** `factory.create_data_source(&table_ref, &schema)` is called for a table the connector knows about
- **THEN** it returns `Ok(Arc<dyn DataSource>)` that can scan the table's data

#### Scenario: Creating a data source for an unknown table
- **WHEN** `factory.create_data_source(&table_ref, &schema)` is called for a table the connector does not know about
- **THEN** it returns `Err(ConnectorError::TableNotFound(...))`

### Requirement: ConnectorRegistry
The system SHALL define a `ConnectorRegistry` struct that maps connector names (strings) to `Arc<dyn ConnectorFactory>` instances. It SHALL provide `register(name, factory)` and `get(name) -> Option<Arc<dyn ConnectorFactory>>` methods.

#### Scenario: Registering and retrieving a connector
- **WHEN** `registry.register("memory", factory)` is called followed by `registry.get("memory")`
- **THEN** the `get` call returns `Some(factory)`

#### Scenario: Retrieving an unregistered connector
- **WHEN** `registry.get("nonexistent")` is called on a registry without that connector
- **THEN** it returns `None`

### Requirement: ConnectorFactory name method
Each `ConnectorFactory` SHALL provide a `name(&self) -> &str` method returning its connector type name (e.g., "memory", "file").

#### Scenario: Getting connector name
- **WHEN** `factory.name()` is called on a memory connector factory
- **THEN** it returns `"memory"`
