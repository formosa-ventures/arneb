## MODIFIED Requirements

### Requirement: ConnectorFactory provides capabilities and pushdown context
The ConnectorFactory trait SHALL expose a capabilities() method returning ConnectorCapabilities, and the create_data_source() method SHALL accept a ConnectorContext containing pushdown-relevant metadata.

#### Scenario: Querying connector capabilities
- **WHEN** the optimizer queries a ConnectorFactory for its capabilities
- **THEN** the factory SHALL return a ConnectorCapabilities struct indicating supported pushdowns

#### Scenario: Creating data source with context
- **WHEN** create_data_source is called with pushdown context
- **THEN** the returned DataSource SHALL be configured to use the provided pushdown information during scan
