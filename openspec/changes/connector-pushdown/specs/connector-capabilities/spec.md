## ADDED Requirements

### Requirement: Connectors declare pushdown capabilities
The system SHALL provide a ConnectorCapabilities struct that declares which pushdown operations a connector supports: filter pushdown, projection pushdown, and limit pushdown.

#### Scenario: Connector supports projection pushdown
- **WHEN** a connector's capabilities indicate supports_projection_pushdown is true
- **THEN** the optimizer SHALL include projection indices in ScanContext when pushing down to that connector

#### Scenario: Connector does not support filter pushdown
- **WHEN** a connector's capabilities indicate supports_filter_pushdown is false
- **THEN** the optimizer SHALL NOT include filter predicates in ScanContext and SHALL leave the FilterExec operator above the scan

#### Scenario: Querying capabilities
- **WHEN** the optimizer needs to decide what to push down
- **THEN** it SHALL query ConnectorCapabilities from the ConnectorFactory before constructing ScanContext
