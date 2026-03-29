## MODIFIED Requirements

### Requirement: Coordinator startup wires distributed components
The coordinator startup SHALL create a `QueryCoordinator` with access to NodeRegistry, QueryTracker, FlightState, CatalogManager, and ConnectorRegistry. The QueryCoordinator SHALL be passed to the protocol handler for distributed query routing.

#### Scenario: Coordinator starts with distributed support
- **WHEN** the server starts with `--role coordinator`
- **THEN** a QueryCoordinator is created and available to the protocol handler for distributed query routing

### Requirement: Worker startup initializes TaskManager
The worker startup SHALL create a `TaskManager` and register it with the FlightState to handle incoming `submit_task` actions. The TaskManager SHALL have access to a local ExecutionContext for executing received plan fragments.

#### Scenario: Worker starts with TaskManager
- **WHEN** the server starts with `--role worker`
- **THEN** a TaskManager is created and registered to handle task submission via Flight RPC
