## ADDED Requirements

### Requirement: ProtocolServer struct
The system SHALL provide a `ProtocolServer` struct that holds shared server state: an `Arc<CatalogManager>`, an `Arc<ConnectorRegistry>`, and a `ProtocolConfig`. It SHALL provide a `new()` constructor accepting these dependencies.

#### Scenario: Creating a ProtocolServer
- **WHEN** `ProtocolServer::new(config, catalog_manager, connector_registry)` is called with valid dependencies
- **THEN** it returns a ProtocolServer ready to accept connections

### Requirement: TCP listener and connection spawning
The system SHALL bind a TCP listener to the address specified in `ProtocolConfig` and accept incoming connections. Each accepted connection SHALL be processed in a separate tokio task using `pgwire`'s `process_socket` function with the server's handler implementation.

#### Scenario: Server starts listening
- **WHEN** `server.start()` is called with config binding to `127.0.0.1:5433`
- **THEN** the server binds to that address and begins accepting TCP connections
- **AND** the method returns a future that runs until the server is stopped

#### Scenario: Accepting a client connection
- **WHEN** a PostgreSQL client connects to the server's listening address
- **THEN** the server spawns a new tokio task to handle the connection
- **AND** the task processes the connection through startup, query loop, and termination

#### Scenario: Multiple simultaneous connections
- **WHEN** three clients connect to the server simultaneously
- **THEN** each connection is handled in its own tokio task, running concurrently

### Requirement: ProtocolConfig
The system SHALL provide a `ProtocolConfig` struct with at minimum a `bind_address` field (String, e.g., "127.0.0.1:5433"). Default bind address SHALL be `127.0.0.1:5433` (port 5433 to avoid conflict with a real PostgreSQL on 5432).

#### Scenario: Default configuration
- **WHEN** `ProtocolConfig::default()` is called
- **THEN** the bind_address is "127.0.0.1:5433"

#### Scenario: Custom bind address
- **WHEN** `ProtocolConfig { bind_address: "0.0.0.0:15432".to_string() }` is created
- **THEN** the server binds to all interfaces on port 15432

### Requirement: Graceful error handling on accept
The system SHALL handle TCP accept errors without crashing. If accepting a connection fails, the server SHALL log the error at WARN level and continue accepting other connections.

#### Scenario: Accept error does not crash server
- **WHEN** a TCP accept call fails (e.g., file descriptor limit reached)
- **THEN** the server logs a warning and continues listening for new connections

### Requirement: Connection handler wiring
Each spawned connection handler SHALL receive clones of `Arc<CatalogManager>` and `Arc<ConnectorRegistry>` from the ProtocolServer. The handler SHALL construct per-connection resources (QueryPlanner, ExecutionContext) using these shared dependencies.

#### Scenario: Handler receives shared state
- **WHEN** a new connection is accepted
- **THEN** the connection handler has access to the server's CatalogManager and ConnectorRegistry
- **AND** can use them to resolve tables and create data sources for query execution
