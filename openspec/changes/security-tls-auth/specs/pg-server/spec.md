## MODIFIED Requirements

### Requirement: TCP listener and connection spawning
The system SHALL bind a TCP listener to the address specified in `ProtocolConfig` and accept incoming connections. Each accepted connection SHALL be processed in a separate tokio task using `pgwire`'s `process_socket` function with the server's handler implementation and the server's TLS acceptor. The acceptor SHALL be `None` when no `[tls]` section is configured, and a rustls acceptor built from the configured certificate otherwise. A failed TLS handshake SHALL close only that connection.

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

#### Scenario: Failed TLS handshake
- **WHEN** `[tls]` is configured and a client aborts or fails the TLS handshake
- **THEN** that connection is closed and the server keeps accepting other connections
