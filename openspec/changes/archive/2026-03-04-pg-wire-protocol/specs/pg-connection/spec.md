## ADDED Requirements

### Requirement: Startup handshake
The system SHALL handle the PostgreSQL startup handshake sequence. Upon receiving a StartupMessage with protocol version 3.0, the server SHALL respond with AuthenticationOk (trust mode, no password required), followed by ParameterStatus messages for `server_version`, `server_encoding` (UTF8), and `client_encoding` (UTF8), followed by BackendKeyData, followed by ReadyForQuery with status 'I'.

#### Scenario: Successful startup
- **WHEN** a client connects and sends a StartupMessage with protocol version 3.0 and parameters user=testuser
- **THEN** the server responds with AuthenticationOk, ParameterStatus messages, BackendKeyData, and ReadyForQuery('I')
- **AND** the connection is ready to accept queries

#### Scenario: SSL negotiation request
- **WHEN** a client sends an SSLRequest message (protocol version 80877103)
- **THEN** the server responds with 'N' (SSL not supported) and waits for the actual StartupMessage

#### Scenario: Unsupported protocol version
- **WHEN** a client sends a StartupMessage with protocol version other than 3.0
- **THEN** the server responds with an ErrorResponse (SQLSTATE 0A000, "unsupported protocol version") and closes the connection

### Requirement: Simple Query flow
The system SHALL implement the Simple Query protocol flow. Upon receiving a Query message, the server SHALL: (1) parse the SQL using the sql-parser crate, (2) plan the query using the planner with CatalogManager, (3) execute the physical plan using ExecutionContext, (4) encode results as RowDescription + DataRow messages, (5) send CommandComplete, (6) send ReadyForQuery.

#### Scenario: Successful SELECT query
- **WHEN** a client sends a Query message with `SELECT id, name FROM users`
- **AND** the table "users" exists with matching columns and data
- **THEN** the server sends RowDescription (id: INT4, name: VARCHAR), DataRow messages for each row, CommandComplete("SELECT N" where N is row count), and ReadyForQuery('I')

#### Scenario: Query with no results
- **WHEN** a client sends a Query message with `SELECT * FROM empty_table` and the table has no rows
- **THEN** the server sends RowDescription with the table's column definitions, CommandComplete("SELECT 0"), and ReadyForQuery('I')

#### Scenario: Empty query string
- **WHEN** a client sends a Query message with an empty string or whitespace only
- **THEN** the server sends EmptyQueryResponse followed by ReadyForQuery('I')

#### Scenario: Parse error in query
- **WHEN** a client sends a Query message with `SELEC * FROM users` (invalid SQL)
- **THEN** the server sends ErrorResponse with SQLSTATE 42601 (syntax_error) and ReadyForQuery('I')
- **AND** the connection remains open for further queries

#### Scenario: Planning error — table not found
- **WHEN** a client sends a Query message with `SELECT * FROM nonexistent`
- **THEN** the server sends ErrorResponse with SQLSTATE 42P01 (undefined_table) and ReadyForQuery('I')

#### Scenario: Execution error
- **WHEN** a query execution fails (e.g., type mismatch at runtime)
- **THEN** the server sends ErrorResponse with SQLSTATE XX000 (internal_error) and ReadyForQuery('I')

### Requirement: Connection termination
The system SHALL gracefully handle connection termination. Upon receiving a Terminate message, the server SHALL close the connection. Upon detecting an unexpected connection drop (TCP reset, EOF), the server SHALL clean up connection resources without logging an error at ERROR level (use DEBUG or WARN).

#### Scenario: Graceful termination
- **WHEN** a client sends a Terminate message
- **THEN** the server closes the TCP connection and frees connection resources

#### Scenario: Unexpected disconnect
- **WHEN** a client drops the TCP connection without sending Terminate
- **THEN** the server detects EOF, cleans up resources, and logs at DEBUG level

### Requirement: Per-connection session state
The system SHALL maintain per-connection session state including the database name and schema from the startup parameters. Each connection SHALL have its own query pipeline (parse → plan → execute) isolated from other connections. Shared state (CatalogManager, ConnectorRegistry) SHALL be accessed via Arc references.

#### Scenario: Connection with database parameter
- **WHEN** a client connects with startup parameter `database=mydb`
- **THEN** the connection's session state records "mydb" as the current database

#### Scenario: Multiple concurrent connections
- **WHEN** two clients connect simultaneously and issue different queries
- **THEN** each query executes independently using the shared CatalogManager and ConnectorRegistry without interference

### Requirement: Query execution via spawn_blocking
The system SHALL execute the synchronous query pipeline (parse → plan → execute) inside `tokio::task::spawn_blocking` to avoid blocking the async runtime. The result SHALL be sent back to the async handler for encoding and transmission.

#### Scenario: CPU-intensive query does not block other connections
- **WHEN** one connection runs a query involving a large sort operation
- **THEN** other connections can still complete their startup handshake and issue queries concurrently
