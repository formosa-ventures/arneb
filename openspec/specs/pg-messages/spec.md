## ADDED Requirements

### Requirement: Frontend message parsing
The system SHALL parse incoming PostgreSQL wire protocol v3 frontend messages from a byte stream. Supported message types SHALL include: StartupMessage, Query, and Terminate. The parser SHALL handle the length-prefixed binary framing format (4-byte message length followed by payload).

#### Scenario: Parsing a StartupMessage
- **WHEN** the server receives a startup message with protocol version 3.0 and parameters `user=testuser, database=testdb`
- **THEN** the parser produces a StartupMessage containing protocol version 196608 (3.0) and a parameter map with keys "user" and "database"

#### Scenario: Parsing a Query message
- **WHEN** the server receives a Query message ('Q') containing the SQL string `SELECT 1`
- **THEN** the parser produces a Query message with the SQL text "SELECT 1"

#### Scenario: Parsing a Terminate message
- **WHEN** the server receives a Terminate message ('X')
- **THEN** the parser produces a Terminate message signaling connection close

#### Scenario: Receiving an unsupported message type
- **WHEN** the server receives a message with an unrecognized type byte
- **THEN** the server responds with an ErrorResponse and continues processing (does not crash or disconnect)

### Requirement: Backend message serialization
The system SHALL serialize outgoing PostgreSQL wire protocol v3 backend messages into byte streams. Supported message types SHALL include: AuthenticationOk, ParameterStatus, BackendKeyData, ReadyForQuery, RowDescription, DataRow, CommandComplete, ErrorResponse, and EmptyQueryResponse.

#### Scenario: Serializing AuthenticationOk
- **WHEN** the server sends an AuthenticationOk message during startup
- **THEN** the output bytes contain message type 'R', length 8, and auth code 0

#### Scenario: Serializing RowDescription
- **WHEN** the server sends a RowDescription for columns (id: INT4, name: VARCHAR)
- **THEN** the output bytes contain message type 'T', field count 2, and field descriptors with correct names, type OIDs (23 for INT4, 1043 for VARCHAR), and text format code (0)

#### Scenario: Serializing DataRow
- **WHEN** the server sends a DataRow with text values ["42", "Alice"]
- **THEN** the output bytes contain message type 'D', column count 2, and each value preceded by its 4-byte length

#### Scenario: Serializing DataRow with NULL
- **WHEN** the server sends a DataRow with values [NULL, "Bob"]
- **THEN** the first column's length is -1 (indicating NULL) and the second column contains "Bob"

#### Scenario: Serializing ErrorResponse
- **WHEN** the server sends an ErrorResponse with severity ERROR, SQLSTATE 42601, and message "syntax error"
- **THEN** the output bytes contain message type 'E' followed by field type indicators 'S' (severity), 'V' (severity non-localized), 'C' (code), 'M' (message), and a null terminator

#### Scenario: Serializing CommandComplete
- **WHEN** the server sends a CommandComplete for a SELECT returning 5 rows
- **THEN** the output bytes contain message type 'C' and the tag string "SELECT 5"

### Requirement: ReadyForQuery transaction status
The system SHALL send ReadyForQuery messages with transaction status indicator 'I' (idle, not in a transaction) after completing each query cycle, since MVP does not support transactions.

#### Scenario: ReadyForQuery after query completion
- **WHEN** a query completes successfully or with an error
- **THEN** the server sends a ReadyForQuery message with status byte 'I'
