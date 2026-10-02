## MODIFIED Requirements

### Requirement: Startup handshake
The system SHALL handle the PostgreSQL startup handshake sequence through the configured authentication method (see `pg-auth`). In `none` mode (the default), upon receiving a StartupMessage with protocol version 3.0 the server SHALL respond with AuthenticationOk, followed by ParameterStatus messages, followed by BackendKeyData, followed by ReadyForQuery with status 'I'. In `password` mode, the server SHALL complete a SCRAM-SHA-256 exchange before sending AuthenticationOk. The HandlerFactory SHALL create a fresh startup handler for every connection, so SASL exchange state is never shared between clients.

#### Scenario: Successful startup (no auth)
- **WHEN** auth is `none` and a client connects with a StartupMessage (protocol 3.0, user=testuser)
- **THEN** the server responds with AuthenticationOk, ParameterStatus messages, BackendKeyData, and ReadyForQuery('I')
- **AND** the connection is ready to accept queries

#### Scenario: Successful startup (password)
- **WHEN** auth is `password` and a configured user completes the SCRAM-SHA-256 exchange
- **THEN** the server sends SASLFinal, AuthenticationOk, ParameterStatus messages, BackendKeyData, and ReadyForQuery('I')

#### Scenario: SSL negotiation request
- **WHEN** a client sends an SSLRequest message (protocol version 80877103)
- **THEN** the server responds with 'N' (SSL not supported) and waits for the actual StartupMessage
