## MODIFIED Requirements

### Requirement: Startup handshake
The system SHALL handle the PostgreSQL startup handshake sequence through the configured transport security (see `transport-tls`) and authentication method (see `pg-auth`). When no `[tls]` section is configured, the server SHALL answer an SSLRequest with 'N'. When `[tls]` is configured, the server SHALL answer an SSLRequest with 'S' and complete a TLS handshake, and SHALL also accept PostgreSQL 17 direct TLS connections (ALPN `postgresql`). When `[tls] pgwire = "required"`, a StartupMessage received over a non-TLS connection SHALL be rejected with `FATAL 28000` before any authentication exchange. In `none` auth mode (the default), upon receiving a StartupMessage with protocol version 3.0 the server SHALL respond with AuthenticationOk, followed by ParameterStatus messages, followed by BackendKeyData, followed by ReadyForQuery with status 'I'. In `password` mode, the server SHALL complete a SCRAM-SHA-256 exchange before sending AuthenticationOk. The HandlerFactory SHALL create a fresh startup handler for every connection, so SASL exchange state is never shared between clients.

#### Scenario: Successful startup (no auth)
- **WHEN** auth is `none` and a client connects with a StartupMessage (protocol 3.0, user=testuser)
- **THEN** the server responds with AuthenticationOk, ParameterStatus messages, BackendKeyData, and ReadyForQuery('I')
- **AND** the connection is ready to accept queries

#### Scenario: Successful startup (password)
- **WHEN** auth is `password` and a configured user completes the SCRAM-SHA-256 exchange
- **THEN** the server sends SASLFinal, AuthenticationOk, ParameterStatus messages, BackendKeyData, and ReadyForQuery('I')

#### Scenario: SSL negotiation request without TLS configured
- **WHEN** no `[tls]` section is configured and a client sends an SSLRequest message (protocol version 80877103)
- **THEN** the server responds with 'N' (SSL not supported) and waits for the actual StartupMessage

#### Scenario: SSL negotiation request with TLS configured
- **WHEN** `[tls]` is configured and a client sends an SSLRequest message
- **THEN** the server responds with 'S', completes a TLS handshake, and continues the startup handshake over the encrypted connection

#### Scenario: Plaintext rejected in required mode
- **WHEN** `[tls] pgwire = "required"` and a client sends a StartupMessage without negotiating TLS
- **THEN** the server responds with ErrorResponse `FATAL 28000` stating that an SSL connection is required, and closes the connection

#### Scenario: Plaintext accepted in optional mode
- **WHEN** `[tls] pgwire = "optional"` and a client sends a StartupMessage without negotiating TLS
- **THEN** the startup handshake proceeds as if no `[tls]` section were configured
