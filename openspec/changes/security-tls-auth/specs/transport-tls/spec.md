## ADDED Requirements

### Requirement: Client-facing TLS certificate
When a `[tls]` section is configured, the system SHALL load one PEM certificate chain (`cert_path`) and private key (`key_path`) at startup and use them for both client-facing listeners: pgwire and the Trino client protocol. TLS SHALL use rustls with the aws-lc-rs provider. The certificate SHALL be read once at startup; changes on disk take effect after a restart.

#### Scenario: Valid certificate
- **WHEN** `[tls]` points at a readable certificate chain and its matching private key
- **THEN** the server starts and logs the TLS mode for pgwire and for the Trino listener on the `arneb::config` target

#### Scenario: Missing or mismatched key material
- **WHEN** `cert_path` or `key_path` is unreadable, contains no PEM items, or the key does not match the certificate
- **THEN** startup fails with a configuration error naming the file

### Requirement: pgwire TLS modes
The `[tls] pgwire` setting SHALL accept `required` (the default) or `optional`. In `required` mode, a pgwire connection that did not negotiate TLS SHALL be rejected with `FATAL 28000` regardless of the `[auth]` mode. In `optional` mode, both TLS and plaintext connections SHALL be accepted.

#### Scenario: verify-full client
- **WHEN** `[tls]` is configured with a certificate whose subject matches the host, and `psql "host=<host> sslmode=verify-full sslrootcert=<ca>"` connects
- **THEN** the connection is established and queries run

#### Scenario: Plaintext client in required mode
- **WHEN** `pgwire = "required"` and a client connects with `sslmode=disable`
- **THEN** the client receives `FATAL 28000` and no query runs

### Requirement: HTTPS on the Trino listener
When `[tls]` is configured, the Trino client protocol listener SHALL serve HTTPS only, on the same configured port, using the `[tls]` certificate. It SHALL negotiate `h2` or `http/1.1` through ALPN. A failed TLS handshake SHALL close only that connection. When `[tls]` is absent, the listener SHALL serve plain HTTP exactly as before.

#### Scenario: Trino client over HTTPS
- **WHEN** `[tls]` is configured and `trino --server https://<host>:8080 --truststore-path <ca>` runs `SELECT 1`
- **THEN** the query returns `1`

#### Scenario: Plain HTTP to an HTTPS listener
- **WHEN** `[tls]` is configured and a client sends a plain HTTP request to the Trino port
- **THEN** the TLS handshake fails, that connection is closed, and the listener keeps serving other clients
