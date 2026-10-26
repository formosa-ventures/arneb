## ADDED Requirements

### Requirement: HTTP Basic authentication on the Trino listener
When `[auth] type = "password"`, the Trino client protocol listener SHALL require `Authorization: Basic` credentials on every endpoint except `/v1/info`, `/v1/info/state` and `/v1/status`. Credentials SHALL be verified against the same `[[auth.users]]` SCRAM-SHA-256 verifiers used by pgwire: the stored salt and iteration count derive the client key, and the derived StoredKey SHALL be compared to the stored one in constant time. Unknown users SHALL be verified against the decoy verifier, so response timing does not reveal which users exist. Missing or invalid credentials SHALL produce HTTP `401` with `WWW-Authenticate: Basic realm="arneb"`. When `[auth] type = "none"`, no credentials SHALL be required.

#### Scenario: Correct password
- **WHEN** password mode is on and `trino --server https://<host>:8080 --user alice --password` supplies alice's password and runs `SELECT 1`
- **THEN** the query returns `1`

#### Scenario: Wrong password
- **WHEN** a request to `/v1/statement` carries Basic credentials with a wrong password
- **THEN** the response is `401` with `WWW-Authenticate: Basic realm="arneb"` and no query is created

#### Scenario: Missing credentials
- **WHEN** password mode is on and a request to `/v1/statement` has no `Authorization` header
- **THEN** the response is `401`

#### Scenario: Health endpoints stay public
- **WHEN** password mode is on and a client requests `/v1/info` without credentials
- **THEN** the response is `200`

#### Scenario: No auth configured
- **WHEN** `[auth]` is absent and a client posts `SELECT 1` with only `X-Trino-User: alice`
- **THEN** the query runs exactly as before this change

### Requirement: Authenticated user binding and query ownership
In password mode, the authenticated user SHALL be the query's user. A request whose `X-Trino-User` (or `X-Presto-User`) header names a different user SHALL be rejected with `403`. Each query SHALL record its owner, and paging (`nextUri`), cancellation, `/v1/query/{id}` and kill requests from a different authenticated user SHALL be rejected with `403`. `/v1/query` SHALL list only the caller's queries.

#### Scenario: Impersonation attempt
- **WHEN** alice authenticates and sends `X-Trino-User: bob`
- **THEN** the response is `403`

#### Scenario: Another user's nextUri
- **WHEN** bob authenticates and fetches the `nextUri` of a query owned by alice
- **THEN** the response is `403` and alice's query is unaffected

### Requirement: Verification cache
To avoid running PBKDF2 on every `nextUri` poll, the system SHALL cache successful verifications in a bounded in-memory map (at most 1024 entries, 5-minute TTL), keyed by the user name and an HMAC of the password under a random per-process key. Plaintext passwords SHALL NOT be stored, and failed verifications SHALL NOT be cached.

#### Scenario: Repeated polls
- **WHEN** a client polls `nextUri` 100 times with the same valid credentials within 5 minutes
- **THEN** the password is fully verified once and the remaining requests hit the cache

### Requirement: Password auth requires Trino TLS
When `[auth] type = "password"` and the Trino listener is enabled, the system SHALL refuse to start unless `[tls]` is configured. The error SHALL name both remedies: configure `[tls]`, or disable the Trino listener (`--no-trino` / `ARNEB_TRINO_ENABLED=false`).

#### Scenario: Password mode without TLS
- **WHEN** `[auth] type = "password"`, the Trino listener is enabled, and no `[tls]` section is configured
- **THEN** startup fails with a configuration error naming `[tls]` and `--no-trino`

#### Scenario: Password mode with the Trino listener disabled
- **WHEN** `[auth] type = "password"`, no `[tls]` section is configured, and the server is started with `--no-trino`
- **THEN** the server starts
