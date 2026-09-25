## Why

The pgwire port accepts every connection in trust mode: the startup handler calls `finish_authentication` right away, so anyone who can reach the port can run queries. That blocks any deployment beyond a laptop or a locked-down private network, and it is one of the adoption gaps to close before go-to-market. Clients (psql, JDBC, psycopg, DBeaver, tokio-postgres) all speak PostgreSQL password authentication already, so the server only needs to challenge them.

## What Changes

- Add an optional `[auth]` config section with `type = "none" | "password"` and a `[[auth.users]]` list of `{ name, password_hash }`. The default stays `none`, so existing configs behave exactly as before.
- `password` mode runs a **SCRAM-SHA-256** SASL exchange (RFC 5802 / RFC 7677), the mechanism PostgreSQL uses for `password_encryption = scram-sha-256`. The password never crosses the wire.
- Store only PostgreSQL-format SCRAM verifiers (`SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>`, the `pg_authid.rolpassword` format), never plaintext. Add an `arneb hash-password` subcommand that prints a verifier for a password read from stdin.
- Reject a wrong password or an unknown user with `FATAL 28P01 password authentication failed`. Unknown users get a stable decoy salt, so an attacker can't tell which user names exist. Force every authentication-phase error to `FATAL`, because pgwire would otherwise move the connection to `ReadyForQuery` after an `ERROR`.
- One per-connection startup handler serves both the Simple and Extended Query paths.
- Log the effective auth mode at startup (`arneb::config` target), and make invalid `[auth]` config a startup error.
- Add `ProtocolServer::with_auth()` and `ProtocolServer::serve(listener)`, so tests can run the real server on an ephemeral port.

## Capabilities

### New Capabilities

- `pg-auth`: pgwire client authentication. Covers the none and SCRAM-SHA-256 password modes, the verifier format, the error semantics, and the rule that authentication-phase errors are fatal.

### Modified Capabilities

- `pg-connection`: The startup handshake is no longer always trust mode. It goes through the configured auth method.
- `server-config`: New `[auth]` section and a `hash-password` CLI subcommand.

## Impact

- **Crates**: `protocol` (new `auth` module, `HandlerFactory.auth`, `ProtocolServer::with_auth`/`serve`), `server` (`[auth]` config, wiring, `hash-password` subcommand).
- **Dependencies**: `aws-lc-rs`, `base64` and `stringprep` become direct dependencies of `protocol`. pgwire's default `server-api-aws-lc-rs` feature already pulls in all three, so the crypto stack doesn't change. `tokio-postgres` and `postgres-protocol` are new dev-dependencies for the interop tests.
- **Compatibility**: The default is `none`, so nothing changes for existing deployments.
- **Out of scope**: TLS on the pgwire port, SCRAM channel binding (`-PLUS`), MD5 or cleartext password modes, and auth for the Web UI and Flight RPC.
