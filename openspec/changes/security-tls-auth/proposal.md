## Why

`pgwire-auth` (#77) added SCRAM-SHA-256 password authentication, but only on the pgwire port, and only over plaintext. Three gaps remain before Arneb can be shared with a team (Phase 3 roadmap, M1 / P3):

1. **pgwire has no TLS.** The server answers every `SSLRequest` with `N`, so `sslmode=require` and `verify-full` clients cannot connect, and query text and results cross the network in the clear.
2. **The Trino listener bypasses `[auth]`.** It accepts any `X-Trino-User` without a password (#85). Today `main.rs` only logs a WARN and tells operators to pass `--no-trino`. Trino clients (CLI, JDBC, `trino-python-client`) refuse to send a password over plain HTTP, so fixing this needs HTTPS as well.
3. **Coordinator↔worker Flight RPC is unauthenticated.** Anyone who can reach port 9090 can register a fake worker through a heartbeat, submit tasks to a worker, or `do_get` another task's output.

## What Changes

- **Client-facing TLS.** Add an optional `[tls]` section with `cert_path`, `key_path` and `pgwire = "required" | "optional"`. Paths are PEM, and the setting is overridable through `ARNEB_TLS_*`.
  - pgwire passes a `TlsAcceptor` to `pgwire::tokio::process_socket`. It accepts both the `SSLRequest` handshake and PG17 direct TLS.
  - In `required` mode, the startup handler rejects a non-TLS connection with `FATAL 28000`.
  - The Trino listener serves HTTPS only whenever `[tls]` is configured.
- **Trino HTTP Basic auth (closes #85).** When `[auth] type = "password"`, every `/v1/statement` and `/v1/query` endpoint requires `Authorization: Basic`.
  - Verification runs against the same `[[auth.users]]` SCRAM verifiers, in constant time. A bounded cache of successful verifications avoids paying PBKDF2 on every `nextUri` poll.
  - Missing or invalid credentials get `401` with `WWW-Authenticate: Basic realm="arneb"`.
  - The authenticated user becomes the query's user. A different `X-Trino-User` gets `403`, and a query's paging and cancel URIs only work for the user who owns the query.
  - `/v1/info` and `/v1/info/state` stay open for health checks.
  - Password mode with the Trino listener enabled but no `[tls]` becomes a **startup error**. This replaces today's WARN, so the listener fails closed.
- **Cluster RPC authentication.** Add `[cluster] secret` (`ARNEB_CLUSTER_SECRET`).
  - Every Flight call carries `authorization: Bearer <secret>`, and a tonic interceptor on every node checks it in constant time. This covers heartbeat registration, task submission, exchange `do_get`, `do_put` and task status.
  - A wrong or missing secret gets `UNAUTHENTICATED`. The coordinator never registers that worker.
- **Optional cluster TLS.** `[cluster.tls]` takes `cert_path`, `key_path` and `ca_path` (`ARNEB_CLUSTER_TLS_*`). Flight servers use tonic `ServerTlsConfig`, clients use `ClientTlsConfig` against `ca_path`, and peer addresses switch from `http://` to `https://`.
- **Web UI is loopback-only under password auth.** The Web UI has no login, and it shows query text and cluster state. When `[auth] type = "password"`, it binds to `127.0.0.1` by default instead of the server's `bind_address`.
  - A new `[web] bind_address` setting (`ARNEB_WEB_BIND_ADDRESS`) binds it elsewhere explicitly.
  - The effective address and the reason for it (default, loopback under auth, or explicit) are logged on `arneb::config`.
  - With auth `none`, the default is unchanged.
- **Startup logging.** Log the effective TLS mode, Trino auth mode and cluster-auth mode on `arneb::config`, without printing secrets. An unreadable cert or key, a short secret, or an inconsistent combination is a startup error.

## Capabilities

### New Capabilities

- `transport-tls`: TLS for the client-facing listeners (pgwire and Trino HTTP). Covers certificate loading, the `required` / `optional` pgwire modes, and HTTPS on the Trino listener.
- `trino-auth`: HTTP Basic authentication on the Trino listener against the shared `[auth]` store. Covers user binding, query ownership, the verification cache and the fail-closed startup rule.
- `cluster-rpc-auth`: shared-secret authentication and optional TLS on the coordinator↔worker Flight RPC.

### Modified Capabilities

- `pg-connection`: the startup handshake answers `SSLRequest` with `S` when TLS is configured, and rejects plaintext in `required` mode.
- `pg-server`: connections are processed with the configured TLS acceptor.
- `server-config`: new `[tls]`, `[cluster] secret`, `[cluster.tls]` and `[web] bind_address` settings, plus their `ARNEB_*` overrides and validation. The Web UI binds to loopback by default under password auth.

## Impact

- **Crates**:
  - `protocol`: TLS acceptor wiring in `server.rs`, a non-TLS check in `AuthStartupHandler`, and a new `trino/auth.rs` middleware plus an HTTPS accept loop in `trino/mod.rs`.
  - `rpc`: an auth interceptor, a client channel builder shared by `task_submission.rs`, `heartbeat.rs` and `exchange_client.rs`, and TLS on `start_flight_server`.
  - `server`: config, validation, wiring, removal of the #85 WARN, and the Web UI bind-address selection.
- **Dependencies**: no new crypto stack. `tokio-rustls` 0.26 and `rustls` 0.23 (aws-lc-rs) are already in the tree through pgwire, and they become direct dependencies of `protocol`. tonic gains its `tls-aws-lc` feature. `rustls-pemfile` (or `rustls-pki-types` PEM parsing) is used for loading certs. `hyper-util` is used for the HTTPS accept loop if axum's own `serve` cannot take a TLS listener.
- **Compatibility**: with no `[tls]`, no `[auth]` password mode and no `[cluster] secret`, behavior is byte-for-byte unchanged. Two intentional behavior changes apply only under `[auth] type = "password"`:
  - **Fail closed**: with the Trino listener enabled, the server now needs either `[tls]` or `--no-trino` to start. Today that config only produces a WARN.
  - **Web UI on loopback**: it binds to `127.0.0.1` unless `[web] bind_address` is set. This affects docker setups that rely on `bind_address = "0.0.0.0"`.
- **Out of scope**: Web UI login/authentication (this change only narrows its default bind); mTLS client-certificate authentication for SQL clients; SCRAM channel binding (`-PLUS`); per-user authorization or ACLs; certificate hot-reload; OAuth/JWT/LDAP.
