## Context

Phase 3 roadmap (`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`), milestone M1, item P3. Today Arneb has three network surfaces:

| Surface | Port | Today |
|---|---|---|
| pgwire | `port` (5432) | SCRAM-SHA-256 when `[auth] type = "password"` (`pgwire-auth`). Always plaintext: `process_socket(socket, None, handler)` in `crates/protocol/src/server.rs`. |
| Trino REST | `[trino] port` (8080) | `axum::serve` over a plain `TcpListener` (`crates/protocol/src/trino/mod.rs`). Trusts `X-Trino-User`. `main.rs` WARNs when password mode is on (#85). |
| Flight RPC | `[cluster] rpc_port` (9090) | tonic `Server::builder().serve(addr)`. Clients use `Channel::from_shared("http://…")` in three places: `task_submission.rs`, `heartbeat.rs` and `exchange_client.rs`. A worker registers itself by sending the `heartbeat` action to the coordinator. Workers also `do_get` from each other during exchange. |

The building blocks are already in the dependency tree:
- pgwire 0.41 `process_socket(tcp, Option<TlsAcceptor>, handlers)` handles both the `SSLRequest` handshake and PG17 direct TLS (ALPN `postgresql`). `ClientInfo::is_secure()` tells the startup handler whether the connection is encrypted.
- `tokio-rustls` 0.26 and `rustls` 0.23 run on aws-lc-rs, the same crypto provider pgwire and `pgwire-auth` already use.
- tonic 0.14 offers a `tls-aws-lc` feature (`ServerTlsConfig` / `ClientTlsConfig`) and per-service interceptors.
- `crates/protocol/src/auth.rs` already has `ScramVerifier` and `UserCredentials`, including decoy verifiers for unknown users.

## Goals / Non-Goals

**Goals**
- `psql "sslmode=verify-full"` connects; `sslmode=require` is enforceable server-side.
- `trino --server https://… --user alice --password` works, and the Trino listener can no longer be used to bypass `[auth]`.
- A worker with a wrong or missing cluster secret is rejected at registration, and no Flight action runs for an unauthenticated peer.
- With nothing configured, behavior is unchanged.

**Non-Goals**
- Web UI login. It has no user model yet, and is listed as a follow-up. This change only narrows where the Web UI listens under password auth (D8).
- Client-certificate (mTLS) authentication for SQL clients.
- SCRAM channel binding.
- Authorization (per-user grants).
- Certificate hot-reload.
- OAuth, JWT, LDAP and Kerberos.

## Decisions

### D1: One `[tls]` section for both client-facing listeners

```toml
[tls]
cert_path = "/etc/arneb/server.crt"   # PEM, leaf first, full chain  (ARNEB_TLS_CERT_PATH)
key_path  = "/etc/arneb/server.key"   # PEM, PKCS#8 / PKCS#1 / SEC1   (ARNEB_TLS_KEY_PATH)
pgwire    = "required"                # "required" (default) | "optional"  (ARNEB_TLS_PGWIRE)
```

pgwire and Trino run in the same process and serve the same host name, so one certificate covers both. Splitting the section per listener is easy to add later and is not needed now.

- When the section is present, the Trino listener is **HTTPS-only**. A single port does not serve both HTTP and HTTPS, because Trino clients choose the scheme up front and sniffing the protocol on one port adds complexity for no gain.
- pgwire defaults to `required`, because configuring a certificate signals intent to encrypt. `optional` exists for migrations, where old plaintext clients keep working while new ones move to TLS.
- The cert and key are loaded once at startup into a `rustls::ServerConfig` with ALPN set to `postgresql` for pgwire and `h2, http/1.1` for Trino. Two `ServerConfig`s are built from the same key material. An unreadable file, a key that does not match the certificate, or an empty chain is a startup error.

### D2: pgwire `required` mode is enforced in the startup handler

pgwire accepts a plaintext StartupMessage even when an acceptor is configured. That is the `optional` behavior, and it cannot be switched off inside pgwire. So `AuthStartupHandler` (from `pgwire-auth`) checks `client.is_secure()` when it sees the StartupMessage. In `required` mode a plaintext connection gets `FATAL 28000 "SSL connection is required"` (the code PostgreSQL uses for `hostssl` rejections) before any SASL exchange, and the connection closes. The check runs in `none` auth mode too, because `required` is about transport, not identity.

### D3: Trino listener — rustls accept loop and an auth middleware layer

`axum::serve` only accepts a plain `TcpListener`. When `[tls]` is set, `TrinoServer::serve` instead runs a small accept loop: TCP accept → `TlsAcceptor::accept` → `hyper_util::server::conn::auto::Builder::serve_connection` with the axum router as a `tower` service. That is about 40 lines and adds no `axum-server` dependency. A handshake failure is logged at `debug` and drops that connection only. When `[tls]` is absent, the existing `axum::serve` path is used unchanged.

Authentication is a `tower` middleware (`axum::middleware::from_fn_with_state`) on the router:
- **Public**: `/v1/info`, `/v1/info/state` and `/v1/status`, which load balancers use for health checks (Trino does the same).
- **Everything else** requires `Authorization: Basic base64(user:password)` when `[auth] type = "password"`.
- **Verification**: the stored verifier's salt and iteration count are used to derive `ClientKey`, then `StoredKey' = H(ClientKey)` is compared to the stored `StoredKey` in constant time. Unknown users go through the decoy verifier, so timing does not reveal which users exist. This is a new `ScramVerifier::verify_password(&str) -> bool` in `auth.rs`, which keeps password handling in one module.
- **Failure**: `401` with `WWW-Authenticate: Basic realm="arneb"` and a Trino-style JSON error body.
- **Cache**: Trino clients resend Basic auth on every `nextUri` poll, and PBKDF2 at high iteration counts (#84) costs milliseconds each time. Successful verifications are cached in a bounded map keyed by `(user, HMAC-SHA256(process_secret, password))`, with a 5-minute TTL and at most 1024 entries. The plaintext password is never stored, failures are never cached, and the per-process secret is random at startup.
- **User binding**: the authenticated user overrides `X-Trino-User`. If that header is present and differs, the request gets `403` (impersonation is not supported). Each `Query` records its owner, so `fetch`, `cancel`, `query_info` and `kill` by another user get `403`, and `/v1/query` lists only the caller's queries.

### D4: Fail closed when password auth has no Trino TLS

`[auth] type = "password"`, with the Trino listener enabled and no `[tls]`, becomes a **startup error**: "configure [tls] or disable the Trino listener (--no-trino)". Real Trino clients will not send a password over HTTP anyway, so this listener would be either unusable or a bypass. This replaces the WARN at `main.rs:697-708`. It is the change's only deliberate break of an existing configuration, and it is called out in the proposal and the docs.

### D5: Cluster secret is a bearer token checked by a tonic interceptor on every node

```toml
[cluster]
secret = "…"     # >= 32 chars; ARNEB_CLUSTER_SECRET (preferred: keeps it out of files / k8s Secret → env)
```

- **Client side**: a single `rpc::channel::connect(address, &ClusterSecurity)` helper replaces the three hand-built `Channel::from_shared(…)` sites. It returns a client wrapped in an interceptor that adds `authorization: Bearer <secret>` metadata.
- **Server side**: `FlightServiceServer::with_interceptor` checks the header in constant time. A missing or wrong value returns `Status::unauthenticated` before any handler runs, so `heartbeat` (registration), `submit_task`, `do_get`, `do_put` and the rest are all covered.
- **Coordinator logging**: rejected heartbeats are logged at WARN on `arneb::cluster` with the peer address, rate-limited to one per peer per minute.
- **Worker behavior**: a worker that gets `UNAUTHENTICATED` on its heartbeat logs an ERROR naming `[cluster] secret` and keeps retrying on the normal heartbeat interval. It does not exit, so fixing the secret and restarting the coordinator recovers the cluster.

A bearer secret was chosen over mTLS-only authentication. mTLS needs a per-node CA workflow, while a shared secret is one value in a Kubernetes Secret, which is what P6's Helm chart can ship. Optional TLS (D6) handles confidentiality. A secret sent without TLS is visible to anyone sniffing the network, and the docs say so.

The standalone role also runs a Flight server. When a secret is set, the check applies there too. When no secret is set anywhere, there is no interceptor at all, so behavior is unchanged.

**Validation:**
- A secret shorter than 32 characters is a startup error.
- A worker configured without a secret is fine at config time, and it is rejected at registration by a coordinator that has one. This is the acceptance scenario.

### D6: Optional cluster TLS through tonic

```toml
[cluster.tls]
cert_path = "..."   # this node's server cert (ARNEB_CLUSTER_TLS_CERT_PATH)
key_path  = "..."   # (ARNEB_CLUSTER_TLS_KEY_PATH)
ca_path   = "..."   # CA that signed every node's cert (ARNEB_CLUSTER_TLS_CA_PATH)
```

- Flight servers use `ServerTlsConfig::identity(...)`, and clients use `ClientTlsConfig::ca_certificate(...)` with the domain taken from the peer address.
- Address normalization (`main.rs` around lines 597 and 822) and the heartbeat's `flight_address` switch to `https://` when `[cluster.tls]` is present.
- Every node in a cluster must agree. A plaintext node talking to a TLS node fails the connection, and the worker logs the error on its heartbeat.
- It is independent of D5, so an operator can enable either or both. The docs recommend both.

### D7: Spec layering with the unarchived `pgwire-auth` and `trino-client-protocol` changes

`pgwire-auth` and `trino-client-protocol` are implemented but not archived, so `openspec/specs/` has no `pg-auth` or `trino-client-protocol` capability yet. This change:
- Modifies `pg-connection` → "Startup handshake". The full-text replacement builds on the `pgwire-auth` version of that requirement and includes its SCRAM scenarios, so archive `pgwire-auth` first.
- Puts all Trino authentication requirements in a new `trino-auth` capability instead of modifying `trino-client-protocol`'s "Client session headers". This avoids a cross-change MODIFIED against a spec that does not exist yet.

### D8: Web UI binds to loopback under password auth

The Web UI (pgwire port + 1000, `main.rs` around line 754) has no login, and it shows query text and cluster state. Today it binds to `config.server.bind_address`. That means a password-protected server on `0.0.0.0` still publishes every query's SQL to anyone who can reach port 6432.

```toml
[web]
bind_address = "0.0.0.0"   # optional; ARNEB_WEB_BIND_ADDRESS
```

How the bind address is resolved, as a pure function `resolve_web_bind(auth_type, server_bind, web_bind) -> (addr, reason)`:

| `[auth] type` | `[web] bind_address` | Web UI binds to | Logged reason |
|---|---|---|---|
| `none` | unset | `server.bind_address` (unchanged) | `default` |
| `password` | unset | `127.0.0.1` | `loopback (auth=password)` |
| any | set | the configured value | `explicit` |

- The env var overrides the file value.
- The port stays at `port + 1000`.
- The effective `web.bind_address` and reason are logged on `arneb::config` at startup.
- If an explicit non-loopback address is set under password auth, the server logs a WARN that the Web UI is unauthenticated.
- An invalid address is a startup error, the same as the other bind addresses.

## Decisions log (resolved review questions, 2026-10-16)

1. **Password auth with the Trino listener and no `[tls]` is a startup error**, kept as drafted in D4. The rejected alternative was to start anyway and answer every Trino request with 401: it is just as secure, but harder to diagnose.
2. **Web UI**: login is still out of scope. Under password auth, the Web UI binds to `127.0.0.1` by default, with `[web] bind_address` / `ARNEB_WEB_BIND_ADDRESS` to bind elsewhere explicitly, and the effective value is logged (D8).
3. **The default pgwire TLS mode is `required`**, kept as drafted in D1. `optional` remains available for migrations.

## Risks / Trade-offs

- **[Web UI loopback default, D8]** Docker and Kubernetes setups that use password auth and reach the Web UI through a published port lose access until they set `[web] bind_address`. → The startup log states the reason, and `configuration.md` / `distributed.md` show the knob.

- **[Fail-closed startup error, D4]** It can break an existing config. → The error message names both fixes. The CHANGELOG and `configuration.md` call it out.
- **[Basic-auth cache, D3]** A changed password stays valid for up to 5 minutes. → This is acceptable given there is no config hot-reload today, since changing a password already needs a restart, which clears the cache. Documented.
- **[Bearer secret without cluster TLS, D5]** The secret can be sniffed. → The docs recommend D6. Startup logs a WARN when a secret is set without `[cluster.tls]` and the bind address is not loopback.
- **[Hand-rolled HTTPS accept loop, D3]** → It is covered by an integration test with a real HTTPS client (reqwest with a test CA) and by the Tier 2 check with the real `trino` CLI.
- **[Interceptor overhead on the exchange path]** It is one header compare per RPC, not per batch, so it is negligible. The Tier 2 SF1 timing confirms there is no regression.

## Migration Plan

1. Land D1 and D2 (pgwire TLS), D3, D4 and D8 (Trino and Web UI), and D5 and D6 (cluster) as separate PRs in that order. Each section of `tasks.md` is self-contained.
2. Docs: `configuration.md` (new TLS and Cluster security sections), `trino-clients.md` (https and password), and `distributed.md` (secret and TLS). Remove the "Trino bypasses auth" limitation and close #85.
3. No data migration is needed. Rollback means removing the sections from the config.

## Open Questions

None. The three review questions are resolved in the "Decisions log" above.
