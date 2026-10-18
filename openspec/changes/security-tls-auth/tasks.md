Sections 1–2, 3–4b and 5 can land as three separate PRs, in that order. Section 6 is the gate for each PR's slice, and section 7 runs once at the end.

## 1. Shared TLS config and loading

- [x] 1.1 `[tls]` config in `crates/server/src/config.rs`: a `TlsConfig { cert_path, key_path, pgwire: PgwireTlsMode }` with `deny_unknown_fields`, overridden by `ARNEB_TLS_CERT_PATH`, `ARNEB_TLS_KEY_PATH` and `ARNEB_TLS_PGWIRE`. Validation: both paths required; reject an unknown mode.
- [x] 1.2 `crates/protocol/src/tls.rs`: load the PEM chain and key into a `rustls::ServerConfig` (aws-lc-rs provider). Reject an empty chain, a missing key, or a key that does not match the cert. Build two configs, with ALPN `postgresql` for pgwire and `h2, http/1.1` for Trino. Make `tokio-rustls` and `rustls` direct dependencies of `protocol`, at the versions already in `Cargo.lock`.
- [x] 1.3 Unit tests with an `rcgen`-generated test CA and leaf (dev-dependency): a valid pair loads; a missing file, an empty PEM and a mismatched key each produce a named error; the env override wins over the file; an unknown `pgwire` mode is rejected.

## 2. pgwire TLS

- [x] 2.1 `ProtocolServer::with_tls(Option<TlsAcceptor>)`, passed to `pgwire::tokio::process_socket` in `serve()`. Log `tls=required|optional|off` in the "listening" line.
- [x] 2.2 `AuthStartupHandler`: in `required` mode, reject a StartupMessage with `!client.is_secure()` with `FATAL 28000 "SSL connection is required"` before any SASL exchange. This applies in both `none` and `password` auth modes.
- [x] 2.3 Integration tests (`crates/protocol/tests/tls.rs`, ephemeral port, tokio-postgres plus `tokio-postgres-rustls` with the test CA):
  - `sslmode=require` and verify-full connect and run queries.
  - Plaintext is rejected with `28000` in `required` mode and accepted in `optional` mode.
  - TLS combined with SCRAM password auth works.
  - Without `[tls]`, the server still answers `N` (regression).
- [x] 2.4 `main.rs`: build the acceptor from config (failure is a startup error) and log the effective mode on `arneb::config`.

## 3. Trino HTTPS

- [x] 3.1 `TrinoServer::with_tls(Option<TlsAcceptor>)`. In `serve()`, run a rustls accept loop feeding `hyper_util` `auto::Builder::serve_connection` with the router when TLS is set, and keep `axum::serve` unchanged otherwise. A handshake error logs at `debug` and drops only that connection.
- [x] 3.2 Integration test: reqwest with the test CA posts `SELECT 1` over https and pages to completion. Plain HTTP to the TLS port fails without killing the listener.

## 4. Trino Basic auth (closes #85)

- [x] 4.1 `ScramVerifier::verify_password(&str) -> bool` in `crates/protocol/src/auth.rs`: SASLprep, then PBKDF2 with the stored salt and iterations, then a constant-time StoredKey compare. Unknown users use the decoy verifier. Unit tests: right password, wrong password, unknown user, and a verifier from `postgres-protocol`.
- [x] 4.2 `trino/auth.rs`: an axum middleware that requires Basic auth in password mode, except on `/v1/info`, `/v1/info/state` and `/v1/status`. On failure it returns `401` with `WWW-Authenticate: Basic realm="arneb"`. It puts the authenticated user in request extensions.
- [x] 4.3 Verification cache: a bounded (1024 entries) TTL (5 min) map keyed by `(user, HMAC-SHA256(per-process key, password))`. Successes only.
- [x] 4.4 User binding: the authenticated user overrides the session user. A differing `X-Trino-User` / `X-Presto-User` gets `403`. `Query` gets an `owner`; `fetch`, `cancel`, `query_info` and `kill` by a non-owner get `403`; `/v1/query` is filtered by owner. In `none` mode, the behavior is unchanged.
- [x] 4.5 Fail closed: `[auth] type = "password"` with the Trino listener enabled and no `[tls]` is a startup error naming `[tls]` and `--no-trino`. Remove the WARN at `main.rs` (around line 697).
- [x] 4.6 Integration tests (`crates/protocol/tests/trino_auth.rs`):
  - Right password, wrong password, missing header and unknown user each produce the expected status.
  - `/v1/info` works without credentials.
  - Impersonation gets `403`.
  - Another user's `nextUri` gets `403`.
  - 100 polls with the same credentials verify once (counted with a test hook).
  - `none` mode regression.

## 4b. Web UI loopback default (design D8)

- [x] 4b.1 Config: `[web] bind_address` and `ARNEB_WEB_BIND_ADDRESS`. Add a pure `resolve_web_bind(auth_type, server_bind, web_bind) -> (addr, reason)` in `crates/server/src/config.rs`. Use it at the Web UI bind in `main.rs` (around line 755), log the effective address and reason on `arneb::config`, and WARN when an explicit non-loopback address is used under password auth.
- [x] 4b.2 Unit tests for `resolve_web_bind`:
  - `none` + unset keeps `server.bind_address` (`default`).
  - `password` + unset gives `127.0.0.1` (`loopback (auth=password)`).
  - An explicit value wins in both modes (`explicit`).
  - The env override beats the file value.
  - An unparseable address is a startup error.

## 5. Cluster RPC auth and TLS

- [x] 5.1 Config: `[cluster] secret` and `ARNEB_CLUSTER_SECRET` (minimum 32 characters, redacted in `Debug`); `[cluster.tls] { cert_path, key_path, ca_path }` and `ARNEB_CLUSTER_TLS_*`. Log `cluster.auth` and `cluster.tls` on `arneb::config`. WARN when there is a secret, no TLS, and a non-loopback bind.
- [x] 5.2 `crates/rpc/src/channel.rs`: `ClusterSecurity { secret: Option<..>, tls: Option<ClientTlsConfig> }` and `connect(address, &ClusterSecurity)`, which returns an intercepted `FlightServiceClient`. Replace the three `Channel::from_shared` sites (`task_submission.rs`, `heartbeat.rs`, `exchange_client.rs`).
- [x] 5.3 Server: `FlightServiceServer::with_interceptor` with a constant-time bearer check when a secret is set. Add `ServerTlsConfig` to `start_flight_server` when `[cluster.tls]` is set. Enable tonic's `tls-aws-lc` feature.
- [x] 5.4 Scheme handling: `rpc::connect` upgrades `http://` peer addresses to `https://` when `[cluster.tls]` is set (one place instead of the `http://` normalization sites in `main.rs` / `coordinator.rs`; see D6).
- [x] 5.5 Rejection handling: the coordinator logs a rate-limited WARN (one per peer per minute) on `arneb::cluster` for `UNAUTHENTICATED` heartbeats. A worker that receives `UNAUTHENTICATED` logs an ERROR naming `[cluster] secret` and keeps its heartbeat loop running.
- [x] 5.6 Tests (`crates/rpc`, ephemeral ports):
  - With the matching secret, heartbeat, `submit_task` and `do_get` succeed.
  - With a wrong or missing secret, each returns `UNAUTHENTICATED` and the heartbeat callback is never invoked, so the worker is not registered.
  - With no secret on either side, behavior is unchanged.
  - With TLS and the test CA, `do_get` succeeds; with a cert from an unknown CA, it fails.

## 6. Tier 1 — local correctness gate (every PR)

- [x] 6.1 `cargo fmt -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` are clean.
- [x] 6.2 `cargo test -p arneb-protocol -p arneb-rpc -p arneb-server` passes, including all new integration tests.
- [x] 6.3 The full `cargo test` workspace run is green. With no security config, there are no behavior diffs, so all existing tests pass unchanged.

## 7. Tier 2 — local distributed check (real clients, real cluster)

- [x] 7.1 Generate a local CA and certs (a script under `scripts/dev-certs.sh`, output git-ignored). Run a coordinator and 2 workers locally with `[tls]`, `[auth] type = "password"`, `[cluster] secret` and `[cluster.tls]`, against the docker-compose Hive SF1 data.
- [x] 7.2 `psql "sslmode=verify-full sslrootcert=…"` runs TPC-H q01 and q03. `sslmode=disable` is rejected with `28000`.
- [x] 7.3 `trino --server https://localhost:<port> --truststore-path … --user … --password` runs q01. A wrong password gets 401.
- [x] 7.4 Restart worker-2 with a wrong secret. The coordinator logs the rejection, the Web UI and node registry show only worker-1, and queries still complete on the remaining worker.
- [x] 7.4b Check the Web UI binding on the coordinator, with `bind_address = "0.0.0.0"` and password auth:
  - `curl http://127.0.0.1:<port+1000>/` answers.
  - A non-loopback interface address on that port is refused.
  - Restart with `ARNEB_WEB_BIND_ADDRESS=0.0.0.0`: the Web UI is reachable on the interface address, and the `explicit` reason plus the unauthenticated WARN appear in the log.
- [x] 7.5 Confirm the new `arneb::config` and `arneb::cluster` log lines actually appear under the default `RUST_LOG` (the CLAUDE.md Tier 2 plumbing rule).
- [x] 7.6 Run the SF1 TPC-H 22 with all security on versus all off: cell-identical, and latency within run-to-run noise. Record both numbers in this file.
- [x] 7.7 Tier 3 (remote SF30) is not needed. This change carries no performance claim, and per-RPC interceptor cost is validated at Tier 2.

### Tier 2 results (2026-10-17)

Native release build, coordinator + 2 workers on one host, against the docker-compose Hive data. The resident dataset was SF10 (re-seeded for #135), not SF1; every check below ran at SF10.

- 7.1: `scripts/dev-certs.sh` writes a CA plus a `localhost` / `127.0.0.1` / `host.docker.internal` certificate to `.dev-certs/` (git-ignored). One pair served pgwire, the Trino listener and all three Flight servers. Both workers registered over secret + TLS.
- 7.2: `psql sslmode=verify-full` ran q01 and q03 (by host name and by IP). `sslmode=disable` got `FATAL: SSL connection is required`; a wrong password over TLS got `password authentication failed`. libpq does not print the SQLSTATE for connection-time errors; `28000` is asserted in `crates/protocol/tests/tls.rs`.
- 7.3: the `trino` 483 CLI (`--server https://host.docker.internal:18080 --truststore-path ca.crt --password`) ran q01 (4 rows). A wrong password got `Authentication failed ... UNAUTHORIZED`, and `--session-user bob` got 403. **Tier 2 found a bug here:** h2 is negotiated on HTTPS and carries the host in `:authority`, not `Host`, so `nextUri` fell back to `https://localhost/...` and the CLI (in a container) aborted. Fixed by copying the authority into `Host`; a regression test (`next_uri_uses_the_h2_authority`) fails without the fix.
- 7.4: worker-2 restarted with a wrong `ARNEB_CLUSTER_SECRET`. It logged `ERROR arneb::cluster: heartbeat rejected ... check [cluster] secret` every 10 s and kept looping. The coordinator logged one rate-limited `WARN arneb::cluster: rejected cluster RPC ... peer=127.0.0.1:…`, marked worker-2 dead after the 30 s heartbeat timeout, and `/api/v1/cluster/workers` listed only worker-1. q01 completed on the remaining worker.
- 7.4b: with `bind_address = "0.0.0.0"` and password auth, `127.0.0.1:16432` answered 200 and the LAN address (`192.168.68.52:16432`) was refused. With `ARNEB_WEB_BIND_ADDRESS=0.0.0.0`, the LAN address answered 200, and the log showed `reason="explicit"` plus the unauthenticated-Web-UI WARN.
- 7.5: under the default `RUST_LOG`, the coordinator logged `cluster_auth="secret" cluster_tls="on"`, `tls_pgwire="required"`, `tls_trino="https" trino_auth="password (basic)"` and the Web UI bind with `reason="loopback (auth=password)"`; the `arneb::cluster` lines in 7.4 also appeared.
- 7.6: TPC-H 22 at SF10, 2 runs each, all security on (pgwire TLS + SCRAM, cluster secret + TLS) vs all off:
  - Results: 22/22 identical within a relative 1e-12. 18/22 are byte-identical; q09, q10, q17 and q22 differ only in the last digits of DOUBLE sums. That is run-to-run noise: two back-to-back q09 runs on the all-off cluster also differ in the same digits.
  - Latency, geomean on/off: run 1 (cold) 1.107×, worst q15 1.62× (40.5 s vs 35.4 s total); run 2 (warm) 1.001×, worst q02 1.14× (35.9 s vs 37.8 s total). Each query opens a new psql connection (TLS handshake + SCRAM, 4096 iterations). The warm run shows no overhead beyond noise. The cold-run spread is within the documented per-query noise (worst 29% at SF10) apart from q15, which was 1318 ms vs 816 ms in run 1 but 797 ms vs 773 ms (1.03×) in run 2: a first-run outlier, not a per-RPC cost.

## 8. Docs

- [x] 8.1 `docs/guide/configuration.md`: add a TLS section and a Cluster security section, document the fail-closed rule and the Web UI loopback default with `[web] bind_address`, and list the new `ARNEB_*` variables.
- [x] 8.2 `docs/guide/trino-clients.md`: https and password examples for the CLI, JDBC and the Python client. Remove the "Trino bypasses auth" limitation.
- [x] 8.3 `docs/guide/distributed.md`: the cluster secret and TLS setup. Recommend using both.
- [ ] 8.4 Close #85 when the section 4 PR merges.
