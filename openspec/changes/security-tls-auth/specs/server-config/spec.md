## ADDED Requirements

### Requirement: [tls] config section
The system SHALL accept an optional `[tls]` TOML section with `cert_path` and `key_path` (both required when the section is present), plus `pgwire` (`"required"` by default, or `"optional"`). Unknown keys SHALL be rejected. `ARNEB_TLS_CERT_PATH`, `ARNEB_TLS_KEY_PATH` and `ARNEB_TLS_PGWIRE` SHALL override the file values, and setting both path variables SHALL enable TLS without a `[tls]` section. The effective TLS mode SHALL be logged at startup on the `arneb::config` target, for example `tls.pgwire="required" tls.trino="https"`, or `tls="off"`.

#### Scenario: Section omitted
- **WHEN** the config has no `[tls]` section and no `ARNEB_TLS_*` variables are set
- **THEN** both listeners run without TLS and the server logs `tls="off"`

#### Scenario: Only one path configured
- **WHEN** `[tls]` sets `cert_path` but not `key_path`
- **THEN** startup fails with a configuration error naming `key_path`

#### Scenario: Unknown pgwire mode
- **WHEN** `[tls] pgwire = "prefer"`
- **THEN** startup fails with a configuration error listing `required` and `optional`

### Requirement: Web UI bind address
The system SHALL accept an optional `[web] bind_address`, overridable by `ARNEB_WEB_BIND_ADDRESS`, for the Web UI listener; the Web UI port stays at the pgwire port + 1000. When `[web] bind_address` is unset, the Web UI SHALL bind to `127.0.0.1` if `[auth] type = "password"`, and to the server's `bind_address` otherwise. An explicitly configured value SHALL always be used. The effective Web UI bind address and the reason for it (`default`, `loopback (auth=password)` or `explicit`) SHALL be logged on the `arneb::config` target. A WARN SHALL be logged when password auth is on and the Web UI is explicitly bound to a non-loopback address. An unparseable address SHALL be a startup error.

#### Scenario: No auth, no web setting
- **WHEN** `[auth]` is absent, `bind_address = "0.0.0.0"`, and `[web]` is not configured
- **THEN** the Web UI binds to `0.0.0.0:<port+1000>` exactly as before, with reason `default`

#### Scenario: Password auth defaults to loopback
- **WHEN** `[auth] type = "password"`, `bind_address = "0.0.0.0"`, and `[web] bind_address` is unset
- **THEN** the Web UI binds to `127.0.0.1:<port+1000>` and logs reason `loopback (auth=password)`

#### Scenario: Explicit bind under password auth
- **WHEN** `[auth] type = "password"` and `ARNEB_WEB_BIND_ADDRESS=0.0.0.0`
- **THEN** the Web UI binds to `0.0.0.0:<port+1000>`, logs reason `explicit`, and logs a WARN that the Web UI is unauthenticated

### Requirement: Cluster security settings
The `[cluster]` section SHALL accept an optional `secret` (overridable by `ARNEB_CLUSTER_SECRET`) and an optional `[cluster.tls]` subsection with `cert_path`, `key_path` and `ca_path`, all required when the subsection is present (overridable by `ARNEB_CLUSTER_TLS_CERT_PATH`, `ARNEB_CLUSTER_TLS_KEY_PATH` and `ARNEB_CLUSTER_TLS_CA_PATH`). A secret shorter than 32 characters SHALL be a startup error. The secret SHALL be redacted from `Debug` and `Display` output and from logs. Startup SHALL log whether cluster auth and cluster TLS are on, without the secret value, and SHALL log a WARN when a secret is set without `[cluster.tls]` on a non-loopback bind address.

#### Scenario: Secret from the environment
- **WHEN** `ARNEB_CLUSTER_SECRET` holds a 40-character value and the file has no `secret`
- **THEN** the node starts with cluster auth on and logs `cluster.auth="secret"` without printing the value

#### Scenario: Short secret
- **WHEN** `[cluster] secret` is 10 characters long
- **THEN** startup fails with a configuration error stating the 32-character minimum

#### Scenario: Secret without cluster TLS on a public interface
- **WHEN** a secret is set, `[cluster.tls]` is absent, and the bind address is `0.0.0.0`
- **THEN** the node starts and logs a WARN that the secret crosses the network unencrypted
