# cluster-rpc-auth Specification

## Purpose
TBD - created by archiving change security-tls-auth. Update Purpose after archive.
## Requirements
### Requirement: Cluster shared-secret authentication
When `[cluster] secret` is configured, every Flight RPC client in the process SHALL send `authorization: Bearer <secret>` metadata on every call, and every Flight RPC server in the process SHALL verify it in constant time before any handler runs. A missing or wrong secret SHALL be rejected with gRPC status `UNAUTHENTICATED`. This SHALL cover every action and stream, including `heartbeat` (worker registration), `submit_task`, task status, `do_get` and `do_put`. When no secret is configured, no interceptor SHALL be installed and RPC behavior SHALL be unchanged.

#### Scenario: Matching secret
- **WHEN** the coordinator and two workers share the same secret and a distributed query runs
- **THEN** the workers register and the query returns the same result as without a secret

#### Scenario: Worker with a wrong secret
- **WHEN** a worker whose secret differs from the coordinator's sends a heartbeat
- **THEN** the coordinator returns `UNAUTHENTICATED`, never adds the worker to the node registry, and logs a rate-limited WARN on `arneb::cluster` with the peer address
- **AND** the worker logs an ERROR naming `[cluster] secret` and keeps retrying on its heartbeat interval

#### Scenario: Worker without a secret
- **WHEN** the coordinator has a secret and a worker has none
- **THEN** the worker's heartbeat is rejected with `UNAUTHENTICATED` and it is never registered

#### Scenario: Unauthenticated task submission
- **WHEN** a client without the secret calls `submit_task` or `do_get` on a worker that has a secret
- **THEN** the call fails with `UNAUTHENTICATED` and no task is created or read

### Requirement: Optional cluster TLS
When `[cluster.tls]` is configured, every Flight RPC server SHALL serve TLS with the node's `cert_path` and `key_path`. Every Flight RPC client SHALL connect over `https://` and verify the peer against `ca_path`. Advertised and normalized peer addresses SHALL use the `https://` scheme. Cluster TLS SHALL be independent of the shared secret.

#### Scenario: TLS cluster
- **WHEN** the coordinator and both workers configure `[cluster.tls]` with certificates signed by the same CA
- **THEN** workers register, and a distributed TPC-H SF1 query returns results identical to the plaintext cluster

#### Scenario: Peer certificate from an unknown CA
- **WHEN** a worker presents a certificate not signed by the coordinator's `ca_path`
- **THEN** the connection fails and the worker is not registered

