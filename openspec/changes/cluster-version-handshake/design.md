## Context

Workers register by sending a `heartbeat` Flight action to the coordinator
every `heartbeat_interval_secs`. The body is `worker_id|flight_address|max_splits`
(`crates/rpc/src/heartbeat.rs`), and `FlightState`'s heartbeat callback
upserts the worker in `NodeRegistry`. Nothing in it says which build the
worker runs.

Everything the coordinator later sends a worker (`TaskDescriptor`, the
serialized plan fragment, dynamic-filter payloads) is serde JSON over types
that change in most releases. Worker-to-worker exchange runs only between
workers the coordinator scheduled, so the coordinator is the one place that
needs to decide whether a worker may join.

## Goals / Non-Goals

**Goals:**
- No query is ever scheduled on a worker running a different release.
- An operator can tell from either side's log, and from the Web UI, why a
  worker is not in the cluster.
- The heartbeat format can grow new fields without another break (G4 adds
  the drain state next).

**Non-Goals:**
- Mixed-version clusters that keep serving queries. A rolling upgrade
  serves from the workers that match the coordinator, and the rest rejoin
  as they are replaced (see Migration Plan).
- Changing the peer address format (#156 item 1). This change makes that
  possible later; it does not do it.
- A version check on worker-to-worker or coordinator-to-worker calls. Those
  peers are registered workers, which have already passed the check.

## Decisions

### Exact release-version match

The coordinator compares the worker's `CARGO_PKG_VERSION` with its own and
rejects any difference.

- *Alternative: a hand-bumped protocol integer* (accept any release with the
  same protocol number). It would let patch releases mix, but correctness
  then depends on a developer remembering to bump it whenever a serde type
  changes, and nothing checks that. Trino chose exact match for the same
  reason.
- *Alternative: semver-compatible ranges.* Same problem. Arneb is 0.x, and
  the serialized types change in patch releases.

The version comes from the workspace `version` (all crates share it), so
`arneb-rpc`'s `env!("CARGO_PKG_VERSION")` is the server's version.

### JSON heartbeat body

`HeartbeatMessage` becomes a serde struct encoded as JSON, like
`TaskDescriptor`. New fields get `#[serde(default)]`, so additions after
this change stay readable by both sides of the same release line.

A body that does not parse as JSON but splits into the three legacy
fields decodes with the version `<0.1.6`. It matches no release, so a
pre-handshake worker goes through the same refusal path as any mismatch:
logged once, removed if registered, and told
`worker '<id>' runs arneb <0.1.6 but the coordinator runs <version>`.

### Rejection is `FailedPrecondition` and leaves the worker unregistered

`do_action("heartbeat")` checks the version before calling the heartbeat
callback. On a mismatch it returns `Status::failed_precondition` with both
versions. If the worker id is already registered (the process was replaced
by a different build under the same id), the coordinator removes it at once
instead of waiting for the heartbeat timeout.

`NodeRegistry` keeps the last rejected version per worker id so the
coordinator logs `worker rejected: version mismatch` (warn, `arneb::cluster`)
once per worker and version, not on every heartbeat.

On the worker, `send_heartbeat_with` maps `FailedPrecondition` to a new
`HeartbeatError::Incompatible`, which the heartbeat loop logs at error level
and keeps retrying, like `Unauthenticated`.

### Version in the worker list

`WorkerInfo` gains `version`. `GET /api/v1/cluster/workers` returns it and
the Cluster page shows it on each card. With only matching workers
registered it always equals the coordinator's version; it is there so an
operator does not have to infer that.

## Risks / Trade-offs

- [A rolling upgrade shrinks the usable cluster until every node is
  replaced] → Documented. Helm (P6) upgrades the coordinator first and the
  workers right after; G4's drain keeps in-flight queries on old workers
  alive meanwhile. An old worker is also dropped as soon as the new
  coordinator starts, because the registry is in memory.
- [Dev builds share a version string across commits] → Accepted. The check
  protects released builds; local clusters are built from one tree.
- [A typo in a hand-built image's version] → Not possible: the version is
  compiled in from `Cargo.toml`.

## Migration Plan

v0.1.5 and v0.1.6 nodes cannot talk to each other in either direction, so
upgrading from v0.1.5 means upgrading the coordinator and all workers
together. This goes in the release notes and `docs/guide/distributed.md`.
Rolling back is the same: downgrade every node.
