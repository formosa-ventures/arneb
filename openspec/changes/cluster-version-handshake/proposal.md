## Why

A coordinator schedules work on any worker that heartbeats, whatever
version it runs. Task descriptors and plan fragments are serde JSON with no
compatibility promise between releases, so a mixed-version cluster (the
normal state during a Helm rolling upgrade, P6) fails queries with decode
errors or, worse, runs a plan the worker reads differently. This is gap G5
in the "Path to production" plan
(`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`). It also
unblocks #156 item 1 (scheme-less peer addresses), which waits on a way to
change the wire format safely.

## What Changes

- **Heartbeats carry the worker's release version.** The heartbeat body
  becomes JSON (`worker_id`, `flight_address`, `max_splits`, `version`),
  so later fields (G4's drain state) are additive. **BREAKING** on the wire:
  a v0.1.5 coordinator cannot read a v0.1.6 heartbeat and the reverse. Both
  directions end in a rejected worker, which is the intended outcome.
- **The coordinator rejects a worker whose version differs from its own.**
  The rule is exact equality of the release version, as Trino does. A
  rejected worker is not registered, so no query is scheduled on it. The
  coordinator answers `FailedPrecondition` with both versions and logs the
  rejection once per worker and version on `arneb::cluster`.
- **Workers report a rejection clearly.** The heartbeat loop logs a
  rejected version at error level, like a rejected cluster secret, and
  keeps retrying, so replacing the coordinator recovers without restarting
  workers.
- **The worker list shows each worker's version**:
  `GET /api/v1/cluster/workers` gains `version`, and the Cluster page shows
  it on each worker card.

## Capabilities

### New Capabilities

- `cluster-version-handshake`: version in the heartbeat, the exact-match
  rule, and how both sides report a rejection.

### Modified Capabilities

- `web-ui-cluster`: worker cards and the workers API include the version.

## Impact

- `crates/rpc/src/{heartbeat,flight_service}.rs` (wire format, check)
- `crates/scheduler/src/node_registry.rs` (`WorkerInfo.version`, rejection log)
- `crates/server/src/main.rs` (heartbeat loop), `crates/server/src/web/api.rs`
- `web/src/{lib/types.ts,pages/Cluster.tsx}` and the rebuilt frontend bundle
- `docs/guide/distributed.md` (upgrade note: upgrade coordinator and
  workers together)
