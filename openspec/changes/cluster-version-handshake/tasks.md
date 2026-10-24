## 1. Wire format

- [x] 1.1 `HeartbeatMessage`: serde JSON with a `version` field (`#[serde(default)]` for later additions); `HeartbeatMessage::new` fills `version` from `CARGO_PKG_VERSION`
- [x] 1.2 Decode reads the legacy three-field body as version `<0.1.6` (`LEGACY_VERSION`), so it is refused like any mismatch
- [x] 1.3 Unit tests: round trip, legacy body, garbage body

## 2. Coordinator check

- [x] 2.1 `do_action("heartbeat")`: compare versions before the callback; `FailedPrecondition` with both versions on a mismatch or legacy body
- [x] 2.2 `NodeRegistry`: `WorkerInfo.version`; `reject(worker_id, version)` removes a registered worker and logs once per worker and version
- [x] 2.3 Tests: matching version registers, mismatch is rejected and not alive, registered worker removed on mismatch, rejection logged once

## 3. Worker side

- [x] 3.1 `HeartbeatError::Incompatible` for `FailedPrecondition`; heartbeat loop logs it at error level on `arneb::cluster` and keeps retrying
- [x] 3.2 Tests: a refused heartbeat is `Incompatible` on the worker; a legacy body reaches the callback with `LEGACY_VERSION`

## 4. Web UI

- [x] 4.1 `WorkerResponse.version` in `GET /api/v1/cluster/workers`
- [x] 4.2 `web/src/lib/types.ts` + `Cluster.tsx` show the version; rebuild the embedded bundle

## 5. Docs and validation

- [x] 5.1 `docs/guide/distributed.md`: version rule and "upgrade every node together from 0.1.5"
- [x] 5.2 Tier 1: `cargo test` for rpc, scheduler and server; clippy and fmt clean
- [x] 5.3 Tier 2 (native, 2026-10-24): coordinator + 2 workers from this tree register, the workers API shows `version`, q06 succeeds. A 0.1.5-format binary as worker-3 is refused: the coordinator logs `worker rejected: version mismatch` once (`version="<0.1.6"`), the worker gets the message on every heartbeat, and it is never listed. Restarting worker-1 on the old binary removes it at once (`removed=true`). The new worker's error-level `heartbeat rejected` line is covered by the Tier 1 test only (no second release to run against).
