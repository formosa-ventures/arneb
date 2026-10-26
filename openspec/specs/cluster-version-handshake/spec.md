# cluster-version-handshake Specification

## Purpose
TBD - created by archiving change cluster-version-handshake. Update Purpose after archive.
## Requirements
### Requirement: Heartbeats carry the worker's release version
Every worker heartbeat SHALL include the worker's release version (the
workspace version it was built from) alongside its id, Flight address and
split capacity. The heartbeat body SHALL be JSON, and fields added in later
releases SHALL default when absent.

#### Scenario: Heartbeat round trip
- **WHEN** a worker built from version 0.1.6 encodes a heartbeat and the coordinator decodes it
- **THEN** the decoded message has `version` `0.1.6` and the same id, address and capacity

### Requirement: The coordinator admits only workers of its own version
The coordinator SHALL register a worker only when the version in its
heartbeat equals the coordinator's own version. Otherwise it SHALL answer
the heartbeat with a `FailedPrecondition` error naming both versions, SHALL
NOT register the worker, and SHALL remove the worker if it was already
registered under that id. No query SHALL be scheduled on a rejected worker.
The coordinator SHALL log the rejection once per worker id and version on
the `arneb::cluster` target.

#### Scenario: Matching version
- **WHEN** a worker with the coordinator's version heartbeats
- **THEN** it is registered and eligible for new queries

#### Scenario: Different version
- **WHEN** a worker with version `0.1.7` heartbeats to a `0.1.6` coordinator
- **THEN** the heartbeat fails with `FailedPrecondition` mentioning `0.1.7` and `0.1.6`, and the worker is not in the alive worker list

#### Scenario: A registered worker comes back with another version
- **WHEN** worker `worker-1` is registered and a heartbeat with id `worker-1` arrives with a different version
- **THEN** `worker-1` is removed from the registry at once

#### Scenario: Pre-handshake worker
- **WHEN** a heartbeat arrives in the legacy `worker_id|flight_address|max_splits` format
- **THEN** it is treated as version `<0.1.6` and rejected like any other mismatch, with an error naming `<0.1.6` and the coordinator's version

#### Scenario: Repeated rejections are logged once
- **WHEN** a rejected worker keeps heartbeating with the same version
- **THEN** the coordinator logs the rejection once, and again only if the worker's version changes

### Requirement: Workers report a version rejection
A worker whose heartbeat is rejected for its version SHALL log the
coordinator's message at error level on the `arneb::cluster` target and
SHALL keep sending heartbeats at the normal interval.

#### Scenario: Coordinator replaced
- **WHEN** a worker is rejected and the coordinator is then replaced by one with the worker's version
- **THEN** the worker registers on its next heartbeat without being restarted

