## ADDED Requirements

### Requirement: Heartbeat settings
`[cluster]` SHALL accept `heartbeat_interval_secs` (how often a worker sends
its heartbeat; default 2) and `heartbeat_timeout_secs` (how long the
coordinator waits before treating a silent worker as lost; default 10).
`ARNEB_HEARTBEAT_INTERVAL_SECS` and `ARNEB_HEARTBEAT_TIMEOUT_SECS` SHALL
override them. Both SHALL be at least 1, the timeout SHALL be greater than
the interval, and the effective values SHALL be logged at startup.

#### Scenario: Defaults
- **WHEN** neither setting nor variable is given
- **THEN** workers heartbeat every 2 s and the coordinator's timeout is 10 s

#### Scenario: Invalid combination
- **WHEN** `heartbeat_timeout_secs = 2` and `heartbeat_interval_secs = 2`
- **THEN** startup fails with an error naming both settings
