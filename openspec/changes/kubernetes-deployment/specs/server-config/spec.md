## ADDED Requirements

### Requirement: Per-node cluster settings from the environment
The server SHALL read `ARNEB_CLUSTER_COORDINATOR_ADDRESS`,
`ARNEB_CLUSTER_WORKER_ID` and `ARNEB_CLUSTER_ADVERTISED_ADDRESS` and, when
set, use them in place of `[cluster]` `coordinator_address`, `worker_id` and
`advertised_address` (env > file). An empty value SHALL be rejected with an
error naming the variable.

#### Scenario: One config file for every worker
- **WHEN** two workers load the same config file, with different `ARNEB_CLUSTER_WORKER_ID` and `ARNEB_CLUSTER_ADVERTISED_ADDRESS` values
- **THEN** each registers with the coordinator under its own id and address

#### Scenario: Env wins over the file
- **WHEN** the file sets `worker_id = "a"` and `ARNEB_CLUSTER_WORKER_ID=b`
- **THEN** the worker registers as `b`

#### Scenario: Empty value
- **WHEN** `ARNEB_CLUSTER_WORKER_ID` is set to an empty string
- **THEN** startup fails with an error naming `ARNEB_CLUSTER_WORKER_ID`
