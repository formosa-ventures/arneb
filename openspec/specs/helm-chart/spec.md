# helm-chart Specification

## Purpose
TBD - created by archiving change kubernetes-deployment. Update Purpose after archive.
## Requirements
### Requirement: Chart workloads
The chart `charts/arneb` SHALL deploy one coordinator (a Deployment with one
replica that is replaced, never rolled, so two coordinators never run at
once) behind a Service exposing pgwire, the Trino port, the Web UI and
Flight RPC, and `worker.replicas` workers (default 2) that register with the
coordinator through that Service. Each worker SHALL use its pod name as its
worker id and its pod IP as its advertised address.

#### Scenario: Default install
- **WHEN** the chart is installed with default values and a config
- **THEN** the coordinator lists two active workers, each named after its pod

### Requirement: Shared config and secrets
The chart SHALL put the `config` value (TOML shared by every pod) in a
ConfigMap, or use `existingConfigSecret` instead when set. It SHALL supply
`ARNEB_CLUSTER_SECRET` to every pod from `clusterSecret.existingSecret`, or
from a random secret it generates on install and keeps across upgrades.
Changing `config` SHALL restart the pods.

#### Scenario: Generated secret survives an upgrade
- **WHEN** the chart is installed without `clusterSecret.existingSecret` and then upgraded
- **THEN** every pod uses the same cluster secret before and after the upgrade

### Requirement: Kubernetes lets a drain finish
Workers SHALL get `ARNEB_DRAIN_TIMEOUT_SECS` from `worker.drainTimeoutSecs`
(default 120) and a `terminationGracePeriodSeconds` greater than it
(default: the drain timeout plus 30 s). A PodDisruptionBudget SHALL allow at
most one worker to be disrupted at a time.

#### Scenario: Grace period follows the drain timeout
- **WHEN** `worker.drainTimeoutSecs` is 300
- **THEN** worker pods have `terminationGracePeriodSeconds: 330`

### Requirement: Acceptance test on kind
CI SHALL install the chart on kind with 1 coordinator and 2 workers, run the
22 TPC-H queries at SF1, and fail unless every result matches DuckDB
running the same query on the same data. It SHALL then delete a worker pod and pass
only if the replacement registers and a query succeeds.

#### Scenario: All 22 match
- **WHEN** the kind workflow runs on a pull request
- **THEN** it reports 22/22 TPC-H queries matching DuckDB, and a query succeeds after a worker pod is replaced

