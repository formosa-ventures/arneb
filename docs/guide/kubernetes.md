# Kubernetes

The Helm chart in `charts/arneb` runs one coordinator and a set of workers
from the release image `ghcr.io/formosa-ventures/arneb`. Every release
publishes the image for `linux/amd64` and `linux/arm64`, tagged with the
full version (`0.1.9`), the minor version (`0.1`) and `latest`. <!-- x-release-please-version -->

## Install

The chart is installed from a checkout of the repository:

```bash
git clone https://github.com/formosa-ventures/arneb
helm install arneb arneb/charts/arneb -f my-values.yaml
```

A minimal `my-values.yaml` declares what every node reads. Tables and
catalogs must be reachable from every pod, usually through object storage:

```yaml
config: |
  [[catalogs]]
  name = "datalake"
  type = "hive"
  metastore_uri = "hive-metastore.data:9083"
  default_schema = "default"

  [storage.s3]
  region = "us-east-1"

worker:
  replicas: 4
  resources:
    requests: { cpu: "4", memory: 16Gi }
    limits: { memory: 16Gi }
```

Then connect through the coordinator's Service:

```bash
kubectl port-forward svc/arneb 5432:5432
psql -h 127.0.0.1 -p 5432
```

The Service also exposes the Trino client protocol (8080), the Web UI and
Prometheus `/metrics` (6432), and Flight RPC (9090), which workers use to
register.

## What the chart deploys

| Object | Purpose |
|---|---|
| Deployment `<release>-coordinator` | One coordinator. It is replaced, never rolled (`strategy: Recreate`), because two coordinators behind one Service would split worker heartbeats between them. |
| Service `<release>` | pgwire, Trino, Web UI and Flight on the coordinator. |
| Deployment `<release>-worker` | `worker.replicas` workers (default 2). Each registers under its pod name and advertises its pod IP. |
| ConfigMap `<release>-config` | `config` as `/etc/arneb/config.toml` on every pod. |
| Secret `<release>-cluster` | The cluster secret, unless `clusterSecret.existingSecret` is set. |
| PodDisruptionBudget `<release>-worker` | At most one worker disrupted at a time. |

The chart sets each pod's role, bind address and pgwire port, and sets
these per pod, so `config` should leave them out of `[cluster]`:

| Variable | Value |
|---|---|
| `ARNEB_CLUSTER_COORDINATOR_ADDRESS` | `<release>:9090` |
| `ARNEB_CLUSTER_WORKER_ID` | the pod name |
| `ARNEB_CLUSTER_ADVERTISED_ADDRESS` | `<pod IP>:9090` |

The chart assumes the default `rpc_port` of 9090. It also turns off
Kubernetes service links (`enableServiceLinks: false`): a Service named
`arneb` would otherwise inject `ARNEB_PORT=tcp://…` into every pod in the
namespace, and Arneb reads `ARNEB_PORT` as its pgwire port. Arneb ignores
an `ARNEB_PORT` or `ARNEB_TRINO_PORT` of that form and logs a warning, but
other service-link variables can still collide, so set the same on any
other pod in the namespace that runs Arneb.

## Values

| Value | Default | Description |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/formosa-ventures/arneb` / chart `appVersion` | Every node must run the same release. |
| `config` | `""` | TOML shared by every pod. |
| `existingConfigSecret` | `""` | Use key `config.toml` of this Secret instead of `config`. |
| `clusterSecret.existingSecret` / `.key` | `""` / `secret` | Cluster secret to use instead of a generated one. |
| `logLevel` | `info,arneb::profile=warn` | `RUST_LOG` for every pod. |
| `extraEnv`, `envFrom` | `[]` | Extra environment, e.g. catalog passwords. |
| `extraVolumes`, `extraVolumeMounts` | `[]` | Extra volumes on every pod, e.g. TLS certificates. |
| `coordinator.service.type` | `ClusterIP` | Use `LoadBalancer` to reach the coordinator from outside the cluster. |
| `coordinator.trino.enabled` | `true` | Trino client protocol listener. |
| `worker.replicas` | `2` | Number of workers. |
| `worker.drainTimeoutSecs` | `120` | `ARNEB_DRAIN_TIMEOUT_SECS` on workers. |
| `worker.terminationGracePeriodSeconds` | drain timeout + 30 | Must stay above the drain timeout. |
| `worker.podDisruptionBudget.enabled` | `true` | |
| `coordinator.*` / `worker.*` `resources`, `nodeSelector`, `tolerations`, `affinity`, `podAnnotations`, `extraArgs` | empty | Per role. |

Pods run as uid 10001 with no added capabilities. Changing `config`
restarts every pod.

## Secrets

- **Cluster secret.** Without `clusterSecret.existingSecret`, the chart
  generates a random 48-character secret on install and keeps it on
  upgrade. Every pod reads it as `ARNEB_CLUSTER_SECRET`. To rotate it,
  update the Secret and restart all pods together.
- **Users and credentials.** A config with `[auth]` users or storage keys
  belongs in a Secret: create one with a `config.toml` key and set
  `existingConfigSecret`. Catalog passwords can also come from
  `ARNEB_CATALOG_<NAME>_PASSWORD` through `extraEnv` or `envFrom`.
- **TLS.** Mount certificates with `extraVolumes` / `extraVolumeMounts` and
  point `[tls]` and `[cluster.tls]` at them. Workers advertise their pod IP,
  so each worker's cluster TLS certificate must carry its pod IP as an IP
  SAN. Certificates have no IP wildcards, so this means one certificate per
  pod, for example from cert-manager's CSI driver. The chart does not yet
  give workers stable DNS names. See
  [Securing the Cluster](/guide/distributed#securing-the-cluster).
- With password auth, the Web UI binds to `127.0.0.1` unless `[web]
  bind_address` is set, so the Service's web port only answers when it is.

## Restarts and upgrades

When Kubernetes stops a worker (a rollout, a node drain, a scale-down), the
worker drains: it stops taking new queries, finishes the ones it runs, and
then exits. Queries keep succeeding while workers roll one at a time. See
[Stopping a Worker](/guide/distributed#stopping-a-worker).

An upgrade to a new Arneb release is a short full restart instead: the
coordinator is replaced, it admits only workers on its own release, and the
queries in flight fail. Run it when the cluster is quiet. See
[Versions](/guide/distributed#versions).

## Acceptance test

`.github/workflows/kubernetes.yml` runs `scripts/k8s-smoke/run.sh` on every
pull request. It installs the chart on kind with 1 coordinator and 2
workers, runs the 22 TPC-H queries at SF1 and checks each result against
DuckDB, deletes a worker pod while queries run on it, and checks that the
queries succeed, the replacement registers, and an upgrade keeps the
cluster secret. To run it locally (needs Docker, kind, Helm, and Python
with `duckdb` and `psycopg[binary]`):

```bash
docker build -f docker/arneb/Dockerfile -t arneb:ci .
python scripts/k8s-smoke/gen_tpch.py /tmp/tpch 1
scripts/k8s-smoke/run.sh /tmp/tpch
```
