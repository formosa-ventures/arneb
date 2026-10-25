## Context

`arneb` is one binary; `--role` picks standalone, coordinator or worker.
A worker needs three per-node settings: the coordinator's Flight address,
its own id, and the address peers dial back (`advertised_address`,
defaulting to `bind_address:rpc_port`, which is useless as `0.0.0.0`). They
exist only in `[cluster]`, so today every worker needs its own TOML file.

Ports: pgwire (`port`, 5432), Web UI and `/metrics` (`port + 1000`), the
Trino REST listener (8080), Flight RPC (`rpc_port`, 9090). Since 0.1.6,
workers drain on SIGTERM for up to `drain_timeout_secs` (default 120), and
the coordinator admits only workers on its own release.

Releases are cut by release-please from `release.yml`. Its tag is pushed
with `GITHUB_TOKEN`, so an `on: push: tags` workflow would never fire; the
image build has to be called from the release run.

## Goals / Non-Goals

**Goals:** a published multi-arch image per release, a chart that runs a
coordinator plus N workers with secrets kept out of the ConfigMap, a drain
that Kubernetes does not cut short, and a CI test on kind that proves
results are correct.

**Non-Goals:** coordinator high availability, autoscaling workers,
operators/CRDs, shipping Hive Metastore or object storage in the chart,
release binaries outside the image.

## Decisions

### Image

`docker/arneb/Dockerfile` is a two-stage build: `rust:1-bookworm` builds
`--release --bin arneb` with BuildKit cache mounts, and
`debian:bookworm-slim` with `ca-certificates` runs it as uid 10001. The
image has no config, so the chart (or `docker run -v`) supplies one.
The frontend bundle is committed, so the build needs no Node toolchain.
A `Dockerfile.dockerignore` keeps `target/` and `.git/` out of the context.

### Publishing

`image.yml` is a reusable workflow (`workflow_call`, plus
`workflow_dispatch` with a `tag` input to publish an existing release). It
builds each platform on a native runner (`ubuntu-24.04` and
`ubuntu-24.04-arm`; the repo is public, so both are free), pushes by
digest, and a merge job creates the manifest list tagged `X.Y.Z`, `X.Y` and
`latest`. Emulated arm64 builds under QEMU were rejected: a Rust release
build takes over an hour that way. `release.yml` calls it when
`release_created` is set.

### Per-pod settings from the environment

`ClusterConfig::apply_env` reads `ARNEB_CLUSTER_COORDINATOR_ADDRESS`,
`ARNEB_CLUSTER_WORKER_ID` and `ARNEB_CLUSTER_ADVERTISED_ADDRESS`
(env > file), called from `AppConfig::load` like the other sections. They
are runtime settings, so an env override fits the configuration rules. The
worker already logs the id and coordinator it uses when its heartbeat loop
starts.

### Chart layout

- **Coordinator:** a Deployment with one replica and `strategy: Recreate`.
  Two coordinators behind one Service would split worker heartbeats between
  them. Service `<release>-arneb` (ClusterIP by default) exposes pgwire,
  the Trino port, the Web UI and Flight.
- **Workers:** a Deployment (`worker.replicas`, default 2). The downward API
  sets `ARNEB_CLUSTER_WORKER_ID` to the pod name and
  `ARNEB_CLUSTER_ADVERTISED_ADDRESS` to `$(POD_IP):9090`;
  `ARNEB_CLUSTER_COORDINATOR_ADDRESS` is the Service's Flight port. A
  StatefulSet would give stable ids, but nothing needs them: a replaced pod
  registers under its new name, and the old one leaves by draining.
- **Drain:** `ARNEB_DRAIN_TIMEOUT_SECS` comes from `worker.drainTimeoutSecs`
  (120), and `terminationGracePeriodSeconds` defaults to that plus 30 s.
  A PodDisruptionBudget (`maxUnavailable: 1`) keeps node drains to one
  worker at a time.
- **Config:** `config` is TOML shared by every pod (tables, catalogs,
  storage, memory, auth). The chart passes role, bind address and port as
  CLI flags, and the per-pod cluster settings as env, so `config` never
  needs a `[cluster]` address. A checksum annotation restarts the pods when
  it changes. `existingConfigSecret` uses a Secret instead, for configs that
  hold `[auth]` users or credentials.
- **Cluster secret:** `clusterSecret.existingSecret` / `.key`, else a
  random 48-character secret generated on install and kept on upgrade
  through `lookup`. It reaches every pod as `ARNEB_CLUSTER_SECRET`.
- **Everything else** goes through `extraEnv`, `envFrom`, `extraVolumes`
  and `extraVolumeMounts` (TLS certificates, catalog passwords, data
  directories), plus the usual `resources`, `nodeSelector`, `tolerations`,
  `affinity` and `podAnnotations` per role.
- **Probes:** TCP on pgwire for the coordinator and on Flight for workers.
  The Web UI is not used, since it binds to loopback under password auth.
- `Chart.yaml` `version` and `appVersion` follow the release;
  release-please bumps both, and the image tag defaults to `appVersion`.

### kind acceptance test

`kubernetes.yml` runs on pull requests (except docs-only ones), on pushes to
main and on demand:

1. Build the image for amd64 and load it into kind.
2. Generate TPC-H SF1 Parquet with DuckDB's `dbgen` into a host directory
   that kind mounts into its node; the chart's `extraVolumes` mount it as a
   `hostPath` on every pod, and `config` declares the eight tables.
3. `helm install` with `charts/arneb/ci/kind-values.yaml`; wait until the
   coordinator lists two workers.
4. `scripts/k8s-smoke/tpch_check.py` runs `benchmarks/tpch/queries/q01..q22`
   through a port-forward and compares each result with DuckDB running the
   same query file on the same data (some of the repo's query files differ
   from the spec's text, so DuckDB's built-in TPC-H answers do not apply):
   same row count, strings equal, numbers within a relative 1e-6 or half a
   cent (Arneb keeps a DECIMAL average at scale 2), compared as sorted rows
   so ties in `ORDER BY` do not matter.
5. Delete one worker pod, wait for its replacement to register, and run
   q01 again.

A local file connector keeps the test free of Hive and S3, which the chart
does not ship.

## Risks / Trade-offs

- [Rolling upgrade across releases still fails in-flight queries] → The
  coordinator restarts (Recreate) and admits only its own release, so a
  version upgrade is a short full restart. The drain covers same-release
  restarts: config changes, node drains, rescheduling. Documented.
- [Cluster TLS needs certificates for pod IPs] → Workers advertise pod
  IPs, so a TLS certificate must carry those IPs, or the deployment must set
  `ARNEB_CLUSTER_ADVERTISED_ADDRESS` to a DNS name the certificate covers.
  Documented; a headless Service per worker is left for later.
- [A new worker id per pod] → The registry's `rejected` map grows by one
  entry per refused pod (#181). It only grows on version mismatches.
- [CI time] → The image build is the slow step; BuildKit's GHA cache keeps
  warm builds short, and the job runs in parallel with the existing CI.
