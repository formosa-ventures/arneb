## Why

Arneb has no supported way to run on Kubernetes: releases ship no binary or
image, and the only Dockerfile (`docker/arneb-bench`) is a throwaway bench
image with configs baked in. P6 in the Phase 3 roadmap closes this, and the
"Path to production" plan puts it in v0.1.7, after worker drain (G4) and the
version handshake (G5) so a rolling restart does not fail queries. Both are
in 0.1.6.

## What Changes

- **A production image.** `docker/arneb/Dockerfile` builds `arneb` into a
  slim Debian image that runs as a non-root user, with no config baked in.
- **Multi-arch images on GHCR for every release.** When release-please
  creates a release, `linux/amd64` and `linux/arm64` images are built on
  native runners and pushed to `ghcr.io/formosa-ventures/arneb` as `X.Y.Z`,
  `X.Y` and `latest`. A manual run publishes an existing tag.
- **A Helm chart** (`charts/arneb`): the coordinator and the workers as
  separate Deployments, the shared TOML config in a ConfigMap (or an
  existing Secret), the cluster secret in a Secret, TLS material and other
  files through extra volumes. Workers get `terminationGracePeriodSeconds`
  above `drain_timeout_secs`, so Kubernetes lets a drain finish.
- **Per-pod cluster settings from the environment.**
  `ARNEB_CLUSTER_COORDINATOR_ADDRESS`, `ARNEB_CLUSTER_WORKER_ID` and
  `ARNEB_CLUSTER_ADVERTISED_ADDRESS` override `[cluster]`
  `coordinator_address`, `worker_id` and `advertised_address`, so one
  ConfigMap serves every pod and each worker advertises its pod IP.
- **CI on kind.** A workflow builds the image, installs the chart on kind
  (1 coordinator + 2 workers), runs the 22 TPC-H queries at SF1 against it,
  and checks every result against DuckDB running the same query files. It then restarts a
  worker and checks that the cluster recovers.
- **Docs:** `docs/guide/kubernetes.md`.

## Capabilities

### New Capabilities

- `container-image`: the image, its publishing on release, and its tags.
- `helm-chart`: the chart's workloads, config, secrets, drain settings and
  the kind acceptance test.

### Modified Capabilities

- `server-config`: the three `ARNEB_CLUSTER_*` address and id overrides.

## Impact

- New: `docker/arneb/`, `charts/arneb/`, `.github/workflows/image.yml`,
  `.github/workflows/kubernetes.yml`, `scripts/k8s-smoke/`,
  `docs/guide/kubernetes.md`.
- Changed: `.github/workflows/release.yml` (calls the image workflow on a
  release), `release-please-config.json` (bumps the chart's version and
  `appVersion`), `crates/server/src/config.rs` (env overrides),
  `docs/guide/configuration.md`, the docs sidebar.
- GHCR package `formosa-ventures/arneb` becomes public on first push (a
  one-time package-visibility setting).
