## 1. Config

- [x] 1.1 `ClusterConfig::apply_env`: `ARNEB_CLUSTER_COORDINATOR_ADDRESS`, `ARNEB_CLUSTER_WORKER_ID`, `ARNEB_CLUSTER_ADVERTISED_ADDRESS` (env > file, empty rejected), called from `AppConfig::load`
- [x] 1.2 Tests: env wins, empty rejected, unset keeps the file value

## 2. Image

- [x] 2.1 `docker/arneb/Dockerfile` + `Dockerfile.dockerignore` (release build, bookworm-slim, uid 10001, no config)
- [x] 2.2 `.github/workflows/image.yml`: native amd64/arm64 builds, push by digest, manifest `X.Y.Z` / `X.Y` / `latest`; `workflow_call` + `workflow_dispatch(tag)`. actionlint clean; not run yet: it first runs on the next release, or by hand for v0.1.6 after merge
- [x] 2.3 `release.yml` calls it when a release is created

## 3. Chart

- [x] 3.1 `charts/arneb`: Chart.yaml, values.yaml, helpers, coordinator Deployment (Recreate) + Service, worker Deployment, ConfigMap, cluster Secret (generated, kept via `lookup`), PDB, NOTES.txt
- [x] 3.2 Worker drain: `ARNEB_DRAIN_TIMEOUT_SECS`, grace period = drain + 30 s by default
- [x] 3.3 `release-please-config.json` bumps `version` and `appVersion` in `Chart.yaml`
- [x] 3.4 `helm lint` and `helm template` clean

## 4. kind acceptance

- [x] 4.1 `scripts/k8s-smoke/gen_tpch.py` (DuckDB dbgen → Parquet) and `tpch_check.py` (22 queries, each checked against DuckDB running the same file)
- [x] 4.2 `charts/arneb/ci/kind-values.yaml` and `scripts/k8s-smoke/kind-config.yaml`
- [x] 4.3 `.github/workflows/kubernetes.yml`: build, kind, install, 22/22 check, replace a worker pod, query again. actionlint clean; first CI run is on this change's PR
- [x] 4.4 Run the whole flow locally on kind before pushing (2026-10-25, kind v0.33 / Kubernetes 1.37 on OrbStack, arm64): 22/22 TPC-H SF1 match DuckDB; a worker pod deleted 0.5 s into q18/q21/q09 drains (`worker draining` → `worker deregistered` 2 s later) and all three queries match; the replacement registers; `helm upgrade` keeps the cluster secret. Found and fixed: Kubernetes service links inject `ARNEB_PORT=tcp://…` for a Service named `arneb` (now `enableServiceLinks: false`); `arneb::profile` events flood the default `info` log (chart `logLevel` default `info,arneb::profile=warn`).

## 5. Docs

- [x] 5.1 `docs/guide/kubernetes.md` (install, values, secrets, TLS with pod IPs, drain, upgrades across releases) and the sidebar
- [x] 5.2 `docs/guide/configuration.md`: the three env vars
- [x] 5.3 Roadmap: P6 status
