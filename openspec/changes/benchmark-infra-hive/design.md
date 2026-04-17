## Context

The TPC-H benchmark infrastructure was built in Phase 2 with local Parquet files and standalone data generation scripts. Now that the Hive connector and MinIO/HMS Docker Compose stack exist, the benchmark should use them — both to test the Hive code path under realistic conditions and to enable fair Trino comparisons where both engines read identical data.

## Goals / Non-Goals

**Goals:**

- Add Trino to Docker Compose with tpch, tpcds, and hive connectors
- Generate TPC-H and TPC-DS data via Trino CTAS into Hive/MinIO
- Enable apple-to-apple benchmark comparisons (same Parquet, same HMS, same machine)
- Clean up legacy data generation tools replaced by Trino CTAS
- Preserve ability to run benchmarks with local Parquet files (keep tpch-config.toml)

**Non-Goals:**

- TPC-DS query adaptation (covered by tpcds-benchmark change)
- Continuous benchmarking in CI
- Trino performance tuning (use default config)
- Replacing MinIO/HMS with other storage (existing infra is sufficient)

## Decisions

### D1: Docker Compose adds Trino with three catalog connectors

**Choice**: Add a `trino` service to `docker-compose.yml` using `trinodb/trino:latest`. Mount catalog properties from `docker/trino/catalog/`: `tpch.properties` (built-in data generator), `hive.properties` (pointing to HMS + MinIO), and `tpcds.properties` (built-in TPC-DS generator). Trino depends on `hive-metastore` health check.

**Rationale**: Trino serves dual purposes — data generation (CTAS from tpch/tpcds connectors into hive) and baseline benchmark engine. Having it always available in the Docker Compose stack means no separate Trino installation is needed.

### D2: Seed via `docker compose run` (manual trigger)

**Choice**: Define `tpch-seed` and `tpcds-seed` services in Docker Compose that run seed scripts. Trigger manually with `docker compose run tpch-seed`. The seed script uses the Trino CLI to execute CTAS for each table. Seeding is idempotent: it drops the schema first, then recreates all tables.

**Rationale**: Seed is a one-time operation per scale factor. Making it a `docker compose run` target (not a persistent service or profile-gated) keeps the default `docker compose up` fast while making seeding a simple one-liner. Data persists in MinIO volumes across restarts.

### D3: New Arneb config with `[[catalogs]]`

**Choice**: Create `benchmarks/tpch/tpch-hive.toml` using `[[catalogs]]` to point at HMS. Keep existing `tpch-config.toml` (local Parquet) for fast local development without Docker.

**Rationale**: Developers can choose: `tpch-config.toml` for quick local testing, `tpch-hive.toml` for Hive-backed benchmarks. The benchmark scripts default to `tpch-hive.toml`.

### D4: Benchmark runner unchanged

**Choice**: The existing `tpch-bench` binary requires no code changes. For Trino benchmarks against Hive tables, invoke with `--engine trino --catalog hive --schema tpch`. For arneb, the runner connects via pgwire as before.

**Rationale**: The runner already supports `--engine`, `--catalog`, `--schema`, and `--queries-dir` flags. No new logic needed.

### D5: Retire legacy data generation tools

**Choice**: Remove `generate_parquet.py` (Python + pyarrow + REST API), `generate_data.sh` (dbgen wrapper), and `hive_demo_setup.rs` (Rust binary for demo tables). Remove the `hive-demo-setup` binary definition from `crates/server/Cargo.toml`.

**Rationale**: All three are replaced by Trino CTAS. `generate_parquet.py` was slow (REST API row-by-row) and fragile (Python deps). `generate_data.sh` required compiling dbgen from source. `hive_demo_setup.rs` created tiny hardcoded demo tables — the seed service now creates full TPC-H/TPC-DS datasets that serve as better demos.

### D6: Update benchmark orchestration scripts

**Choice**: Rewrite `run_benchmark.sh` for the new workflow: ensure Docker Compose is up, seed if needed, run arneb benchmark, optionally run Trino benchmark, generate report. Update `run_trino.sh` to use the Docker Compose Trino instance (port 8080) instead of requiring a separate Trino installation.

**Rationale**: The current `run_benchmark.sh` spawns its own Trino container on port 18080 and generates data via `generate_parquet.py`. The new flow is simpler: everything runs through Docker Compose.

## Risks / Trade-offs

**[Trino startup time]** -> Trino takes ~30s to start. **Mitigation**: Trino stays running via `docker compose up -d`. Health check ensures seed waits for readiness.

**[Seed time at large scale factors]** -> CTAS for SF10+ may take several minutes. **Mitigation**: Default to SF1 for development. Document expected seed times. Data persists across restarts.

**[MinIO volume space]** -> SF1 TPC-H is ~1GB, SF1 TPC-DS is ~1GB. **Mitigation**: Manageable for development. Use `docker compose down -v` to reclaim space.

**[Breaking local workflow]** -> Developers used to `tpch-config.toml` must now run Docker Compose. **Mitigation**: Keep `tpch-config.toml` working with local Parquet files. Docker Compose is only required for Hive-backed benchmarks and Trino comparison.
