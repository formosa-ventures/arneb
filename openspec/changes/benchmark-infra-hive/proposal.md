## Why

The TPC-H benchmark currently reads local Parquet files (`benchmarks/tpch/data/sf1/*.parquet`), completely bypassing the Hive Metastore and MinIO infrastructure. To make a fair apple-to-apple comparison with Trino, both engines must read the exact same Parquet files from the same HMS-registered tables on MinIO. This eliminates format bias (row group size, compression, statistics) and ensures identical data access paths.

The existing data generation tools (`generate_parquet.py` via REST API, `generate_data.sh` via dbgen) are slow, fragile, and produce data that only arneb reads. Trino's built-in `tpch` connector with CTAS replaces all of them: it generates standard-compliant TPC-H data, writes Parquet directly to MinIO, and registers tables in HMS — all in one step.

## What Changes

- Add Trino service to Docker Compose with tpch, tpcds, and hive catalog connectors
- Add tpch-seed and tpcds-seed services for data generation via `docker compose run`
- Create Arneb config using `[[catalogs]]` to read TPC-H data from Hive/MinIO
- Update benchmark orchestration scripts for the new Docker Compose workflow
- Retire `generate_parquet.py`, `generate_data.sh`, and `hive_demo_setup.rs`

## Capabilities

### New Capabilities

- `docker-compose-trino`: Trino service in Docker Compose with tpch, tpcds, and hive connectors
- `tpch-seed`: TPC-H data seeding via Trino CTAS into Hive tables on MinIO
- `tpcds-seed`: TPC-DS data seeding via Trino CTAS into Hive tables on MinIO
- `arneb-hive-config`: Arneb configuration for Hive-backed TPC-H benchmarks
- `benchmark-flow`: Updated benchmark execution workflow using Docker Compose

### Modified Capabilities

- Existing benchmark runner (`tpch-bench`) unchanged; invoked with `--catalog hive --schema tpch` for Trino

## Impact

- **Docker Compose**: `docker-compose.yml` gains 3 services (trino, tpch-seed, tpcds-seed)
- **New directories**: `docker/trino/catalog/`, `docker/tpch-seed/`, `docker/tpcds-seed/`
- **Modified**: `benchmarks/tpch/scripts/run_benchmark.sh`, `run_trino.sh`, `README.md`
- **New config**: `benchmarks/tpch/tpch-hive.toml`
- **Removed**: `generate_parquet.py`, `generate_data.sh`, `hive_demo_setup.rs`, hive-demo-setup binary definition
- **Dependencies**: None new (Trino runs in Docker, runner already has all deps)
