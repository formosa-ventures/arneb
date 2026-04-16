## ADDED Requirements

### Requirement: Updated benchmark orchestration
The system SHALL provide an updated `benchmarks/tpch/scripts/run_benchmark.sh` that uses the Docker Compose workflow for benchmark execution.

#### Scenario: Full benchmark run
- **WHEN** `./benchmarks/tpch/scripts/run_benchmark.sh` is executed
- **THEN** it ensures Docker Compose services are running (minio, hive-metastore, trino)
- **AND** seeds TPC-H data if not already present
- **AND** starts arneb with tpch-hive.toml
- **AND** runs benchmark against arneb
- **AND** runs benchmark against Trino
- **AND** generates comparison report

#### Scenario: Skip Trino baseline
- **WHEN** `./benchmarks/tpch/scripts/run_benchmark.sh --skip-trino` is executed
- **THEN** only the arneb benchmark runs, Trino baseline is skipped

### Requirement: Updated Trino baseline script
The `benchmarks/tpch/scripts/run_trino.sh` SHALL use the Docker Compose Trino instance on port 8080 by default, instead of requiring a separate Trino installation.

#### Scenario: Default Trino connection
- **WHEN** `./benchmarks/tpch/scripts/run_trino.sh` is executed
- **THEN** it connects to Trino at localhost:8080 using catalog=hive and schema=tpch

#### Scenario: Custom connection
- **WHEN** `./benchmarks/tpch/scripts/run_trino.sh custom-host 9090 my_catalog` is executed
- **THEN** it connects to the specified host, port, and catalog

## MODIFIED Requirements

### Requirement: Legacy tool removal
The following files SHALL be removed as they are replaced by Trino CTAS seeding:

- `benchmarks/tpch/scripts/generate_parquet.py` (Python + pyarrow + REST API)
- `benchmarks/tpch/scripts/generate_data.sh` (dbgen wrapper)
- `crates/server/src/bin/hive_demo_setup.rs` (Rust demo seeder)
- `scripts/arneb-hive-demo.toml` (superseded by tpch-hive.toml)
- `[[bin]] name = "hive-demo-setup"` entry in `crates/server/Cargo.toml`

#### Scenario: Files removed
- **WHEN** this change is implemented
- **THEN** the 5 items listed above no longer exist in the repository

#### Scenario: Documentation updated
- **WHEN** this change is implemented
- **THEN** references to hive-demo-setup, generate_parquet.py, and generate_data.sh are removed from CLAUDE.md, README.md, and docs/
