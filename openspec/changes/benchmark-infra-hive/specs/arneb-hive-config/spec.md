## ADDED Requirements

### Requirement: Hive-backed TPC-H config
The system SHALL provide `benchmarks/tpch/tpch-hive.toml` that configures arneb to read TPC-H data from Hive Metastore and MinIO instead of local Parquet files. The config uses `[[catalogs]]` and `[storage.s3]` sections.

#### Scenario: Config structure
- **WHEN** tpch-hive.toml is loaded
- **THEN** it defines a catalog named "datalake" of type "hive" with metastore_uri "127.0.0.1:9083" and default_schema "tpch"
- **AND** it defines [storage.s3] with endpoint "http://localhost:9000" and MinIO credentials

#### Scenario: Arneb starts with Hive config
- **WHEN** `cargo run --bin arneb -- --config benchmarks/tpch/tpch-hive.toml` is executed
- **THEN** arneb connects to HMS and makes TPC-H tables available as `datalake.tpch.*`

#### Scenario: Query execution
- **WHEN** arneb is running with tpch-hive.toml and a benchmark query references table "lineitem"
- **THEN** the query resolves to `datalake.tpch.lineitem` and reads Parquet data from MinIO

### Requirement: Local config preserved
The existing `benchmarks/tpch/tpch-config.toml` (local Parquet files) SHALL be preserved for fast local development without Docker Compose.

#### Scenario: Local development
- **WHEN** a developer runs arneb with tpch-config.toml
- **THEN** it reads local Parquet files from `benchmarks/tpch/data/sf1/` as before
- **AND** no Docker Compose services are required
