## ADDED Requirements

### Requirement: TPC-H seed service
The system SHALL provide a `tpch-seed` service in Docker Compose that generates TPC-H data by running CTAS from Trino's built-in `tpch` connector into the `hive.tpch` schema. The service is triggered manually via `docker compose run tpch-seed`.

#### Scenario: Seed TPC-H SF1 data
- **WHEN** `docker compose run tpch-seed` is executed
- **THEN** all 8 TPC-H tables are created in the `hive.tpch` schema
- **AND** Parquet data is stored in MinIO under `s3://warehouse/tpch/`

#### Scenario: Seed waits for Trino
- **WHEN** the tpch-seed service starts
- **THEN** it waits for the trino service health check to pass before executing CTAS

#### Scenario: All 8 tables created
- **WHEN** the seed script completes
- **THEN** the following tables exist in hive.tpch: lineitem, orders, customer, part, partsupp, supplier, nation, region

### Requirement: Idempotent seeding
The seed script SHALL be idempotent: it drops `hive.tpch` schema (CASCADE) if it exists, creates the schema, then runs CTAS for all 8 tables. Running the seed multiple times produces the same result.

#### Scenario: Re-seed after previous run
- **WHEN** `docker compose run tpch-seed` is executed and the schema already exists
- **THEN** the existing schema is dropped and recreated with fresh data
- **AND** the final state is identical to a first-time seed

### Requirement: Configurable scale factor
The seed script SHALL accept a `TPCH_SF` environment variable to control the scale factor. Default is `sf1`. Supported values include `tiny`, `sf1`, `sf10`.

#### Scenario: Seed with SF1
- **WHEN** `docker compose run tpch-seed` is executed with default settings
- **THEN** data is generated from `tpch.sf1` (approximately 6 million lineitem rows)

#### Scenario: Seed with tiny
- **WHEN** `TPCH_SF=tiny docker compose run tpch-seed` is executed
- **THEN** data is generated from `tpch.tiny` for quick testing

### Requirement: Both engines can read seeded data
After seeding, the TPC-H tables SHALL be readable by both Trino (via `hive.tpch.*`) and arneb (via `datalake.tpch.*` with Hive catalog config).

#### Scenario: Trino reads seeded data
- **WHEN** `SELECT COUNT(*) FROM hive.tpch.nation` is executed in Trino
- **THEN** it returns 25

#### Scenario: Arneb reads seeded data
- **WHEN** arneb is started with tpch-hive.toml and `SELECT COUNT(*) FROM datalake.tpch.nation` is executed
- **THEN** it returns 25
