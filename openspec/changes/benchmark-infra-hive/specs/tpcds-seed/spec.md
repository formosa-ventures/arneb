## ADDED Requirements

### Requirement: TPC-DS seed service
The system SHALL provide a `tpcds-seed` service in Docker Compose that generates TPC-DS data by running CTAS from Trino's built-in `tpcds` connector into the `hive.tpcds` schema. The service is triggered manually via `docker compose run tpcds-seed`.

#### Scenario: Seed TPC-DS SF1 data
- **WHEN** `docker compose run tpcds-seed` is executed
- **THEN** all 24 TPC-DS tables are created in the `hive.tpcds` schema
- **AND** Parquet data is stored in MinIO under `s3://warehouse/tpcds/`

#### Scenario: Fact tables created
- **WHEN** the seed script completes
- **THEN** the 7 fact tables exist: store_sales, catalog_sales, web_sales, store_returns, catalog_returns, web_returns, inventory

#### Scenario: Dimension tables created
- **WHEN** the seed script completes
- **THEN** the 17 dimension tables exist: call_center, catalog_page, customer, customer_address, customer_demographics, date_dim, household_demographics, income_band, item, promotion, reason, ship_mode, store, time_dim, warehouse, web_page, web_site

### Requirement: Idempotent seeding
The seed script SHALL be idempotent: it drops `hive.tpcds` schema (CASCADE) if it exists, creates the schema, then runs CTAS for all 24 tables.

#### Scenario: Re-seed
- **WHEN** `docker compose run tpcds-seed` is executed and the schema already exists
- **THEN** the existing schema is dropped and recreated with fresh data

### Requirement: Configurable scale factor
The seed script SHALL accept a `TPCDS_SF` environment variable. Default is `sf1`. Supported values include `tiny`, `sf1`, `sf10`.

#### Scenario: Default scale factor
- **WHEN** `docker compose run tpcds-seed` is executed with default settings
- **THEN** data is generated from `tpcds.sf1`
