## ADDED Requirements

### Requirement: Docker Compose tpcds-seed service
The system SHALL provide a Docker Compose service `tpcds-seed` that generates TPC-DS data by running CTAS (CREATE TABLE AS SELECT) from Trino's built-in tpcds connector into Hive tables stored on MinIO. The service SHALL wait for Trino, HMS, and MinIO to be healthy before executing.

#### Scenario: Seed SF1 data
- **WHEN** `docker compose --profile tpcds up tpcds-seed` is executed
- **THEN** all 24 TPC-DS tables are created in the Hive metastore with data stored in MinIO
- **AND** each table contains the correct number of rows for SF1

#### Scenario: Seed service dependencies
- **WHEN** the tpcds-seed service starts
- **THEN** it waits for Trino, HMS, and MinIO health checks to pass before executing CTAS statements

#### Scenario: Idempotent seeding
- **WHEN** the seed service runs and tables already exist
- **THEN** it drops and recreates the schema to ensure a clean state
- **AND** the final state is a complete set of 24 tables with correct data

### Requirement: TPC-DS table coverage
The seed script SHALL create all 24 TPC-DS tables: 7 fact tables (store_sales, catalog_sales, web_sales, store_returns, catalog_returns, web_returns, inventory) and 17 dimension tables (call_center, catalog_page, customer, customer_address, customer_demographics, date_dim, household_demographics, income_band, item, promotion, reason, ship_mode, store, time_dim, warehouse, web_page, web_site).

#### Scenario: Fact tables created
- **WHEN** the seed script completes
- **THEN** the 7 fact tables exist with data: store_sales, catalog_sales, web_sales, store_returns, catalog_returns, web_returns, inventory

#### Scenario: Dimension tables created
- **WHEN** the seed script completes
- **THEN** the 17 dimension tables exist with data: call_center, catalog_page, customer, customer_address, customer_demographics, date_dim, household_demographics, income_band, item, promotion, reason, ship_mode, store, time_dim, warehouse, web_page, web_site

### Requirement: Configurable scale factor
The seed script SHALL accept a scale factor parameter defaulting to SF1. Supported values SHALL include tiny (~10MB), sf1 (~1GB), and sf10 (~10GB).

#### Scenario: SF1 data volume
- **WHEN** the seed script runs with scale factor sf1
- **THEN** store_sales contains approximately 2.88 million rows

#### Scenario: Tiny scale factor
- **WHEN** the seed script runs with scale factor tiny
- **THEN** all tables are created with minimal data for quick testing
