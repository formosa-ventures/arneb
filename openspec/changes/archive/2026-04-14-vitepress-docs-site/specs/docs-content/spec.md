## ADDED Requirements

### Requirement: Introduction page
The file `docs/guide/introduction.md` SHALL provide an overview of Arneb: what it is (a Trino alternative built in Rust), key features (Arrow-native, PostgreSQL wire compatible, federated queries, distributed execution), current status (TPC-H progress), and supported data sources.

#### Scenario: Reader understands what Arneb is
- **WHEN** a user reads the introduction page
- **THEN** the page explains Arneb's purpose, lists core features, states it speaks the PostgreSQL wire protocol, and mentions supported connectors (file, object store, Hive)

### Requirement: Quickstart guide
The file `docs/guide/quickstart.md` SHALL walk a user from zero to running their first query in under 5 minutes. It SHALL cover: building from source (`cargo build`), starting the server (`cargo run --bin arneb`), connecting with `psql`, and running a sample query against a bundled or local Parquet file.

#### Scenario: User completes quickstart
- **WHEN** a user follows the quickstart instructions sequentially
- **THEN** they have a running Arneb instance and have executed at least one SQL query via `psql`

#### Scenario: Prerequisites are listed
- **WHEN** a user reads the quickstart page
- **THEN** the page lists prerequisites: Rust toolchain (with minimum version), a PostgreSQL client (`psql`), and optionally sample data

### Requirement: Configuration reference
The file `docs/guide/configuration.md` SHALL document all configuration options for `arneb.toml` including: `bind_address`, `port`, `[[tables]]` entries (name, path, format), `[storage.s3]` / `[storage.gcs]` sections, `[[catalogs]]` entries, and `[cluster]` settings. It SHALL document precedence: CLI > env > file > defaults.

#### Scenario: All config fields documented
- **WHEN** a user searches for a configuration option (e.g., `bind_address`, `storage.s3.endpoint`)
- **THEN** the configuration page contains that option with its type, default value, and description

#### Scenario: Example configurations
- **WHEN** a user reads the configuration page
- **THEN** the page includes at least two complete example `arneb.toml` files: one for standalone mode with local Parquet files, and one for distributed mode with Hive catalog and S3

### Requirement: Distributed mode guide
The file `docs/guide/distributed.md` SHALL explain how to run Arneb in distributed mode: coordinator and worker roles, `[cluster]` configuration, Flight RPC communication, how to add workers, and a complete multi-node example.

#### Scenario: Coordinator-worker setup
- **WHEN** a user reads the distributed mode guide
- **THEN** the page provides step-by-step instructions to start a coordinator and at least one worker, including example config files for each

#### Scenario: Role differences explained
- **WHEN** a user reads the distributed mode guide
- **THEN** the page explains which ports each role exposes (coordinator: pgwire + Web UI + Flight RPC; worker: Flight RPC only) and what each role is responsible for

### Requirement: SQL overview page
The file `docs/sql/overview.md` SHALL list all supported SQL statement types (SELECT, EXPLAIN, CREATE TABLE, DROP TABLE, CREATE TABLE AS SELECT, INSERT INTO, DELETE FROM, CREATE VIEW, DROP VIEW) with brief descriptions and links to detailed sections.

#### Scenario: Statement inventory
- **WHEN** a user reads the SQL overview
- **THEN** every supported statement type is listed with a one-line description and a link or anchor to its detailed documentation

### Requirement: Expressions reference
The file `docs/sql/expressions.md` SHALL document all supported expression types: CASE WHEN, COALESCE, NULLIF, CAST, BETWEEN, IN, LIKE, IS NULL/NOT NULL, arithmetic operators, comparison operators, logical operators, and subquery expressions (IN/EXISTS/scalar).

#### Scenario: Expression syntax documented
- **WHEN** a user looks up a specific expression (e.g., CASE WHEN)
- **THEN** the page shows the expression's syntax, a description, and at least one SQL example

### Requirement: Functions reference
The file `docs/sql/functions.md` SHALL document all 19 built-in scalar functions organized by category: String (UPPER, LOWER, SUBSTRING, TRIM, LTRIM, RTRIM, CONCAT, LENGTH, REPLACE, POSITION), Math (ABS, ROUND, CEIL, FLOOR, MOD, POWER), Date (EXTRACT, CURRENT_DATE, DATE_TRUNC).

#### Scenario: Every function documented
- **WHEN** a user searches for a specific function (e.g., SUBSTRING)
- **THEN** the page contains that function with its signature, parameter descriptions, return type, and at least one example

#### Scenario: Functions grouped by category
- **WHEN** a user reads the functions page
- **THEN** functions are organized under "String Functions", "Math Functions", and "Date Functions" headings

### Requirement: Advanced SQL reference
The file `docs/sql/advanced.md` SHALL document CTEs (WITH clauses), window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/COUNT/MIN/MAX OVER with PARTITION BY and ORDER BY), UNION ALL/UNION/INTERSECT/EXCEPT, and GROUP BY with HAVING.

#### Scenario: CTE syntax and example
- **WHEN** a user reads the advanced SQL page
- **THEN** the page shows CTE syntax with at least one multi-CTE example query

#### Scenario: Window function reference
- **WHEN** a user reads the advanced SQL page
- **THEN** each supported window function is listed with syntax, partitioning/ordering clauses, and an example

### Requirement: Connectors overview
The file `docs/connectors/overview.md` SHALL explain the connector model: what the `DataSource` trait provides, how connectors are registered, and what pushdown capabilities are supported (filter, projection, limit).

#### Scenario: Connector model explained
- **WHEN** a user reads the connectors overview
- **THEN** the page explains how Arneb discovers and loads data sources, and how pushdown optimization works

### Requirement: File connector guide
The file `docs/connectors/file.md` SHALL document reading CSV and Parquet files from local filesystem paths. It SHALL show how to configure tables in `arneb.toml` with `path` and `format` fields.

#### Scenario: Parquet table configuration
- **WHEN** a user reads the file connector page
- **THEN** the page shows a complete `[[tables]]` config block for a local Parquet file

#### Scenario: CSV table configuration
- **WHEN** a user reads the file connector page
- **THEN** the page shows a complete `[[tables]]` config block for a local CSV file

### Requirement: Object store connector guide
The file `docs/connectors/object-store.md` SHALL document reading files from S3, GCS, and Azure Blob Storage. It SHALL cover `[storage.s3]`, `[storage.gcs]` configuration, credential precedence (config > env > IAM), and endpoint overrides for MinIO/LocalStack.

#### Scenario: S3 configuration
- **WHEN** a user reads the object store page
- **THEN** the page shows a complete configuration for reading Parquet from S3, including the `[storage.s3]` section and `[[tables]]` entry with an `s3://` path

#### Scenario: MinIO/LocalStack setup
- **WHEN** a user reads the object store page
- **THEN** the page explains how to set `endpoint` and `allow_http` for local S3-compatible services

### Requirement: Hive connector guide
The file `docs/connectors/hive.md` SHALL document setting up a Hive Metastore catalog: `[[catalogs]]` config with `type = "hive"`, `metastore_uri`, per-catalog storage overrides, and the `docker compose` demo workflow.

#### Scenario: Hive catalog configuration
- **WHEN** a user reads the Hive connector page
- **THEN** the page shows a complete `[[catalogs]]` configuration block for a Hive catalog with storage overrides

#### Scenario: Demo walkthrough
- **WHEN** a user reads the Hive connector page
- **THEN** the page includes the `docker compose` demo steps: start HMS + MinIO, seed data, start Arneb, run a query, tear down

### Requirement: Architecture overview
The file `docs/architecture/overview.md` SHALL describe the crate layout, the query data flow (SQL → Parser → Planner → Optimizer → ExecutionContext → PhysicalPlan → Stream → Wire Protocol), key design principles (Arrow-native, async streaming, trait-based connectors, pushdown), and major dependencies.

#### Scenario: Data flow explained
- **WHEN** a user reads the architecture overview
- **THEN** the page presents the query processing pipeline from SQL string to PostgreSQL wire response with the role of each stage

#### Scenario: Crate map
- **WHEN** a user reads the architecture overview
- **THEN** the page lists all crates (common, sql-parser, catalog, planner, execution, connectors, hive, hive-metastore, protocol, scheduler, rpc, server) with a one-line description of each

### Requirement: Contributing guide
The file `docs/contributing.md` SHALL document: development prerequisites, how to build and test (`cargo build`, `cargo test`, `cargo clippy`, `cargo fmt`), how to run the TPC-H benchmark, PR workflow, and a note about updating docs alongside code changes.

#### Scenario: Developer can set up environment
- **WHEN** a new contributor reads the contributing guide
- **THEN** the page lists all prerequisites (Rust toolchain, Node/pnpm for Web UI, Docker for Hive demo) and the commands to build, test, and lint
