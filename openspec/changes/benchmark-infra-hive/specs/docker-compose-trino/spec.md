## ADDED Requirements

### Requirement: Trino service in Docker Compose
The system SHALL provide a `trino` service in `docker-compose.yml` using the `trinodb/trino:latest` image. The service SHALL expose port 8080 and mount catalog configuration from `docker/trino/catalog/`.

#### Scenario: Trino starts successfully
- **WHEN** `docker compose up -d` is executed
- **THEN** the trino service starts and becomes healthy within 60 seconds
- **AND** port 8080 is accessible

#### Scenario: Trino depends on HMS
- **WHEN** the trino service starts
- **THEN** it waits for the hive-metastore service to be healthy before accepting queries

#### Scenario: Health check
- **WHEN** the trino service is running
- **THEN** the health check queries `/v1/info` and verifies `starting` is `false`

### Requirement: Trino catalog connectors
The system SHALL provide three catalog property files in `docker/trino/catalog/`:

- `tpch.properties`: Built-in TPC-H data generator (`connector.name=tpch`)
- `tpcds.properties`: Built-in TPC-DS data generator (`connector.name=tpcds`)
- `hive.properties`: Hive connector pointing to HMS and MinIO (`connector.name=hive`)

#### Scenario: TPC-H connector available
- **WHEN** Trino is running
- **THEN** `SELECT COUNT(*) FROM tpch.sf1.nation` returns 25

#### Scenario: TPC-DS connector available
- **WHEN** Trino is running
- **THEN** `SELECT COUNT(*) FROM tpcds.sf1.call_center` returns a non-zero result

#### Scenario: Hive connector configured
- **WHEN** Trino is running
- **THEN** the hive catalog connects to HMS at `thrift://hive-metastore:9083`
- **AND** S3 is configured with MinIO endpoint `http://minio:9000`, path-style access, and minioadmin credentials

#### Scenario: CTAS into Hive works
- **WHEN** `CREATE SCHEMA IF NOT EXISTS hive.test` followed by `CREATE TABLE hive.test.nations AS SELECT * FROM tpch.sf1.nation` is executed
- **THEN** the table is created with Parquet data in MinIO and metadata in HMS
