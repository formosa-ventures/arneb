## 1. Directory Setup

- [ ] 1.1 Create benchmarks/tpcds/ directory structure (queries/, scripts/, results/)
- [ ] 1.2 Create benchmarks/tpcds/README.md with overview and usage instructions

## 2. TPC-DS Data Generation

- [ ] 2.1 Add Trino service to docker-compose.yml with tpcds and hive connectors configured
- [ ] 2.2 Create docker/tpcds-seed/ with seed script for CTAS operations
- [ ] 2.3 Implement seed script: CTAS for all 24 TPC-DS tables (7 fact + 17 dimension) from tpcds connector into Hive
- [ ] 2.4 Add tpcds-seed service to docker-compose.yml with health check dependencies
- [ ] 2.5 Add Docker Compose profile "tpcds" to gate TPC-DS seeding
- [ ] 2.6 Test data generation for SF1 (verify all 24 tables created in MinIO with correct row counts)

## 3. TPC-DS Queries

- [ ] 3.1 Add TPC-DS queries q01-q20 adapted from Trino's tpcds query set
- [ ] 3.2 Add TPC-DS queries q21-q40 adapted from Trino's tpcds query set
- [ ] 3.3 Add TPC-DS queries q41-q60 adapted from Trino's tpcds query set
- [ ] 3.4 Add TPC-DS queries q61-q80 adapted from Trino's tpcds query set
- [ ] 3.5 Add TPC-DS queries q81-q99 adapted from Trino's tpcds query set
- [ ] 3.6 Annotate each query with SKIP markers and reasons for unsupported SQL features
- [ ] 3.7 Validate all non-skipped queries execute successfully against Trino with SF1 data
- [ ] 3.8 Validate all non-skipped queries execute successfully against arneb with SF1 data

## 4. Benchmark Runner Integration

- [ ] 4.1 Create benchmarks/tpcds/scripts/run.sh wrapper invoking tpch-bench with --queries-dir benchmarks/tpcds/queries
- [ ] 4.2 Add convenience targets for arneb and trino engines
- [ ] 4.3 Verify runner discovers and executes all 99 query files (passing and skipped)

## 5. Comparison Report

- [ ] 5.1 Create benchmarks/tpcds/scripts/report.sh wrapping the existing report generator for tpcds results
- [ ] 5.2 Add coverage summary section: passing/skipped/failed counts with skip reason breakdown
- [ ] 5.3 Generate comparison table for passing queries (arneb vs Trino median times)

## 6. Documentation

- [ ] 6.1 Write benchmarks/tpcds/README.md with full setup, data generation, and run instructions
- [ ] 6.2 Document skip reason categories and which SQL feature changes unblock which queries
- [ ] 6.3 Add expected query pass counts at current and projected SQL capability levels
