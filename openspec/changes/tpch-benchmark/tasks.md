## 1. Directory Setup

- [x] 1.1 Create benchmarks/tpch/ directory structure (src/, queries/, scripts/, data/)
- [x] 1.2 Create benchmarks/tpch/Cargo.toml for the runner binary
- [x] 1.3 Add tokio-postgres and serde_json dependencies

## 2. TPC-H Data Generation

- [x] 2.1 Create scripts/generate_data.sh wrapping dbgen for SF1/SF10/SF100
- [x] 2.2 Create Rust helper to convert CSV to Parquet with correct Arrow schemas
- [x] 2.3 Define schemas for all 8 TPC-H tables (lineitem, orders, customer, part, partsupp, supplier, nation, region)
- [x] 2.4 Test data generation for SF1

## 3. TPC-H Queries

- [x] 3.1 Adapt TPC-H queries Q1-Q6 (simple aggregation/filter queries)
- [x] 3.2 Adapt TPC-H queries Q7-Q12 (multi-table join queries)
- [x] 3.3 Adapt TPC-H queries Q13-Q18 (subquery and complex queries)
- [x] 3.4 Adapt TPC-H queries Q19-Q22 (advanced queries)
- [x] 3.5 Validate each query runs against SF1 data

## 4. Benchmark Runner

- [x] 4.1 Implement CLI with options: host, port, scale_factor, num_runs, warm_up, output_dir
- [x] 4.2 Implement query executor using tokio-postgres
- [x] 4.3 Implement timing collection (wall clock per query per run)
- [x] 4.4 Output JSON results: {query_id, runs: [{wall_clock_ms, rows_returned}]}
- [x] 4.5 Handle query failures gracefully (log and continue)

## 5. Trino Baseline

- [x] 5.1 Create scripts/run_trino.sh using trino CLI
- [x] 5.2 Configure Trino with TPC-H connector at matching scale factor
- [x] 5.3 Capture timing in same JSON format as our runner

## 6. Report Generator

- [x] 6.1 Create scripts/report.py reading both JSON result files
- [x] 6.2 Generate markdown table: query, trino_ms, trino_alt_ms, speedup
- [x] 6.3 Calculate summary statistics (geometric mean speedup, median, p95)

## 7. Documentation

- [x] 7.1 Write benchmarks/tpch/README.md with setup and run instructions
- [x] 7.2 Document hardware requirements and test environment
