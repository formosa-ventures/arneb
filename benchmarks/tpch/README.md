# TPC-H Benchmark

Performance comparison of arneb against Trino using TPC-H queries.
Both engines read the same Parquet data from MinIO via Hive Metastore.

## Quick Start

```bash
# 1. Start infrastructure (MinIO + HMS + Trino)
docker compose up -d

# 2. Seed TPC-H data (SF1, ~1GB, takes ~2 minutes)
docker compose run --rm tpch-seed

# 3. Start arneb with Hive config
cargo run --release --bin arneb -- --config benchmarks/tpch/tpch-hive.toml

# 4. Run benchmark
cd benchmarks/tpch
cargo run --release -- --host 127.0.0.1 --port 5432

# 5. Generate report
python3 scripts/report.py results/arneb_*.json
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│              docker compose up -d                │
│                                                  │
│  MinIO (:9000)   HMS (:9083)   Trino (:8080)    │
│       │               │              │           │
│       └───────┬───────┘              │           │
│               │                      │           │
│    s3://warehouse/tpch/              │           │
│    ├── lineitem/ (Parquet)           │           │
│    ├── orders/                       │           │
│    └── ... (8 tables)                │           │
└─────────────────────────────────────────────────┘
                │                      │
          ┌─────┴─────┐          ┌─────┴─────┐
          │   Arneb    │          │   Trino    │
          │ (pgwire)   │          │ (REST API) │
          │ :5432      │          │ :8080      │
          └────────────┘          └────────────┘
               Same Parquet data → fair comparison
```

## Data Generation

### Via Docker Compose (recommended)

```bash
# Start services
docker compose up -d

# Seed SF1 data (~1GB, ~2 minutes)
docker compose run --rm tpch-seed

# Seed with different scale factor
TPCH_SF=tiny docker compose run --rm tpch-seed   # ~10MB, quick
TPCH_SF=sf10 docker compose run --rm tpch-seed   # ~10GB, slower

# Verify data
docker compose exec trino trino --execute "SELECT COUNT(*) FROM hive.tpch.lineitem"
```

### Local Parquet files (alternative)

For quick local development without the full Hive stack. Parquet files
are generated via a single `docker run` that uses DuckDB's built-in
`tpch` extension and DuckDB's `dbgen`. The script CASTs money columns
to `DOUBLE` so the resulting Parquet schema matches what Trino writes
during the Hive CTAS seed — queries in `queries/` therefore run
identically against both local Parquet and Hive-backed data.

```bash
# 1. Generate SF 0.01 Parquet files (one docker run, ~5s on first run)
./benchmarks/tpch/scripts/generate_sf001.sh

# Larger scale factors:
TPCH_SF=0.1 ./benchmarks/tpch/scripts/generate_sf001.sh
TPCH_SF=1   ./benchmarks/tpch/scripts/generate_sf001.sh

# 2. Start arneb against the local files
cargo run --release --bin arneb -- --config benchmarks/tpch/tpch-sf001.toml
```

> `tpch-sf001.toml` uses repo-relative paths. Run `arneb` with the
> repository root as the working directory, or edit the config to use
> absolute paths.

### TPC-H Tables

| Table | Description | SF1 Rows |
|-------|------------|----------|
| lineitem | Line items | ~6M |
| orders | Orders | ~1.5M |
| customer | Customers | ~150K |
| part | Parts | ~200K |
| partsupp | Part suppliers | ~800K |
| supplier | Suppliers | ~10K |
| nation | Nations | 25 |
| region | Regions | 5 |

## Benchmark Runner

```bash
cd benchmarks/tpch

# Run all queries against arneb
cargo run --release -- --host 127.0.0.1 --port 5432

# Run against Trino (reads from same Hive tables)
cargo run --release -- --engine trino --host 127.0.0.1 --port 8080 --catalog hive --schema tpch

# Run specific queries
cargo run --release -- --queries 1,3,6

# Custom number of runs
cargo run --release -- --num-runs 10 --warm-up 3
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--engine` | arneb | Engine to benchmark (arneb\|trino) |
| `--host` | 127.0.0.1 | Database host |
| `--port` | 5432 | Database port |
| `--catalog` | tpch | Trino catalog name |
| `--schema` | sf1 | Trino schema name |
| `--queries-dir` | benchmarks/tpch/queries | Query SQL files |
| `--num-runs` | 5 | Total runs per query |
| `--warm-up` | 2 | Warm-up runs to discard |
| `--output-dir` | benchmarks/tpch/results | JSON output directory |
| `--queries` | (all) | Comma-separated query numbers |

## Full Benchmark (Arneb vs Trino)

```bash
# Automated: runs both engines and generates report
./benchmarks/tpch/scripts/run_benchmark.sh

# Skip Trino baseline
./benchmarks/tpch/scripts/run_benchmark.sh --skip-trino
```

## Report Generation

```bash
# arneb only
python3 scripts/report.py results/arneb_*.json

# Comparison with Trino
python3 scripts/report.py results/arneb_*.json results/trino_*.json
```

Output format:

```
| Query    | arneb (ms)  |  Trino (ms)  |  Speedup |
|----------|-----------------|--------------|----------|
| q01      |           45.2  |       120.5  |    2.67x |
| q03      |           82.1  |       195.3  |    2.38x |
...

Geometric mean speedup: 2.15x
Queries tested: 7
```

## Queries

Adapted TPC-H queries for arneb's SQL dialect:

| Query | Description | Complexity |
|-------|------------|------------|
| Q1 | Pricing Summary Report | Aggregation + filter |
| Q3 | Shipping Priority | 3-way join + group by |
| Q4 | Order Priority Checking | Aggregation + filter |
| Q5 | Local Supplier Volume | 6-way join + group by |
| Q6 | Forecasting Revenue | Single table aggregate |
| Q10 | Returned Item Reporting | 4-way join + group by |
| Q12 | Shipping Modes | 2-way join + group by |

## Configuration Files

| File | Description |
|------|-------------|
| `tpch-hive.toml` | Arneb config reading from Hive/MinIO (for benchmarks) |
| `tpch-config.toml` | Arneb config reading local Parquet files (for dev) |
