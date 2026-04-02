# TPC-H Benchmark

Performance comparison of arneb against Trino using TPC-H queries.

## Quick Start

```bash
# 1. Start arneb with TPC-H data
cargo run --release -- --config benchmarks/tpch/tpch-config.toml

# 2. Run benchmark
cd benchmarks/tpch
cargo run --release -- --host 127.0.0.1 --port 5432

# 3. Generate report
python3 scripts/report.py results/arneb_*.json
```

## Data Generation

### Using dbgen (official TPC-H tool)

```bash
# Install dbgen
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen && make
export PATH=$PATH:$(pwd)

# Generate SF1 (1GB) data
./benchmarks/tpch/scripts/generate_data.sh 1
```

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

# Run all queries
cargo run --release -- --host 127.0.0.1 --port 5432

# Run specific queries
cargo run --release -- --queries 1,3,6

# Custom number of runs
cargo run --release -- --num-runs 10 --warm-up 3
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | 127.0.0.1 | Database host |
| `--port` | 5432 | Database port |
| `--queries-dir` | benchmarks/tpch/queries | Query SQL files |
| `--num-runs` | 5 | Total runs per query |
| `--warm-up` | 2 | Warm-up runs to discard |
| `--output-dir` | benchmarks/tpch/results | JSON output directory |
| `--queries` | (all) | Comma-separated query numbers |

## Trino Baseline

```bash
# Start Trino with tpch connector
# (requires Trino installation)

# Run baseline
./benchmarks/tpch/scripts/run_trino.sh localhost 8080 tpch
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
