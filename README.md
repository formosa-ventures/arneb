# Arneb

A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

Trino (formerly PrestoSQL) lets users query data where it lives — across object stores, databases, and other systems — using standard SQL. This project aims to achieve similar goals with Rust's performance and safety guarantees.

## Features

- **SQL Support**: SELECT, JOIN (INNER/LEFT/RIGHT/FULL/CROSS), GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, CASE/COALESCE/NULLIF, CTEs, UNION/INTERSECT/EXCEPT, window functions, subqueries (IN/EXISTS/scalar), DDL/DML (CREATE/DROP TABLE, INSERT, DELETE, views)
- **19 Scalar Functions**: UPPER, LOWER, SUBSTRING, TRIM, CONCAT, LENGTH, REPLACE, POSITION, ABS, ROUND, CEIL, FLOOR, MOD, POWER, EXTRACT, CURRENT_DATE, DATE_TRUNC
- **Arrow-native Execution**: Vectorized columnar processing using Apache Arrow
- **Connectors**: In-memory tables, CSV/Parquet files, S3/GCS/Azure object stores, Hive Metastore catalog (HMS 4.x via `_req` API)
- **PostgreSQL Wire Protocol**: Compatible with psql, DBeaver, JDBC, psycopg2, node-postgres, and all standard PostgreSQL clients
- **Extended Query Protocol**: Full prepared statement support (Parse/Bind/Describe/Execute/Sync)
- **pg_catalog / information_schema**: System catalog tables for client schema browser compatibility
- **Distributed Architecture**: Coordinator/Worker separation with Arrow Flight RPC
- **Web UI**: Dashboard with query monitoring, cluster overview, and worker status
- **TPC-H Benchmark**: 22/22 queries matching Trino's results and 1.76x faster by geomean at SF1 in the default configuration ([published run](benchmarks/tpch/official/v1.0.0/sf1/)), with a three-engine comparison harness

## Quick Start

```bash
# Build
cargo build --release

# Start server (standalone mode)
./target/release/arneb

# Start with data tables
./target/release/arneb --config arneb.toml

# Connect with psql
psql -h 127.0.0.1 -p 5432

# Open Web UI
open http://127.0.0.1:6432
```

### Configuration

```toml
# Arneb.toml
bind_address = "127.0.0.1"
port = 5432

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[[tables]]
name = "orders"
path = "/data/orders.csv"
format = "csv"
schema = [
    { name = "id", type = "int32" },
    { name = "customer", type = "utf8" },
    { name = "total", type = "float64" },
]
```

### Distributed Mode

Coordinator handles SQL parsing, planning, and task dispatch. Workers execute plan fragments and serve data via Arrow Flight RPC.

```bash
# Start coordinator (accepts SQL on port 5432, Web UI on 6432)
./target/release/arneb --config arneb.toml --port 5432 --role coordinator

# Start worker (separate terminal — Flight RPC only, no pgwire)
./target/release/arneb --config worker.toml --role worker
```

Worker config (`worker.toml` — no `port` needed since worker has no pgwire):
```toml
bind_address = "127.0.0.1"

# Worker needs access to the same data files as coordinator
[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[cluster]
rpc_port = 9091
coordinator_address = "127.0.0.1:9090"
worker_id = "worker-1"
```

## Architecture

```
Standalone / Coordinator local path:
  SQL → Parser → AST → Planner → LogicalPlan → Optimizer
    → ExecutionContext → PhysicalPlan → execute() → Results

Distributed path (coordinator + worker):
  SQL → Parser → AST → Planner → LogicalPlan → Optimizer
    → PlanFragmenter → Fragments
    → QueryCoordinator:
        → Submit leaf fragments to workers via Flight RPC
        → Workers execute fragments, write to OutputBuffer
        → Coordinator executes root fragment locally
    → Results → PostgreSQL wire format
```

### Crate Layout

```
crates/
├── common/        # Shared types, error hierarchy, identifiers
├── sql-parser/    # SQL → AST via sqlparser-rs
├── catalog/       # Catalog/Schema/Table provider traits
├── planner/       # AST → LogicalPlan, optimizer, plan fragmenter
├── execution/     # Physical operators, scalar functions, DataSource trait
├── connectors/    # Memory + File connectors, object store abstraction (S3/GCS/Azure)
├── hive/          # Hive Metastore catalog provider + HiveDataSource
├── hive-metastore/# Auto-generated Thrift bindings from Hive 4.2.0 IDL
├── protocol/      # PostgreSQL wire protocol (Simple + Extended Query)
├── scheduler/     # QueryTracker, NodeRegistry, resource groups
├── rpc/           # Arrow Flight RPC for distributed execution
└── server/        # Main binary, CLI, config, Web UI
```

## Hive Metastore + S3

Run Arneb against a real Hive Metastore backed by S3-compatible storage:

```bash
# 1. Start MinIO + HMS + Trino
docker compose up -d

# 2. Seed TPC-H data (Parquet on MinIO, tables registered in HMS)
docker compose run --rm tpch-seed

# 3. Start Arneb with Hive catalog config
cargo run --bin arneb -- --config benchmarks/tpch/tpch-hive.toml

# 4. Query via psql (or DBeaver / any Postgres client)
psql -h 127.0.0.1 -p 5432 -c "SELECT COUNT(*) FROM datalake.tpch.nation;"

# 5. Tear down
docker compose down
```

## TPC-H Benchmark

Arneb completes all 22 TPC-H queries with results matching Trino and DataFusion,
in its **default configuration** — a stock `cargo build --release` with no tuning
options enabled.

**At TPC-H SF1**, Arneb is **1.76x faster than Trino by geometric mean** and
leads on 21 of 22 queries:

| | Arneb | Trino | DataFusion |
|---|---:|---:|---:|
| Queries completed | 22/22 | 22/22 | 22/22 |
| Geomean p50 | 293.1 ms | 515.3 ms | 141.5 ms |
| vs Arneb | — | 0.57x | 2.07x |

This figure covers SF1 and nothing else. Larger scale factors are not published
yet, and nothing here should be read as a claim about them — run the benchmark at
the scale you care about rather than extrapolating.

DataFusion is roughly 2x ahead of Arneb. It runs in-process with no distribution
layer, and it is in the comparison as a correctness cross-check and an
Arrow-native reference point rather than as the engine Arneb is positioned
against.

Both engines read the same Parquet from MinIO via Hive Metastore and run as a
coordinator plus two workers with the same CPU allocation. Only the engine being
measured is running — an idle engine still holds its heap, so leaving them all up
measures each under memory pressure from its rivals.

Measured 2026-07-25 on an Apple M1 Pro (10 cores, 32 GB, macOS 26.5.2, arm64).
Per-query figures, the raw result documents and the run's provenance are
published in
[`benchmarks/tpch/official/v1.0.0/sf1/`](benchmarks/tpch/official/v1.0.0/sf1/) —
see [`comparison.md`](benchmarks/tpch/official/v1.0.0/sf1/comparison.md). These
are laptop numbers on arm64 macOS; they are reproducible on comparable hardware
and do not generalize to Linux x86 servers.

The full guide — prerequisites, the fully containerized three-engine run, and
how to read the comparison report — is in the
[**TPC-H comparison tutorial**](benchmarks/tpch/TUTORIAL.md). Everything runs
in Docker, including the benchmark runner, so no host toolchain is needed and
all engines are measured under the same isolation. In short:

```bash
git clone https://github.com/formosa-ventures/arneb.git && cd arneb

# Starts the engines, seeds TPC-H SF1, runs all three engines from inside the
# runner container, and writes benchmarks/tpch/results/comparison.md.
./benchmarks/tpch/scripts/run_benchmark.sh
```

A separate [dual-axis harness](benchmarks/tpch/README.md) measures peak memory
alongside latency under per-engine container isolation. It runs Arneb with
tuning options that are off by default, so its numbers are not comparable with
the default-configuration figures above.

## Development

```bash
cargo build                      # Build all crates
cargo test                       # Run all tests
cargo clippy -- -D warnings      # Lint
cargo fmt -- --check             # Check formatting
```

## License

Apache-2.0
