# Arneb

A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

Trino (formerly PrestoSQL) lets users query data where it lives — across object stores, databases, and other systems — using standard SQL. This project aims to achieve similar goals with Rust's performance and safety guarantees.

## Why Arneb

Measured against Trino on the full 22-query TPC-H suite at SF10 — both engines as a
coordinator plus two workers, in containers with the same CPU allocation, reading the
same Parquet from MinIO through the same Hive Metastore:

| | Arneb vs Trino |
|---|---|
| **Peak memory** | **0.16× on average — about a sixth of Trino's**, and lower on **all 22** queries (best 0.06×, worst 0.42×) |
| **Latency** | **2.0× faster** (geomean), faster on **21 of 22** queries |
| **Correctness** | **Cell-identical to Trino on all 22**, and deterministic run-to-run |
| **Footprint** | One self-contained binary (~60 MB). No JVM, no heap tuning, no GC pauses |

The memory result is the one to notice: a Trino cluster that needs 9.3 GB to run q09
is replaced by one that needs 1.8 GB, on identical hardware and identical data. That is
the difference between a query that fits on your node and one that does not.

**The one query Arneb loses is q21**, the deepest correlated-join pipeline in the suite
(0.78× and 0.68× across two runs) — it still uses 0.23× the memory there. Numbers are
warm-run; the cold-run figures, the per-query tables, and the method are in
[the benchmark section](#tpc-h-benchmark) below. Everything here is reproducible with one
command on one machine.

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
- **TPC-H Benchmark**: 22/22 queries cell-identical to Trino, ~6× lower peak memory on all 22 and faster on 21 of 22 at SF10, with a one-command benchmark harness and comparison tooling

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

All 22 TPC-H queries return results **cell-identical to Trino**, and arneb's
peak memory is lower than Trino's on **every one of the 22**. On latency the
answer depends on whether the engines are measured cold or warm, and the
difference is large enough that reporting only one of them would misrepresent
the result.

Measured 2026-09-20 at **SF10** (`lineitem` 59,986,052 rows), both engines as a
coordinator + 2 workers in containers with the same per-node CPU cap, reading
the same Snappy Parquet from MinIO via Hive Metastore. Host: 10 CPUs, 15.66 GiB
of container-runtime memory.

| | Cold | Warm |
|---|---|---|
| Geomean latency speedup | 4.4x | **2.0x** |
| Mean peak memory vs Trino | 0.19x | **0.16x** |
| Queries where arneb is faster | 22 / 22 | **21 / 22** |
| Queries where arneb uses less memory | 22 / 22 | 22 / 22 |

Ratios understate what the memory result means in practice, so here it is in
absolute terms — the five queries with the largest gap, warm run, peak
simultaneous RSS summed across all three cluster nodes:

| Query | Arneb | Trino | Saved | Ratio |
|---|--:|--:|--:|--:|
| q09 Product Type Profit Measure | 1.8 GB | 9.3 GB | **7.5 GB** | 0.20× |
| q15 Top Supplier | 0.4 GB | 7.1 GB | **6.6 GB** | 0.06× |
| q21 Suppliers Who Kept Orders Waiting | 2.0 GB | 8.6 GB | **6.6 GB** | 0.23× |
| q17 Small-Quantity-Order Revenue | 1.6 GB | 7.9 GB | **6.3 GB** | 0.20× |
| q18 Large Volume Customer | 3.8 GB | 9.2 GB | **5.4 GB** | 0.42× |

Trino's floor is high before any query runs. Measured across the same restarts,
its three-node cluster sits at **1.87 GB** of committed heap at idle (1.77–1.98
GB) — a real deployment cost whether or not it is in use. Arneb allocates
lazily: the same measurement puts its idle cluster at **0.03 GB** (0.01–0.05
GB), roughly sixty times less. Both are counted the way a container memory limit
or `kubectl top` counts them.

**Read the warm column.** Cold means each query is timed on a freshly restarted
cluster with nothing warmed; warm means one untimed run precedes the timed one,
applied identically to both engines. Warming is worth 43–67% to Trino (median
57%) because a restarted JVM re-JITs and re-reads from MinIO, and 4% to arneb,
which has no equivalent start-up cost. The cold column is therefore mostly a
measurement of JVM start-up, and the 4.4x it reports is not a claim about query
execution.

**q21 is the exception, and it is a real one.** Warm, arneb loses it — 8.3 s and
9.9 s across two runs against Trino's 6.4 s and 6.7 s, a ratio of 0.78x and
0.68x. It reproduces; it is not noise. arneb still uses 0.23x the peak memory on
that query. q21 is the four-way correlated `EXISTS`/`NOT EXISTS` self-join over
`lineitem`, the deepest join pipeline in the suite.

Correctness is a separate gate and it passes outright: every query is
deterministic run-to-run and cell-identical to Trino, compared as an
order-independent multiset at 6 significant figures
(`benchmarks/tpch/scripts/blast_radius_oracle.py`, 22/22).

SF30 figures cited previously in this repository were produced under the cold
protocol and have not been re-measured warm.

The full guide — prerequisites, host floors, per-query tables, and the
comparison report — is in
[`benchmarks/tpch/README.md`](benchmarks/tpch/README.md). In short:

```bash
git clone https://github.com/formosa-ventures/arneb.git && cd arneb

# Build the engine images (first run only; compiles arneb in release mode).
docker compose -f docker-compose.yml \
               -f docker/arneb-bench/docker-compose.bench.yml build

# One command: preflight the host, seed SF10, measure both engines, report.
BENCH_WARMUP=1 ./benchmarks/tpch/scripts/run_benchmark.sh --scenario=sf10

# Correctness gate: determinism + cell-diff vs Trino, all 22.
python3 benchmarks/tpch/scripts/blast_radius_oracle.py --runs 2
```

Drop `BENCH_WARMUP=1` to reproduce the cold column. The run writes its raw CSV,
a provenance record naming the scale and warm-up state, and the rendered table
into `benchmarks/tpch/results/`.

## Development

```bash
cargo build                      # Build all crates
cargo test                       # Run all tests
cargo clippy -- -D warnings      # Lint
cargo fmt -- --check             # Check formatting
```

## License

Apache-2.0
