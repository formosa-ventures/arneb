# Arneb

A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

Trino (formerly PrestoSQL) lets users query data where it lives — across object stores, databases, and other systems — using standard SQL. This project aims to achieve similar goals with Rust's performance and safety guarantees.

## Arneb vs Trino — TPC-H SF10

Measured 2026-09-20 on the full 22-query suite. Both engines run as a coordinator
plus two workers, in containers with the same per-node CPU cap, reading the same
Snappy Parquet from MinIO through the same Hive Metastore. `lineitem` is
59,986,052 rows. Host: 10 CPUs, 15.66 GiB of container-runtime memory.

| | Arneb vs Trino |
|---|---|
| **Peak memory** | **0.16× on average — about a sixth of Trino's**, lower on **all 22** queries (best 0.06×, worst 0.42×) |
| **Latency** | **2.0× faster** (geomean), faster on **21 of 22** queries |
| **Correctness** | **Cell-identical to Trino on all 22**, deterministic run-to-run |
| **Footprint** | One self-contained binary (~60 MB). No JVM, no heap tuning, no GC pauses |

### Every query, both protocols

Latency in milliseconds, peak memory in MB. Latency ratios are Trino ÷ arneb
(higher is better for arneb); memory ratios are arneb ÷ Trino (lower is better).
Memory is the peak simultaneous resident set summed across all three cluster
nodes, from the warm run.

| Query | | cold arneb | cold Trino | cold | warm arneb | warm Trino | **warm** | mem arneb | mem Trino | **mem** |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `q01` | Pricing Summary | 1,894 | 6,283 | 3.3× | 1,818 | 3,039 | **1.7×** | 366 | 5,468 | **0.07×** |
| `q02` | Minimum Cost Supplier | 671 | 5,991 | 8.9× | 506 | 2,122 | **4.2×** | 261 | 3,806 | **0.07×** |
| `q03` | Shipping Priority | 2,543 | 9,047 | 3.6× | 2,184 | 3,832 | **1.8×** | 1,918 | 5,409 | **0.35×** |
| `q04` | Order Priority Checking | 1,989 | 7,402 | 3.7× | 1,952 | 2,781 | **1.4×** | 1,704 | 6,923 | **0.25×** |
| `q05` | Local Supplier Volume | 2,270 | 8,887 | 3.9× | 2,414 | 4,302 | **1.8×** | 1,048 | 6,559 | **0.16×** |
| `q06` | Forecasting Revenue | 724 | 4,629 | 6.4× | 656 | 1,883 | **2.9×** | 349 | 4,670 | **0.07×** |
| `q07` | Volume Shipping | 2,028 | 8,433 | 4.2× | 1,942 | 3,858 | **2.0×** | 1,217 | 5,537 | **0.22×** |
| `q08` | National Market Share | 3,075 | 9,835 | 3.2× | 3,124 | 4,902 | **1.6×** | 1,558 | 7,380 | **0.21×** |
| `q09` | Product Type Profit | 4,265 | 11,779 | 2.8× | 4,487 | 6,443 | **1.4×** | 1,849 | 9,338 | **0.20×** |
| `q10` | Returned Item Reporting | 2,056 | 9,983 | 4.9× | 2,175 | 4,183 | **1.9×** | 1,009 | 5,593 | **0.18×** |
| `q11` | Important Stock ID | 689 | 5,218 | 7.6× | 737 | 1,942 | **2.6×** | 319 | 3,277 | **0.10×** |
| `q12` | Shipping Modes | 1,216 | 6,619 | 5.4× | 1,081 | 2,508 | **2.3×** | 330 | 5,054 | **0.07×** |
| `q13` | Customer Distribution | 1,815 | 7,738 | 4.3× | 1,632 | 4,394 | **2.7×** | 414 | 6,478 | **0.06×** |
| `q14` | Promotion Effect | 1,195 | 7,078 | 5.9× | 1,074 | 2,879 | **2.7×** | 691 | 4,904 | **0.14×** |
| `q15` | Top Supplier | 1,037 | 6,975 | 6.7× | 997 | 3,314 | **3.3×** | 446 | 7,093 | **0.06×** |
| `q16` | Parts/Supplier Relationship | 1,089 | 7,956 | 7.3× | 1,079 | 2,616 | **2.4×** | 414 | 4,533 | **0.09×** |
| `q17` | Small-Quantity-Order Revenue | 3,337 | 11,353 | 3.4× | 3,032 | 5,300 | **1.7×** | 1,586 | 7,859 | **0.20×** |
| `q18` | Large Volume Customer | 4,566 | 10,798 | 2.4× | 4,331 | 5,145 | **1.2×** | 3,827 | 9,180 | **0.42×** |
| `q19` | Discounted Revenue | 1,453 | 7,419 | 5.1× | 1,279 | 3,239 | **2.5×** | 642 | 5,514 | **0.12×** |
| `q20` | Potential Part Promotion | 3,037 | 8,465 | 2.8× | 2,939 | 3,942 | **1.3×** | 1,123 | 6,683 | **0.17×** |
| `q21` | Suppliers Who Kept Orders Waiting | 7,936 | 15,185 | 1.9× | 8,262 | 6,442 | **0.78×** ⚠️ | 1,994 | 8,560 | **0.23×** |
| `q22` | Global Sales Opportunity | 962 | 5,659 | 5.9× | 946 | 2,136 | **2.3×** | 740 | 4,710 | **0.16×** |
| | **aggregate** | | | **4.4×** | | | **2.0×** | | | **0.16×** |

Aggregate is the geometric mean for the latency ratios and the arithmetic mean
for the memory ratios.

### What the two protocols mean

**Cold** times each query on a freshly restarted cluster. **Warm** runs one
untimed query first, identically for both engines, then times the next one.

That choice carries most of the headline, which is why both columns are here.
Warming is worth a median 57% to Trino (43–67% across the 22) because a
restarted JVM re-JITs and re-reads from MinIO, and 4% to arneb (−7% to +24%),
which has no equivalent start-up cost. **The cold column is therefore largely a
measurement of JVM start-up — quote the warm one.**

### Memory, in absolute terms

Ratios understate what this means for a deployment, so the five largest gaps:

| Query | Arneb | Trino | Saved |
|---|--:|--:|--:|
| `q09` Product Type Profit | 1.8 GB | 9.3 GB | **7.5 GB** |
| `q15` Top Supplier | 0.4 GB | 7.1 GB | **6.6 GB** |
| `q21` Suppliers Who Kept Orders Waiting | 2.0 GB | 8.6 GB | **6.6 GB** |
| `q17` Small-Quantity-Order Revenue | 1.6 GB | 7.9 GB | **6.3 GB** |
| `q18` Large Volume Customer | 3.8 GB | 9.2 GB | **5.4 GB** |

A Trino cluster that needs 9.3 GB to run q09 is replaced by one that needs
1.8 GB, on identical hardware and identical data — the difference between a
query that fits on your node and one that does not.

Trino's floor is also high before any query runs: measured across the same
restarts, its three-node cluster sits at **1.87 GB** of committed heap at idle
(1.77–1.98 GB). Arneb allocates lazily — the same measurement puts its idle
cluster at **0.03 GB** (0.01–0.05 GB), roughly sixty times less. Both are counted
the way a container memory limit or `kubectl top` counts them.

### The one query Arneb loses

Warm, arneb loses `q21` — 8,262 ms and 9,852 ms across two runs against Trino's
6,442 ms and 6,739 ms, a ratio of 0.78× and 0.68×. It reproduces; it is not
noise. Memory there is still 0.23×. `q21` is the four-way correlated
`EXISTS` / `NOT EXISTS` self-join over `lineitem`, the deepest join pipeline in
the suite.

### Reproduce it

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

Drop `BENCH_WARMUP=1` for the cold column. Method, host floors and what is *not*
claimed are in [benchmark methodology](#benchmark-methodology);
[`benchmarks/tpch/README.md`](benchmarks/tpch/README.md) has the full guide.

## Features

- **SQL Support**: SELECT, JOIN (INNER/LEFT/RIGHT/FULL/CROSS), GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, CASE/COALESCE/NULLIF, CTEs, UNION/INTERSECT/EXCEPT, window functions, subqueries (IN/EXISTS/scalar), DDL/DML (CREATE/DROP TABLE, INSERT, DELETE, views)
- **19 Scalar Functions**: UPPER, LOWER, SUBSTRING, TRIM, CONCAT, LENGTH, REPLACE, POSITION, ABS, ROUND, CEIL, FLOOR, MOD, POWER, EXTRACT, CURRENT_DATE, DATE_TRUNC
- **Arrow-native Execution**: Vectorized columnar processing using Apache Arrow
- **Connectors**: In-memory tables, CSV/Parquet files, S3/GCS/Azure object stores, Hive Metastore catalog (HMS 4.x via `_req` API), Apache Iceberg tables via HMS (read-only; field-ID schema evolution, manifest-based file pruning)
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
├── iceberg/       # Iceberg (HMS-backed) catalog + scan planning over manifests
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

Iceberg tables in the same metastore are served by a `type = "iceberg"`
catalog (`docker compose run --rm iceberg-seed` creates sample tables via
Trino). See [docs/connectors/iceberg.md](docs/connectors/iceberg.md).

## Benchmark methodology

The numbers at the top of this README rest on these choices. They are the parts
that decide whether a comparison is fair, so they are stated rather than left in
the scripts.

- **Identical isolation.** Both engines run as a coordinator plus two workers in
  containers with the same per-node CPU cap, reading the same Parquet from the
  same object store through the same metastore. Neither engine runs natively
  against a containerized rival.
- **Engine rotation.** Only the engine being measured is resident; the other is
  stopped, not idle. Otherwise Trino's committed JVM heap counts against arneb's
  memory reading, and arneb's footprint against Trino's.
- **A clean baseline per query.** The cluster restarts before each query, so a
  measurement starts from its own baseline rather than its predecessor's
  high-water mark.
- **Memory is simultaneous, not a sum of lifetime peaks.** Peak is sampled
  during the query as the maximum of the summed cgroup RSS across all three
  nodes — a footprint that actually existed at one instant. Summing each node's
  lifetime peak would report a cluster total that never simultaneously existed.
- **Correctness is a separate gate.** Each query runs twice and is compared
  run-to-run and against Trino as an order-independent, float-tolerant multiset
  at 6 significant figures
  (`benchmarks/tpch/scripts/blast_radius_oracle.py`, 22/22 clean). Row-count
  agreement is not this guarantee — a query can return the right number of wrong
  rows, which is the failure this gate exists to catch.
- **A scenario declares its host floors.** `--scenario=sf10` requires 15 GiB of
  container-runtime memory, 6 CPUs and 40 GB free disk, and refuses the run
  before starting anything if the host cannot meet them. The scale factor
  travels with the measurement into the result document and the report header,
  so a number cannot be read without the scale it was measured at.

### What is not claimed

One host, one scale factor, one run per protocol per query — two for `q21`.
Run-to-run variance under identical settings has not been characterised, so
small differences in these tables are not results. Numbers from two different
hosts are not comparable; a scenario makes a run repeatable, not
hardware-independent.

SF30 figures cited previously in this repository were produced under the cold
protocol and have not been re-measured warm.

The full guide — prerequisites, host floors, the scenario reference and the
per-query report tooling — is in
[`benchmarks/tpch/README.md`](benchmarks/tpch/README.md).

## Development

```bash
cargo build                      # Build all crates
cargo test                       # Run all tests
cargo clippy -- -D warnings      # Lint
cargo fmt -- --check             # Check formatting
```

## License

Apache-2.0
