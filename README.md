# Arneb

[![Latest release](https://img.shields.io/github/v/release/formosa-ventures/arneb?sort=semver)](https://github.com/formosa-ventures/arneb/releases/latest)
[![CI](https://github.com/formosa-ventures/arneb/actions/workflows/ci.yml/badge.svg)](https://github.com/formosa-ventures/arneb/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)
[![Docs](https://img.shields.io/badge/docs-arneb.io-blue)](https://arneb.io)

A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

Trino (formerly PrestoSQL) lets users query data where it lives — across object stores, databases, and other systems — using standard SQL. This project aims to achieve similar goals with Rust's performance and safety guarantees.

## Arneb vs Trino — TPC-H SF10

Measured 2026-10-13 on the full 22-query suite. Both engines run as a coordinator
plus two workers, in containers with the same per-node CPU cap, reading the same
Snappy Parquet from RustFS through the same Hive Metastore. `lineitem` is
59,986,052 rows. Host: 10 CPUs, 15.66 GiB of container-runtime memory.

| | Arneb vs Trino |
|---|---|
| **Peak memory** | **0.18× on average — about a sixth of Trino's**, lower on **all 22** queries (best 0.07×, worst 0.50×) |
| **Latency** | **2.0× faster** warm (geomean), faster on **21 of 22** queries |
| **Correctness** | **Cell-identical to Trino on all 22**, deterministic run-to-run |
| **Footprint** | One self-contained binary (~60 MB). No JVM, no heap tuning, no GC pauses |

### Every query, both protocols

Latency in milliseconds, peak memory in MB. Latency ratios are Trino ÷ arneb
(higher is better for arneb); memory ratios are arneb ÷ Trino (lower is better).
Memory is the peak simultaneous resident set summed across all three cluster
nodes, from the warm run.

| Query | | cold arneb | cold Trino | cold | warm arneb | warm Trino | **warm** | mem arneb | mem Trino | **mem** |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `q01` | Pricing Summary | 2,055 | 6,758 | 3.3× | 1,773 | 2,604 | **1.5×** | 349 | 4,461 | **0.08×** |
| `q02` | Minimum Cost Supplier | 600 | 5,950 | 9.9× | 580 | 2,134 | **3.7×** | 371 | 3,890 | **0.10×** |
| `q03` | Shipping Priority | 2,324 | 8,539 | 3.7× | 2,167 | 3,863 | **1.8×** | 2,037 | 5,685 | **0.36×** |
| `q04` | Order Priority Checking | 2,163 | 7,115 | 3.3× | 1,927 | 3,060 | **1.6×** | 1,724 | 6,925 | **0.25×** |
| `q05` | Local Supplier Volume | 2,261 | 7,815 | 3.5× | 2,043 | 3,945 | **1.9×** | 1,034 | 7,040 | **0.15×** |
| `q06` | Forecasting Revenue | 764 | 4,748 | 6.2× | 688 | 1,853 | **2.7×** | 334 | 4,649 | **0.07×** |
| `q07` | Volume Shipping | 1,891 | 7,658 | 4.1× | 1,882 | 3,772 | **2.0×** | 1,288 | 5,823 | **0.22×** |
| `q08` | National Market Share | 2,937 | 9,080 | 3.1× | 2,831 | 4,478 | **1.6×** | 1,715 | 6,448 | **0.27×** |
| `q09` | Product Type Profit | 4,260 | 11,400 | 2.7× | 4,186 | 5,819 | **1.4×** | 1,836 | 8,306 | **0.22×** |
| `q10` | Returned Item Reporting | 2,602 | 9,472 | 3.6× | 1,861 | 4,088 | **2.2×** | 1,053 | 4,580 | **0.23×** |
| `q11` | Important Stock ID | 794 | 5,591 | 7.0× | 683 | 1,908 | **2.8×** | 405 | 3,951 | **0.10×** |
| `q12` | Shipping Modes | 1,192 | 6,379 | 5.4× | 1,084 | 2,158 | **2.0×** | 429 | 4,390 | **0.10×** |
| `q13` | Customer Distribution | 1,504 | 7,471 | 5.0× | 1,516 | 3,601 | **2.4×** | 444 | 5,799 | **0.08×** |
| `q14` | Promotion Effect | 1,188 | 6,786 | 5.7× | 1,117 | 2,838 | **2.5×** | 877 | 5,238 | **0.17×** |
| `q15` | Top Supplier | 1,052 | 6,638 | 6.3× | 1,002 | 3,101 | **3.1×** | 486 | 5,851 | **0.08×** |
| `q16` | Parts/Supplier Relationship | 1,460 | 7,456 | 5.1× | 1,000 | 2,503 | **2.5×** | 426 | 4,405 | **0.10×** |
| `q17` | Small-Quantity-Order Revenue | 2,942 | 11,737 | 4.0× | 2,897 | 5,262 | **1.8×** | 1,643 | 8,201 | **0.20×** |
| `q18` | Large Volume Customer | 4,161 | 10,944 | 2.6× | 4,335 | 5,140 | **1.2×** | 4,786 | 9,521 | **0.50×** |
| `q19` | Discounted Revenue | 1,407 | 7,322 | 5.2× | 1,278 | 3,037 | **2.4×** | 809 | 5,473 | **0.15×** |
| `q20` | Potential Part Promotion | 3,004 | 8,195 | 2.7× | 3,088 | 3,781 | **1.2×** | 1,093 | 6,956 | **0.16×** |
| `q21` | Suppliers Who Kept Orders Waiting | 8,344 | 14,800 | 1.8× | 8,232 | 6,954 | **0.84×** ⚠️ | 1,735 | 8,906 | **0.19×** |
| `q22` | Global Sales Opportunity | 1,029 | 5,555 | 5.4× | 894 | 2,095 | **2.3×** | 795 | 4,823 | **0.16×** |
| | **aggregate** | | | **4.2×** | | | **2.0×** | | | **0.18×** |

Aggregate is the geometric mean for the latency ratios and the arithmetic mean
for the memory ratios.

### What the two protocols mean

**Cold** times each query on a freshly recreated cluster with the storage side
flushed too: RustFS restarted and the container VM's page cache dropped, so
every byte is re-read from disk. **Warm** recreates the cluster, runs one
untimed query first, identically for both engines, then times the next one.

That choice carries most of the headline, which is why both columns are here.
Warming is worth a median 56% to Trino (49–66% across the 22) because a
recreated JVM re-JITs, and 6% to arneb (−4% to +32%), which has no equivalent
start-up cost. Flushing storage barely moves either engine at SF10 — the
2026-09-20 run, which left RustFS and the page cache warm, got 4.4× cold /
2.0× warm against this run's 4.2× / 2.0×. **The cold
column is therefore largely a measurement of JVM start-up — quote the warm
one.**

### Memory, in absolute terms

Ratios understate what this means for a deployment, so the five largest gaps:

| Query | Arneb | Trino | Saved |
|---|--:|--:|--:|
| `q21` Suppliers Who Kept Orders Waiting | 1.7 GB | 8.7 GB | **7.0 GB** |
| `q17` Small-Quantity-Order Revenue | 1.6 GB | 8.0 GB | **6.4 GB** |
| `q09` Product Type Profit | 1.8 GB | 8.1 GB | **6.3 GB** |
| `q05` Local Supplier Volume | 1.0 GB | 6.9 GB | **5.9 GB** |
| `q20` Potential Part Promotion | 1.1 GB | 6.8 GB | **5.7 GB** |

A Trino cluster that needs 8.7 GB to run q21 is replaced by one that needs
1.7 GB, on identical hardware and identical data — the difference between a
query that fits on your node and one that does not.

Trino's floor is also high before any query runs: measured across the same
restarts, its three-node cluster sits at **2.00 GB** of committed heap at idle
(1.83–2.17 GB across 44 restarts). Arneb allocates lazily — the same
measurement puts its idle cluster at **0.07 GB** (0.06–0.07 GB), roughly thirty
times less. Both are counted
the way a container memory limit or `kubectl top` counts them.

### The one query Arneb loses

Warm, arneb loses `q21` — 8,232 ms against Trino's 6,954 ms, 0.84×. It has
lost `q21` warm in all four runs so far (0.78× and 0.68× on 2026-09-20, 0.80×
and 0.84× on 2026-10-13); it is not noise. Memory there is still 0.19×. `q21` is the four-way correlated
`EXISTS` / `NOT EXISTS` self-join over `lineitem`, the deepest join pipeline in
the suite.

### Reproduce it

```bash
git clone https://github.com/formosa-ventures/arneb.git && cd arneb

# Build the engine images (first run only; compiles arneb in release mode).
docker compose -f docker-compose.yml \
               -f docker/arneb-bench/docker-compose.bench.yml build

# One command: preflight the host, seed SF10, measure both engines cold and
# warm, report both side by side.
./benchmarks/tpch/scripts/run_benchmark.sh --scenario=sf10 --mode=both

# Correctness gate: determinism + cell-diff vs Trino, all 22.
python3 benchmarks/tpch/scripts/blast_radius_oracle.py --runs 2
```

`--mode=warm` or `--mode=cold` measures one protocol only. Method, host floors and what is *not*
claimed are in [benchmark methodology](#benchmark-methodology);
[`benchmarks/tpch/README.md`](benchmarks/tpch/README.md) has the full guide.

## Features

- **SQL Support**: SELECT, JOIN (INNER/LEFT/RIGHT/FULL/CROSS), GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, CASE/COALESCE/NULLIF, CTEs, UNION/INTERSECT/EXCEPT, window functions, subqueries (IN/EXISTS/scalar), DDL/DML (CREATE/DROP TABLE, INSERT, DELETE, views)
- **90 Trino-compatible Scalar Functions**: conditional (IF, TRY, GREATEST, LEAST), string (SPLIT_PART, STRPOS, LPAD/RPAD, CONCAT_WS, `||`, ...), regex (REGEXP_LIKE/EXTRACT/REPLACE/COUNT), math (SQRT, LN, LOG, TRUNCATE, RANDOM, trig, ...), date/time (DATE_ADD, DATE_DIFF, DATE_TRUNC, DATE_FORMAT, DATE_PARSE, FORMAT_DATETIME, YEAR/MONTH/DAY_OF_WEEK/..., FROM_UNIXTIME, NOW) — see [docs/sql/functions.md](docs/sql/functions.md)
- **Arrow-native Execution**: Vectorized columnar processing using Apache Arrow
- **Connectors**: In-memory tables, CSV/Parquet files, S3/GCS/Azure object stores, Hive Metastore catalog (HMS 4.x via `_req` API) with Parquet and ORC tables and partitioned tables (partitions read from HMS, as in Trino), Apache Iceberg tables in the same HMS via table redirection (read-only; field-ID schema evolution) — see [Hive](docs/connectors/hive.md)
- **PostgreSQL Wire Protocol**: Compatible with psql, DBeaver, JDBC, psycopg2, node-postgres, and all standard PostgreSQL clients; per-connection `SET search_path`; SCRAM-SHA-256 password authentication
- **Extended Query Protocol**: Full prepared statement support (Parse/Bind/Describe/Execute/Sync)
- **Trino Client Protocol**: Trino's REST protocol on port 8080, so the `trino` CLI, Trino JDBC, `trino-python-client` (Superset), Metabase's Trino driver and dbt-trino can connect unchanged — see [Trino client compatibility](docs/guide/trino-clients.md)
- **pg_catalog / information_schema**: System catalog tables for client schema browser compatibility
- **Distributed Architecture**: Coordinator/Worker separation with Arrow Flight RPC
- **Web UI**: Dashboard with query monitoring, cluster overview, and worker status
- **TPC-H Benchmark**: 22/22 queries cell-identical to Trino, ~6× lower peak memory on all 22 and faster on 21 of 22 at SF10, with a one-command benchmark harness and comparison tooling

## Releases

The latest release is **v0.1.1** <!-- x-release-please-version --> — see the [release notes](https://github.com/formosa-ventures/arneb/releases/latest) and [`CHANGELOG.md`](CHANGELOG.md). Releases are source releases (no prebuilt binaries yet); build one from its tag:

```bash
git clone --branch v0.1.1 https://github.com/formosa-ventures/arneb.git  # x-release-please-version
cd arneb && cargo build --release
```

`main` moves faster than releases. How releases are cut and versioned is described in [Contributing → Releases](docs/contributing.md#releases).

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

# ...or with any Trino client (Trino REST protocol, port 8080)
trino --server http://127.0.0.1:8080 --catalog memory --schema default

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

# Trino client protocol listener (default: enabled on port 8080). It does not
# check [auth] yet — set enabled = false (or --no-trino) if you need passwords.
[trino]
enabled = true
port = 8080

# Optional: require SCRAM-SHA-256 passwords (default: type = "none").
# Enforced on the pgwire port; the Trino listener follows in a later release.
[auth]
type = "password"

[[auth.users]]
name = "alice"
password_hash = "SCRAM-SHA-256$4096:..."   # generate with: arneb hash-password
```

See [docs/guide/configuration.md](docs/guide/configuration.md#authentication) for authentication details.

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
├── iceberg/       # Iceberg reader (hive catalogs redirect Iceberg tables here)
├── hive-metastore/# Auto-generated Thrift bindings from Hive 4.2.0 IDL
├── protocol/      # PostgreSQL wire protocol (Simple + Extended Query), Trino client REST protocol
├── scheduler/     # QueryTracker, NodeRegistry, resource groups
├── rpc/           # Arrow Flight RPC for distributed execution
└── server/        # Main binary, CLI, config, Web UI
```

## Hive Metastore + S3

Run Arneb against a real Hive Metastore backed by S3-compatible storage:

```bash
# 1. Start RustFS + HMS + Trino
docker compose up -d

# 2. Seed TPC-H data (Parquet on RustFS, tables registered in HMS)
docker compose run --rm tpch-seed

# 3. Start Arneb with Hive catalog config
cargo run --bin arneb -- --config benchmarks/tpch/tpch-hive.toml

# 4. Query via psql (or DBeaver / any Postgres client)
psql -h 127.0.0.1 -p 5432 -c "SELECT COUNT(*) FROM datalake.tpch.nation;"

# 5. Tear down
docker compose down
```

Iceberg tables in the same metastore are served by the same `hive` catalog
(`docker compose run --rm iceberg-seed` creates sample tables via Trino). See
[docs/connectors/iceberg.md](docs/connectors/iceberg.md).

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
- **A clean baseline per query.** The cluster is recreated before each
  measurement, so it starts from its own baseline rather than its
  predecessor's high-water mark. A cold measurement also restarts the object
  store and drops the container VM's page cache, so neither engine reads
  bytes the other — or its own previous run — left cached.
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

One host, one scale factor, one run per protocol per query in the table. A
second full run under identical settings the same day moved per-query
speedups by a median of 5–6% (worst 29%, `q16` cold) and the geomeans by at
most 0.1×; arneb's `q18` peak memory was the one unstable figure (3.0 GB vs
4.7 GB). Single-query differences under ~30% are not results. Numbers from two different
hosts are not comparable; a scenario makes a run repeatable, not
hardware-independent.

SF30 figures cited previously in this repository were produced under the cold
protocol and have not been re-measured warm.

`q11` numbers measured at SF10 before 2026-10-13 timed an empty result: the
query hard-coded the SF1 threshold fraction, so above SF1 both engines filtered
out every row. It now scales with SF; see
[`benchmarks/tpch/README.md`](benchmarks/tpch/README.md#q11).

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
