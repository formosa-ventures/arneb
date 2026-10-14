# Arneb

[![Latest release](https://img.shields.io/github/v/release/formosa-ventures/arneb?sort=semver)](https://github.com/formosa-ventures/arneb/releases/latest)
[![CI](https://github.com/formosa-ventures/arneb/actions/workflows/ci.yml/badge.svg)](https://github.com/formosa-ventures/arneb/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)
[![Docs](https://img.shields.io/badge/docs-arneb.io-blue)](https://arneb.io)

A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

Trino (formerly PrestoSQL) lets users query data where it lives — across object stores, databases, and other systems — using standard SQL. This project aims to achieve similar goals with Rust's performance and safety guarantees.

## Arneb vs Trino — TPC-H SF10

Measured 2026-10-14 on the full 22-query suite. Both engines run as a coordinator
plus two workers, in containers with the same per-node CPU cap, reading the same
Snappy Parquet from RustFS through the same Hive Metastore. `lineitem` is
59,986,052 rows. Host: 10 CPUs, 15.66 GiB of container-runtime memory.

| | Arneb vs Trino |
|---|---|
| **Peak memory** | **0.15× on average — about a seventh of Trino's**, lower on **all 22** queries (best 0.07×, worst 0.29×) |
| **Latency** | **2.3× faster** warm (geomean), faster on **all 22** queries |
| **Correctness** | **Cell-identical to Trino on all 22**, deterministic run-to-run |
| **Footprint** | One self-contained binary (~60 MB). No JVM, no heap tuning, no GC pauses |

### Every query, both protocols

Latency in milliseconds, peak memory in MB. Latency ratios are Trino ÷ arneb
(higher is better for arneb); memory ratios are arneb ÷ Trino (lower is better).
Memory is the peak simultaneous resident set summed across all three cluster
nodes, from the warm run.

| Query | | cold arneb | cold Trino | cold | warm arneb | warm Trino | **warm** | mem arneb | mem Trino | **mem** |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `q01` | Pricing Summary | 2,043 | 6,657 | 3.3× | 1,927 | 2,899 | **1.5×** | 382 | 4,972 | **0.08×** |
| `q02` | Minimum Cost Supplier | 673 | 6,256 | 9.3× | 553 | 2,241 | **4.0×** | 336 | 3,899 | **0.09×** |
| `q03` | Shipping Priority | 2,594 | 8,620 | 3.3× | 2,472 | 4,395 | **1.8×** | 1,953 | 6,646 | **0.29×** |
| `q04` | Order Priority Checking | 1,443 | 7,541 | 5.2× | 1,334 | 3,495 | **2.6×** | 606 | 6,591 | **0.09×** |
| `q05` | Local Supplier Volume | 2,284 | 8,912 | 3.9× | 2,150 | 4,178 | **1.9×** | 1,055 | 6,563 | **0.16×** |
| `q06` | Forecasting Revenue | 806 | 5,214 | 6.5× | 731 | 1,906 | **2.6×** | 354 | 4,863 | **0.07×** |
| `q07` | Volume Shipping | 2,062 | 8,101 | 3.9× | 2,091 | 3,981 | **1.9×** | 1,107 | 5,444 | **0.20×** |
| `q08` | National Market Share | 3,079 | 9,791 | 3.2× | 2,825 | 5,413 | **1.9×** | 1,517 | 7,570 | **0.20×** |
| `q09` | Product Type Profit | 4,947 | 11,734 | 2.4× | 4,813 | 6,638 | **1.4×** | 1,706 | 9,097 | **0.19×** |
| `q10` | Returned Item Reporting | 2,061 | 9,775 | 4.7× | 2,034 | 4,283 | **2.1×** | 846 | 5,292 | **0.16×** |
| `q11` | Important Stock ID | 770 | 5,296 | 6.9× | 668 | 2,305 | **3.5×** | 522 | 3,380 | **0.15×** |
| `q12` | Shipping Modes | 1,208 | 6,339 | 5.2× | 1,137 | 2,473 | **2.2×** | 418 | 4,261 | **0.10×** |
| `q13` | Customer Distribution | 1,648 | 7,596 | 4.6× | 1,492 | 4,125 | **2.8×** | 457 | 6,053 | **0.08×** |
| `q14` | Promotion Effect | 1,310 | 6,903 | 5.3× | 1,185 | 2,671 | **2.3×** | 907 | 4,795 | **0.19×** |
| `q15` | Top Supplier | 1,136 | 6,910 | 6.1× | 1,011 | 3,304 | **3.3×** | 515 | 5,461 | **0.09×** |
| `q16` | Parts/Supplier Relationship | 1,109 | 6,991 | 6.3× | 989 | 2,438 | **2.5×** | 548 | 4,690 | **0.12×** |
| `q17` | Small-Quantity-Order Revenue | 3,100 | 10,280 | 3.3× | 2,939 | 5,282 | **1.8×** | 1,692 | 8,796 | **0.19×** |
| `q18` | Large Volume Customer | 4,154 | 10,080 | 2.4× | 4,001 | 5,272 | **1.3×** | 2,103 | 9,273 | **0.23×** |
| `q19` | Discounted Revenue | 1,478 | 7,052 | 4.8× | 1,344 | 3,139 | **2.3×** | 807 | 5,347 | **0.15×** |
| `q20` | Potential Part Promotion | 1,365 | 8,568 | 6.3× | 1,326 | 4,130 | **3.1×** | 855 | 6,697 | **0.13×** |
| `q21` | Suppliers Who Kept Orders Waiting | 3,346 | 15,156 | 4.5× | 3,264 | 6,894 | **2.1×** | 1,157 | 7,629 | **0.15×** |
| `q22` | Global Sales Opportunity | 835 | 5,517 | 6.6× | 789 | 2,137 | **2.7×** | 327 | 4,138 | **0.08×** |
| | **aggregate** | | | **4.6×** | | | **2.3×** | | | **0.15×** |

Aggregate is the geometric mean for the latency ratios and the arithmetic mean
for the memory ratios.

### What the two protocols mean

**Cold** times each query on a freshly recreated cluster with the storage side
flushed too: RustFS restarted and the container VM's page cache dropped, so
every byte is re-read from disk. **Warm** recreates the cluster, runs one
untimed query first, identically for both engines, then times the next one.

That choice carries most of the headline, which is why both columns are here.
Warming is worth a median 54% to Trino (43–65% across the 22) because a
recreated JVM re-JITs, and 6% to arneb (−1% to +18%), which has no equivalent
start-up cost. Flushing storage barely moves either engine at SF10 — an earlier
run of the same build that left RustFS and the page cache warm got 4.7× cold /
2.2× warm against this run's 4.6× / 2.3×. **The cold
column is therefore largely a measurement of JVM start-up — quote the warm
one.**

### Memory, in absolute terms

Ratios understate what this means for a deployment, so the five largest gaps:

| Query | Arneb | Trino | Saved |
|---|--:|--:|--:|
| `q09` Product Type Profit | 1.7 GB | 8.9 GB | **7.2 GB** |
| `q18` Large Volume Customer | 2.1 GB | 9.1 GB | **7.0 GB** |
| `q17` Small-Quantity-Order Revenue | 1.7 GB | 8.6 GB | **6.9 GB** |
| `q21` Suppliers Who Kept Orders Waiting | 1.1 GB | 7.4 GB | **6.3 GB** |
| `q08` National Market Share | 1.5 GB | 7.4 GB | **5.9 GB** |

A Trino cluster that needs 8.9 GB to run q09 is replaced by one that needs
1.7 GB, on identical hardware and identical data — the difference between a
query that fits on your node and one that does not.

Trino's floor is also high before any query runs: measured across the same
restarts, its three-node cluster sits at **2.01 GB** of committed heap at idle
(1.81–2.12 GB across 44 restarts). Arneb allocates lazily — the same
measurement puts its idle cluster at **0.07 GB**, roughly thirty
times less. Both are counted
the way a container memory limit or `kubectl top` counts them.

### q21, the query Arneb used to lose

`q21` is the four-way correlated `EXISTS` / `NOT EXISTS` self-join over
`lineitem`, the deepest join pipeline in the suite. Warm, arneb lost it in all
five runs through the morning of 2026-10-14 (0.68×–0.84×). It now takes
3,264 ms against Trino's 6,894 ms, **2.1×**, at 0.15× the memory. The semi and
anti joins are hash-partitioned across both workers, and the small outer side
publishes its key set to the `lineitem` scans under `EXISTS` and `NOT EXISTS`,
so about 1.7M matching rows are shuffled instead of all 60M and 38M.

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
- **TPC-H Benchmark**: 22/22 queries cell-identical to Trino, ~7× lower peak memory and faster on all 22 at SF10, with a one-command benchmark harness and comparison tooling

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

One host, one scale factor, one run per protocol per query in the table.
Three full runs under identical settings on 2026-10-13/14 put the
run-to-run noise at a median of 4–6% per-query speedup (worst 29%, `q16`
cold) and at most 0.1× on the geomeans; arneb's `q18` peak memory is the one
unstable figure (2.0, 3.0 and 4.7 GB across the three; tracked in
[#126](https://github.com/formosa-ventures/arneb/issues/126)). Single-query
differences under ~30% are not results. Numbers from two different
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
