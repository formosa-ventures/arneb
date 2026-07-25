---
layout: home

hero:
  name: Arneb
  text: Distributed SQL Query Engine
  tagline: A Trino alternative built in Rust. Federated queries across heterogeneous data sources with PostgreSQL wire compatibility.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/quickstart
    - theme: alt
      text: GitHub
      link: https://github.com/formosa-ventures/arneb

features:
  - title: Arrow-Native
    details: All intermediate data in Apache Arrow columnar format. No row-by-row processing.
  - title: PostgreSQL Compatible
    details: Full Simple and Extended Query protocol. Works with psql, DBeaver, JDBC, and psycopg2 out of the box.
  - title: Federated Queries
    details: Query CSV, Parquet, S3, GCS, Azure, and Hive Metastore catalogs from a single SQL interface.
  - title: Distributed Execution
    details: Coordinator-worker architecture with Apache Arrow Flight RPC for high-throughput data exchange.
---

## Measured against Trino on TPC-H

Not claims you have to take on trust — one command reproduces them on your own
machine, and they include the result that does not flatter us.

| | SF1 | SF10 |
|---|---:|---:|
| Arneb geomean p50 | 293.1 ms | 2566.6 ms |
| Trino geomean p50 | 515.3 ms | 1954.8 ms |
| **Arneb vs Trino** | **1.76x faster** | **1.32x slower** |
| Queries where Arneb leads | 21 / 22 | 7 / 22 |
| Queries completed | 22 / 22 both | 22 / 22 both |

Arneb leads at SF1 and falls behind at SF10, and the split is informative: at
SF10 it still wins on scans and simple aggregation — q06 at 1.56x, q14 at
1.47x — while multi-table joins reverse hardest, q02 dropping to 0.15x. The scan
path scales; join execution does not yet. That is where 1.0.0 stands and where
the next work goes.

Results agree across Arneb, Trino and DataFusion at both scales. These are
**default-configuration** figures — a stock build, no tuning options — so the
setup you reproduce is the setup they came from.

```bash
git clone https://github.com/formosa-ventures/arneb.git && cd arneb
./benchmarks/tpch/scripts/run_benchmark.sh              # SF1
TPCH_SF=sf10 ./benchmarks/tpch/scripts/run_benchmark.sh # SF10
```

Measured 2026-07-25 on an Apple M1 Pro (10 cores, 32 GB, macOS 26.5.2, arm64).
Laptop numbers on arm64 macOS — reproducible on comparable hardware, not a
projection to Linux x86 servers. Per-query results, the raw result documents and
each run's provenance are published in
[`benchmarks/tpch/official/v1.0.0/`](https://github.com/formosa-ventures/arneb/tree/main/benchmarks/tpch/official/v1.0.0).

[Full reproduction tutorial →](https://github.com/formosa-ventures/arneb/blob/main/benchmarks/tpch/TUTORIAL.md)
