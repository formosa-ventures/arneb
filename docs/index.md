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

## 1.76x faster than Trino at TPC-H SF1

Not a claim you have to take on trust — one command reproduces it on your own
machine.

| | Arneb | Trino |
|---|---:|---:|
| Queries completed | 22 / 22 | 22 / 22 |
| Geomean p50 latency | **293.1 ms** | 515.3 ms |
| Queries where Arneb leads | **21 / 22** | — |

Both engines read the same Parquet from MinIO through Hive Metastore and run as a
coordinator plus two workers with identical CPU allocations. Only the engine being
measured is running, so neither is competing with the other for memory. Their
results agree on every query, as do DataFusion's.

These are **default-configuration** figures at **SF1**: a stock build with no
tuning options enabled, so the setup you reproduce is the setup they came from.
Larger scale factors are not published yet — run the benchmark at the scale you
care about rather than extrapolating from this one.

```bash
git clone https://github.com/formosa-ventures/arneb.git && cd arneb
./benchmarks/tpch/scripts/run_benchmark.sh
```

Measured 2026-07-25 on an Apple M1 Pro (10 cores, 32 GB, macOS 26.5.2, arm64).
Laptop numbers on arm64 macOS — reproducible on comparable hardware, not a
projection to Linux x86 servers. Per-query results, the raw result documents and
the run's provenance are published in
[`benchmarks/tpch/official/v1.0.0/sf1/`](https://github.com/formosa-ventures/arneb/tree/main/benchmarks/tpch/official/v1.0.0/sf1).

[Full reproduction tutorial →](https://github.com/formosa-ventures/arneb/blob/main/benchmarks/tpch/TUTORIAL.md)
