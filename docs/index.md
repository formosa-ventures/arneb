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

## 1.59x faster than Trino on TPC-H

Not a claim you have to take on trust — one command reproduces it on your own
machine.

| | Arneb | Trino |
|---|---:|---:|
| Queries completed | 22 / 22 | 22 / 22 |
| Geomean p50 latency | **277.6 ms** | 442.8 ms |
| Queries where Arneb is ahead | **22 / 22** | — |
| Range across queries | 1.07x – 2.60x faster | — |

Both engines read the same Parquet from MinIO through Hive Metastore, run as a
coordinator plus two workers with identical CPU allocations, and execute inside
the same container stack. Their results agree on every query.

These are **default-configuration** figures: a stock build with no tuning options
enabled, so the setup you reproduce is the setup they came from.

```bash
git clone https://github.com/formosa-ventures/arneb.git && cd arneb
./benchmarks/tpch/scripts/run_benchmark.sh
```

TPC-H SF1, measured 2026-07-25 on an Apple M1 Pro (10 cores, 32 GB, macOS
26.5.2, arm64). Laptop numbers on arm64 macOS — reproducible on comparable
hardware, not a projection to Linux x86 servers. Per-query results, the raw
result documents and the run's provenance are published in
[`benchmarks/tpch/official/v1.0.0/`](https://github.com/formosa-ventures/arneb/tree/main/benchmarks/tpch/official/v1.0.0).

[Full reproduction tutorial →](https://github.com/formosa-ventures/arneb/blob/main/benchmarks/tpch/TUTORIAL.md)
