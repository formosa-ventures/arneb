---
layout: home

hero:
  name: Arneb
  text: Distributed SQL Query Engine
  tagline: A Trino alternative built in Rust. Federated queries across heterogeneous data sources, served over the PostgreSQL wire protocol and Trino's client protocol.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/quickstart
    - theme: alt
      text: GitHub
      link: https://github.com/formosa-ventures/arneb
    - theme: alt
      text: Release notes
      link: https://github.com/formosa-ventures/arneb/releases/latest

features:
  - title: Faster and Leaner than Trino
    details: On all 22 TPC-H queries, cell-identical results. SF10 2.3× faster warm at about a seventh of Trino's peak memory; SF30 3.5× faster cold at 0.22× the memory.
    link: /guide/performance
    linkText: See the numbers
  - title: Arrow-Native
    details: All intermediate data in Apache Arrow columnar format. No row-by-row processing.
  - title: PostgreSQL Compatible
    details: Full Simple and Extended Query protocol. Works with psql, DBeaver, JDBC, and psycopg2 out of the box.
  - title: Trino Client Compatible
    details: Trino's REST protocol, so the trino CLI, Trino JDBC, Superset, Metabase and dbt-trino connect unchanged.
  - title: Federated Queries
    details: Query CSV and Parquet files, S3/GCS/Azure object stores, Hive Metastore catalogs (Parquet, ORC, partitioned and Iceberg tables) and PostgreSQL databases from a single SQL interface.
  - title: Distributed Execution
    details: Coordinator-worker architecture with Apache Arrow Flight RPC for high-throughput data exchange.
  - title: Secure and Governed
    details: TLS and password authentication on both client protocols, a secret plus TLS between nodes, a query queue, per-query memory limits and statement timeouts.
    link: /guide/configuration#admission-control
    linkText: Configure it
---
