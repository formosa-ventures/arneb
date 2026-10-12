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
  - title: Arrow-Native
    details: All intermediate data in Apache Arrow columnar format. No row-by-row processing.
  - title: PostgreSQL Compatible
    details: Full Simple and Extended Query protocol. Works with psql, DBeaver, JDBC, and psycopg2 out of the box.
  - title: Trino Client Compatible
    details: Trino's REST protocol, so the trino CLI, Trino JDBC, Superset, Metabase and dbt-trino connect unchanged.
  - title: Federated Queries
    details: Query CSV and Parquet files, S3/GCS/Azure object stores, and Hive Metastore catalogs (Parquet, ORC, partitioned and Iceberg tables) from a single SQL interface.
  - title: Distributed Execution
    details: Coordinator-worker architecture with Apache Arrow Flight RPC for high-throughput data exchange.
---
