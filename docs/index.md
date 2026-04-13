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
