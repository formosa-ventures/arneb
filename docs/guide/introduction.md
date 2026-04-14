# Introduction

Arneb is a distributed SQL query engine built in Rust — a Trino alternative designed for federated queries across heterogeneous data sources.

## Key Features

- **Arrow-Native**: All intermediate data uses [Apache Arrow](https://arrow.apache.org/) columnar format. No row-by-row processing.
- **PostgreSQL Compatible**: Full Simple and Extended Query wire protocol (v3). Connect with `psql`, DBeaver, JDBC drivers, or `psycopg2` — no special client needed.
- **Federated Queries**: Query data across CSV files, Parquet files, S3/GCS/Azure object stores, and Hive Metastore catalogs from a single SQL interface.
- **Distributed Execution**: Coordinator-worker architecture with [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) RPC for high-throughput data exchange between nodes.
- **Async Streaming**: Operators return async record batch streams, enabling pipelined execution without materializing full intermediate results.
- **Pushdown Optimization**: Filters, projections, and limits are pushed into connectors when supported.

## Supported Data Sources

| Source | Format | Description |
|--------|--------|-------------|
| Local files | CSV, Parquet | Read from local filesystem paths |
| S3 | CSV, Parquet | Amazon S3 and S3-compatible stores (MinIO, LocalStack) |
| GCS | CSV, Parquet | Google Cloud Storage |
| Azure | CSV, Parquet | Azure Blob Storage |
| Hive Metastore | Parquet | HMS 4.x catalog with automatic table discovery |

## Current Status

Arneb has completed Phase 1 (single-node) and Phase 2 (distributed execution). The engine passes 16 out of 22 TPC-H benchmark queries. SQL support includes SELECT, DDL/DML, CTEs, window functions, subqueries, and set operations.

## Next Steps

- [Quickstart](/guide/quickstart) — get Arneb running and execute your first query
- [Configuration](/guide/configuration) — learn about all configuration options
- [SQL Reference](/sql/overview) — see supported SQL statements and functions
