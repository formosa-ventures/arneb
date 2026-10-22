# Introduction

Arneb is a distributed SQL query engine built in Rust — a Trino alternative designed for federated queries across heterogeneous data sources.

## Key Features

- **Arrow-Native**: All intermediate data uses [Apache Arrow](https://arrow.apache.org/) columnar format. No row-by-row processing.
- **PostgreSQL Compatible**: Full Simple and Extended Query wire protocol (v3). Connect with `psql`, DBeaver, JDBC drivers, or `psycopg2` — no special client needed.
- **Trino Client Compatible**: Trino's client REST protocol, so the `trino` CLI, Trino JDBC, `trino-python-client`, Metabase and dbt-trino connect unchanged. See [Trino Clients](/guide/trino-clients).
- **Federated Queries**: Query data across CSV files, Parquet files, S3/GCS/Azure object stores, Hive Metastore catalogs (including Iceberg tables) and PostgreSQL databases from a single SQL interface.
- **Distributed Execution**: Coordinator-worker architecture with [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) RPC for high-throughput data exchange between nodes.
- **Async Streaming**: Operators return async record batch streams, enabling pipelined execution without materializing full intermediate results.
- **Pushdown Optimization**: Filters, projections, and limits are pushed into connectors when supported.
- **Low Memory**: No JVM and no committed heap; an idle three-node cluster holds about 70 MB, and TPC-H peaks average 0.14× of Trino's at SF10.
- **Secure**: TLS and password authentication on pgwire and the Trino listener, and a shared secret plus TLS on the Flight RPC between nodes. See [Authentication](/guide/configuration#authentication) and [Securing the Cluster](/guide/distributed#securing-the-cluster).
- **Admission Control**: A cap on running queries with a queue behind it, a per-query memory limit on every node, `statement_timeout`, and cancellation that reaches the workers. See [Admission Control](/guide/configuration#admission-control).

## Supported Data Sources

| Source | Format | Description |
|--------|--------|-------------|
| Local files | CSV, Parquet | Read from local filesystem paths |
| S3 | CSV, Parquet | Amazon S3 and S3-compatible stores (MinIO, LocalStack) |
| GCS | CSV, Parquet | Google Cloud Storage |
| Azure | CSV, Parquet | Azure Blob Storage |
| Hive Metastore | Parquet, ORC | HMS 4.x catalog with automatic table discovery; partitioned tables read the partitions registered in HMS. See [Hive](/connectors/hive) |
| Iceberg (via HMS) | Parquet | Read-only, current snapshot, through the Hive catalog. See [Iceberg](/connectors/iceberg) |
| PostgreSQL | Tables and views | Read-only catalog with filter, projection and limit pushdown and parallel key-range reads. See [PostgreSQL](/connectors/postgresql) |

## Current Status

The latest release is **v0.1.5** <!-- x-release-please-version --> ([release notes](https://github.com/formosa-ventures/arneb/releases/latest), [changelog](https://github.com/formosa-ventures/arneb/blob/main/CHANGELOG.md)). Releases are source releases; see [Quickstart](/guide/quickstart#build-a-release).

All 22 TPC-H queries return results cell-identical to Trino, single-node and distributed. Against Trino on the same hardware, Arneb is faster on all 22 at SF10 (2.3× warm, geomean) at about a seventh of the peak memory, and faster on all 22 at SF30 cold (3.5× at 0.22× the memory); see [Performance](/guide/performance). SQL support includes SELECT, DDL/DML, CTEs, window functions, subqueries, set operations and 90 Trino-compatible scalar functions.

## Next Steps

- [Quickstart](/guide/quickstart) — get Arneb running and execute your first query
- [Configuration](/guide/configuration) — learn about all configuration options
- [Performance](/guide/performance) — TPC-H results against Trino at SF10 and SF30
- [SQL Reference](/sql/overview) — see supported SQL statements and functions
