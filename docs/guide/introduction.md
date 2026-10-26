# Introduction

Arneb is a distributed SQL query engine built in Rust — a Trino alternative designed for federated queries across heterogeneous data sources.

## Key Features

- **Arrow-Native**: All intermediate data uses [Apache Arrow](https://arrow.apache.org/) columnar format. No row-by-row processing.
- **PostgreSQL Compatible**: Full Simple and Extended Query wire protocol (v3) and a queryable `pg_catalog`. Connect with `psql`, DBeaver, JDBC drivers, `psycopg2`, psycopg 3, SQLAlchemy or dbt-postgres — no special client needed.
- **Trino Client Compatible**: Trino's client REST protocol for the `trino` CLI, Trino JDBC, `trino-python-client`, Superset's SQLAlchemy dialect and dbt-trino. See [Trino Clients](/guide/trino-clients), and [Client Compatibility](/guide/clients) for what CI checks and what is not supported yet.
- **Federated Queries**: Query data across CSV files, Parquet files, S3/GCS/Azure object stores, Hive Metastore catalogs (including Iceberg tables) and PostgreSQL databases from a single SQL interface, and join across them in one statement. See [Cross-source joins](/connectors/postgresql#cross-source-joins).
- **Distributed Execution**: Coordinator-worker architecture with [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) RPC for high-throughput data exchange between nodes.
- **Async Streaming**: Operators return async record batch streams, enabling pipelined execution without materializing full intermediate results.
- **Pushdown Optimization**: Filters, projections, and limits are pushed into connectors when supported.
- **Low Memory**: No JVM and no committed heap; an idle three-node cluster holds about 70 MB, and TPC-H peaks average 0.14× of Trino's at SF10.
- **Secure**: TLS and password authentication on pgwire and the Trino listener, and a shared secret plus TLS on the Flight RPC between nodes. See [Authentication](/guide/configuration#authentication) and [Securing the Cluster](/guide/distributed#securing-the-cluster).
- **Access Control**: Per-user read rules on catalogs, schemas and tables, in Trino's file-based access control format. See [Access Control](/guide/access-control).
- **Admission Control**: A cap on running queries with a queue behind it, a per-query memory limit on every node, `statement_timeout`, and cancellation that reaches the workers. See [Admission Control](/guide/configuration#admission-control).
- **Worker Failure Handling**: A worker that crashes or hangs fails its queries with an error naming it, instead of hanging them; the rest of the cluster keeps serving. With `retry_policy = "query"` (off by default), a read-only query re-runs on the remaining workers. See [When a Worker Fails](/guide/distributed#when-a-worker-fails).
- **Rolling Upgrades and Kubernetes**: Workers drain on SIGTERM and the coordinator refuses workers on another release. Multi-arch images and a Helm chart are published with every release. See [Stopping a Worker](/guide/distributed#stopping-a-worker) and [Kubernetes](/guide/kubernetes).
- **Observable**: Prometheus metrics on the Web UI port, a slow-query log, an example Grafana dashboard, and the query history as `system.runtime.queries`. See [Observability](/guide/observability).

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

The latest release is **v0.1.7** <!-- x-release-please-version --> ([release notes](https://github.com/formosa-ventures/arneb/releases/latest), [changelog](https://github.com/formosa-ventures/arneb/blob/main/CHANGELOG.md)). Releases are source releases; see [Quickstart](/guide/quickstart#build-a-release).

All 22 TPC-H queries return results cell-identical to Trino, single-node and distributed. Against Trino on the same hardware, Arneb is faster on all 22 at SF10 (2.3× warm, geomean) at about a seventh of the peak memory, and faster on all 22 at SF30 cold (3.5× at 0.22× the memory); see [Performance](/guide/performance). With dimensions in PostgreSQL and facts in Hive, all 22 are still cell-identical to Trino on the same layout. SQL support includes SELECT, DDL/DML, CTEs, window functions, subqueries, set operations and 90 Trino-compatible scalar functions.

## Next Steps

- [Quickstart](/guide/quickstart) — get Arneb running and execute your first query
- [Configuration](/guide/configuration) — learn about all configuration options
- [Kubernetes](/guide/kubernetes) — install the Helm chart
- [Client Compatibility](/guide/clients) — which clients are checked, and the known gaps
- [Performance](/guide/performance) — TPC-H results against Trino at SF10 and SF30
- [Observability](/guide/observability) — metrics, the slow-query log and the query history
- [SQL Reference](/sql/overview) — see supported SQL statements and functions
