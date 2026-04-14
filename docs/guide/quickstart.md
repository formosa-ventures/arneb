# Quickstart

Get Arneb running and execute your first query.

## Prerequisites

- **Rust toolchain** — version 1.85.0 or later ([install](https://rustup.rs/))
- **PostgreSQL client** — `psql` or any PostgreSQL-compatible client
- **Sample data** (optional) — a Parquet or CSV file to query

## Build from Source

```bash
git clone https://github.com/formosa-ventures/arneb.git
cd arneb
cargo build
```

For an optimized build:

```bash
cargo build --release
```

## Start the Server

Create a minimal configuration file `arneb.toml`:

```toml
bind_address = "127.0.0.1"
port = 5432

[[tables]]
name = "example"
path = "/path/to/your/data.parquet"
format = "parquet"
```

Start Arneb:

```bash
cargo run --bin arneb -- --config arneb.toml
```

Without a config file, Arneb starts with defaults (bind to `127.0.0.1:5432`, no tables registered):

```bash
cargo run --bin arneb
```

## Connect and Query

Connect using `psql`:

```bash
psql -h 127.0.0.1 -p 5432
```

Run a query:

```sql
SELECT * FROM example LIMIT 10;
```

Use `EXPLAIN` to inspect query plans:

```sql
EXPLAIN SELECT count(*) FROM example WHERE id > 100;
```

## Web UI

When running in standalone or coordinator mode, Arneb serves a Web UI at port `port + 1000`. With the default port of 5432, the Web UI is available at:

```
http://127.0.0.1:6432
```

## What's Next

- [Configuration Reference](/guide/configuration) — all config options, env vars, and CLI args
- [Distributed Mode](/guide/distributed) — run Arneb across multiple nodes
- [Connectors](/connectors/overview) — connect to S3, GCS, Azure, and Hive
