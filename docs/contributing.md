# Contributing

Thank you for your interest in contributing to Arneb!

## Prerequisites

- **Rust** — version 1.85.0 or later ([install via rustup](https://rustup.rs/))
- **Node.js** — version 24 or later (for the Web UI)
- **pnpm** — version 10 or later (`npm install -g pnpm`)
- **Docker** — for running the Hive Metastore demo (optional)

## Building

```bash
# Debug build
cargo build

# Release build
cargo build --release
```

The Web UI (in `web/`) must be built before the Rust binary if you need embedded frontend assets:

```bash
cd web && pnpm install && pnpm build
```

## Running Tests

```bash
# Run all tests
cargo test

# Run tests with stdout output
cargo test -- --nocapture

# Run a specific test
cargo test test_name
```

## Linting and Formatting

```bash
# Check formatting
cargo fmt -- --check

# Auto-format
cargo fmt

# Lint with clippy (warnings as errors)
cargo clippy -- -D warnings
```

## Running the TPC-H Benchmark

```bash
cd benchmarks/tpch
cargo run --release -- --engine arneb --port 5432
```

Make sure an Arneb instance is running with the TPC-H data loaded before running the benchmark.

## Running the Hive Demo

```bash
# Start MinIO + HMS + Trino
docker compose up -d

# Seed TPC-H data
docker compose run --rm tpch-seed

# Start Arneb with Hive catalog
cargo run --bin arneb -- --config benchmarks/tpch/tpch-hive.toml

# Query
psql -h 127.0.0.1 -p 5432 -c "SELECT COUNT(*) FROM datalake.tpch.nation;"

# Tear down
docker compose down
```

## Pull Request Workflow

1. Fork the repository and create a feature branch
2. Make your changes
3. Ensure `cargo fmt -- --check` and `cargo clippy -- -D warnings` pass
4. Run `cargo test` and verify all tests pass
5. Submit a pull request against `main`

## Code Conventions

- Use `thiserror` for library error types, `anyhow` only in the server binary
- Use `Arc<dyn Trait>` for polymorphic plan nodes and operators
- All public APIs get doc comments; internal functions don't need them
- Tests live in `#[cfg(test)] mod tests` within source files (unit) or the `tests/` directory (integration)
- Use `tracing` for logging, not `log`
- Quoted identifiers: use `Ident.value`, not `to_string()`

## Updating Documentation

When changing user-facing behavior (SQL syntax, configuration options, connector features), please update the relevant documentation in the `docs/` directory alongside your code changes.
