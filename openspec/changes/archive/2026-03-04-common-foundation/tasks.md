## 1. Workspace & Crate Setup

- [x] 1.1 Create root `Cargo.toml` with workspace configuration, add `crates/common` as member
- [x] 1.2 Create `crates/common/Cargo.toml` with package name `trino-common`, add dependencies: `thiserror`, `serde` (with `derive` feature), `toml`, `tracing`, `arrow` (with `datatypes` feature)
- [x] 1.3 Create `crates/common/src/lib.rs` with public module declarations: `pub mod error;`, `pub mod types;`, `pub mod config;` and re-exports

## 2. Error Types (`error` module)

- [x] 2.1 Implement `ParseError` enum with `#[non_exhaustive]`: `InvalidSyntax(String)`, `UnsupportedFeature(String)`
- [x] 2.2 Implement `PlanError` enum with `#[non_exhaustive]`: `TableNotFound(String)`, `ColumnNotFound(String)`, `TypeMismatch { expected, found }`, `InvalidExpression(String)`
- [x] 2.3 Implement `ExecutionError` enum with `#[non_exhaustive]`: `ArrowError(arrow::error::ArrowError)`, `InvalidOperation(String)`, `ResourceExhausted(String)`
- [x] 2.4 Implement `ConnectorError` enum with `#[non_exhaustive]`: `ConnectionFailed(String)`, `TableNotFound(String)`, `ReadError(String)`, `UnsupportedOperation(String)`
- [x] 2.5 Implement `CatalogError` enum with `#[non_exhaustive]`: `CatalogNotFound(String)`, `SchemaNotFound(String)`, `TableAlreadyExists(String)`
- [x] 2.6 Implement `ConfigError` enum with `#[non_exhaustive]`: `FileNotFound(String)`, `ParseError(String)`, `InvalidValue { key, value, reason }`
- [x] 2.7 Implement top-level `TrinoError` enum composing all domain errors via `#[from]`, with `#[non_exhaustive]`
- [x] 2.8 Write unit tests: error creation, `Display` formatting, `From` conversions, `source()` chaining

## 3. Data Types (`types` module)

- [x] 3.1 Implement `TimeUnit` enum (`Second`, `Millisecond`, `Microsecond`, `Nanosecond`) with `From`/`Into` conversions to `arrow::datatypes::TimeUnit`
- [x] 3.2 Implement `DataType` enum with all MVP variants (`Boolean`, `Int8`..`Int64`, `Float32`/`Float64`, `Decimal128 { precision, scale }`, `Utf8`, `LargeUtf8`, `Binary`, `Date32`, `Timestamp { unit, timezone }`, `Null`)
- [x] 3.3 Implement `From<DataType> for arrow::datatypes::DataType` (SQL → Arrow conversion)
- [x] 3.4 Implement `TryFrom<arrow::datatypes::DataType> for DataType` (Arrow → SQL conversion, error on unsupported types)
- [x] 3.5 Implement `TableReference` struct with `parse()` method supporting 1/2/3-part names, `Display` formatting
- [x] 3.6 Implement `ColumnInfo` struct with `name`, `data_type`, `nullable` fields and `Into<arrow::datatypes::Field>` conversion
- [x] 3.7 Implement `ScalarValue` enum with all MVP variants and `data_type()` method returning the corresponding `DataType`
- [x] 3.8 Write unit tests: DataType ↔ Arrow round-trip conversions, TableReference parsing/display, ColumnInfo to Arrow Field, ScalarValue data_type inference

## 4. Server Config (`config` module)

- [x] 4.1 Define `ServerConfig` struct with `bind_address`, `port`, `max_worker_threads`, `max_memory_mb` fields, derive `Deserialize`, `Debug`, `Clone`
- [x] 4.2 Implement `Default` for `ServerConfig` with specified defaults (127.0.0.1, 5432, num_cpus, 1024)
- [x] 4.3 Implement TOML file loading: `ServerConfig::from_file(path)` returning `Result<Self, ConfigError>`
- [x] 4.4 Implement environment variable override: `ServerConfig::apply_env_overrides(&mut self)` with `TRINO_` prefix
- [x] 4.5 Implement `ServerConfig::load(optional_path)` combining file loading + env overrides + validation
- [x] 4.6 Implement validation: port > 0, max_memory_mb > 0, returning `ConfigError::InvalidValue`
- [x] 4.7 Implement `Display` for `ServerConfig` showing all parameters in human-readable format
- [x] 4.8 Write unit tests: default values, TOML parsing, env var overrides, validation errors, missing file fallback to defaults

## 5. Integration & Quality

- [x] 5.1 Verify `cargo build` compiles without warnings
- [x] 5.2 Verify `cargo test -p trino-common` passes all tests
- [x] 5.3 Run `cargo clippy -- -D warnings` and fix any lints
- [x] 5.4 Run `cargo fmt -- --check` and ensure formatting is correct
