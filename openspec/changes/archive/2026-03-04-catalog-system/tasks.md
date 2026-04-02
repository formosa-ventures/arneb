## 1. Crate Setup

- [x] 1.1 Add `crates/catalog` to workspace members in root `Cargo.toml`
- [x] 1.2 Create `crates/catalog/Cargo.toml` with package name `arneb-catalog`, dependencies: `arneb-common` (path)
- [x] 1.3 Create `crates/catalog/src/lib.rs` with module declarations and re-exports

## 2. Trait Definitions

- [x] 2.1 Define `CatalogProvider` trait with `schema_names() -> Vec<String>` and `schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>`
- [x] 2.2 Define `SchemaProvider` trait with `table_names() -> Vec<String>` and `table(&self, name: &str) -> Option<Arc<dyn TableProvider>>`
- [x] 2.3 Define `TableProvider` trait with `schema(&self) -> Vec<ColumnInfo>`
- [x] 2.4 Ensure all traits require `Send + Sync` and have `fmt::Debug`

## 3. In-Memory Implementations

- [x] 3.1 Implement `MemoryTable` struct storing `Vec<ColumnInfo>`, implementing `TableProvider`
- [x] 3.2 Implement `MemorySchema` struct with `RwLock<HashMap<String, Arc<dyn TableProvider>>>`, implementing `SchemaProvider` with `register_table` and `deregister_table` methods
- [x] 3.3 Implement `MemoryCatalog` struct with `RwLock<HashMap<String, Arc<dyn SchemaProvider>>>`, implementing `CatalogProvider` with `register_schema` and `deregister_schema` methods

## 4. CatalogManager

- [x] 4.1 Implement `CatalogManager` struct with catalog registry (`RwLock<HashMap>`), default catalog name, and default schema name
- [x] 4.2 Implement `register_catalog`, `deregister_catalog`, `catalog`, and `catalog_names` methods
- [x] 4.3 Implement `resolve_table(&self, reference: &TableReference) -> Result<Arc<dyn TableProvider>, CatalogError>` with three-part/two-part/one-part resolution logic
- [x] 4.4 Implement `default_catalog()` and `default_schema()` accessor methods

## 5. Tests

- [x] 5.1 Unit tests for `MemoryTable`: construction, schema retrieval, empty table
- [x] 5.2 Unit tests for `MemorySchema`: register/deregister tables, list table names, lookup existing/missing
- [x] 5.3 Unit tests for `MemoryCatalog`: register/deregister schemas, list schema names, lookup existing/missing
- [x] 5.4 Unit tests for `CatalogManager`: register/deregister catalogs, list catalog names
- [x] 5.5 Unit tests for `resolve_table`: fully-qualified, two-part, one-part, catalog not found, schema not found, table not found
- [x] 5.6 Unit tests for thread safety: verify `Arc<dyn CatalogProvider>` is `Send + Sync`

## 6. Integration & Quality

- [x] 6.1 Verify `cargo build` compiles without warnings
- [x] 6.2 Verify `cargo test -p arneb-catalog` passes all tests
- [x] 6.3 Run `cargo clippy -- -D warnings` and fix any lints
- [x] 6.4 Run `cargo fmt -- --check` and ensure formatting is correct
