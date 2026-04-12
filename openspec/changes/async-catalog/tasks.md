## 1. Catalog Trait Conversion

- [x] 1.1 Add `async_trait` dependency to `crates/catalog/Cargo.toml`
- [x] 1.2 Convert `CatalogProvider` trait methods to async with `#[async_trait]`
- [x] 1.3 Convert `SchemaProvider` trait methods to async with `#[async_trait]`
- [x] 1.4 Convert `CatalogManager::resolve_table()` and related methods to async

## 2. Implementation Updates

- [x] 2.1 Update `MemoryCatalog` and `MemorySchema` impls for async traits
- [x] 2.2 Update `FileCatalog` and `FileSchema` impls in `crates/connectors/` for async traits
- [x] 2.3 Update DDL provider call sites for async catalog operations

## 3. Caller Updates

- [x] 3.1 Update `crates/planner/` — table resolution calls add `.await`
- [x] 3.2 Update `crates/protocol/src/handler.rs` — metadata queries, pg_catalog interception
- [x] 3.3 Update `crates/protocol/src/metadata.rs` — information_schema and pg_catalog handlers
- [x] 3.4 Update `crates/server/src/main.rs` and `coordinator.rs` — catalog setup
- [x] 3.5 Update `crates/execution/src/planner.rs` — data source registration

## 4. Test Updates

- [x] 4.1 Update all unit tests in `crates/catalog/` for async
- [x] 4.2 Update integration tests in `crates/protocol/tests/` and `crates/server/tests/`
- [x] 4.3 Run full test suite — verify no behavioral changes
