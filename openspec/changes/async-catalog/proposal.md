## Why

The `CatalogProvider`, `SchemaProvider`, and `TableProvider` traits are currently synchronous, but Arneb needs to support remote catalog sources (Hive Metastore, REST catalogs) that require async I/O. Since the rest of the engine is already async-first (execution, protocol, RPC), the catalog layer is the last sync holdout. Making it async aligns the architecture and unblocks the Hive connector.

## What Changes

- Convert `CatalogProvider`, `SchemaProvider`, and `TableProvider` traits to async
- Update all existing implementations (memory catalog, file catalog) — trivial changes since they are in-memory operations
- Update all callers: planner, protocol handler, server startup, DDL operations
- Add `async_trait` to catalog crate dependencies

## Capabilities

### New Capabilities

_(none — this is a refactor of existing traits)_

### Modified Capabilities
- `catalog-traits`: CatalogProvider, SchemaProvider, and TableProvider methods become async
- `catalog-memory`: MemoryCatalog/MemorySchema implementations updated for async traits
- `file-connector`: FileCatalog/FileSchema implementations updated for async traits

## Impact

- **Crates modified**: `catalog`, `connectors`, `planner`, `execution`, `protocol`, `server`
- **No new dependencies** (catalog crate already uses `async_trait` indirectly)
- **No breaking external API**: This is an internal trait change. The pgwire protocol interface is unaffected.
- **All callers are already in async contexts**, so adding `.await` is straightforward
