## Context

trino-alt has `common` (shared types, errors) and `sql-parser` (SQL → AST) crates. The next step is a catalog system so the planner can resolve table references to column schemas. In Trino's architecture, catalogs are the top-level namespace: `catalog.schema.table`. Each catalog is backed by a connector, but at the catalog layer we only care about metadata (table names and column schemas), not data access.

Project conventions: trait-based abstractions with `Arc<dyn Trait>`, `thiserror` errors, `#[non_exhaustive]` on extensible enums.

## Goals / Non-Goals

**Goals:**

- Define trait-based catalog abstractions that connectors can implement later
- Provide in-memory implementations for MVP testing
- Support Trino's three-part naming (`catalog.schema.table`) with configurable defaults
- Resolve `TableReference` → `Vec<ColumnInfo>` for planner consumption
- Comprehensive unit test coverage

**Non-Goals:**

- No persistent storage (catalogs are rebuilt at startup for MVP)
- No catalog DDL (CREATE SCHEMA, CREATE TABLE via SQL)
- No statistics or partition metadata (needed for cost-based optimization, Phase 2)
- No concurrent mutation guarantees beyond basic thread safety

## Decisions

### D1: Trait hierarchy — Three-level vs Flat

**Choice**: Three-level trait hierarchy: `CatalogProvider` → `SchemaProvider` → `TableProvider`, mirroring Trino's `catalog.schema.table` naming.

**Rationale**: Matches the mental model users expect from Trino. Each level has distinct responsibilities (catalog = namespace of schemas, schema = namespace of tables, table = column metadata). The planner resolves references top-down.

**Alternative**: Flat `Catalog` with `get_table(catalog, schema, table)`. Rejected because it prevents connectors from managing their own schema enumeration and forces one monolithic implementation.

### D2: TableProvider scope — Metadata only vs Metadata + Data access

**Choice**: Metadata only for now. `TableProvider` exposes `schema() -> Vec<ColumnInfo>`. Data access (`scan()`) will be added when the execution/connectors crates are built.

**Rationale**: The catalog crate's purpose is metadata resolution for the planner. Coupling data access here would create a dependency on Arrow RecordBatch streaming before it's needed. The trait can be extended later since it uses `Arc<dyn Trait>`.

### D3: Storage — HashMap vs BTreeMap

**Choice**: `HashMap<String, ...>` for all in-memory storage.

**Rationale**: Table/schema lookups are by exact name (no range queries needed). HashMap provides O(1) lookup which is sufficient for MVP. The number of schemas/tables is small enough that ordering doesn't matter.

### D4: Thread safety — Interior mutability approach

**Choice**: Wrap mutable collections in `RwLock` so the traits can use `&self` (not `&mut self`), enabling `Arc<dyn Trait>` sharing across threads.

**Rationale**: The planner will hold `Arc<CatalogManager>` and call it from potentially multiple planning tasks. `RwLock` allows concurrent reads (schema lookups) with exclusive writes (registration). Registration happens at startup; lookups happen during query planning.

### D5: Error handling — CatalogError reuse

**Choice**: Reuse `CatalogError` from `trino-common`. The `resolve_table` method returns `Result<Arc<dyn TableProvider>, CatalogError>`.

**Rationale**: `CatalogError` already has `CatalogNotFound`, `SchemaNotFound`, and `TableAlreadyExists` variants defined. No new error types needed.

## Risks / Trade-offs

**[Trait evolution]** → Adding methods to the traits later is a breaking change for implementors. **Mitigation**: Keep traits minimal for MVP. When extending, add methods with default implementations.

**[RwLock contention]** → Under high concurrency, writers (schema registration) block readers (table lookups). **Mitigation**: Registration happens at startup only; during query execution all access is read-only, so contention won't occur in practice.

**[No statistics]** → Without table statistics, the optimizer can't make cost-based decisions. **Mitigation**: Acceptable for MVP rule-based optimization. Statistics can be added to `TableProvider` in a future phase.
