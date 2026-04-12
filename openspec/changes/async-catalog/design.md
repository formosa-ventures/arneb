## Context

Arneb's catalog traits (`CatalogProvider`, `SchemaProvider`, `TableProvider`) in `crates/catalog/src/lib.rs` use synchronous method signatures. All current implementations are in-memory and return immediately, but the upcoming Hive Metastore connector requires async I/O to fetch metadata from a remote Thrift service.

The entire engine downstream of catalog resolution is already async: `DataSource::scan()`, protocol handler, execution operators. The catalog is the only sync layer, and wrapping async HMS calls in `block_on` within a tokio runtime would panic.

## Goals / Non-Goals

**Goals:**
- Make CatalogProvider, SchemaProvider, and TableProvider async traits
- Update all callers to use `.await`
- Keep existing in-memory implementations working with minimal changes
- Maintain full test suite passing

**Non-Goals:**
- Adding new catalog implementations (Hive connector is a separate change)
- Changing the CatalogManager's resolution logic or 3-part naming
- Adding caching or lazy loading mechanisms

## Decisions

### 1. Full async conversion over hybrid approach

**Choice**: Convert all catalog traits to async rather than maintaining parallel sync/async traits.

**Alternatives considered**:
- **Hybrid (sync + async traits)**: Two trait hierarchies with bridge code. Rejected because `block_on` inside async context panics, and maintaining two parallel hierarchies doubles maintenance cost.
- **Cache-only**: Keep sync traits, cache remote metadata. Rejected because it adds staleness concerns and defers the inevitable async conversion.

**Rationale**: Arneb is async-first. In-memory catalogs trivially implement async (just add the keyword, no `.await` needed). The call chain from protocol handler through planner to catalog is already async, so propagating `.await` is mechanical.

### 2. Use `#[async_trait]` macro

**Choice**: Use `async_trait` crate for async trait methods.

**Rationale**: Rust stable does not yet have native async trait methods with dynamic dispatch (`dyn Trait`). Since catalog traits are used as `Arc<dyn CatalogProvider>`, we need `#[async_trait]` for object safety.

### 3. Mechanical refactor approach

**Choice**: Change traits first, then fix all compilation errors. No behavioral changes.

The refactor follows a strict pattern:
1. Add `#[async_trait]` to trait definitions
2. Add `async` to method signatures
3. Add `#[async_trait]` to all `impl` blocks
4. Add `.await` at all call sites
5. Run tests — behavior unchanged

## Risks / Trade-offs

- **[Compile-time overhead]** → `async_trait` adds `Box::pin(async move { ... })` overhead per call. Mitigation: For in-memory catalogs this is negligible (~nanoseconds). Only matters for hot paths, and catalog resolution is not a hot path.
- **[Large diff]** → Many files touched for a mechanical change. Mitigation: Pure refactor, no behavioral changes. Easy to review by verifying tests still pass.
- **[Ordering dependency]** → Must be completed before hive-connector can be implemented. Mitigation: Small, well-scoped change that can be done in one session.
