## MODIFIED Requirements

### Requirement: Memory catalog implements async traits
The `MemoryCatalog` and `MemorySchema` implementations SHALL implement the async versions of `CatalogProvider` and `SchemaProvider`. Behavior SHALL remain identical (in-memory HashMap lookups).

#### Scenario: Async memory schema lookup
- **WHEN** `MemoryCatalog::schema("default").await` is called
- **THEN** the system SHALL return the schema from the in-memory HashMap immediately (no actual async I/O)
