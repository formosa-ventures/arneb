## MODIFIED Requirements

### Requirement: File catalog implements async traits
The `FileCatalog` and `FileSchema` implementations SHALL implement the async versions of `CatalogProvider` and `SchemaProvider`. Behavior SHALL remain identical (in-memory registry lookups).

#### Scenario: Async file schema table lookup
- **WHEN** `FileSchema::table("events").await` is called
- **THEN** the system SHALL return the table from the in-memory registry immediately (no actual async I/O)
