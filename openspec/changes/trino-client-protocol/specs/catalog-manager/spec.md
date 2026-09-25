## ADDED Requirements

### Requirement: Session views with their own defaults
The system SHALL provide `CatalogManager::with_session_defaults(catalog, schema)` returning a manager that shares every registered catalog (including ones registered later on the original) but resolves unqualified table references against the given catalog and schema. `qualify_table_reference` SHALL return the reference unchanged on the original manager and fully qualified (missing parts filled from the view's defaults) on a session view.

#### Scenario: Resolving through a session view
- **WHEN** the server default is `memory.default` and a view is created with defaults `lake.sales`
- **THEN** resolving the unqualified table `t` through the view finds `lake.sales.t`, and `qualify_table_reference` yields `lake.sales.t`

#### Scenario: Root manager unchanged
- **WHEN** `qualify_table_reference` is called on the original manager
- **THEN** the reference is returned unchanged

#### Scenario: Shared registrations
- **WHEN** a catalog is registered on the original manager after a view was created
- **THEN** the view can see that catalog
