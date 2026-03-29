## MODIFIED Requirements

### Requirement: QueryId type
The system SHALL add a `QueryId` newtype wrapping `uuid::Uuid` to the common crate. `QueryId` SHALL derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash` and implement `Display` (formatting as the UUID string).

#### Scenario: Creating a new QueryId
- **WHEN** `QueryId::new()` is called
- **THEN** a unique `QueryId` is returned using UUID v7 (time-ordered)

#### Scenario: Display formatting
- **WHEN** a `QueryId` is formatted with `Display`
- **THEN** it produces a standard UUID string (e.g., "01912345-6789-7abc-8def-0123456789ab")

### Requirement: QueryId uniqueness
Each call to `QueryId::new()` SHALL produce a globally unique identifier. Two calls SHALL never return the same value.

### Requirement: QueryId ordering
`QueryId` values generated later SHALL sort after values generated earlier due to UUID v7's time-based prefix.
