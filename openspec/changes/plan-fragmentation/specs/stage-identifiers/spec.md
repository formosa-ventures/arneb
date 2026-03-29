## ADDED Requirements

### Requirement: StageId type
The system SHALL define a `StageId(u32)` newtype in the common crate representing a unique identifier for a query stage. It SHALL derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, and implement `Display` (formatting as the inner u32 value).

#### Scenario: Creating and displaying a StageId
- **WHEN** `StageId(3)` is created and formatted with `Display`
- **THEN** it produces the string `"3"`

#### Scenario: StageId equality
- **WHEN** `StageId(1)` is compared with `StageId(1)` and `StageId(2)`
- **THEN** `StageId(1) == StageId(1)` is true and `StageId(1) == StageId(2)` is false

### Requirement: TaskId type
The system SHALL define a `TaskId` struct with fields `stage_id: StageId` and `partition_id: u32`, identifying a specific parallel instance of a stage. It SHALL derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, and implement `Display` (formatting as `"stage_id.partition_id"`).

#### Scenario: Creating and displaying a TaskId
- **WHEN** `TaskId { stage_id: StageId(2), partition_id: 5 }` is formatted with `Display`
- **THEN** it produces the string `"2.5"`

#### Scenario: TaskId as HashMap key
- **WHEN** `TaskId` values are used as keys in a `HashMap`
- **THEN** equal TaskId values hash to the same bucket and retrieve the same value

### Requirement: SplitId type
The system SHALL define a `SplitId(String)` newtype representing a unique identifier for a data split (a unit of input data assigned to a task). It SHALL derive `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`, and implement `Display`.

#### Scenario: Creating a SplitId from a string
- **WHEN** `SplitId("file:///data/part-00001.parquet".to_string())` is created
- **THEN** `Display` produces `"file:///data/part-00001.parquet"`

#### Scenario: SplitId equality
- **WHEN** two `SplitId` values with the same string are compared
- **THEN** they are equal
