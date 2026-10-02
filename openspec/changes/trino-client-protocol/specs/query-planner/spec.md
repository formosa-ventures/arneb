## ADDED Requirements

### Requirement: ORDER BY columns outside the SELECT list
The system SHALL plan `ORDER BY` keys that are not part of the SELECT output when the query's final operator is a projection, by resolving the keys against the projection's input (keys naming an output column or alias use that column's projection expression) and sorting beneath the projection. Keys that resolve against neither SHALL still fail with the original planning error.

#### Scenario: Sort by a non-selected column
- **WHEN** planning `SELECT name FROM users ORDER BY id DESC`
- **THEN** the plan is a Projection of `name` over a Sort on `id` descending

#### Scenario: Alias and hidden column together
- **WHEN** planning `SELECT name AS n FROM users ORDER BY n, id`
- **THEN** planning succeeds

#### Scenario: Unknown column
- **WHEN** planning `SELECT name FROM users ORDER BY nope`
- **THEN** planning fails with a column-not-found error

### Requirement: Session-qualified table scans
The system SHALL record in each `TableScan` the reference returned by the catalog manager's `qualify_table_reference`, so plans built against a session view carry fully qualified table references.

#### Scenario: Planning against a session view
- **WHEN** `SELECT * FROM orders` is planned against a view with defaults `lake.sales`
- **THEN** the TableScan's table reference is `lake.sales.orders`
