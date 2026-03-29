## ADDED Requirements

### Requirement: Distributed sort plan generation
The system SHALL generate a distributed sort plan consisting of: local `SortExec` on each worker followed by `MergeOperator` on the coordinator that merges the N sorted streams into a single globally sorted output.

#### Scenario: Distributed ORDER BY
- **WHEN** a distributed query contains `SELECT * FROM orders ORDER BY order_date DESC`
- **THEN** each worker sorts its partition of `orders` locally by `order_date DESC`
- **AND** the coordinator merges all worker outputs using `MergeOperator` with `order_date DESC`

#### Scenario: Multi-key distributed sort
- **WHEN** a distributed query contains `ORDER BY region ASC, revenue DESC`
- **THEN** workers sort locally by `(region ASC, revenue DESC)`
- **AND** the coordinator merges with the same sort keys and directions

### Requirement: Sort with LIMIT optimization
The system SHALL push LIMIT into worker-local sorts when a distributed query includes both ORDER BY and LIMIT. Each worker produces at most LIMIT rows (after local sort), reducing data transferred to the coordinator. The coordinator's MergeOperator then applies the final LIMIT.

#### Scenario: Distributed ORDER BY with LIMIT
- **WHEN** a distributed query contains `ORDER BY revenue DESC LIMIT 10` across 4 workers
- **THEN** each worker sorts locally and emits at most 10 rows
- **AND** the coordinator merges up to 40 rows and returns the top 10

### Requirement: Coordinator merge output
The `MergeOperator` on the coordinator SHALL produce a single output stream of `RecordBatch` results in globally sorted order. This output feeds into any downstream operators (LIMIT, projection) on the coordinator fragment.

#### Scenario: Merge feeds into LIMIT
- **WHEN** the coordinator fragment has `MergeOperator` → `LimitExec`
- **THEN** the merge produces sorted batches and the limit truncates the result
