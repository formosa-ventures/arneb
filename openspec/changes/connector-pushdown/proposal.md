## Why

The current execution engine performs full table scans with no ability to push filters, projections, or limits into connectors. This wastes I/O bandwidth and memory — especially for Parquet files where column pruning and row-group filtering can eliminate 90%+ of unnecessary reads. A pushdown framework is the prerequisite for efficient distributed queries where every byte not read is a byte not shuffled.

## What Changes

- Add `ScanContext` struct to `common` containing optional filter predicates, projection column indices, and limit
- Extend `DataSource::scan()` to accept `&ScanContext`
- Add `ConnectorCapabilities` trait declaring supported pushdowns per connector
- Extend `ConnectorFactory::create_data_source()` to accept pushdown context
- Add `PhysicalPlanOptimizer` framework: ordered list of `OptimizationRule` trait objects
- Implement `ProjectionPushdown` and `FilterPushdown` physical optimization rules
- Update Parquet connector to support projection pushdown and row-group filtering
- Update CSV connector to support projection pushdown
- Update memory connector to support basic filter pushdown

## Capabilities

### New Capabilities
- `scan-context`: ScanContext struct with optional filter, projection, and limit for pushdown
- `connector-capabilities`: ConnectorCapabilities trait declaring what pushdowns a connector supports
- `pushdown-rules`: ProjectionPushdown and FilterPushdown physical optimization rules
- `optimizer-framework`: PhysicalPlanOptimizer pipeline for applying optimization rules in sequence

### Modified Capabilities
- `datasource`: DataSource::scan() accepts ScanContext parameter
- `connector-traits`: ConnectorFactory gains pushdown context, connectors declare capabilities
- `file-connector`: Parquet projection pushdown, CSV projection pushdown
- `memory-connector`: Basic filter pushdown support

## Impact

- **Crates**: execution, connectors, common
- **Breaking**: DataSource::scan() signature change, ConnectorFactory::create_data_source() signature change
- **Dependencies**: No new external deps
