# query-memory-limit Specification

## Purpose
TBD - created by archiving change admission-control. Update Purpose after archive.
## Requirements
### Requirement: Per-query memory pool on every node
Each node that executes part of a query SHALL account that query's memory in its own pool, a child of the node's memory pool, shared by every task of that query on that node. When `[memory] query_max_memory_per_node` (or `ARNEB_QUERY_MAX_MEMORY_BYTES`) is set, a reservation that would take the query's total on that node above the limit SHALL fail with `ExecutionError::ResourceExhausted`, without affecting reservations of other queries on the same node. The query's pool SHALL be released when its last task on the node ends or when the query is cancelled. When no limit is set, a query SHALL be bounded only by the node pool.

#### Scenario: One query over its limit
- **WHEN** `query_max_memory_per_node` is 1 GiB, query A tries to reserve 1.5 GiB on worker-1, and query B runs concurrently on worker-1 using 200 MiB
- **THEN** query A fails with a memory-limit error naming worker-1 and the 1 GiB limit, and query B completes with correct results

#### Scenario: Limit is per query, not per node
- **WHEN** `query_max_memory_per_node` is 1 GiB and three concurrent queries each reserve 600 MiB on the same worker, within the node pool's budget
- **THEN** all three queries complete

#### Scenario: Pool released after the query
- **WHEN** a query finishes, fails or is cancelled
- **THEN** its per-query pool on each node is dropped and the node pool's reserved bytes return to what other queries hold

### Requirement: Memory-limit error class
A query that fails because of a memory limit SHALL be reported to pgwire clients with SQLSTATE `53200` (`out_of_memory`) and to Trino clients with `EXCEEDED_LOCAL_MEMORY_LIMIT` (code 131079, type `INSUFFICIENT_RESOURCES`), whether the limit was hit on the coordinator or on a worker. The message SHALL name the node and the limit.

#### Scenario: Worker-side limit over pgwire
- **WHEN** a distributed query exceeds its per-node limit on a worker and the client is psql
- **THEN** psql receives `ERROR 53200` with a message naming the worker and the limit

#### Scenario: Worker-side limit over the Trino protocol
- **WHEN** the same query is submitted through the Trino listener
- **THEN** the final `QueryResults` has `error.errorName = "EXCEEDED_LOCAL_MEMORY_LIMIT"` and `error.errorType = "INSUFFICIENT_RESOURCES"`

### Requirement: Typed resource errors across Flight
A worker task that fails with `ResourceExhausted` SHALL be reported to its consumers as tonic status `RESOURCE_EXHAUSTED`, and the coordinator SHALL map that status back to `ExecutionError::ResourceExhausted`. Other task failures SHALL keep their existing mapping.

#### Scenario: Exchange consumer sees the class
- **WHEN** a worker task fails with `ResourceExhausted` and the coordinator fetches its output
- **THEN** the coordinator's error is `ExecutionError::ResourceExhausted`, not `InvalidOperation`

