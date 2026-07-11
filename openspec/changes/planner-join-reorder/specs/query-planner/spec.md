## MODIFIED Requirements

### Requirement: Planning SELECT statements

The `QueryPlanner` SHALL convert a parsed `Statement::Query` into a `LogicalPlan` by:
1. Resolving FROM clause tables via the catalog
2. Building an initial join tree from FROM items (the textual order is treated as a starting point, NOT as the final join order)
3. Adding a Filter node for the WHERE clause
4. Adding an Aggregate node for GROUP BY / HAVING
5. Adding a Projection node for the SELECT list (expanding wildcards)
6. Adding a Sort node for ORDER BY
7. Adding a Limit node for LIMIT/OFFSET

The final join order is determined by the `JoinReorder` analyzer pass, which runs after `TypeCoercion` and rewrites inner-join sub-trees according to the cost model. `plan_from` no longer establishes the authoritative join order.

#### Scenario: Simple SELECT with filter

- **WHEN** `SELECT name FROM users WHERE id > 10` is planned with a catalog containing "users" table (id: Int64, name: Utf8)
- **THEN** it produces `Projection(Filter(TableScan(users), id > 10), [name])`

#### Scenario: Multi-table SELECT goes through reorder pass

- **GIVEN** `SELECT * FROM small_table s, big_table b WHERE s.k = b.k` with statistics showing `big_table` ≫ `small_table`
- **WHEN** the query is planned
- **THEN** the resulting plan after the analyzer has `big_table` on the probe side of the hash join (smallest relation builds the hash table)
- **AND** the order differs from the SQL textual order (`small_table` listed first)

#### Scenario: SELECT with wildcard expansion

- **WHEN** `SELECT * FROM users` is planned with a catalog containing "users" table (id: Int64, name: Utf8)
- **THEN** the Projection contains columns for both "id" and "name"

### Requirement: QueryPlanner gathers statistics at planning time

`QueryPlanner::plan_query` SHALL traverse all `TableScan` nodes in the constructed `LogicalPlan` and populate a `CatalogStats` by calling `TableProvider::statistics()` on each referenced table. The populated `CatalogStats` is then threaded through `AnalyzerContext` so the `JoinReorder` pass and cost model can access it.

When a `TableProvider::statistics()` call is potentially expensive (e.g. HMS Thrift round-trip), the planner SHALL batch the calls per catalog to amortize latency where the connector supports batched fetch.

#### Scenario: Stats populated for every FROM table

- **GIVEN** a query referencing tables `t1`, `t2`, `t3`
- **WHEN** `plan_query` runs
- **THEN** `CatalogStats` contains entries for `t1`, `t2`, `t3` (some may be `None` if a connector has no stats)

#### Scenario: Stats failure does not break planning

- **GIVEN** a `TableProvider::statistics()` implementation that returns an error result wrapped in `Option::None`
- **WHEN** `plan_query` runs
- **THEN** the planner logs a `tracing::warn` and proceeds with default-size estimates; the query plans and executes successfully
