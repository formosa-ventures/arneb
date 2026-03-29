# Design: Subquery Support

## Overview

Subqueries are transformed during query planning into join-based or nested-loop-based execution strategies. The goal is to avoid naive per-row subquery execution whenever possible by rewriting subqueries as joins.

## Subquery Type Strategies

### IN Subquery → Semi-Join

An `IN` subquery of the form:

```sql
SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'US')
```

is rewritten during planning as a **left semi-join** between the outer table and the subquery result:

```
SemiJoin(left=Scan(orders), right=Filter(Scan(customers), region='US'), on=customer_id=id)
```

A semi-join returns rows from the left side where at least one matching row exists on the right side, without duplicating left rows. This avoids materializing the full subquery result into a list.

`NOT IN` is rewritten as an **anti-join** (returns left rows with no match on the right).

### EXISTS Subquery → Semi-Join with Boolean

An `EXISTS` subquery of the form:

```sql
SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem l WHERE l.order_id = o.id)
```

is rewritten as a **left semi-join** with the correlation predicate as the join condition:

```
SemiJoin(left=Scan(orders), right=Scan(lineitem), on=orders.id=lineitem.order_id)
```

`NOT EXISTS` is rewritten as an anti-join.

### Scalar Subquery → Execute and Extract Single Value

A scalar subquery of the form:

```sql
SELECT name, (SELECT MAX(total) FROM orders) AS max_total FROM customers
```

is executed independently during planning or at the start of execution. The single resulting value is inlined as a literal in the outer query's expression tree.

**Error handling**: If the scalar subquery returns more than one row, a runtime error SHALL be raised. If it returns zero rows, the result is NULL.

### Correlated Subqueries → Nested Loop

For correlated subqueries that cannot be decorrelated into joins, a nested-loop strategy is used:

1. For each row in the outer query, bind the correlated column values.
2. Execute the inner subquery with those bindings.
3. Use the result to evaluate the predicate or expression.

This is the fallback strategy and is expected to be slow for large datasets. Future optimization passes can attempt decorrelation.

## Planning Phase Changes

The `QueryPlanner` is extended with a subquery detection pass:

1. **Walk the WHERE clause** looking for `Expr::InSubquery`, `Expr::Exists`, and `Expr::Subquery` nodes in the AST.
2. **Classify** each subquery as uncorrelated or correlated by checking whether it references columns from the outer query scope.
3. **Rewrite** uncorrelated IN/EXISTS as semi-joins or anti-joins.
4. **Rewrite** scalar subqueries as a plan node that executes the subquery and projects the scalar result.
5. **Fall back** to nested-loop execution for correlated subqueries that resist decorrelation.

## Execution Phase Changes

New plan nodes and operators:

- **SemiJoinExec**: Executes a semi-join by building a hash set from the right side and probing with the left side. Returns left rows with at least one match.
- **AntiJoinExec**: Inverse of SemiJoinExec — returns left rows with no match.
- **ScalarSubqueryExec**: Executes a subquery, asserts it returns at most one row, and provides the scalar value to the parent expression evaluator.

## Expression Evaluator Changes

The expression evaluator is extended to handle `Expr::ScalarSubquery` by:

1. Looking up the pre-computed scalar value from the `ScalarSubqueryExec` result.
2. Returning it as a `ScalarValue` that can participate in arithmetic, comparison, and other expressions.

## Data Flow

```
AST with subquery expressions
  → Subquery detection pass (planner)
  → Rewrite IN/EXISTS as SemiJoin/AntiJoin nodes in LogicalPlan
  → Rewrite scalar subqueries as ScalarSubquery nodes
  → Physical planning maps to SemiJoinExec / AntiJoinExec / ScalarSubqueryExec
  → Execute
```
