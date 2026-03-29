# predicate-pushdown-rule

**Status**: ADDED
**Crate**: planner

## Overview

Push Filter nodes closer to data sources by moving them through Projection nodes and into the correct side of Join nodes.

## ADDED Requirements

### Requirement: Push filter through projection

#### Scenario: Filter above a Projection that does not change column names

- WHEN a Filter(predicate) sits above a Projection
- AND the predicate references columns that exist in the Projection's input
- THEN the Filter is moved below the Projection
- AND column references in the predicate are rewritten to match the Projection's input schema

#### Scenario: Filter references a computed expression from the Projection

- WHEN a Filter references a column that is a computed alias in the Projection
- THEN that part of the predicate is NOT pushed below the Projection
- AND any other conjuncts that can be pushed are still pushed

### Requirement: Push filter into join sides

#### Scenario: Filter predicate references only left-side columns

- WHEN a Filter sits above a Join
- AND the predicate references only columns from the left input
- THEN the predicate is pushed into the left side of the Join as a new Filter

#### Scenario: Filter predicate references only right-side columns

- WHEN a Filter sits above a Join
- AND the predicate references only columns from the right input
- THEN the predicate is pushed into the right side of the Join as a new Filter

#### Scenario: Filter predicate references columns from both sides

- WHEN a Filter sits above a Join
- AND the predicate references columns from both the left and right inputs
- THEN the predicate is NOT pushed into either side
- AND the Filter remains above the Join

### Requirement: Conjunctive predicates are split and pushed independently

#### Scenario: AND predicate with pushable and non-pushable parts

- WHEN a Filter has predicate `a.x > 5 AND a.x = b.y`
- AND `a.x > 5` references only the left side of a Join
- THEN `a.x > 5` is pushed into the left side
- AND `a.x = b.y` remains as a Filter above the Join

### Requirement: Filter is NOT pushed through Aggregate

#### Scenario: Filter above an Aggregate

- WHEN a Filter sits above an Aggregate node
- THEN the Filter is not pushed below the Aggregate
- AND the plan is returned unchanged for that subtree
