# logical-plan

**Status**: MODIFIED
**Crate**: planner

## Overview

Add plan rewriter and visitor patterns to LogicalPlan to support optimizer rule implementations that need to traverse and transform the plan tree.

## MODIFIED Requirements

### Requirement: LogicalPlan supports recursive transformation via rewriter

#### Scenario: Rewriting a specific node type

- WHEN a rewriter function is applied to a LogicalPlan tree
- AND the function transforms Filter nodes
- THEN all Filter nodes in the tree are visited and potentially transformed
- AND non-Filter nodes are left unchanged
- AND child nodes are recursively visited

#### Scenario: Rewriter replaces a subtree

- WHEN a rewriter replaces a node with a different LogicalPlan variant
- THEN the replacement is incorporated into the parent plan
- AND the rewriter continues to visit sibling and parent nodes

### Requirement: LogicalPlan supports visitor pattern for read-only traversal

#### Scenario: Collecting column references from the plan

- WHEN a visitor traverses the plan tree
- THEN it visits each node exactly once
- AND it can accumulate state (e.g., referenced columns) across all visited nodes

#### Scenario: Visitor traversal order

- WHEN a visitor is applied to a plan tree
- THEN nodes are visited top-down (parent before children)
- AND all children of a node are visited before moving to siblings

### Requirement: LogicalPlan provides access to child plans

#### Scenario: Enumerating children of a Join node

- WHEN `children()` is called on a Join LogicalPlan
- THEN it returns references to both the left and right child plans

#### Scenario: Replacing children of a node

- WHEN `with_new_children(new_children)` is called on a plan node
- THEN a new plan node of the same type is created with the new children
- AND the original node's non-child fields (predicates, columns, etc.) are preserved
