# constant-folding-rule

**Status**: ADDED
**Crate**: planner

## Overview

Evaluate constant expressions at plan time and simplify boolean logic to reduce runtime computation.

## ADDED Requirements

### Requirement: Binary operations on literals are evaluated at plan time

#### Scenario: Arithmetic on integer literals

- WHEN an expression is `BinaryOp(Literal(3), Add, Literal(5))`
- THEN it is replaced with `Literal(8)`

#### Scenario: Comparison of literals

- WHEN an expression is `BinaryOp(Literal(1), Eq, Literal(1))`
- THEN it is replaced with `Literal(true)`

- WHEN an expression is `BinaryOp(Literal(1), Eq, Literal(2))`
- THEN it is replaced with `Literal(false)`

#### Scenario: String comparison

- WHEN an expression is `BinaryOp(Literal("abc"), Eq, Literal("abc"))`
- THEN it is replaced with `Literal(true)`

### Requirement: Boolean simplification

#### Scenario: AND with true

- WHEN an expression is `x AND true`
- THEN it is simplified to `x`

#### Scenario: AND with false

- WHEN an expression is `x AND false`
- THEN it is simplified to `Literal(false)`

#### Scenario: OR with false

- WHEN an expression is `x OR false`
- THEN it is simplified to `x`

#### Scenario: OR with true

- WHEN an expression is `x OR true`
- THEN it is simplified to `Literal(true)`

### Requirement: NOT simplification

#### Scenario: Double negation

- WHEN an expression is `NOT NOT x`
- THEN it is simplified to `x`

#### Scenario: NOT on a literal

- WHEN an expression is `NOT Literal(true)`
- THEN it is replaced with `Literal(false)`

### Requirement: Folding is applied recursively through expressions

#### Scenario: Nested constant expression

- WHEN an expression is `BinaryOp(BinaryOp(Literal(1), Add, Literal(2)), Eq, Literal(3))`
- THEN it is folded bottom-up to `Literal(true)`
