# logical-optimizer-framework

**Status**: ADDED
**Crate**: planner

## Overview

LogicalRule trait and LogicalOptimizer pipeline that applies optimization rules in sequence to a LogicalPlan tree.

## ADDED Requirements

### Requirement: LogicalRule trait defines the optimization interface

#### Scenario: A rule is applied to a logical plan

- WHEN a struct implements `LogicalRule`
- THEN it must provide `fn name(&self) -> &str` returning a human-readable rule name
- AND it must provide `fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>` that returns a transformed plan

#### Scenario: A rule that makes no changes returns the plan unchanged

- WHEN a LogicalRule's optimize method finds nothing to optimize
- THEN it returns the original LogicalPlan unmodified
- AND no error is raised

### Requirement: LogicalOptimizer applies rules in sequence

#### Scenario: Optimizer with multiple rules

- WHEN a LogicalOptimizer is constructed with rules [A, B, C]
- AND `optimize(plan)` is called
- THEN rule A is applied first to produce plan_a
- AND rule B is applied to plan_a to produce plan_b
- AND rule C is applied to plan_b to produce the final plan

#### Scenario: Optimizer with no rules

- WHEN a LogicalOptimizer is constructed with an empty rule list
- AND `optimize(plan)` is called
- THEN the original plan is returned unchanged

#### Scenario: A rule returns an error

- WHEN rule B in the sequence returns an error
- THEN the optimizer stops and propagates that error
- AND subsequent rules are not applied

### Requirement: Default optimizer includes all standard rules

#### Scenario: Creating optimizer with default rules

- WHEN `LogicalOptimizer::default()` or `LogicalOptimizer::new_default()` is called
- THEN the optimizer contains rules in order: SimplifyFilters, ConstantFolding, PredicatePushdown, ProjectionPruning, LimitPushdown
