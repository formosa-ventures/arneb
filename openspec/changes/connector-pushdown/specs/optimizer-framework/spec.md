## ADDED Requirements

### Requirement: PhysicalPlanOptimizer applies rules in sequence
The system SHALL provide a PhysicalPlanOptimizer that takes an ordered list of OptimizationRule implementations and applies them sequentially to a physical execution plan.

#### Scenario: Multiple rules applied in order
- **WHEN** the optimizer has rules [ProjectionPushdown, FilterPushdown]
- **THEN** it SHALL apply ProjectionPushdown first, then FilterPushdown to the resulting plan

#### Scenario: Rule returns unchanged plan
- **WHEN** an optimization rule finds no optimization opportunity
- **THEN** it SHALL return the plan unchanged and the optimizer SHALL proceed to the next rule

### Requirement: OptimizationRule trait
The system SHALL define an OptimizationRule trait with a method that takes an execution plan and returns an optimized execution plan.

#### Scenario: Custom rule implementation
- **WHEN** a new optimization rule is implemented
- **THEN** it SHALL implement the OptimizationRule trait and can be added to the optimizer's rule list
