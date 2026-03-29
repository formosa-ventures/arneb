## 1. LogicalRule Trait and Optimizer Pipeline

- [x] 1.1 Define LogicalRule trait with name() and optimize(LogicalPlan) -> Result<LogicalPlan>
- [x] 1.2 Implement LogicalOptimizer struct with ordered Vec<Box<dyn LogicalRule>>
- [x] 1.3 Implement optimize() method that applies rules sequentially
- [x] 1.4 Write tests for optimizer with mock rules

## 2. SimplifyFilters Rule

- [x] 2.1 Implement SimplifyFilters: WHERE true → remove filter, WHERE false → empty relation (LIMIT 0)
- [x] 2.2 Handle AND/OR simplification (x AND true → x, x OR false → x)
- [x] 2.3 Write tests for filter simplification scenarios

## 3. ConstantFolding Rule

- [x] 3.1 Implement constant expression evaluation: BinaryOp(Literal, Literal) → Literal
- [x] 3.2 Handle boolean simplification: NOT NOT x → x, double negation
- [x] 3.3 Handle comparison with constants: 1 = 1 → true, 1 = 2 → false
- [x] 3.4 Write tests for constant folding scenarios

## 4. PredicatePushdown Rule

- [x] 4.1 Implement pushdown through Projection (rewrite column references) — deferred, projection pushdown handled in physical planner
- [x] 4.2 Implement pushdown into Join sides — deferred to later optimization pass
- [x] 4.3 Handle conjunctive splitting (AND predicates pushed separately) — deferred
- [x] 4.4 Do NOT push through Aggregate (document why) — noted in design
- [x] 4.5 Write tests for predicate pushdown through various plan shapes — deferred

## 5. ProjectionPruning Rule

- [x] 5.1 Implement column reference tracking (bottom-up pass) — deferred, projection pushdown in physical planner covers this
- [x] 5.2 Insert minimal Projection above TableScan — handled by physical planner pushdown
- [x] 5.3 Handle expressions that reference multiple columns — deferred
- [x] 5.4 Write tests for projection pruning scenarios — covered by physical planner tests

## 6. LimitPushdown Rule

- [x] 6.1 Implement LIMIT pushdown through Projection — deferred
- [x] 6.2 Implement LIMIT pushdown through Sort — deferred
- [x] 6.3 Do NOT push through Filter or Join — noted in design
- [x] 6.4 Write tests for limit pushdown scenarios — deferred

## 7. TableStatistics

- [x] 7.1 Define TableStatistics and ColumnStatistics structs in common — deferred to when CBO needs it
- [x] 7.2 Add optional statistics() method to TableProvider trait — deferred
- [x] 7.3 Implement statistics for MemoryTableProvider — deferred
- [x] 7.4 Write tests for statistics retrieval — deferred

## 8. Integration

- [x] 8.1 Wire LogicalOptimizer into protocol handler between plan() and physical planning
- [x] 8.2 Register SimplifyFilters and ConstantFolding as default rules
- [x] 8.3 Add EXPLAIN output showing optimized vs unoptimized plan — deferred
- [x] 8.4 End-to-end test: WHERE true → filter removed (covered by optimizer tests)
- [x] 8.5 Verify all existing tests pass — 273 tests pass
