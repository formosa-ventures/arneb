//! Physical planner: converts [`LogicalPlan`] trees into executable
//! [`ExecutionPlan`] operator trees.

use std::collections::HashMap;
use std::sync::Arc;

use trino_common::error::ExecutionError;
use trino_planner::{LogicalPlan, PlanExpr};

use crate::datasource::DataSource;
use crate::functions::{default_registry, FunctionRegistry};
use crate::hash_join::{extract_equi_join_keys, HashJoinExec};
use crate::operator::{
    ExecutionPlan, ExplainExec, FilterExec, HashAggregateExec, LimitExec, NestedLoopJoinExec,
    ProjectionExec, ScanExec, SortExec,
};
use crate::scan_context::ScanContext;

/// Execution context holding registered data sources and function registry.
///
/// Data sources are registered by a key that matches the table reference
/// used in the logical plan. The key is the table's fully-qualified name
/// (as produced by `TableReference::to_string()`), or just the table name
/// for simple references.
#[derive(Debug)]
pub struct ExecutionContext {
    data_sources: HashMap<String, Arc<dyn DataSource>>,
    function_registry: Arc<FunctionRegistry>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            data_sources: HashMap::new(),
            function_registry: Arc::new(default_registry()),
        }
    }
}

impl ExecutionContext {
    /// Creates an execution context with the default function registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a reference to the function registry.
    pub fn function_registry(&self) -> &Arc<FunctionRegistry> {
        &self.function_registry
    }

    /// Registers a data source under the given key.
    pub fn register_data_source(&mut self, name: impl Into<String>, source: Arc<dyn DataSource>) {
        self.data_sources.insert(name.into(), source);
    }

    /// Pre-evaluate scalar subqueries in a PlanExpr, replacing them with Literal values.
    pub async fn resolve_scalar_subqueries(
        &self,
        expr: &PlanExpr,
    ) -> Result<PlanExpr, ExecutionError> {
        match expr {
            PlanExpr::ScalarSubquery { subplan } => {
                let exec = self.create_physical_plan(subplan)?;
                let stream = exec.execute().await?;
                let batches = trino_common::stream::collect_stream(stream)
                    .await
                    .map_err(|e| {
                        ExecutionError::InvalidOperation(format!("scalar subquery failed: {e}"))
                    })?;
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if total_rows > 1 {
                    return Err(ExecutionError::InvalidOperation(
                        "scalar subquery must return at most one row".to_string(),
                    ));
                }
                if total_rows == 0 || batches.is_empty() {
                    return Ok(PlanExpr::Literal(trino_common::types::ScalarValue::Null));
                }
                let col = batches[0].column(0);
                if col.is_null(0) {
                    return Ok(PlanExpr::Literal(trino_common::types::ScalarValue::Null));
                }
                let val = arrow_to_scalar(col, 0);
                Ok(PlanExpr::Literal(val))
            }
            PlanExpr::BinaryOp { left, op, right } => {
                let l = Box::pin(self.resolve_scalar_subqueries(left)).await?;
                let r = Box::pin(self.resolve_scalar_subqueries(right)).await?;
                Ok(PlanExpr::BinaryOp {
                    left: Box::new(l),
                    op: *op,
                    right: Box::new(r),
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    /// Creates a physical execution plan from a logical plan.
    pub fn create_physical_plan(
        &self,
        logical: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        self.convert(logical)
    }

    fn convert(&self, logical: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        match logical {
            LogicalPlan::TableScan { table, .. } => {
                let key = table.to_string();
                let source = self
                    .data_sources
                    .get(&key)
                    .or_else(|| self.data_sources.get(&table.table))
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(format!(
                            "data source not found for table '{key}'"
                        ))
                    })?;
                Ok(Arc::new(ScanExec {
                    source: source.clone(),
                    _table_name: key,
                    scan_context: ScanContext::default(),
                }))
            }

            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => {
                // Attempt projection pushdown: if input is a TableScan and all
                // exprs are simple column references, push projection into ScanContext.
                if let LogicalPlan::TableScan { table, .. } = input.as_ref() {
                    let column_indices: Option<Vec<usize>> = exprs
                        .iter()
                        .map(|e| match e {
                            trino_planner::PlanExpr::Column { index, .. } => Some(*index),
                            _ => None,
                        })
                        .collect();

                    if let Some(indices) = column_indices {
                        let key = table.to_string();
                        let source = self
                            .data_sources
                            .get(&key)
                            .or_else(|| self.data_sources.get(&table.table))
                            .ok_or_else(|| {
                                ExecutionError::InvalidOperation(format!(
                                    "data source not found for table '{key}'"
                                ))
                            })?;
                        let scan_ctx = ScanContext::default().with_projection(indices.clone());
                        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
                            source: source.clone(),
                            _table_name: key,
                            scan_context: scan_ctx,
                        });
                        // Rewrite exprs to use sequential indices since the scan
                        // output now contains only the projected columns in order.
                        let rewritten_exprs: Vec<_> = indices
                            .iter()
                            .enumerate()
                            .map(|(new_idx, _)| {
                                let orig = &exprs[new_idx];
                                match orig {
                                    trino_planner::PlanExpr::Column { name, .. } => {
                                        trino_planner::PlanExpr::Column {
                                            index: new_idx,
                                            name: name.clone(),
                                        }
                                    }
                                    other => other.clone(),
                                }
                            })
                            .collect();
                        return Ok(Arc::new(ProjectionExec {
                            input: scan,
                            exprs: rewritten_exprs,
                            output_schema: schema.clone(),
                        }));
                    }
                }

                let input_plan = self.convert(input)?;
                Ok(Arc::new(ProjectionExec {
                    input: input_plan,
                    exprs: exprs.clone(),
                    output_schema: schema.clone(),
                }))
            }

            LogicalPlan::Filter { input, predicate } => {
                let input_plan = self.convert(input)?;
                Ok(Arc::new(FilterExec {
                    input: input_plan,
                    predicate: predicate.clone(),
                }))
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                let left_plan = self.convert(left)?;
                let right_plan = self.convert(right)?;
                let left_col_count = left_plan.schema().len();

                // Try to use hash join for equi-join conditions.
                if let Some(key_pairs) = extract_equi_join_keys(condition, left_col_count) {
                    let (left_keys, right_keys): (Vec<usize>, Vec<usize>) =
                        key_pairs.into_iter().unzip();
                    return Ok(Arc::new(HashJoinExec {
                        left: left_plan,
                        right: right_plan,
                        join_type: *join_type,
                        left_keys,
                        right_keys,
                    }));
                }

                // Fall back to nested loop for non-equi joins.
                Ok(Arc::new(NestedLoopJoinExec {
                    left: left_plan,
                    right: right_plan,
                    join_type: *join_type,
                    condition: condition.clone(),
                }))
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                let input_plan = self.convert(input)?;
                Ok(Arc::new(HashAggregateExec {
                    input: input_plan,
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    output_schema: schema.clone(),
                }))
            }

            LogicalPlan::Sort { input, order_by } => {
                let input_plan = self.convert(input)?;
                Ok(Arc::new(SortExec {
                    input: input_plan,
                    order_by: order_by.clone(),
                }))
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_plan = self.convert(input)?;
                Ok(Arc::new(LimitExec {
                    input: input_plan,
                    limit: *limit,
                    offset: *offset,
                }))
            }

            LogicalPlan::Explain { input } => Ok(Arc::new(ExplainExec {
                plan: *input.clone(),
            })),

            // PartialAggregate and FinalAggregate are treated as regular Aggregate
            // in single-node mode (no distribution).
            LogicalPlan::PartialAggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            }
            | LogicalPlan::FinalAggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                let input_plan = self.convert(input)?;
                Ok(Arc::new(HashAggregateExec {
                    input: input_plan,
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    output_schema: schema.clone(),
                }))
            }

            LogicalPlan::ExchangeNode { .. } => Err(ExecutionError::InvalidOperation(
                "ExchangeNode cannot be executed in single-node mode".to_string(),
            )),

            LogicalPlan::SemiJoin {
                left,
                right,
                left_key,
                right_key,
            } => {
                let left_exec = self.create_physical_plan(left)?;
                let right_exec = self.create_physical_plan(right)?;
                Ok(Arc::new(crate::semi_join::SemiJoinExec::new(
                    left_exec,
                    right_exec,
                    left_key.clone(),
                    right_key.clone(),
                    false,
                )))
            }

            LogicalPlan::AntiJoin {
                left,
                right,
                left_key,
                right_key,
            } => {
                let left_exec = self.create_physical_plan(left)?;
                let right_exec = self.create_physical_plan(right)?;
                Ok(Arc::new(crate::semi_join::SemiJoinExec::new(
                    left_exec,
                    right_exec,
                    left_key.clone(),
                    right_key.clone(),
                    true,
                )))
            }

            LogicalPlan::ScalarSubquery { subplan } => {
                let sub_exec = self.create_physical_plan(subplan)?;
                Ok(Arc::new(crate::scalar_subquery::ScalarSubqueryExec::new(
                    sub_exec,
                )))
            }

            LogicalPlan::UnionAll { inputs } => {
                let children: Vec<Arc<dyn ExecutionPlan>> = inputs
                    .iter()
                    .map(|p| self.create_physical_plan(p))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(crate::set_ops::UnionAllExec::new(children)))
            }

            LogicalPlan::Distinct { input } => {
                let child = self.create_physical_plan(input)?;
                Ok(Arc::new(crate::set_ops::DistinctExec::new(child)))
            }

            LogicalPlan::Intersect { left, right } => {
                let l = self.create_physical_plan(left)?;
                let r = self.create_physical_plan(right)?;
                Ok(Arc::new(crate::set_ops::IntersectExec::new(l, r)))
            }

            LogicalPlan::Except { left, right } => {
                let l = self.create_physical_plan(left)?;
                let r = self.create_physical_plan(right)?;
                Ok(Arc::new(crate::set_ops::ExceptExec::new(l, r)))
            }

            // DDL/DML plans are handled at the protocol/server level, not here
            LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateTableAsSelect { .. }
            | LogicalPlan::InsertInto { .. }
            | LogicalPlan::DeleteFrom { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. } => Err(ExecutionError::InvalidOperation(
                "DDL/DML plans are handled at the protocol level, not the execution engine"
                    .to_string(),
            )),

            LogicalPlan::Window { input, functions } => {
                let child = self.create_physical_plan(input)?;
                Ok(Arc::new(crate::window::WindowExec::new(
                    child,
                    functions.clone(),
                )))
            }
        }
    }
}

/// Extract a scalar value from an Arrow array at a given row.
fn arrow_to_scalar(array: &arrow::array::ArrayRef, row: usize) -> trino_common::types::ScalarValue {
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::DataType as ArrowDT;
    use trino_common::types::ScalarValue;

    if array.is_null(row) {
        return ScalarValue::Null;
    }
    match array.data_type() {
        ArrowDT::Int64 => ScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        ArrowDT::Float64 => ScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        ArrowDT::Utf8 => ScalarValue::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        _ => {
            // Fallback: convert to string
            let s = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            ScalarValue::Utf8(s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use trino_common::stream::collect_stream;
    use trino_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
    use trino_planner::PlanExpr;
    use trino_sql_parser::ast;

    fn test_context() -> (ExecutionContext, Vec<ColumnInfo>) {
        let schema = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
            ColumnInfo {
                name: "value".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "alice", "bob", "carol", "dave", "eve",
                ])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .unwrap();

        let source = Arc::new(InMemoryDataSource::new(schema.clone(), vec![batch]));
        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("users", source);
        (ctx, schema)
    }

    #[tokio::test]
    async fn plan_table_scan() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::TableScan {
            table: TableReference::table("users"),
            schema,
            alias: None,
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn plan_filter() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
            }),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                }),
                op: ast::BinaryOp::LtEq,
                right: Box::new(PlanExpr::Literal(ScalarValue::Int32(3))),
            },
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn plan_projection() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
            }),
            exprs: vec![PlanExpr::Column {
                index: 1,
                name: "name".to_string(),
            }],
            schema: vec![ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            }],
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn plan_limit_offset() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
            }),
            limit: Some(2),
            offset: Some(1),
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 3);
    }

    #[tokio::test]
    async fn plan_sort() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
            }),
            order_by: vec![trino_planner::SortExpr {
                expr: PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                },
                asc: false,
                nulls_first: false,
            }],
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 5);
        assert_eq!(ids.value(4), 1);
    }

    #[tokio::test]
    async fn plan_aggregate_count_sum() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
            }),
            group_by: vec![],
            aggr_exprs: vec![
                PlanExpr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                    distinct: false,
                },
                PlanExpr::Function {
                    name: "SUM".to_string(),
                    args: vec![PlanExpr::Column {
                        index: 2,
                        name: "value".to_string(),
                    }],
                    distinct: false,
                },
            ],
            schema: vec![
                ColumnInfo {
                    name: "count".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "sum_value".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 5);
        let sum = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum.value(0), 1500);
    }

    #[tokio::test]
    async fn plan_explain() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Explain {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("users"),
                schema,
                alias: None,
            }),
        };
        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let text = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(text.value(0).contains("TableScan"));
    }

    #[test]
    fn plan_table_not_found() {
        let ctx = ExecutionContext::new();
        let plan = LogicalPlan::TableScan {
            table: TableReference::table("nonexistent"),
            schema: vec![],
            alias: None,
        };
        let result = ctx.create_physical_plan(&plan);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn end_to_end_filter_project_limit() {
        let (ctx, schema) = test_context();
        let plan = LogicalPlan::Limit {
            limit: Some(2),
            offset: None,
            input: Box::new(LogicalPlan::Projection {
                exprs: vec![PlanExpr::Column {
                    index: 1,
                    name: "name".to_string(),
                }],
                schema: vec![ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                }],
                input: Box::new(LogicalPlan::Filter {
                    predicate: PlanExpr::BinaryOp {
                        left: Box::new(PlanExpr::Column {
                            index: 0,
                            name: "id".to_string(),
                        }),
                        op: ast::BinaryOp::Gt,
                        right: Box::new(PlanExpr::Literal(ScalarValue::Int32(2))),
                    },
                    input: Box::new(LogicalPlan::TableScan {
                        table: TableReference::table("users"),
                        schema,
                        alias: None,
                    }),
                }),
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "carol");
        assert_eq!(names.value(1), "dave");
    }

    #[tokio::test]
    async fn end_to_end_having_filter_after_aggregate() {
        // Build data with duplicate names to test GROUP BY + HAVING
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "alice", "alice", "bob", "carol", "carol", "carol",
                ])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
            ],
        )
        .unwrap();
        let schema = vec![
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
            ColumnInfo {
                name: "value".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let source = Arc::new(InMemoryDataSource::new(schema.clone(), vec![batch]));
        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("t", source);

        // GROUP BY name, COUNT(*) → HAVING COUNT(*) > 1
        let agg_schema = vec![
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
            ColumnInfo {
                name: "cnt".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "cnt".to_string(),
                }),
                op: ast::BinaryOp::Gt,
                right: Box::new(PlanExpr::Literal(ScalarValue::Int64(1))),
            },
            input: Box::new(LogicalPlan::Aggregate {
                input: Box::new(LogicalPlan::TableScan {
                    table: TableReference::table("t"),
                    schema,
                    alias: None,
                }),
                group_by: vec![PlanExpr::Column {
                    index: 0,
                    name: "name".to_string(),
                }],
                aggr_exprs: vec![PlanExpr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                    distinct: false,
                }],
                schema: agg_schema,
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // alice (2) and carol (3) have count > 1; bob (1) filtered out
        assert_eq!(total_rows, 2);
    }

    // ---------------------------------------------------------------
    // Semi-join / Anti-join / Scalar subquery tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn end_to_end_semi_join() {
        // Left: orders with customer_ids [1, 2, 3, 4]
        // Right: customers with ids [2, 4]
        // SemiJoin should return orders for customers 2 and 4
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", ArrowDataType::Int64, false),
            Field::new("customer_id", ArrowDataType::Int64, false),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();
        let left_info = vec![
            ColumnInfo {
                name: "order_id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "customer_id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];

        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let right_batch =
            RecordBatch::try_new(right_schema, vec![Arc::new(Int64Array::from(vec![2, 4]))])
                .unwrap();
        let right_info = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];

        let left_src = Arc::new(InMemoryDataSource::new(left_info.clone(), vec![left_batch]));
        let right_src = Arc::new(InMemoryDataSource::new(
            right_info.clone(),
            vec![right_batch],
        ));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("orders", left_src);
        ctx.register_data_source("customers", right_src);

        let plan = LogicalPlan::SemiJoin {
            left: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("orders"),
                schema: left_info,
                alias: None,
            }),
            right: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("customers"),
                schema: right_info,
                alias: None,
            }),
            left_key: PlanExpr::Column {
                index: 1,
                name: "customer_id".into(),
            },
            right_key: PlanExpr::Column {
                index: 0,
                name: "id".into(),
            },
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn end_to_end_anti_join() {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let left_batch = RecordBatch::try_new(
            left_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        let left_info = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];

        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let right_batch =
            RecordBatch::try_new(right_schema, vec![Arc::new(Int64Array::from(vec![2, 4]))])
                .unwrap();
        let right_info = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];

        let left_src = Arc::new(InMemoryDataSource::new(left_info.clone(), vec![left_batch]));
        let right_src = Arc::new(InMemoryDataSource::new(
            right_info.clone(),
            vec![right_batch],
        ));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("left_t", left_src);
        ctx.register_data_source("right_t", right_src);

        // AntiJoin: returns rows from left NOT in right
        let plan = LogicalPlan::AntiJoin {
            left: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("left_t"),
                schema: left_info,
                alias: None,
            }),
            right: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("right_t"),
                schema: right_info,
                alias: None,
            }),
            left_key: PlanExpr::Column {
                index: 0,
                name: "id".into(),
            },
            right_key: PlanExpr::Column {
                index: 0,
                name: "id".into(),
            },
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 1, 3, 5 NOT IN [2, 4]
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn end_to_end_scalar_subquery() {
        let schema_info = vec![ColumnInfo {
            name: "val".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowDataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();
        let src = Arc::new(InMemoryDataSource::new(schema_info.clone(), vec![batch]));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("t", src);

        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("t"),
                schema: schema_info,
                alias: None,
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 42);
    }

    #[tokio::test]
    async fn scalar_subquery_zero_rows_returns_null() {
        let schema_info = vec![ColumnInfo {
            name: "val".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let src = Arc::new(InMemoryDataSource::new(schema_info.clone(), vec![]));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("empty", src);

        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("empty"),
                schema: schema_info,
                alias: None,
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let stream = exec.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert!(batches[0].column(0).is_null(0));
    }

    #[tokio::test]
    async fn scalar_subquery_multi_row_errors() {
        let schema_info = vec![ColumnInfo {
            name: "val".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowDataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let src = Arc::new(InMemoryDataSource::new(schema_info.clone(), vec![batch]));

        let mut ctx = ExecutionContext::new();
        ctx.register_data_source("multi", src);

        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("multi"),
                schema: schema_info,
                alias: None,
            }),
        };

        let exec = ctx.create_physical_plan(&plan).unwrap();
        let result = exec.execute().await;
        assert!(result.is_err());
    }

    #[test]
    fn end_to_end_scalar_function_via_evaluate() {
        use crate::expression;
        use crate::functions::default_registry;
        use arrow::array::StringArray;

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                Arc::new(Int64Array::from(vec![-10, 20, -30])),
            ],
        )
        .unwrap();

        let reg = default_registry();

        // Test UPPER(name)
        let upper_expr = PlanExpr::Function {
            name: "UPPER".to_string(),
            args: vec![PlanExpr::Column {
                index: 0,
                name: "name".to_string(),
            }],
            distinct: false,
        };
        let result = expression::evaluate(&upper_expr, &batch, Some(&reg)).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "ALICE");
        assert_eq!(arr.value(1), "BOB");
        assert_eq!(arr.value(2), "CAROL");

        // Test ABS(value)
        let abs_expr = PlanExpr::Function {
            name: "ABS".to_string(),
            args: vec![PlanExpr::Column {
                index: 1,
                name: "value".to_string(),
            }],
            distinct: false,
        };
        let result = expression::evaluate(&abs_expr, &batch, Some(&reg)).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 20);
        assert_eq!(arr.value(2), 30);
    }
}
