//! Physical planner: converts [`LogicalPlan`] trees into executable
//! [`ExecutionPlan`] operator trees.

use std::collections::HashMap;
use std::sync::Arc;

use trino_common::error::ExecutionError;
use trino_planner::LogicalPlan;

use crate::datasource::DataSource;
use crate::hash_join::{extract_equi_join_keys, HashJoinExec};
use crate::operator::{
    ExecutionPlan, ExplainExec, FilterExec, HashAggregateExec, LimitExec, NestedLoopJoinExec,
    ProjectionExec, ScanExec, SortExec,
};
use crate::scan_context::ScanContext;

/// Execution context holding registered data sources.
///
/// Data sources are registered by a key that matches the table reference
/// used in the logical plan. The key is the table's fully-qualified name
/// (as produced by `TableReference::to_string()`), or just the table name
/// for simple references.
#[derive(Debug, Default)]
pub struct ExecutionContext {
    data_sources: HashMap<String, Arc<dyn DataSource>>,
}

impl ExecutionContext {
    /// Creates an empty execution context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a data source under the given key.
    pub fn register_data_source(&mut self, name: impl Into<String>, source: Arc<dyn DataSource>) {
        self.data_sources.insert(name.into(), source);
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
}
