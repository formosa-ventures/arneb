//! Physical execution operators.
//!
//! Each operator implements [`ExecutionPlan`] and produces a
//! [`SendableRecordBatchStream`] from its children. Operators are assembled
//! into a tree by the physical planner in [`super::planner`].

use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arneb_common::error::ExecutionError;
use arneb_common::stream::{
    collect_stream, stream_from_batches, RecordBatchStream, SendableRecordBatchStream,
};
use arneb_common::types::{ColumnInfo, ScalarValue};
use arneb_planner::{LogicalPlan, PlanExpr, SortExpr};
use arneb_sql_parser::ast;
use arrow::array::{
    self, Array, ArrayRef, AsArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::compute;
use arrow::datatypes::{self, DataType as ArrowDataType, Field, Schema};
use async_trait::async_trait;
use futures::stream::Stream;

use crate::aggregate::{self, Accumulator};
use crate::datasource::DataSource;
use crate::expression;
use crate::scan_context::ScanContext;

/// A physical execution operator that produces a stream of record batches.
#[async_trait]
pub trait ExecutionPlan: Send + Sync + Debug {
    /// The output schema of this operator.
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Executes the operator and returns a stream of result batches.
    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError>;

    /// A short display name for EXPLAIN output.
    fn display_name(&self) -> &str;
}

// ===========================================================================
// ScanExec
// ===========================================================================

/// Reads all data from a [`DataSource`].
#[derive(Debug)]
pub(crate) struct ScanExec {
    pub(crate) source: Arc<dyn DataSource>,
    pub(crate) _table_name: String,
    pub(crate) scan_context: ScanContext,
}

#[async_trait]
impl ExecutionPlan for ScanExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.source.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        self.source.scan(&self.scan_context).await
    }

    fn display_name(&self) -> &str {
        "ScanExec"
    }
}

// ===========================================================================
// ProjectionExec
// ===========================================================================

/// Evaluates expressions to produce new columns.
#[derive(Debug)]
pub(crate) struct ProjectionExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) exprs: Vec<PlanExpr>,
    pub(crate) output_schema: Vec<ColumnInfo>,
}

#[async_trait]
impl ExecutionPlan for ProjectionExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let input_stream = self.input.execute().await?;
        let exprs = self.exprs.clone();
        let output_schema = self.output_schema.clone();
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&output_schema);

        Ok(Box::pin(MapStream::new(
            input_stream,
            arrow_schema.clone(),
            move |batch| {
                let columns: Vec<ArrayRef> = exprs
                    .iter()
                    .map(|e| expression::evaluate(e, &batch, None))
                    .collect::<Result<_, _>>()?;

                let columns = columns
                    .into_iter()
                    .zip(arrow_schema.fields())
                    .map(|(col, field)| {
                        if col.data_type() != field.data_type() {
                            compute::cast(&col, field.data_type()).map_err(ExecutionError::from)
                        } else {
                            Ok(col)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(RecordBatch::try_new(arrow_schema.clone(), columns)?)
            },
        )))
    }

    fn display_name(&self) -> &str {
        "ProjectionExec"
    }
}

// ===========================================================================
// FilterExec
// ===========================================================================

/// Filters rows by a boolean predicate.
#[derive(Debug)]
pub(crate) struct FilterExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) predicate: PlanExpr,
}

#[async_trait]
impl ExecutionPlan for FilterExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let input_stream = self.input.execute().await?;
        let predicate = self.predicate.clone();
        let schema = input_stream.schema();

        Ok(Box::pin(FilterMapStream::new(
            input_stream,
            schema,
            move |batch| {
                let mask_arr = expression::evaluate(&predicate, &batch, None)?;
                let mask = mask_arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(
                            "filter predicate must produce a boolean array".to_string(),
                        )
                    })?;

                let filtered = compute::filter_record_batch(&batch, mask)?;
                if filtered.num_rows() > 0 {
                    Ok(Some(filtered))
                } else {
                    Ok(None)
                }
            },
        )))
    }

    fn display_name(&self) -> &str {
        "FilterExec"
    }
}

// ===========================================================================
// NestedLoopJoinExec
// ===========================================================================

/// Nested-loop join for all join types.
#[derive(Debug)]
pub(crate) struct NestedLoopJoinExec {
    pub(crate) left: Arc<dyn ExecutionPlan>,
    pub(crate) right: Arc<dyn ExecutionPlan>,
    pub(crate) join_type: ast::JoinType,
    pub(crate) condition: arneb_planner::JoinCondition,
}

#[async_trait]
impl ExecutionPlan for NestedLoopJoinExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let left_batches = self.collect_input(&self.left).await?;
        let right_batches = self.collect_input(&self.right).await?;

        let left_batch = match left_batches {
            Some(b) => b,
            None => {
                let schema = crate::datasource::column_info_to_arrow_schema(&self.schema());
                return Ok(stream_from_batches(schema, vec![]));
            }
        };
        let right_batch = match right_batches {
            Some(b) => b,
            None => {
                return match self.join_type {
                    ast::JoinType::Left | ast::JoinType::Full => {
                        let result =
                            self.left_unmatched_output(&left_batch, self.right.schema().len())?;
                        let schema = result.first().map(|b| b.schema()).unwrap_or_else(|| {
                            crate::datasource::column_info_to_arrow_schema(&self.schema())
                        });
                        Ok(stream_from_batches(schema, result))
                    }
                    _ => {
                        let schema = crate::datasource::column_info_to_arrow_schema(&self.schema());
                        Ok(stream_from_batches(schema, vec![]))
                    }
                };
            }
        };

        let result = self.execute_join(&left_batch, &right_batch)?;
        let schema = result
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| crate::datasource::column_info_to_arrow_schema(&self.schema()));
        Ok(stream_from_batches(schema, result))
    }

    fn display_name(&self) -> &str {
        "NestedLoopJoinExec"
    }
}

impl NestedLoopJoinExec {
    /// Collect all input batches into a single concatenated batch.
    async fn collect_input(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<RecordBatch>, ExecutionError> {
        let stream = plan.execute().await?;
        let batches = collect_stream(stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect input: {e}"))
        })?;
        if batches.is_empty() {
            return Ok(None);
        }
        let batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&batches[0].schema(), batches.iter())?
        };
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    fn execute_join(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let left_rows = left_batch.num_rows();
        let right_rows = right_batch.num_rows();
        let output_schema = self.build_output_schema(left_batch, right_batch);

        let mut left_matched = vec![false; left_rows];
        let mut right_matched = vec![false; right_rows];
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        for (l, l_matched) in left_matched.iter_mut().enumerate() {
            for (r, r_matched) in right_matched.iter_mut().enumerate() {
                let pass = self.eval_join_condition(l, r, left_batch, right_batch)?;
                if pass {
                    left_indices.push(l as u32);
                    right_indices.push(r as u32);
                    *l_matched = true;
                    *r_matched = true;
                }
            }
        }

        let mut result_columns = Vec::new();
        let left_idx_arr = UInt32Array::from(left_indices.clone());
        let right_idx_arr = UInt32Array::from(right_indices.clone());

        for col_i in 0..left_batch.num_columns() {
            let col = compute::take(left_batch.column(col_i), &left_idx_arr, None)?;
            result_columns.push(col);
        }
        for col_i in 0..right_batch.num_columns() {
            let col = compute::take(right_batch.column(col_i), &right_idx_arr, None)?;
            result_columns.push(col);
        }

        let mut all_batches = Vec::new();

        if !result_columns.is_empty() && !left_indices.is_empty() {
            all_batches.push(RecordBatch::try_new(output_schema.clone(), result_columns)?);
        }

        // Handle unmatched rows for outer joins.
        match self.join_type {
            ast::JoinType::Left | ast::JoinType::Full => {
                let unmatched: Vec<u32> = left_matched
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| !**m)
                    .map(|(i, _)| i as u32)
                    .collect();
                if !unmatched.is_empty() {
                    let idx = UInt32Array::from(unmatched);
                    let mut cols: Vec<ArrayRef> = Vec::new();
                    for col_i in 0..left_batch.num_columns() {
                        cols.push(compute::take(left_batch.column(col_i), &idx, None)?);
                    }
                    let null_len = idx.len();
                    for col_i in 0..right_batch.num_columns() {
                        cols.push(arrow::array::new_null_array(
                            right_batch.column(col_i).data_type(),
                            null_len,
                        ));
                    }
                    all_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
                }
            }
            _ => {}
        }

        match self.join_type {
            ast::JoinType::Right | ast::JoinType::Full => {
                let unmatched: Vec<u32> = right_matched
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| !**m)
                    .map(|(i, _)| i as u32)
                    .collect();
                if !unmatched.is_empty() {
                    let idx = UInt32Array::from(unmatched);
                    let null_len = idx.len();
                    let mut cols: Vec<ArrayRef> = Vec::new();
                    for col_i in 0..left_batch.num_columns() {
                        cols.push(arrow::array::new_null_array(
                            left_batch.column(col_i).data_type(),
                            null_len,
                        ));
                    }
                    for col_i in 0..right_batch.num_columns() {
                        cols.push(compute::take(right_batch.column(col_i), &idx, None)?);
                    }
                    all_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
                }
            }
            _ => {}
        }

        Ok(all_batches)
    }

    fn build_output_schema(&self, left: &RecordBatch, right: &RecordBatch) -> Arc<Schema> {
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| {
                if matches!(self.join_type, ast::JoinType::Right | ast::JoinType::Full) {
                    Field::new(f.name(), f.data_type().clone(), true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        fields.extend(right.schema().fields().iter().map(|f| {
            if matches!(self.join_type, ast::JoinType::Left | ast::JoinType::Full) {
                Field::new(f.name(), f.data_type().clone(), true)
            } else {
                f.as_ref().clone()
            }
        }));
        Arc::new(Schema::new(fields))
    }

    fn eval_join_condition(
        &self,
        left_row: usize,
        right_row: usize,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
    ) -> Result<bool, ExecutionError> {
        match &self.condition {
            arneb_planner::JoinCondition::None => Ok(true),
            arneb_planner::JoinCondition::On(expr) => {
                let combined =
                    self.build_combined_row(left_row, right_row, left_batch, right_batch)?;
                let result = expression::evaluate(expr, &combined, None)?;
                let bool_arr = result
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(
                            "join condition must produce boolean".to_string(),
                        )
                    })?;
                Ok(bool_arr.value(0))
            }
        }
    }

    fn build_combined_row(
        &self,
        left_row: usize,
        right_row: usize,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
    ) -> Result<RecordBatch, ExecutionError> {
        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for (i, field) in left_batch.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(left_batch.column(i).slice(left_row, 1));
        }
        for (i, field) in right_batch.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(right_batch.column(i).slice(right_row, 1));
        }

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn left_unmatched_output(
        &self,
        left_batch: &RecordBatch,
        right_cols: usize,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let schema = self.schema();
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&schema);
        let mut cols: Vec<ArrayRef> = Vec::new();
        for i in 0..left_batch.num_columns() {
            cols.push(left_batch.column(i).clone());
        }
        for i in 0..right_cols {
            let dt: ArrowDataType = schema[left_batch.num_columns() + i]
                .data_type
                .clone()
                .into();
            cols.push(arrow::array::new_null_array(&dt, left_batch.num_rows()));
        }
        Ok(vec![RecordBatch::try_new(arrow_schema, cols)?])
    }
}

// ===========================================================================
// HashAggregateExec
// ===========================================================================

/// Hash-based grouping and aggregation.
#[derive(Debug)]
pub(crate) struct HashAggregateExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) group_by: Vec<PlanExpr>,
    pub(crate) aggr_exprs: Vec<PlanExpr>,
    pub(crate) output_schema: Vec<ColumnInfo>,
}

#[async_trait]
impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.input.execute().await?;
        let batches = collect_stream(stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect input: {e}"))
        })?;

        let result = self.execute_sync(&batches)?;
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        Ok(stream_from_batches(arrow_schema, result))
    }

    fn display_name(&self) -> &str {
        "HashAggregateExec"
    }
}

#[derive(Debug)]
struct AggrInfo {
    name: String,
    args: Vec<PlanExpr>,
    is_count_star: bool,
}

type GroupState = (Vec<ScalarValue>, Vec<Box<dyn Accumulator>>);

impl HashAggregateExec {
    fn execute_sync(&self, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, ExecutionError> {
        let aggr_info: Vec<AggrInfo> = self
            .aggr_exprs
            .iter()
            .map(|e| match e {
                PlanExpr::Function { name, args, .. } => {
                    let is_count_star =
                        args.is_empty() || args.iter().any(|a| matches!(a, PlanExpr::Wildcard));
                    Ok(AggrInfo {
                        name: name.clone(),
                        args: args.clone(),
                        is_count_star,
                    })
                }
                other => Err(ExecutionError::InvalidOperation(format!(
                    "expected aggregate function, got {other:?}"
                ))),
            })
            .collect::<Result<_, _>>()?;

        if self.group_by.is_empty() {
            return self.execute_no_grouping(batches, &aggr_info);
        }

        let mut groups: HashMap<String, GroupState> = HashMap::new();

        for batch in batches {
            let group_cols: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|e| expression::evaluate(e, batch, None))
                .collect::<Result<_, _>>()?;

            let aggr_input_cols: Vec<ArrayRef> = aggr_info
                .iter()
                .map(|info| {
                    if info.is_count_star {
                        Ok(batch.column(0).clone())
                    } else {
                        expression::evaluate(&info.args[0], batch, None)
                    }
                })
                .collect::<Result<_, _>>()?;

            for row in 0..batch.num_rows() {
                let key = group_key(&group_cols, row)?;
                let group_values: Vec<ScalarValue> = group_cols
                    .iter()
                    .map(|col| extract_scalar(col, row))
                    .collect::<Result<_, _>>()?;

                let entry = groups.entry(key).or_insert_with(|| {
                    let accumulators: Vec<Box<dyn Accumulator>> = aggr_info
                        .iter()
                        .map(|info| {
                            aggregate::create_accumulator(&info.name, info.is_count_star).unwrap()
                        })
                        .collect();
                    (group_values, accumulators)
                });

                for (acc_i, acc) in entry.1.iter_mut().enumerate() {
                    let col = &aggr_input_cols[acc_i];
                    let slice = col.slice(row, 1);
                    acc.update_batch(&slice)?;
                }
            }
        }

        self.build_aggregate_output(groups)
    }

    fn execute_no_grouping(
        &self,
        batches: &[RecordBatch],
        aggr_info: &[AggrInfo],
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let mut accumulators: Vec<Box<dyn Accumulator>> = aggr_info
            .iter()
            .map(|info| aggregate::create_accumulator(&info.name, info.is_count_star))
            .collect::<Result<_, _>>()?;

        for batch in batches {
            for (i, info) in aggr_info.iter().enumerate() {
                let col = if info.is_count_star {
                    batch.column(0).clone()
                } else {
                    expression::evaluate(&info.args[0], batch, None)?
                };
                accumulators[i].update_batch(&col)?;
            }
        }

        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        let columns: Vec<ArrayRef> = accumulators
            .iter()
            .map(|acc| {
                let val = acc.evaluate()?;
                expression::scalar_to_array(&val, 1)
            })
            .collect::<Result<_, _>>()?;

        Ok(vec![RecordBatch::try_new(arrow_schema, columns)?])
    }

    fn build_aggregate_output(
        &self,
        groups: HashMap<String, GroupState>,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        if groups.is_empty() {
            return Ok(vec![]);
        }

        let num_groups = groups.len();
        let num_group_cols = self.group_by.len();
        let num_aggr_cols = self.aggr_exprs.len();

        let mut group_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_group_cols];
        let mut aggr_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_aggr_cols];

        for (_key, (gv, accumulators)) in groups {
            for (i, v) in gv.into_iter().enumerate() {
                group_values[i].push(v);
            }
            for (i, acc) in accumulators.iter().enumerate() {
                aggr_values[i].push(acc.evaluate()?);
            }
        }

        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_group_cols + num_aggr_cols);

        for col_vals in &group_values {
            columns.push(scalars_to_array(col_vals, num_groups)?);
        }
        for col_vals in &aggr_values {
            columns.push(scalars_to_array(col_vals, num_groups)?);
        }

        Ok(vec![RecordBatch::try_new(arrow_schema, columns)?])
    }
}

// ===========================================================================
// SortExec
// ===========================================================================

/// In-memory sort operator.
#[derive(Debug)]
pub(crate) struct SortExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) order_by: Vec<SortExpr>,
}

#[async_trait]
impl ExecutionPlan for SortExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.input.execute().await?;
        let batches = collect_stream(stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect input: {e}"))
        })?;

        if batches.is_empty() {
            let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.input.schema());
            return Ok(stream_from_batches(arrow_schema, vec![]));
        }

        let schema = batches[0].schema();
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&schema, batches.iter())?
        };

        if combined.num_rows() == 0 {
            return Ok(stream_from_batches(schema, vec![combined]));
        }

        let sort_columns: Vec<arrow::compute::SortColumn> = self
            .order_by
            .iter()
            .map(|s| {
                let col = expression::evaluate(&s.expr, &combined, None)?;
                Ok(arrow::compute::SortColumn {
                    values: col,
                    options: Some(arrow::compute::SortOptions {
                        descending: !s.asc,
                        nulls_first: s.nulls_first,
                    }),
                })
            })
            .collect::<Result<_, ExecutionError>>()?;

        let indices = compute::lexsort_to_indices(&sort_columns, None)?;

        let sorted_columns: Vec<ArrayRef> = (0..combined.num_columns())
            .map(|i| compute::take(combined.column(i), &indices, None).map_err(Into::into))
            .collect::<Result<_, ExecutionError>>()?;

        let result = RecordBatch::try_new(schema.clone(), sorted_columns)?;
        Ok(stream_from_batches(schema, vec![result]))
    }

    fn display_name(&self) -> &str {
        "SortExec"
    }
}

// ===========================================================================
// LimitExec
// ===========================================================================

/// Applies LIMIT and OFFSET to the input.
#[derive(Debug)]
pub(crate) struct LimitExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

#[async_trait]
impl ExecutionPlan for LimitExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.input.execute().await?;
        let batches = collect_stream(stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect input: {e}"))
        })?;

        if batches.is_empty() {
            let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.input.schema());
            return Ok(stream_from_batches(arrow_schema, vec![]));
        }

        let schema = batches[0].schema();
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&schema, batches.iter())?
        };

        let total = combined.num_rows();
        let offset = self.offset.unwrap_or(0);
        let start = offset.min(total);
        let remaining = total - start;
        let take = self.limit.map_or(remaining, |l| l.min(remaining));

        if take == 0 {
            let empty_cols: Vec<ArrayRef> = (0..combined.num_columns())
                .map(|i| combined.column(i).slice(0, 0))
                .collect();
            let empty = RecordBatch::try_new(schema.clone(), empty_cols)?;
            return Ok(stream_from_batches(schema, vec![empty]));
        }

        let sliced = combined.slice(start, take);
        Ok(stream_from_batches(schema, vec![sliced]))
    }

    fn display_name(&self) -> &str {
        "LimitExec"
    }
}

// ===========================================================================
// ExplainExec
// ===========================================================================

/// Produces the textual plan description as a single-column Utf8 batch.
#[derive(Debug)]
pub(crate) struct ExplainExec {
    pub(crate) plan: LogicalPlan,
}

#[async_trait]
impl ExecutionPlan for ExplainExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            name: "plan".to_string(),
            data_type: arneb_common::types::DataType::Utf8,
            nullable: false,
        }]
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let plan_text = format!("{}", self.plan);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "plan",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![plan_text.as_str()]))],
        )?;
        Ok(stream_from_batches(schema, vec![batch]))
    }

    fn display_name(&self) -> &str {
        "ExplainExec"
    }
}

// ===========================================================================
// Stream adapters
// ===========================================================================

/// A stream that applies a mapping function to each batch from an input stream.
struct MapStream<F> {
    input: SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
    map_fn: F,
}

impl<F> MapStream<F>
where
    F: FnMut(RecordBatch) -> Result<RecordBatch, ExecutionError> + Send + Unpin,
{
    fn new(
        input: SendableRecordBatchStream,
        schema: arrow::datatypes::SchemaRef,
        map_fn: F,
    ) -> Self {
        Self {
            input,
            schema,
            map_fn,
        }
    }
}

impl<F> Stream for MapStream<F>
where
    F: FnMut(RecordBatch) -> Result<RecordBatch, ExecutionError> + Send + Unpin,
{
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = (self.map_fn)(batch).map_err(arneb_common::error::ArnebError::from);
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> RecordBatchStream for MapStream<F>
where
    F: FnMut(RecordBatch) -> Result<RecordBatch, ExecutionError> + Send + Unpin,
{
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// A stream that applies a filter-map function (returning `Option<RecordBatch>`)
/// to each input batch, skipping `None` results.
struct FilterMapStream<F> {
    input: SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
    map_fn: F,
}

impl<F> FilterMapStream<F>
where
    F: FnMut(RecordBatch) -> Result<Option<RecordBatch>, ExecutionError> + Send + Unpin,
{
    fn new(
        input: SendableRecordBatchStream,
        schema: arrow::datatypes::SchemaRef,
        map_fn: F,
    ) -> Self {
        Self {
            input,
            schema,
            map_fn,
        }
    }
}

impl<F> Stream for FilterMapStream<F>
where
    F: FnMut(RecordBatch) -> Result<Option<RecordBatch>, ExecutionError> + Send + Unpin,
{
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.input).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    match (self.map_fn)(batch) {
                        Ok(Some(result)) => {
                            return Poll::Ready(Some(Ok(result)));
                        }
                        Ok(None) => {
                            // Skip this batch, try next
                            continue;
                        }
                        Err(e) => {
                            return Poll::Ready(Some(Err(arneb_common::error::ArnebError::from(
                                e,
                            ))));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<F> RecordBatchStream for FilterMapStream<F>
where
    F: FnMut(RecordBatch) -> Result<Option<RecordBatch>, ExecutionError> + Send + Unpin,
{
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

fn group_key(group_cols: &[ArrayRef], row: usize) -> Result<String, ExecutionError> {
    let mut key = String::new();
    for (i, col) in group_cols.iter().enumerate() {
        if i > 0 {
            key.push('|');
        }
        let s = extract_scalar(col, row)?;
        key.push_str(&format!("{s}"));
    }
    Ok(key)
}

fn extract_scalar(arr: &ArrayRef, index: usize) -> Result<ScalarValue, ExecutionError> {
    if arr.is_null(index) {
        return Ok(ScalarValue::Null);
    }
    match arr.data_type() {
        ArrowDataType::Int32 => {
            let a = arr.as_primitive::<datatypes::Int32Type>();
            Ok(ScalarValue::Int32(a.value(index)))
        }
        ArrowDataType::Int64 => {
            let a = arr.as_primitive::<datatypes::Int64Type>();
            Ok(ScalarValue::Int64(a.value(index)))
        }
        ArrowDataType::Float32 => {
            let a = arr.as_primitive::<datatypes::Float32Type>();
            Ok(ScalarValue::Float32(a.value(index)))
        }
        ArrowDataType::Float64 => {
            let a = arr.as_primitive::<datatypes::Float64Type>();
            Ok(ScalarValue::Float64(a.value(index)))
        }
        ArrowDataType::Utf8 => {
            let a = arr.as_string::<i32>();
            Ok(ScalarValue::Utf8(a.value(index).to_string()))
        }
        ArrowDataType::Boolean => {
            let a = arr.as_boolean();
            Ok(ScalarValue::Boolean(a.value(index)))
        }
        ArrowDataType::Date32 => {
            let a = arr.as_primitive::<datatypes::Date32Type>();
            Ok(ScalarValue::Date32(a.value(index)))
        }
        dt => Err(ExecutionError::InvalidOperation(format!(
            "cannot extract scalar from type {dt:?}"
        ))),
    }
}

fn scalars_to_array(values: &[ScalarValue], _len: usize) -> Result<ArrayRef, ExecutionError> {
    if values.is_empty() {
        return Ok(Arc::new(array::NullArray::new(0)));
    }

    let first_type = values.iter().find(|v| !matches!(v, ScalarValue::Null));
    match first_type {
        Some(ScalarValue::Int32(_)) => {
            let arr: Int32Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Int32(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Int64(_)) => {
            let arr: Int64Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Int64(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Float32(_)) => {
            let arr: Float32Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Float32(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Float64(_)) => {
            let arr: Float64Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Float64(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Utf8(_)) => {
            let arr: StringArray = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Utf8(s) => Some(s.as_str()),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Boolean(_)) => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Boolean(b) => Some(*b),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        Some(ScalarValue::Date32(_)) => {
            let arr: Date32Array = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Date32(n) => Some(*n),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        _ => Ok(Arc::new(array::NullArray::new(values.len()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::DataType;

    fn make_test_source() -> Arc<dyn DataSource> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        Arc::new(InMemoryDataSource::new(
            vec![
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
            ],
            vec![batch],
        ))
    }

    #[test]
    fn extract_scalar_supports_date32() {
        // TPC-H Q3 groups by a date column; before this fix the aggregate
        // operator failed with "cannot extract scalar from type Date32".
        let arr: ArrayRef = Arc::new(Date32Array::from(vec![Some(19000), None, Some(19500)]));
        assert_eq!(extract_scalar(&arr, 0).unwrap(), ScalarValue::Date32(19000));
        assert_eq!(extract_scalar(&arr, 1).unwrap(), ScalarValue::Null);
        assert_eq!(extract_scalar(&arr, 2).unwrap(), ScalarValue::Date32(19500));
    }

    #[test]
    fn scalars_to_array_supports_date32() {
        let values = vec![
            ScalarValue::Date32(19000),
            ScalarValue::Null,
            ScalarValue::Date32(19500),
        ];
        let arr = scalars_to_array(&values, 3).unwrap();
        let date_arr = arr.as_primitive::<datatypes::Date32Type>();
        assert_eq!(date_arr.value(0), 19000);
        assert!(date_arr.is_null(1));
        assert_eq!(date_arr.value(2), 19500);
    }

    #[tokio::test]
    async fn scan_exec() {
        let source = make_test_source();
        let scan = ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        };
        let stream = scan.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn filter_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        });
        let filter = FilterExec {
            input: scan,
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                }),
                op: ast::BinaryOp::Gt,
                right: Box::new(PlanExpr::Literal(ScalarValue::Int32(1))),
            },
        };
        let stream = filter.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn projection_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        });
        let proj = ProjectionExec {
            input: scan,
            exprs: vec![PlanExpr::Column {
                index: 1,
                name: "name".to_string(),
            }],
            output_schema: vec![ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            }],
        };
        let stream = proj.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[tokio::test]
    async fn limit_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        });
        let limit = LimitExec {
            input: scan,
            limit: Some(2),
            offset: None,
        };
        let stream = limit.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn limit_with_offset() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        });
        let limit = LimitExec {
            input: scan,
            limit: Some(1),
            offset: Some(1),
        };
        let stream = limit.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
    }

    #[tokio::test]
    async fn sort_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        });
        let sort = SortExec {
            input: scan,
            order_by: vec![SortExpr {
                expr: PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                },
                asc: false,
                nulls_first: false,
            }],
        };
        let stream = sort.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 1);
    }

    #[tokio::test]
    async fn aggregate_no_grouping() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        });
        let agg = HashAggregateExec {
            input: scan,
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
            output_schema: vec![
                ColumnInfo {
                    name: "count".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "sum".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
        };
        let stream = agg.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 3);
        let sum = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum.value(0), 600);
    }

    #[tokio::test]
    async fn cross_join() {
        let schema1 = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            false,
        )]));
        let batch1 =
            RecordBatch::try_new(schema1, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let src1 = Arc::new(InMemoryDataSource::from_batch(batch1).unwrap()) as Arc<dyn DataSource>;

        let schema2 = Arc::new(Schema::new(vec![Field::new(
            "b",
            ArrowDataType::Int32,
            false,
        )]));
        let batch2 =
            RecordBatch::try_new(schema2, vec![Arc::new(Int32Array::from(vec![10, 20, 30]))])
                .unwrap();
        let src2 = Arc::new(InMemoryDataSource::from_batch(batch2).unwrap()) as Arc<dyn DataSource>;

        let left: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: src1,
            _table_name: "t1".to_string(),
            scan_context: ScanContext::default(),
        });
        let right: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: src2,
            _table_name: "t2".to_string(),
            scan_context: ScanContext::default(),
        });

        let join = NestedLoopJoinExec {
            left,
            right,
            join_type: ast::JoinType::Cross,
            condition: arneb_planner::JoinCondition::None,
        };

        let stream = join.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6);
    }

    #[tokio::test]
    async fn explain_exec() {
        let plan = LogicalPlan::TableScan {
            table: arneb_common::types::TableReference::table("test"),
            schema: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
            properties: Default::default(),
        };
        let explain = ExplainExec { plan };
        let stream = explain.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        let text = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(text.value(0).contains("TableScan"));
    }
}
