//! Physical execution operators.
//!
//! Each operator implements [`ExecutionPlan`] and produces [`RecordBatch`]es
//! from its children. Operators are assembled into a tree by the physical
//! planner in [`super::planner`].

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{
    self, Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::compute;
use arrow::datatypes::{self, DataType as ArrowDataType, Field, Schema};
use trino_common::error::ExecutionError;
use trino_common::types::{ColumnInfo, ScalarValue};
use trino_planner::{LogicalPlan, PlanExpr, SortExpr};
use trino_sql_parser::ast;

use crate::aggregate::{self, Accumulator};
use crate::datasource::DataSource;
use crate::expression;

/// A physical execution operator that produces record batches.
pub trait ExecutionPlan: Send + Sync + Debug {
    /// The output schema of this operator.
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Executes the operator and returns all result batches.
    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError>;

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
}

impl ExecutionPlan for ScanExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.source.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        self.source.scan()
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

impl ExecutionPlan for ProjectionExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let batches = self.input.execute()?;
        let arrow_schema = crate::datasource::column_info_to_arrow_schema(&self.output_schema);

        let mut result = Vec::with_capacity(batches.len());
        for batch in &batches {
            let columns: Vec<ArrayRef> = self
                .exprs
                .iter()
                .map(|e| expression::evaluate(e, batch))
                .collect::<Result<_, _>>()?;

            // Cast columns to match output schema types if needed.
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

            result.push(RecordBatch::try_new(arrow_schema.clone(), columns)?);
        }
        Ok(result)
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

impl ExecutionPlan for FilterExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let batches = self.input.execute()?;
        let mut result = Vec::new();

        for batch in &batches {
            let mask_arr = expression::evaluate(&self.predicate, batch)?;
            let mask = mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation(
                        "filter predicate must produce a boolean array".to_string(),
                    )
                })?;

            let filtered = compute::filter_record_batch(batch, mask)?;
            if filtered.num_rows() > 0 {
                result.push(filtered);
            }
        }
        Ok(result)
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
    pub(crate) condition: trino_planner::JoinCondition,
}

impl ExecutionPlan for NestedLoopJoinExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let left_batches = self.input_to_single_batch(&self.left)?;
        let right_batches = self.input_to_single_batch(&self.right)?;

        let left_batch = match left_batches {
            Some(b) => b,
            None => return Ok(vec![]),
        };
        let right_batch = match right_batches {
            Some(b) => b,
            None => {
                return match self.join_type {
                    ast::JoinType::Left | ast::JoinType::Full => {
                        self.left_unmatched_output(&left_batch, self.right.schema().len())
                    }
                    _ => Ok(vec![]),
                };
            }
        };

        let left_rows = left_batch.num_rows();
        let right_rows = right_batch.num_rows();

        // Build combined schema.
        let output_schema = self.build_output_schema(&left_batch, &right_batch);

        // Track which rows are matched (for outer joins).
        let mut left_matched = vec![false; left_rows];
        let mut right_matched = vec![false; right_rows];

        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        for (l, l_matched) in left_matched.iter_mut().enumerate() {
            for (r, r_matched) in right_matched.iter_mut().enumerate() {
                let pass = self.eval_join_condition(l, r, &left_batch, &right_batch)?;
                if pass {
                    left_indices.push(l as u32);
                    right_indices.push(r as u32);
                    *l_matched = true;
                    *r_matched = true;
                }
            }
        }

        // Build matched rows.
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

    fn display_name(&self) -> &str {
        "NestedLoopJoinExec"
    }
}

impl NestedLoopJoinExec {
    /// Concatenate all input batches into one.
    fn input_to_single_batch(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<RecordBatch>, ExecutionError> {
        let batches = plan.execute()?;
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

    fn build_output_schema(&self, left: &RecordBatch, right: &RecordBatch) -> Arc<Schema> {
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| {
                // For outer joins, left columns become nullable.
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
            trino_planner::JoinCondition::None => Ok(true), // CROSS JOIN
            trino_planner::JoinCondition::On(expr) => {
                // Build a single-row combined batch for evaluating the join expr.
                let combined =
                    self.build_combined_row(left_row, right_row, left_batch, right_batch)?;
                let result = expression::evaluate(expr, &combined)?;
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
        // Return left rows with nulls for right side.
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

impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let batches = self.input.execute()?;

        // Extract aggregate function metadata.
        let aggr_info: Vec<AggrInfo> = self
            .aggr_exprs
            .iter()
            .map(|e| match e {
                PlanExpr::Function { name, args, .. } => {
                    let is_count_star = args.is_empty();
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
            // No grouping — single global aggregate.
            return self.execute_no_grouping(&batches, &aggr_info);
        }

        // Group-by aggregation using a HashMap keyed by stringified group values.
        let mut groups: HashMap<String, GroupState> = HashMap::new();

        for batch in &batches {
            // Evaluate group-by columns.
            let group_cols: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|e| expression::evaluate(e, batch))
                .collect::<Result<_, _>>()?;

            // Evaluate aggregate input columns.
            let aggr_input_cols: Vec<ArrayRef> = aggr_info
                .iter()
                .map(|info| {
                    if info.is_count_star {
                        // For COUNT(*), pass the first column as a placeholder.
                        Ok(batch.column(0).clone())
                    } else {
                        expression::evaluate(&info.args[0], batch)
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

        // Build output batch.
        self.build_aggregate_output(groups)
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

/// Group state: group-by column values + accumulators for each aggregate.
type GroupState = (Vec<ScalarValue>, Vec<Box<dyn Accumulator>>);

impl HashAggregateExec {
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
                    expression::evaluate(&info.args[0], batch)?
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

        // Collect group values and aggregate results.
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

impl ExecutionPlan for SortExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let batches = self.input.execute()?;
        if batches.is_empty() {
            return Ok(vec![]);
        }

        // Concatenate all batches.
        let schema = batches[0].schema();
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&schema, batches.iter())?
        };

        if combined.num_rows() == 0 {
            return Ok(vec![combined]);
        }

        // Build sort columns for Arrow's lexsort_to_indices.
        let sort_columns: Vec<arrow::compute::SortColumn> = self
            .order_by
            .iter()
            .map(|s| {
                let col = expression::evaluate(&s.expr, &combined)?;
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

        // Reorder all columns.
        let sorted_columns: Vec<ArrayRef> = (0..combined.num_columns())
            .map(|i| compute::take(combined.column(i), &indices, None).map_err(Into::into))
            .collect::<Result<_, ExecutionError>>()?;

        Ok(vec![RecordBatch::try_new(schema, sorted_columns)?])
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

impl ExecutionPlan for LimitExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let batches = self.input.execute()?;
        if batches.is_empty() {
            return Ok(vec![]);
        }

        // Concatenate, then slice.
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
            // Return empty batch with correct schema.
            let empty_cols: Vec<ArrayRef> = (0..combined.num_columns())
                .map(|i| combined.column(i).slice(0, 0))
                .collect();
            return Ok(vec![RecordBatch::try_new(schema, empty_cols)?]);
        }

        let sliced = combined.slice(start, take);
        Ok(vec![sliced])
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

impl ExecutionPlan for ExplainExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            name: "plan".to_string(),
            data_type: trino_common::types::DataType::Utf8,
            nullable: false,
        }]
    }

    fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let plan_text = format!("{}", self.plan);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "plan",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![plan_text.as_str()]))],
        )?;
        Ok(vec![batch])
    }

    fn display_name(&self) -> &str {
        "ExplainExec"
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Generate a string key for a group-by row.
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

/// Extract a single scalar value from an array at the given index.
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
        dt => Err(ExecutionError::InvalidOperation(format!(
            "cannot extract scalar from type {dt:?}"
        ))),
    }
}

/// Convert a list of ScalarValues to a single ArrayRef.
fn scalars_to_array(values: &[ScalarValue], _len: usize) -> Result<ArrayRef, ExecutionError> {
    if values.is_empty() {
        return Ok(Arc::new(array::NullArray::new(0)));
    }

    // Determine type from first non-null value.
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
        _ => {
            // All nulls or unsupported type — produce null array.
            Ok(Arc::new(array::NullArray::new(values.len())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use trino_common::types::DataType;

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
    fn scan_exec() {
        let source = make_test_source();
        let scan = ScanExec {
            source,
            _table_name: "test".to_string(),
        };
        let batches = scan.execute().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn filter_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
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
        let batches = filter.execute().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn projection_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
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
        let batches = proj.execute().unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[test]
    fn limit_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
        });
        let limit = LimitExec {
            input: scan,
            limit: Some(2),
            offset: None,
        };
        let batches = limit.execute().unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn limit_with_offset() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
        });
        let limit = LimitExec {
            input: scan,
            limit: Some(1),
            offset: Some(1),
        };
        let batches = limit.execute().unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2); // skipped row 0 (id=1)
    }

    #[test]
    fn sort_exec() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
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
        let batches = sort.execute().unwrap();
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 1);
    }

    #[test]
    fn aggregate_no_grouping() {
        let source = make_test_source();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "test".to_string(),
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
        let batches = agg.execute().unwrap();
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

    #[test]
    fn cross_join() {
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
        });
        let right: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: src2,
            _table_name: "t2".to_string(),
        });

        let join = NestedLoopJoinExec {
            left,
            right,
            join_type: ast::JoinType::Cross,
            condition: trino_planner::JoinCondition::None,
        };

        let batches = join.execute().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6); // 2 × 3
    }

    #[test]
    fn explain_exec() {
        let plan = LogicalPlan::TableScan {
            table: trino_common::types::TableReference::table("test"),
            schema: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
        };
        let explain = ExplainExec { plan };
        let batches = explain.execute().unwrap();
        assert_eq!(batches.len(), 1);
        let text = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(text.value(0).contains("TableScan"));
    }
}
