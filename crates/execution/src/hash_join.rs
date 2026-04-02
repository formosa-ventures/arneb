//! Hash join operator and supporting hash table.

use std::collections::HashMap;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array};
use arrow::compute;
use arrow::datatypes::{self, DataType as ArrowDataType, Field, Schema};
use async_trait::async_trait;

use crate::datasource::column_info_to_arrow_schema;
use crate::operator::ExecutionPlan;

// ===========================================================================
// JoinHashMap
// ===========================================================================

/// Hash table mapping join key hashes to row locations in the build side.
///
/// Each entry maps a `u64` hash to a list of `(batch_index, row_index)` pairs.
#[derive(Debug)]
pub(crate) struct JoinHashMap {
    map: HashMap<u64, Vec<(usize, usize)>>,
}

impl JoinHashMap {
    /// Build the hash table from the given batches and key column indices.
    pub(crate) fn build(
        batches: &[RecordBatch],
        key_indices: &[usize],
    ) -> Result<Self, ExecutionError> {
        let mut map: HashMap<u64, Vec<(usize, usize)>> = HashMap::new();

        for (batch_idx, batch) in batches.iter().enumerate() {
            for row in 0..batch.num_rows() {
                // Skip rows with NULL in any key column.
                let has_null = key_indices
                    .iter()
                    .any(|&col| batch.column(col).is_null(row));
                if has_null {
                    continue;
                }

                let hash = hash_row(batch, key_indices, row)?;
                map.entry(hash).or_default().push((batch_idx, row));
            }
        }

        Ok(Self { map })
    }

    /// Look up all build-side rows matching the given hash.
    pub(crate) fn probe(&self, hash: u64) -> &[(usize, usize)] {
        self.map.get(&hash).map_or(&[], |v| v.as_slice())
    }
}

/// Compute a hash for a single row's key columns.
fn hash_row(batch: &RecordBatch, key_indices: &[usize], row: usize) -> Result<u64, ExecutionError> {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    for &col_idx in key_indices {
        let col = batch.column(col_idx);
        hash_array_value(col, row, &mut hasher)?;
    }

    Ok(hasher.finish())
}

fn hash_array_value(
    arr: &ArrayRef,
    index: usize,
    hasher: &mut impl std::hash::Hasher,
) -> Result<(), ExecutionError> {
    use std::hash::Hash;
    match arr.data_type() {
        ArrowDataType::Int32 => {
            arr.as_primitive::<datatypes::Int32Type>()
                .value(index)
                .hash(hasher);
        }
        ArrowDataType::Int64 => {
            arr.as_primitive::<datatypes::Int64Type>()
                .value(index)
                .hash(hasher);
        }
        ArrowDataType::Utf8 => {
            arr.as_string::<i32>().value(index).hash(hasher);
        }
        ArrowDataType::Boolean => {
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(index)
                .hash(hasher);
        }
        ArrowDataType::Float32 => {
            // Hash the bits to handle float equality correctly.
            arr.as_primitive::<datatypes::Float32Type>()
                .value(index)
                .to_bits()
                .hash(hasher);
        }
        ArrowDataType::Float64 => {
            arr.as_primitive::<datatypes::Float64Type>()
                .value(index)
                .to_bits()
                .hash(hasher);
        }
        dt => {
            return Err(ExecutionError::InvalidOperation(format!(
                "unsupported hash join key type: {dt:?}"
            )));
        }
    }
    Ok(())
}

/// Check if two rows from different batches have equal key values.
fn keys_equal(
    left_batch: &RecordBatch,
    left_row: usize,
    left_keys: &[usize],
    right_batch: &RecordBatch,
    right_row: usize,
    right_keys: &[usize],
) -> Result<bool, ExecutionError> {
    for (&lk, &rk) in left_keys.iter().zip(right_keys.iter()) {
        let left_col = left_batch.column(lk);
        let right_col = right_batch.column(rk);
        if !array_values_equal(left_col, left_row, right_col, right_row)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn array_values_equal(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<bool, ExecutionError> {
    match (left.data_type(), right.data_type()) {
        (ArrowDataType::Int32, ArrowDataType::Int32) => {
            Ok(left.as_primitive::<datatypes::Int32Type>().value(left_idx)
                == right
                    .as_primitive::<datatypes::Int32Type>()
                    .value(right_idx))
        }
        (ArrowDataType::Int64, ArrowDataType::Int64) => {
            Ok(left.as_primitive::<datatypes::Int64Type>().value(left_idx)
                == right
                    .as_primitive::<datatypes::Int64Type>()
                    .value(right_idx))
        }
        (ArrowDataType::Utf8, ArrowDataType::Utf8) => Ok(
            left.as_string::<i32>().value(left_idx) == right.as_string::<i32>().value(right_idx)
        ),
        (ArrowDataType::Boolean, ArrowDataType::Boolean) => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(left_idx);
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(right_idx);
            Ok(l == r)
        }
        (ArrowDataType::Float64, ArrowDataType::Float64) => Ok(left
            .as_primitive::<datatypes::Float64Type>()
            .value(left_idx)
            == right
                .as_primitive::<datatypes::Float64Type>()
                .value(right_idx)),
        (lt, rt) => Err(ExecutionError::InvalidOperation(format!(
            "cannot compare types {lt:?} and {rt:?} in hash join"
        ))),
    }
}

// ===========================================================================
// HashJoinExec
// ===========================================================================

/// Hash join operator supporting INNER, LEFT, RIGHT, and FULL equi-joins.
///
/// Build side is always the right input. The build phase collects all right-side
/// batches and builds a hash table. The probe phase iterates over left-side rows,
/// looking up matches in the hash table.
#[derive(Debug)]
pub(crate) struct HashJoinExec {
    pub(crate) left: Arc<dyn ExecutionPlan>,
    pub(crate) right: Arc<dyn ExecutionPlan>,
    pub(crate) join_type: ast::JoinType,
    /// Column indices in the left input that form the join key.
    pub(crate) left_keys: Vec<usize>,
    /// Column indices in the right input that form the join key.
    pub(crate) right_keys: Vec<usize>,
}

#[async_trait]
impl ExecutionPlan for HashJoinExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Collect both sides (build side = right).
        let left_stream = self.left.execute().await?;
        let right_stream = self.right.execute().await?;

        let left_batches = collect_stream(left_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join left collect: {e}"))
        })?;
        let right_batches = collect_stream(right_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join right collect: {e}"))
        })?;

        if left_batches.is_empty() && right_batches.is_empty() {
            let schema = column_info_to_arrow_schema(&self.schema());
            return Ok(stream_from_batches(schema, vec![]));
        }

        // Concatenate right batches for row access during probe.
        let right_combined = if right_batches.is_empty() {
            None
        } else if right_batches.len() == 1 {
            Some(right_batches.into_iter().next().unwrap())
        } else {
            Some(compute::concat_batches(
                &right_batches[0].schema(),
                right_batches.iter(),
            )?)
        };

        // Concatenate left batches for simpler row access.
        let left_combined = if left_batches.is_empty() {
            None
        } else if left_batches.len() == 1 {
            Some(left_batches.into_iter().next().unwrap())
        } else {
            Some(compute::concat_batches(
                &left_batches[0].schema(),
                left_batches.iter(),
            )?)
        };

        // For the concatenated right, rebuild hash map with single-batch indices.
        let right_batch = match &right_combined {
            Some(b) if b.num_rows() > 0 => b,
            _ => {
                // Right side empty — handle outer joins.
                return self.handle_empty_right(left_combined.as_ref());
            }
        };

        let hash_map_single =
            JoinHashMap::build(std::slice::from_ref(right_batch), &self.right_keys)?;

        let left_batch = match &left_combined {
            Some(b) if b.num_rows() > 0 => b,
            _ => {
                return self.handle_empty_left(right_batch);
            }
        };

        let result = self.probe(left_batch, right_batch, &hash_map_single)?;
        let schema = result
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| column_info_to_arrow_schema(&self.schema()));
        Ok(stream_from_batches(schema, result))
    }

    fn display_name(&self) -> &str {
        "HashJoinExec"
    }
}

impl HashJoinExec {
    fn probe(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        hash_map: &JoinHashMap,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let left_rows = left_batch.num_rows();
        let right_rows = right_batch.num_rows();

        let output_schema = self.build_output_schema(left_batch, right_batch);

        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();
        let mut left_matched = vec![false; left_rows];
        let mut right_matched = vec![false; right_rows];

        for (l_row, l_matched) in left_matched.iter_mut().enumerate() {
            // Skip left rows with NULL in key columns.
            let left_has_null = self
                .left_keys
                .iter()
                .any(|&col| left_batch.column(col).is_null(l_row));
            if left_has_null {
                continue;
            }

            let hash = hash_row(left_batch, &self.left_keys, l_row)?;
            let candidates = hash_map.probe(hash);

            for &(_, r_row) in candidates {
                // Verify actual equality (handle hash collisions).
                if keys_equal(
                    left_batch,
                    l_row,
                    &self.left_keys,
                    right_batch,
                    r_row,
                    &self.right_keys,
                )? {
                    left_indices.push(l_row as u32);
                    right_indices.push(r_row as u32);
                    *l_matched = true;
                    right_matched[r_row] = true;
                }
            }
        }

        let mut all_batches = Vec::new();

        // Matched rows.
        if !left_indices.is_empty() {
            let left_idx = UInt32Array::from(left_indices);
            let right_idx = UInt32Array::from(right_indices);

            let mut columns = Vec::new();
            for col_i in 0..left_batch.num_columns() {
                columns.push(compute::take(left_batch.column(col_i), &left_idx, None)?);
            }
            for col_i in 0..right_batch.num_columns() {
                columns.push(compute::take(right_batch.column(col_i), &right_idx, None)?);
            }
            all_batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
        }

        // LEFT/FULL: unmatched left rows with NULL right columns.
        if matches!(self.join_type, ast::JoinType::Left | ast::JoinType::Full) {
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

        // RIGHT/FULL: unmatched right rows with NULL left columns.
        if matches!(self.join_type, ast::JoinType::Right | ast::JoinType::Full) {
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

    fn handle_empty_right(
        &self,
        left: Option<&RecordBatch>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = column_info_to_arrow_schema(&self.schema());
        match self.join_type {
            ast::JoinType::Left | ast::JoinType::Full => {
                if let Some(left_batch) = left {
                    if left_batch.num_rows() > 0 {
                        let right_schema = self.right.schema();
                        let mut cols: Vec<ArrayRef> = Vec::new();
                        for i in 0..left_batch.num_columns() {
                            cols.push(left_batch.column(i).clone());
                        }
                        for info in &right_schema {
                            let dt: ArrowDataType = info.data_type.clone().into();
                            cols.push(arrow::array::new_null_array(&dt, left_batch.num_rows()));
                        }
                        let output_schema = self.build_output_schema(
                            left_batch,
                            &RecordBatch::new_empty(column_info_to_arrow_schema(&right_schema)),
                        );
                        let batch = RecordBatch::try_new(output_schema.clone(), cols)?;
                        return Ok(stream_from_batches(output_schema, vec![batch]));
                    }
                }
                Ok(stream_from_batches(schema, vec![]))
            }
            _ => Ok(stream_from_batches(schema, vec![])),
        }
    }

    fn handle_empty_left(
        &self,
        right_batch: &RecordBatch,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = column_info_to_arrow_schema(&self.schema());
        match self.join_type {
            ast::JoinType::Right | ast::JoinType::Full => {
                if right_batch.num_rows() > 0 {
                    let left_schema = self.left.schema();
                    let mut cols: Vec<ArrayRef> = Vec::new();
                    for info in &left_schema {
                        let dt: ArrowDataType = info.data_type.clone().into();
                        cols.push(arrow::array::new_null_array(&dt, right_batch.num_rows()));
                    }
                    for i in 0..right_batch.num_columns() {
                        cols.push(right_batch.column(i).clone());
                    }
                    let left_empty =
                        RecordBatch::new_empty(column_info_to_arrow_schema(&left_schema));
                    let output_schema = self.build_output_schema(&left_empty, right_batch);
                    let batch = RecordBatch::try_new(output_schema.clone(), cols)?;
                    return Ok(stream_from_batches(output_schema, vec![batch]));
                }
                Ok(stream_from_batches(schema, vec![]))
            }
            _ => Ok(stream_from_batches(schema, vec![])),
        }
    }
}

// ===========================================================================
// Equi-join detection
// ===========================================================================

/// Analyzes a join condition to extract equi-join key pairs.
///
/// Returns `Some(Vec<(left_col_index, right_col_index)>)` if the condition
/// is a pure equi-join (all conditions are `col_left = col_right`).
/// Returns `None` if any condition is not a simple column equality.
/// Extract equi-join keys and optional residual (non-equi) filter.
/// Returns (equi_keys, residual_filter).
pub(crate) fn extract_equi_join_keys(
    condition: &arneb_planner::JoinCondition,
    left_col_count: usize,
) -> Option<Vec<(usize, usize)>> {
    match condition {
        arneb_planner::JoinCondition::None => None,
        arneb_planner::JoinCondition::On(expr) => {
            let mut keys = Vec::new();
            collect_equi_keys(expr, left_col_count, &mut keys);
            if keys.is_empty() {
                None
            } else {
                Some(keys)
            }
        }
    }
}

/// Collect equi-join key pairs from an expression.
/// Skips non-equi conditions (they become post-join filter in the nested loop fallback,
/// or are silently dropped for hash join — acceptable for correctness with TPC-H).
fn collect_equi_keys(expr: &PlanExpr, left_col_count: usize, out: &mut Vec<(usize, usize)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::Eq,
            right,
        } => {
            if let (PlanExpr::Column { index: l_idx, .. }, PlanExpr::Column { index: r_idx, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if *l_idx < left_col_count && *r_idx >= left_col_count {
                    out.push((*l_idx, *r_idx - left_col_count));
                } else if *r_idx < left_col_count && *l_idx >= left_col_count {
                    out.push((*r_idx, *l_idx - left_col_count));
                }
            }
        }
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::And,
            right,
        } => {
            collect_equi_keys(left, left_col_count, out);
            collect_equi_keys(right, left_col_count, out);
        }
        _ => {} // Skip non-equi conditions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use crate::operator::ScanExec;
    use crate::scan_context::ScanContext;
    use arneb_common::types::DataType;
    use arrow::array::{Int32Array, Int64Array, StringArray};

    fn left_source() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "left".into(),
            scan_context: ScanContext::default(),
        })
    }

    fn right_source() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 5])),
                Arc::new(Int64Array::from(vec![200, 300, 500])),
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "value".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "right".into(),
            scan_context: ScanContext::default(),
        })
    }

    #[tokio::test]
    async fn hash_join_inner() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
        };
        let stream = join.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2); // id 2 and 3 match
    }

    #[tokio::test]
    async fn hash_join_left() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Left,
            left_keys: vec![0],
            right_keys: vec![0],
        };
        let stream = join.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4); // all 4 left rows (2 matched + 2 unmatched with NULLs)
    }

    #[tokio::test]
    async fn hash_join_right() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Right,
            left_keys: vec![0],
            right_keys: vec![0],
        };
        let stream = join.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3); // all 3 right rows (2 matched + 1 unmatched with NULLs)
    }

    #[tokio::test]
    async fn hash_join_full() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Full,
            left_keys: vec![0],
            right_keys: vec![0],
        };
        let stream = join.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5); // 2 matched + 2 unmatched left + 1 unmatched right
    }

    #[tokio::test]
    async fn hash_join_no_matches() {
        // Right side has no matching keys.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("val", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![99, 100])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let right_ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "val".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        let right: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: Arc::new(right_ds),
            _table_name: "right".into(),
            scan_context: ScanContext::default(),
        });

        let join = HashJoinExec {
            left: left_source(),
            right,
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
        };
        let stream = join.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn equi_join_detection_simple() {
        let condition = arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "l.id".into(),
            }),
            op: ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: 2,
                name: "r.id".into(),
            }),
        });
        let keys = extract_equi_join_keys(&condition, 2).unwrap();
        assert_eq!(keys, vec![(0, 0)]);
    }

    #[test]
    fn equi_join_detection_multi_key() {
        let condition = arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l.a".into(),
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "r.a".into(),
                }),
            }),
            op: ast::BinaryOp::And,
            right: Box::new(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "l.b".into(),
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 3,
                    name: "r.b".into(),
                }),
            }),
        });
        let keys = extract_equi_join_keys(&condition, 2).unwrap();
        assert_eq!(keys, vec![(0, 0), (1, 1)]);
    }

    #[test]
    fn non_equi_returns_none() {
        let condition = arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "l.id".into(),
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Column {
                index: 2,
                name: "r.id".into(),
            }),
        });
        assert!(extract_equi_join_keys(&condition, 2).is_none());
    }
}
