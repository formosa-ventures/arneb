//! Table-schema adaptation shared by the file-backed lake connectors
//! (Hive Parquet/ORC, Iceberg).
//!
//! A data file rarely matches the table schema exactly: columns may be
//! missing (added after the file was written), have a narrower physical
//! type (`int` file column in a `bigint` table column), live outside the
//! file entirely (Hive partition columns), or be ordered differently.
//! Each output column is described by a [`ColumnSource`]; [`adapt_batch`]
//! assembles the table-schema batch from a projected file batch.
//! [`remap_filter`] rewrites pushed-down filters from table column indices
//! to file column indices.

use arrow::array::{new_null_array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::compute::{cast_with_options, take, CastOptions};
use arrow::datatypes::SchemaRef;

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::types::ColumnInfo;
use arneb_planner::PlanExpr;

/// How one output column is produced from a file batch.
#[derive(Debug, Clone)]
pub enum ColumnSource {
    /// Column at this position of the (projected) file batch, cast to the
    /// table type when the file type differs (e.g. `int` → `bigint`).
    File(usize),
    /// Column absent from the file (added after the file was written).
    Null,
    /// The same value for every row (a Hive partition column). Holds a
    /// one-row array already in the table type.
    Constant(ArrayRef),
}

/// Assemble a table-schema batch from a projected file batch: cast
/// promoted columns, null-fill columns the file does not have, and
/// repeat constant (partition) values.
///
/// Casts never silently turn an out-of-range value into NULL: overflow is
/// an error.
pub fn adapt_batch(
    schema: &SchemaRef,
    sources: &[ColumnSource],
    file_path: &str,
    batch: RecordBatch,
) -> Result<RecordBatch, ArnebError> {
    let n = batch.num_rows();
    let strict = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns: Vec<ArrayRef> = sources
        .iter()
        .zip(schema.fields())
        .map(|(src, field)| -> Result<ArrayRef, ArnebError> {
            let err = |e: arrow::error::ArrowError, from: &arrow::datatypes::DataType| {
                ExecutionError::InvalidOperation(format!(
                    "column '{}' in '{file_path}': cannot read {from} as {}: {e}",
                    field.name(),
                    field.data_type()
                ))
                .into()
            };
            match src {
                ColumnSource::Null => Ok(new_null_array(field.data_type(), n)),
                ColumnSource::Constant(value) => {
                    let indices = UInt32Array::from(vec![0u32; n]);
                    take(value.as_ref(), &indices, None).map_err(|e| err(e, value.data_type()))
                }
                ColumnSource::File(pos) => {
                    let col = batch.column(*pos);
                    if col.data_type() == field.data_type() {
                        return Ok(col.clone());
                    }
                    cast_with_options(col, field.data_type(), &strict)
                        .map_err(|e| err(e, col.data_type()))
                }
            }
        })
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| {
        ExecutionError::InvalidOperation(format!("batch assembly for '{file_path}': {e}")).into()
    })
}

/// Rewrite column references in a pushed-down filter from table column
/// indices to file column indices. Returns `None` when the filter touches a
/// column that cannot be pushed into this file (`map` returns `None`: the
/// column is missing, nested, a partition column, or has a different
/// physical type) or uses an expression shape the pushdown does not
/// understand; the filter still runs above the scan.
pub fn remap_filter(e: &PlanExpr, map: &dyn Fn(usize) -> Option<usize>) -> Option<PlanExpr> {
    Some(match e {
        PlanExpr::Column { index, name, span } => PlanExpr::Column {
            index: map(*index)?,
            name: name.clone(),
            span: *span,
        },
        PlanExpr::Literal { .. } => e.clone(),
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(remap_filter(left, map)?),
            op: *op,
            right: Box::new(remap_filter(right, map)?),
            span: *span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(remap_filter(expr, map)?),
            list: list
                .iter()
                .map(|x| remap_filter(x, map))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
            span: *span,
        },
        _ => return None,
    })
}

/// `true` when every `column <op> literal` comparison in `e` compares
/// against a literal of exactly the column's type. The shared Parquet
/// predicate kernels compare Arrow arrays directly and error on mixed
/// types (e.g. `Int64` column vs `Int32` scalar), so anything else stays
/// above the scan.
pub fn literal_types_match(e: &PlanExpr, columns: &[ColumnInfo]) -> bool {
    let col_type = |c: &PlanExpr| match c {
        PlanExpr::Column { index, .. } => columns.get(*index).map(|c| &c.data_type),
        _ => None,
    };
    let lit_ok = |c: &PlanExpr, l: &PlanExpr| match (col_type(c), l) {
        (Some(ct), PlanExpr::Literal { value, .. }) => &value.data_type() == ct,
        _ => true,
    };
    match e {
        PlanExpr::BinaryOp { left, right, .. } => {
            lit_ok(left, right)
                && lit_ok(right, left)
                && literal_types_match(left, columns)
                && literal_types_match(right, columns)
        }
        PlanExpr::InList { expr, list, .. } => list.iter().all(|l| lit_ok(expr, l)),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn adapt_batch_casts_null_fills_and_repeats_constants() {
        let file = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Utf8, true),
            Field::new("a", DataType::Int64, true),
            Field::new("missing", DataType::Int32, true),
        ]));
        let sources = vec![
            ColumnSource::Constant(Arc::new(StringArray::from(vec!["x"]))),
            ColumnSource::File(0),
            ColumnSource::Null,
        ];
        let out = adapt_batch(&schema, &sources, "f", file).unwrap();
        assert_eq!(out.num_rows(), 3);
        let p = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((p.value(0), p.value(2)), ("x", "x"));
        let a = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.values(), &[1, 2, 3]);
        assert_eq!(out.column(2).null_count(), 3);
    }

    #[test]
    fn adapt_batch_errors_on_overflow_instead_of_null() {
        let file = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let err = adapt_batch(&schema, &[ColumnSource::File(0)], "f", file).unwrap_err();
        assert!(
            err.to_string().contains("cannot read Int64 as Int32"),
            "{err}"
        );
    }
}
