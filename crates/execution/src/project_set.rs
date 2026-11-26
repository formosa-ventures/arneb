//! `ProjectSetExec`: evaluates the set-returning functions of a `SELECT`
//! list (`unnest`, `generate_subscripts`).

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::SendableRecordBatchStream;
use arneb_common::types::ColumnInfo;
use arneb_planner::{PlanExpr, SrfKind};
use arrow::array::{Array, ArrayRef, Int32Array, ListArray, StructArray, UInt32Array};
use arrow::buffer::NullBuffer;
use arrow::compute;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use crate::expression;
use crate::operator::{profile_stream, ExecutionPlan, MapStream};

/// For each input row, emits `max(len_i)` rows over the SRF arguments: the
/// input columns repeated, then one column per SRF, padded with NULL where
/// an array is shorter. A NULL or empty array has length 0, so a row whose
/// arrays are all NULL or empty yields no rows (PostgreSQL 10+ semantics).
#[derive(Debug)]
pub(crate) struct ProjectSetExec {
    input: Arc<dyn ExecutionPlan>,
    srfs: Vec<(SrfKind, PlanExpr)>,
    output_schema: Vec<ColumnInfo>,
}

impl ProjectSetExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        srfs: Vec<(SrfKind, PlanExpr)>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            input,
            srfs,
            output_schema,
        }
    }
}

#[async_trait]
impl ExecutionPlan for ProjectSetExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    fn output_partitioning(&self) -> crate::partitioning::Partitioning {
        // Works row by row: inherit the input's partitioning.
        self.input.output_partitioning()
    }

    fn inject_dynamic_filter(&self, filter: PlanExpr, target_index: usize) {
        // Input columns pass through at their own index; SRF columns are
        // computed and never a filter target.
        if target_index < self.input.schema().len() {
            self.input.inject_dynamic_filter(filter, target_index);
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        let input_stream = self.input.execute(partition).await?;
        let srf_count = self.srfs.len();
        let srf_fields = crate::datasource::column_info_to_arrow_schema(
            &self.output_schema[self.output_schema.len() - srf_count..],
        );
        let fields: Vec<Field> = input_stream
            .schema()
            .fields()
            .iter()
            .chain(srf_fields.fields().iter())
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let srfs = self.srfs.clone();
        let out_schema = schema.clone();
        let stream: SendableRecordBatchStream =
            Box::pin(MapStream::new(input_stream, schema, move |batch| {
                project_set_batch(&batch, &srfs, &out_schema)
            }));
        Ok(profile_stream("ProjectSetExec", partition, stream))
    }

    fn display_name(&self) -> &str {
        "ProjectSetExec"
    }
}

/// The array argument of an SRF, or `None` when it is NULL for every row
/// (an untyped `NULL` literal).
fn srf_argument(array: &ArrayRef) -> Result<Option<&ListArray>, ExecutionError> {
    if array.null_count() == array.len() && array.as_any().downcast_ref::<ListArray>().is_none() {
        return Ok(None);
    }
    array
        .as_any()
        .downcast_ref::<ListArray>()
        .map(Some)
        .ok_or_else(|| {
            ExecutionError::InvalidOperation(format!(
                "set-returning function argument must be an array, got {}",
                array.data_type()
            ))
        })
}

fn list_len(list: Option<&ListArray>, row: usize) -> usize {
    match list {
        Some(l) if l.is_valid(row) => {
            let offsets = l.value_offsets();
            (offsets[row + 1] - offsets[row]) as usize
        }
        _ => 0,
    }
}

/// The `STRUCT(x, n)` column of `_pg_expandarray`: `x` the element at each
/// output row's position, `n` its 1-based subscript within its input row's
/// array; NULL on padded rows.
fn expand_array_column(
    list: Option<&ListArray>,
    positions: &[Option<u32>],
    take_rows: &UInt32Array,
    data_type: &ArrowDataType,
) -> Result<ArrayRef, ExecutionError> {
    let ArrowDataType::Struct(fields) = data_type else {
        return Err(ExecutionError::InvalidOperation(format!(
            "_pg_expandarray: expected a struct output type, got {data_type}"
        )));
    };
    let Some(list) = list else {
        return Ok(arrow::array::new_null_array(data_type, positions.len()));
    };
    let indices = UInt32Array::from(positions.to_vec());
    let x = compute::take(list.values().as_ref(), &indices, None)?;
    let x = if x.data_type() != fields[0].data_type() {
        compute::cast(&x, fields[0].data_type())?
    } else {
        x
    };
    let offsets = list.value_offsets();
    let n: Int32Array = positions
        .iter()
        .zip(take_rows.values())
        .map(|(p, row)| p.map(|p| p as i32 - offsets[*row as usize] + 1))
        .collect();
    let nulls = NullBuffer::from(positions.iter().map(Option::is_some).collect::<Vec<_>>());
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![x, Arc::new(n)],
        Some(nulls),
    )?))
}

fn project_set_batch(
    batch: &RecordBatch,
    srfs: &[(SrfKind, PlanExpr)],
    schema: &SchemaRef,
) -> Result<RecordBatch, ExecutionError> {
    let args: Vec<ArrayRef> = srfs
        .iter()
        .map(|(_, e)| expression::evaluate(e, batch, None))
        .collect::<Result<_, _>>()?;
    let lists: Vec<Option<&ListArray>> = args.iter().map(srf_argument).collect::<Result<_, _>>()?;

    // Row `r` of the input repeats once per output row it yields.
    let mut take_rows: Vec<u32> = Vec::new();
    // Per SRF: the element index (unnest) or subscript of each output row.
    let mut positions: Vec<Vec<Option<u32>>> = vec![Vec::new(); srfs.len()];
    for row in 0..batch.num_rows() {
        let lens: Vec<usize> = lists.iter().map(|l| list_len(*l, row)).collect();
        let n = lens.iter().copied().max().unwrap_or(0);
        for k in 0..n {
            take_rows.push(row as u32);
            for (i, (kind, _)) in srfs.iter().enumerate() {
                positions[i].push((k < lens[i]).then(|| match kind {
                    SrfKind::Unnest | SrfKind::ExpandArray => {
                        let list = lists[i].expect("non-empty array");
                        (list.value_offsets()[row] as usize + k) as u32
                    }
                    SrfKind::GenerateSubscripts => (k + 1) as u32,
                }));
            }
        }
    }

    let take_rows = UInt32Array::from(take_rows);
    let mut columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| compute::take(c.as_ref(), &take_rows, None))
        .collect::<Result<_, _>>()?;
    let input_width = batch.num_columns();
    for (i, (kind, _)) in srfs.iter().enumerate() {
        let field = schema.field(input_width + i);
        let column: ArrayRef = match kind {
            SrfKind::Unnest => match lists[i] {
                Some(list) => {
                    let indices = UInt32Array::from(std::mem::take(&mut positions[i]));
                    compute::take(list.values().as_ref(), &indices, None)?
                }
                None => arrow::array::new_null_array(field.data_type(), positions[i].len()),
            },
            SrfKind::ExpandArray => {
                expand_array_column(lists[i], &positions[i], &take_rows, field.data_type())?
            }
            SrfKind::GenerateSubscripts => Arc::new(Int32Array::from(
                positions[i]
                    .iter()
                    .map(|p| p.map(|v| v as i32))
                    .collect::<Vec<_>>(),
            )),
        };
        let column = if column.data_type() != field.data_type() {
            compute::cast(&column, field.data_type())?
        } else {
            column
        };
        columns.push(column);
    }
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, Int64Builder, ListBuilder, StringArray};
    use arrow::datatypes::DataType as ArrowDataType;

    fn col(index: usize) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: format!("c{index}"),
            span: None,
        }
    }

    #[test]
    fn zips_pads_and_skips_null_and_empty_arrays() {
        // Rows: ([1,2,3], [10]), (NULL, []), ([], [20,30]).
        let mut a = ListBuilder::new(Int64Builder::new());
        a.values().append_slice(&[1, 2, 3]);
        a.append(true);
        a.append_null();
        a.append(true);
        let mut b = ListBuilder::new(Int64Builder::new());
        b.values().append_value(10);
        b.append(true);
        b.append(true);
        b.values().append_slice(&[20, 30]);
        b.append(true);
        let a = a.finish();
        let b = b.finish();
        let tag = StringArray::from(vec!["x", "y", "z"]);
        let in_schema = Arc::new(Schema::new(vec![
            Field::new("tag", ArrowDataType::Utf8, false),
            Field::new("a", a.data_type().clone(), true),
            Field::new("b", b.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            in_schema.clone(),
            vec![Arc::new(tag), Arc::new(a), Arc::new(b)],
        )
        .unwrap();
        let mut fields: Vec<Field> = in_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        fields.push(Field::new("u", ArrowDataType::Int64, true));
        fields.push(Field::new("s", ArrowDataType::Int32, true));
        fields.push(Field::new("v", ArrowDataType::Int64, true));
        let out_schema = Arc::new(Schema::new(fields));
        let srfs = vec![
            (SrfKind::Unnest, col(1)),
            (SrfKind::GenerateSubscripts, col(1)),
            (SrfKind::Unnest, col(2)),
        ];
        let out = project_set_batch(&batch, &srfs, &out_schema).unwrap();
        assert_eq!(out.num_rows(), 5);
        let tags = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            tags.iter().collect::<Vec<_>>(),
            [Some("x"), Some("x"), Some("x"), Some("z"), Some("z")]
        );
        let u = out.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            u.iter().collect::<Vec<_>>(),
            [Some(1), Some(2), Some(3), None, None]
        );
        let s = out.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            s.iter().collect::<Vec<_>>(),
            [Some(1), Some(2), Some(3), None, None]
        );
        let v = out.column(5).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            v.iter().collect::<Vec<_>>(),
            [Some(10), None, None, Some(20), Some(30)]
        );
    }

    #[test]
    fn expand_array_yields_element_and_subscript_structs() {
        // Rows: ([7, 8], [1, 2, 3]), (NULL, NULL); the shorter array pads
        // with a NULL struct.
        let mut a = ListBuilder::new(Int64Builder::new());
        a.values().append_slice(&[0, 0]);
        a.append(true);
        a.values().append_slice(&[7, 8]);
        a.append(true);
        a.append_null();
        // Sliced, so the offsets do not start at 0.
        let a: ArrayRef = Arc::new(a.finish().slice(1, 2));
        let mut b = ListBuilder::new(Int64Builder::new());
        b.values().append_slice(&[1, 2, 3]);
        b.append(true);
        b.append_null();
        let b: ArrayRef = Arc::new(b.finish());
        let in_schema = Arc::new(Schema::new(vec![
            Field::new("a", a.data_type().clone(), true),
            Field::new("b", b.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(in_schema.clone(), vec![a, b]).unwrap();
        let struct_type: ArrowDataType = arneb_common::types::DataType::Struct(vec![
            ("x".to_string(), arneb_common::types::DataType::Int64),
            ("n".to_string(), arneb_common::types::DataType::Int32),
        ])
        .into();
        let out_schema = Arc::new(Schema::new(vec![
            in_schema.field(0).clone(),
            in_schema.field(1).clone(),
            Field::new("e", struct_type, true),
            Field::new("u", ArrowDataType::Int64, true),
        ]));
        let srfs = vec![(SrfKind::ExpandArray, col(0)), (SrfKind::Unnest, col(1))];
        let out = project_set_batch(&batch, &srfs, &out_schema).unwrap();
        assert_eq!(out.num_rows(), 3);
        let e = out
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            (0..3).map(|i| e.is_valid(i)).collect::<Vec<_>>(),
            [true, true, false]
        );
        let x = e.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let n = e.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!((x.value(0), n.value(0)), (7, 1));
        assert_eq!((x.value(1), n.value(1)), (8, 2));
        // Read through `(e).x`, the padded row is NULL.
        let fx = crate::expression::get_field(out.column(2), "X").unwrap();
        let fx = fx.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(fx.iter().collect::<Vec<_>>(), [Some(7), Some(8), None]);
    }

    #[test]
    fn sliced_list_arrays_use_their_own_offsets() {
        let mut a = ListBuilder::new(Int64Builder::new());
        a.values().append_slice(&[1, 2]);
        a.append(true);
        a.values().append_slice(&[3, 4, 5]);
        a.append(true);
        let a: ArrayRef = Arc::new(a.finish().slice(1, 1));
        let in_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            a.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(in_schema.clone(), vec![a]).unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            in_schema.field(0).clone(),
            Field::new("u", ArrowDataType::Int64, true),
        ]));
        let out = project_set_batch(&batch, &[(SrfKind::Unnest, col(0))], &out_schema).unwrap();
        let u = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(u.iter().collect::<Vec<_>>(), [Some(3), Some(4), Some(5)]);
    }
}
