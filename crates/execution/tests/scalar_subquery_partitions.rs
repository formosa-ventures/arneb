//! A scalar subquery reads every partition of its input. A Hive table spread
//! over several files is a multi-partition source; the one row a subquery
//! such as `(SELECT d_week_seq FROM date_dim WHERE d_date = ...)` returns
//! (TPC-DS q14b, q58) can sit in any partition.

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{collect_stream, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
use arneb_execution::{DataSource, ExecutionContext, InMemoryDataSource, ScanContext};
use arneb_planner::{LogicalPlan, PlanExpr};

/// One in-memory source per partition.
#[derive(Debug)]
struct Partitioned {
    schema: Vec<ColumnInfo>,
    parts: Vec<InMemoryDataSource>,
}

#[async_trait]
impl DataSource for Partitioned {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }

    fn partition_count(&self) -> usize {
        self.parts.len()
    }

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        self.parts[partition].scan(ctx, 0).await
    }
}

fn schema() -> Vec<ColumnInfo> {
    vec![ColumnInfo {
        name: "val".into(),
        data_type: DataType::Int64,
        nullable: false,
    }]
}

/// `partitions[i]` holds the rows of partition `i`.
fn context(partitions: &[&[i64]]) -> ExecutionContext {
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "val",
        ArrowDataType::Int64,
        false,
    )]));
    let parts = partitions
        .iter()
        .map(|rows| {
            let batches = if rows.is_empty() {
                vec![]
            } else {
                vec![RecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![Arc::new(Int64Array::from(rows.to_vec()))],
                )
                .unwrap()]
            };
            InMemoryDataSource::new(schema(), batches)
        })
        .collect();
    let mut ctx = ExecutionContext::new();
    ctx.register_data_source(
        "parted",
        Arc::new(Partitioned {
            schema: schema(),
            parts,
        }),
    );
    ctx
}

fn scan() -> LogicalPlan {
    LogicalPlan::TableScan {
        table: TableReference::table("parted"),
        schema: schema(),
        alias: None,
        properties: Default::default(),
        dynamic_filters_consumed: Vec::new(),
    }
}

/// The value of `(subquery)` as resolved inside an expression.
async fn resolved(partitions: &[&[i64]]) -> Result<PlanExpr, ExecutionError> {
    let expr = PlanExpr::ScalarSubquery {
        subplan: Box::new(scan()),
        span: None,
    };
    context(partitions).resolve_scalar_subqueries(&expr).await
}

/// The value of a `ScalarSubquery` plan node; `Err` text on failure.
async fn executed(partitions: &[&[i64]]) -> Result<Option<i64>, String> {
    let plan = LogicalPlan::ScalarSubquery {
        subplan: Box::new(scan()),
    };
    let ctx = context(partitions);
    let exec = ctx.create_physical_plan(&plan).map_err(|e| e.to_string())?;
    let stream = exec.execute(0).await.map_err(|e| e.to_string())?;
    let batches = collect_stream(stream).await.map_err(|e| e.to_string())?;
    let col = batches[0].column(0);
    if col.is_null(0) {
        return Ok(None);
    }
    Ok(Some(
        col.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
    ))
}

fn literal_value(expr: PlanExpr) -> ScalarValue {
    match expr {
        PlanExpr::Literal { value, .. } => value,
        other => panic!("expected a literal, got {other:?}"),
    }
}

#[tokio::test]
async fn expression_subquery_finds_the_row_in_a_later_partition() {
    let value = literal_value(resolved(&[&[], &[42]]).await.unwrap());
    assert_eq!(value, ScalarValue::Int64(42));
}

#[tokio::test]
async fn expression_subquery_rejects_one_row_in_each_of_two_partitions() {
    assert!(resolved(&[&[1], &[2]]).await.is_err());
}

#[tokio::test]
async fn expression_subquery_over_empty_partitions_is_null() {
    let value = literal_value(resolved(&[&[], &[]]).await.unwrap());
    assert_eq!(value, ScalarValue::Null);
}

#[tokio::test]
async fn plan_node_finds_the_row_in_a_later_partition() {
    assert_eq!(executed(&[&[], &[42]]).await, Ok(Some(42)));
}

#[tokio::test]
async fn plan_node_rejects_one_row_in_each_of_two_partitions() {
    assert!(executed(&[&[1], &[2]]).await.is_err());
}

#[tokio::test]
async fn plan_node_over_empty_partitions_is_null() {
    assert_eq!(executed(&[&[], &[]]).await, Ok(None));
}
