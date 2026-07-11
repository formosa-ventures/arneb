//! Set operation physical operators: UNION ALL, DISTINCT, INTERSECT, EXCEPT.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{
    collect_stream, stream_from_batches, RecordBatchStream, SendableRecordBatchStream,
};
use arneb_common::types::ColumnInfo;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use async_trait::async_trait;

use crate::fast_hash::{FastHashSet, FastHasher};
use crate::operator::ExecutionPlan;

fn build_schema(cols: &[ColumnInfo]) -> Arc<Schema> {
    let fields: Vec<arrow::datatypes::Field> = cols
        .iter()
        .map(|c| arrow::datatypes::Field::new(&c.name, c.data_type.clone().into(), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

fn hash_row(batch: &RecordBatch, row: usize) -> u64 {
    let mut hasher = FastHasher::default();
    for col in 0..batch.num_columns() {
        let s =
            arrow::util::display::array_value_to_string(batch.column(col), row).unwrap_or_default();
        s.hash(&mut hasher);
    }
    hasher.finish()
}

// -- UNION ALL --

#[derive(Debug)]
pub(crate) struct UnionAllExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl UnionAllExec {
    pub(crate) fn new(children: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        Self { children }
    }
}

impl fmt::Display for UnionAllExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnionAllExec")
    }
}

#[async_trait]
impl ExecutionPlan for UnionAllExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        if self.children.is_empty() {
            vec![]
        } else {
            self.children[0].schema()
        }
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = build_schema(&self.schema());
        let children = self.children.clone();
        // Stream the children SEQUENTIALLY — drain child[0] fully, then
        // child[1], … — yielding each batch as it arrives. UNION ALL is
        // order-independent concatenation, so this preserves every row.
        //
        // Critically it never materializes: the previous implementation
        // `collect_stream`'d every child into one `all_batches` Vec before
        // emitting a single row. As the GATHER node above the partitioned
        // exchanges in a distributed join, that buffered the ENTIRE input
        // (q18 SF30: ~90M-row, ~10 GB) untracked by the MemoryPool → the
        // worker RSS blew the cgroup (Exit 137). A jemalloc heap profile
        // pinned this at 95.7% of the peak; see
        // project_2026-06-08_q18_oom_heapprofile_rootcause. No back-pressure
        // deadlock risk: the admission semaphore is gone (Phase A) and this
        // forwards batches holding no reservation/lock across an `.await`.
        let stream = async_stream::try_stream! {
            for child in children {
                let mut s = child.execute(0).await?;
                while let Some(batch_res) = futures::StreamExt::next(&mut s).await {
                    let batch = batch_res.map_err(|e| {
                        ExecutionError::InvalidOperation(format!("union all input: {e}"))
                    })?;
                    yield batch;
                }
            }
        };
        let out: SendableRecordBatchStream = Box::pin(UnionAllStream {
            schema,
            inner: Box::pin(stream),
        });
        Ok(out)
    }

    fn display_name(&self) -> &str {
        "UnionAllExec"
    }
}

/// `RecordBatchStream` wrapper for [`UnionAllExec`]'s lazy, sequential
/// child-chaining stream. Mirrors `LimitOutputStream` — maps the inner
/// `ExecutionError` items to `ArnebError` at the stream boundary.
struct UnionAllStream {
    schema: Arc<Schema>,
    inner: Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, ExecutionError>> + Send>>,
}

impl futures::Stream for UnionAllStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(arneb_common::error::ArnebError::Execution)))
    }
}

impl RecordBatchStream for UnionAllStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// -- DISTINCT --

#[derive(Debug)]
pub(crate) struct DistinctExec {
    child: Arc<dyn ExecutionPlan>,
}

impl DistinctExec {
    pub(crate) fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

impl fmt::Display for DistinctExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DistinctExec")
    }
}

#[async_trait]
impl ExecutionPlan for DistinctExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.child.schema()
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.child.execute(0).await?;
        let batches = collect_stream(stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("distinct collect: {e}")))?;

        let output_schema = build_schema(&self.child.schema());
        let mut seen: FastHashSet<u64> = FastHashSet::default();
        let mut result_batches = Vec::new();

        for batch in &batches {
            let mut indices = Vec::new();
            for row in 0..batch.num_rows() {
                let h = hash_row(batch, row);
                if seen.insert(h) {
                    indices.push(row as u32);
                }
            }
            if !indices.is_empty() {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col| {
                        arrow::compute::take(batch.column(col), &idx_array, None)
                            .map_err(|e| ExecutionError::InvalidOperation(format!("take: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                result_batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        "DistinctExec"
    }
}

// -- INTERSECT --

#[derive(Debug)]
pub(crate) struct IntersectExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
}

impl IntersectExec {
    pub(crate) fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self {
        Self { left, right }
    }
}

impl fmt::Display for IntersectExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntersectExec")
    }
}

#[async_trait]
impl ExecutionPlan for IntersectExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.left.schema()
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let right_stream = self.right.execute(0).await?;
        let right_batches = collect_stream(right_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("intersect right: {e}")))?;

        let mut right_set: FastHashSet<u64> = FastHashSet::default();
        for batch in &right_batches {
            for row in 0..batch.num_rows() {
                right_set.insert(hash_row(batch, row));
            }
        }

        let left_stream = self.left.execute(0).await?;
        let left_batches = collect_stream(left_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("intersect left: {e}")))?;

        let output_schema = build_schema(&self.left.schema());
        let mut seen: FastHashSet<u64> = FastHashSet::default();
        let mut result_batches = Vec::new();

        for batch in &left_batches {
            let mut indices = Vec::new();
            for row in 0..batch.num_rows() {
                let h = hash_row(batch, row);
                if right_set.contains(&h) && seen.insert(h) {
                    indices.push(row as u32);
                }
            }
            if !indices.is_empty() {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col| {
                        arrow::compute::take(batch.column(col), &idx_array, None)
                            .map_err(|e| ExecutionError::InvalidOperation(format!("take: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                result_batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        "IntersectExec"
    }
}

// -- EXCEPT --

#[derive(Debug)]
pub(crate) struct ExceptExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
}

impl ExceptExec {
    pub(crate) fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self {
        Self { left, right }
    }
}

impl fmt::Display for ExceptExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExceptExec")
    }
}

#[async_trait]
impl ExecutionPlan for ExceptExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.left.schema()
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let right_stream = self.right.execute(0).await?;
        let right_batches = collect_stream(right_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("except right: {e}")))?;

        let mut right_set: FastHashSet<u64> = FastHashSet::default();
        for batch in &right_batches {
            for row in 0..batch.num_rows() {
                right_set.insert(hash_row(batch, row));
            }
        }

        let left_stream = self.left.execute(0).await?;
        let left_batches = collect_stream(left_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("except left: {e}")))?;

        let output_schema = build_schema(&self.left.schema());
        let mut seen: FastHashSet<u64> = FastHashSet::default();
        let mut result_batches = Vec::new();

        for batch in &left_batches {
            let mut indices = Vec::new();
            for row in 0..batch.num_rows() {
                let h = hash_row(batch, row);
                if !right_set.contains(&h) && seen.insert(h) {
                    indices.push(row as u32);
                }
            }
            if !indices.is_empty() {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col| {
                        arrow::compute::take(batch.column(col), &idx_array, None)
                            .map_err(|e| ExecutionError::InvalidOperation(format!("take: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                result_batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        "ExceptExec"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field};
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock leaf that records how many times `execute` is called, so a
    /// test can assert UnionAllExec is lazy (children executed on demand,
    /// not all up front) — the property that keeps it from materializing
    /// the whole input. See project_2026-06-08_q18_oom_heapprofile_rootcause.
    #[derive(Debug)]
    struct CountingSource {
        batches: Vec<RecordBatch>,
        execute_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ExecutionPlan for CountingSource {
        fn schema(&self) -> Vec<ColumnInfo> {
            vec![ColumnInfo {
                name: "v".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }]
        }

        async fn execute(
            &self,
            _partition: usize,
        ) -> Result<SendableRecordBatchStream, ExecutionError> {
            self.execute_count.fetch_add(1, Ordering::SeqCst);
            Ok(stream_from_batches(
                build_schema(&self.schema()),
                self.batches.clone(),
            ))
        }

        fn display_name(&self) -> &str {
            "CountingSource"
        }
    }

    fn int_batch(vals: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals.to_vec()))]).unwrap()
    }

    fn col_values(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[tokio::test]
    async fn union_all_streams_children_lazily_without_materializing() {
        let c0 = Arc::new(AtomicUsize::new(0));
        let c1 = Arc::new(AtomicUsize::new(0));
        let src0 = Arc::new(CountingSource {
            batches: vec![int_batch(&[1, 2]), int_batch(&[3])],
            execute_count: c0.clone(),
        });
        let src1 = Arc::new(CountingSource {
            batches: vec![int_batch(&[4, 5])],
            execute_count: c1.clone(),
        });
        let union = UnionAllExec::new(vec![
            src0 as Arc<dyn ExecutionPlan>,
            src1 as Arc<dyn ExecutionPlan>,
        ]);

        let mut stream = union.execute(0).await.unwrap();
        // Lazy: returning the stream must NOT have executed any child (the
        // old collect_stream impl executed + buffered ALL children here).
        assert_eq!(c0.load(Ordering::SeqCst), 0);
        assert_eq!(c1.load(Ordering::SeqCst), 0);

        // First pull executes only child[0].
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(col_values(&b), vec![1, 2]);
        assert_eq!(c0.load(Ordering::SeqCst), 1);
        assert_eq!(
            c1.load(Ordering::SeqCst),
            0,
            "child[1] must not execute until child[0] is fully drained"
        );

        // Drain child[0]'s remaining batch; child[1] still not executed.
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(col_values(&b), vec![3]);
        assert_eq!(c1.load(Ordering::SeqCst), 0);

        // Crossing into child[1] executes it exactly once.
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(col_values(&b), vec![4, 5]);
        assert_eq!(c1.load(Ordering::SeqCst), 1);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn union_all_concatenates_all_child_rows_in_order() {
        let src0 = Arc::new(CountingSource {
            batches: vec![int_batch(&[10, 20])],
            execute_count: Arc::new(AtomicUsize::new(0)),
        });
        let src1 = Arc::new(CountingSource {
            batches: vec![int_batch(&[30]), int_batch(&[40, 50])],
            execute_count: Arc::new(AtomicUsize::new(0)),
        });
        let union = UnionAllExec::new(vec![
            src0 as Arc<dyn ExecutionPlan>,
            src1 as Arc<dyn ExecutionPlan>,
        ]);

        let stream = union.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let all: Vec<i32> = batches.iter().flat_map(col_values).collect();
        assert_eq!(all, vec![10, 20, 30, 40, 50]);
    }
}
