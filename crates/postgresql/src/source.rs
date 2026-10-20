//! Execution side: one scan partition = one `COPY (SELECT …) TO STDOUT`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arneb_common::error::{ArnebError, ConnectorError, ExecutionError};
use arneb_common::memory_pool::{MemoryConsumer, MemoryReservation};
use arneb_common::stream::{RecordBatchStream, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_connectors::ConnectorFactory;
use arneb_execution::{column_info_to_arrow_schema, DataSource, ScanContext};
use arneb_rdbms::{
    build_select, translate_filters_with, RemoteTable, SplitPlan, TranslateOptions, Translation,
};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::{BoxStream, Stream, StreamExt};
use tokio_postgres::CopyOutStream;

use crate::catalog::{pool_error, CatalogState};
use crate::copy::{CopyDecoder, Kind};
use crate::{pg_error, PostgresDialect};

const DEFAULT_BATCH_SIZE: usize = 8192;

/// Creates [`PostgresDataSource`]s from the table properties the catalog
/// wrote at planning time.
#[derive(Debug, Clone)]
pub struct PostgresConnectorFactory {
    state: Arc<CatalogState>,
}

impl PostgresConnectorFactory {
    pub(crate) fn new(state: Arc<CatalogState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ConnectorFactory for PostgresConnectorFactory {
    fn name(&self) -> &str {
        "postgresql"
    }

    async fn create_data_source(
        &self,
        table: &TableReference,
        schema: &[ColumnInfo],
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        let err = |e: String| ConnectorError::ReadError(format!("postgresql table '{table}': {e}"));
        let remote = RemoteTable::from_properties(properties).map_err(err)?;
        let split = SplitPlan::from_properties(properties).map_err(err)?;
        if schema.len() != remote.columns.len() {
            return Err(err(format!(
                "plan schema has {} columns, remote table has {}",
                schema.len(),
                remote.columns.len()
            )));
        }
        Ok(Arc::new(PostgresDataSource {
            state: self.state.clone(),
            schema: schema.to_vec(),
            remote,
            split,
        }))
    }
}

/// One partition's `COPY` statement, output columns, per-column
/// (name, decoder), and the filter translation it was built from.
type PartitionPlan = (String, Vec<ColumnInfo>, Vec<(String, Kind)>, Translation);

/// A PostgreSQL table (or view) as a partitioned data source.
#[derive(Debug)]
pub struct PostgresDataSource {
    state: Arc<CatalogState>,
    schema: Vec<ColumnInfo>,
    remote: RemoteTable,
    split: SplitPlan,
}

impl PostgresDataSource {
    /// The `COPY` statement partition `partition` runs for `ctx`, plus the
    /// output columns' names and decoders.
    fn plan_partition(&self, ctx: &ScanContext, partition: usize) -> Result<PartitionPlan, String> {
        let all: Vec<usize>;
        let indices: &[usize] = match &ctx.projection {
            Some(p) => p,
            None => {
                all = (0..self.schema.len()).collect();
                &all
            }
        };
        let mut out_schema = Vec::with_capacity(indices.len());
        let mut kinds = Vec::with_capacity(indices.len());
        for &i in indices {
            let info = self
                .schema
                .get(i)
                .ok_or_else(|| format!("projection index {i} out of range"))?;
            let remote = &self.remote.columns[i];
            kinds.push((
                info.name.clone(),
                Kind::for_column(&info.name, &remote.remote_type, &info.data_type)?,
            ));
            out_schema.push(info.clone());
        }
        let translation = translate_filters_with(
            &ctx.filters,
            &self.schema,
            &self.remote,
            &PostgresDialect,
            TranslateOptions {
                dynamic_from: ctx.dynamic_filters_start,
                compaction_threshold: self.state.config.domain_compaction_threshold,
            },
        );
        let select = build_select(
            &self.remote,
            Some(indices),
            &translation,
            self.split.range(partition).as_ref(),
            ctx.limit,
            &PostgresDialect,
        )?;
        Ok((
            format!("COPY ({select}) TO STDOUT (FORMAT binary)"),
            out_schema,
            kinds,
            translation,
        ))
    }
}

#[async_trait]
impl DataSource for PostgresDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }

    fn partition_count(&self) -> usize {
        self.split.partition_count()
    }

    fn pushdown_stacked_filters(&self) -> bool {
        true
    }

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let table = format!("{}.{}", self.remote.schema, self.remote.name);
        let fail = |e: String| {
            ExecutionError::InvalidOperation(format!(
                "postgresql catalog '{}', table '{table}': {e}",
                self.state.name
            ))
        };
        if partition >= self.partition_count() {
            return Err(fail(format!(
                "partition {partition} out of range ({} partitions)",
                self.partition_count()
            )));
        }
        let (copy_sql, out_schema, kinds, translation) =
            self.plan_partition(ctx, partition).map_err(fail)?;
        tracing::debug!(
            target: "arneb::rdbms",
            catalog = %self.state.name,
            table = %table,
            partition,
            dynamic_pushed = translation.dynamic_pushed,
            dynamic_compacted = translation.dynamic_compacted,
            dynamic_skipped = translation.dynamic_skipped,
            sql = %copy_sql,
            "remote query"
        );
        let schema = column_info_to_arrow_schema(&out_schema);
        let batch_size = ctx.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
        let scan = ScanState {
            state: self.state.clone(),
            context: format!("postgresql catalog '{}', table '{table}'", self.state.name),
            copy_sql,
            decoder: CopyDecoder::new(schema.clone(), kinds, batch_size.min(DEFAULT_BATCH_SIZE)),
            batch_size,
            copy: None,
            conn: None,
            reservation: MemoryConsumer::new("PostgresScan").register(ctx.memory_pool.clone()),
            done: false,
        };
        let inner = futures::stream::try_unfold(scan, ScanState::next).boxed();
        Ok(Box::pin(PostgresStream { schema, inner }))
    }
}

/// A pooled connection that is returned to the pool only after its COPY
/// finished; dropped mid-COPY it is detached and closed (with a cancel
/// request), since a connection with an unfinished COPY cannot be reused.
struct Connection {
    object: Option<deadpool_postgres::Object>,
    completed: bool,
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(object) = self.object.take() {
            let token = object.cancel_token();
            let client = deadpool_postgres::Object::take(object);
            drop(client);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = token.cancel_query(tokio_postgres::NoTls).await;
                });
            }
        }
    }
}

struct ScanState {
    state: Arc<CatalogState>,
    context: String,
    copy_sql: String,
    decoder: CopyDecoder,
    batch_size: usize,
    copy: Option<Pin<Box<CopyOutStream>>>,
    conn: Option<Connection>,
    reservation: MemoryReservation,
    done: bool,
}

impl ScanState {
    fn error(&self, e: String) -> ArnebError {
        ExecutionError::InvalidOperation(format!("{}: {e}", self.context)).into()
    }

    async fn next(mut self) -> Result<Option<(RecordBatch, Self)>, ArnebError> {
        if self.done {
            return Ok(None);
        }
        if self.copy.is_none() {
            let object = self
                .state
                .pool
                .get()
                .await
                .map_err(|e| self.error(pool_error(&e)))?;
            let copy = object
                .copy_out(self.copy_sql.as_str())
                .await
                .map_err(|e| self.error(pg_error(&e)))?;
            self.conn = Some(Connection {
                object: Some(object),
                completed: false,
            });
            self.copy = Some(Box::pin(copy));
        }
        loop {
            self.decoder
                .decode(self.batch_size)
                .map_err(|e| self.error(e))?;
            if self.decoder.rows() >= self.batch_size {
                let batch = self.decoder.take_batch().map_err(|e| self.error(e))?;
                self.account(&batch);
                return Ok(Some((batch, self)));
            }
            if self.decoder.is_finished() {
                // Drain to CommandComplete so the connection is reusable.
                let copy = self.copy.as_mut().expect("copy started");
                let mut drained = Ok(());
                while let Some(chunk) = copy.next().await {
                    if let Err(e) = chunk {
                        drained = Err(pg_error(&e));
                        break;
                    }
                }
                drained.map_err(|e| self.error(e))?;
                if let Some(conn) = self.conn.as_mut() {
                    conn.completed = true;
                }
                self.conn = None;
                self.done = true;
                self.reservation.free();
                if self.decoder.rows() == 0 {
                    return Ok(None);
                }
                let batch = self.decoder.take_batch().map_err(|e| self.error(e))?;
                return Ok(Some((batch, self)));
            }
            let copy = self.copy.as_mut().expect("copy started");
            match copy.next().await {
                Some(Ok(chunk)) => self.decoder.extend(&chunk),
                Some(Err(e)) => {
                    let e = pg_error(&e);
                    return Err(self.error(e));
                }
                None => {
                    self.decoder.check_complete().map_err(|e| self.error(e))?;
                }
            }
        }
    }

    /// Track the decode buffer at the size of the batch just produced (the
    /// builders refill to about that size). Infallible, like the Parquet
    /// scan: a scan cannot spill.
    fn account(&mut self, batch: &RecordBatch) {
        let bytes = batch.get_array_memory_size();
        let held = self.reservation.size();
        if bytes > held {
            self.reservation.grow(bytes - held);
        } else {
            self.reservation.shrink(held - bytes);
        }
    }
}

struct PostgresStream {
    schema: SchemaRef,
    inner: BoxStream<'static, Result<RecordBatch, ArnebError>>,
}

impl Stream for PostgresStream {
    type Item = Result<RecordBatch, ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for PostgresStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
