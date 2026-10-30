//! Scans read only the columns their query uses, also when the query
//! cross-joins subqueries that each contain a join. TPC-DS q88 cross-joins
//! eight such subqueries; measured at SF1, every one of its 32 scans read all
//! of its table's columns (store_sales: 23 instead of 3), and q88 took 2.6 to
//! 3.3 s and 9.6 to 10.1 s of CPU. The same eight subqueries run one by one
//! read 3 columns each and took 0.42 to 0.50 s and 1.0 s of CPU together.
//!
//! Every scan records the columns it is asked for.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use async_trait::async_trait;

use arneb_catalog::CatalogManager;
use arneb_common::error::{ConnectorError, ExecutionError};
use arneb_common::stream::SendableRecordBatchStream;
use arneb_common::types::{ColumnInfo, DataType, TableReference};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::{ConnectorFactory, ConnectorRegistry};
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use arneb_execution::{DataSource, ScanContext};

/// Column names each scan of a table was asked for (all of them when the
/// scan got no projection).
type ScanLog = Arc<Mutex<Vec<(String, Vec<String>)>>>;

#[derive(Debug)]
struct RecordingSource {
    table: String,
    inner: Arc<dyn DataSource>,
    log: ScanLog,
}

#[async_trait]
impl DataSource for RecordingSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.inner.schema()
    }

    fn partition_count(&self) -> usize {
        self.inner.partition_count()
    }

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = self.inner.schema();
        let columns = match &ctx.projection {
            Some(indices) => indices.iter().map(|&i| schema[i].name.clone()).collect(),
            None => schema.iter().map(|c| c.name.clone()).collect(),
        };
        self.log.lock().unwrap().push((self.table.clone(), columns));
        self.inner.scan(ctx, partition).await
    }
}

#[derive(Debug)]
struct RecordingFactory {
    inner: MemoryConnectorFactory,
    log: ScanLog,
}

#[async_trait]
impl ConnectorFactory for RecordingFactory {
    fn name(&self) -> &str {
        "memory"
    }

    async fn create_data_source(
        &self,
        table: &TableReference,
        schema: &[ColumnInfo],
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        let inner = self
            .inner
            .create_data_source(table, schema, properties)
            .await?;
        Ok(Arc::new(RecordingSource {
            table: table.table.clone(),
            inner,
            log: Arc::clone(&self.log),
        }))
    }
}

fn int_col(name: &str, values: Vec<i32>) -> (Field, ArrayRef, ColumnInfo) {
    (
        Field::new(name, ArrowDataType::Int32, true),
        Arc::new(Int32Array::from(values)),
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: true,
        },
    )
}

fn str_col(name: &str, values: Vec<&str>) -> (Field, ArrayRef, ColumnInfo) {
    (
        Field::new(name, ArrowDataType::Utf8, true),
        Arc::new(StringArray::from(values)),
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: true,
        },
    )
}

fn table(cols: Vec<(Field, ArrayRef, ColumnInfo)>) -> Arc<MemoryTable> {
    let fields: Vec<Field> = cols.iter().map(|c| c.0.clone()).collect();
    let arrays: Vec<ArrayRef> = cols.iter().map(|c| c.1.clone()).collect();
    let info: Vec<ColumnInfo> = cols.iter().map(|c| c.2.clone()).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    Arc::new(MemoryTable::new(info, vec![batch]))
}

/// - `f(fk, fa, fb, fc)`: fk = 1, 2, 2, 3; fa, fb, fc are never read.
/// - `d(dk, dname, dx)`: dk = 1, 2, 3 named 'a', 'b', 'c'; dx is never read.
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>, ScanLog) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "f",
        table(vec![
            int_col("fk", vec![1, 2, 2, 3]),
            int_col("fa", vec![10, 20, 30, 40]),
            int_col("fb", vec![0, 0, 0, 0]),
            int_col("fc", vec![7, 7, 7, 7]),
        ]),
    );
    schema.register_table(
        "d",
        table(vec![
            int_col("dk", vec![1, 2, 3]),
            str_col("dname", vec!["a", "b", "c"]),
            int_col("dx", vec![5, 5, 5]),
        ]),
    );
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let log: ScanLog = Arc::new(Mutex::new(Vec::new()));
    let factory = RecordingFactory {
        inner: MemoryConnectorFactory::new(catalog.clone(), "default"),
        log: Arc::clone(&log),
    };
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry), log)
}

/// Run `sql`; return its rows as text and the columns every scan read.
async fn run(sql: &str) -> (Vec<Vec<String>>, Vec<(String, Vec<String>)>) {
    let (cm, cr, log) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            out.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(r) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(c, r).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    let scans = log.lock().unwrap().clone();
    (out, scans)
}

/// Every scan of `f` reads only `fk`, every scan of `d` only `dk` and `dname`.
fn assert_only_used_columns(sql: &str, scans: &[(String, Vec<String>)]) {
    assert!(!scans.is_empty(), "no scans recorded: {sql}");
    for (table, columns) in scans {
        let allowed: &[&str] = match table.as_str() {
            "f" => &["fk"],
            "d" => &["dk", "dname"],
            other => panic!("unexpected table {other}"),
        };
        let extra: Vec<&String> = columns
            .iter()
            .filter(|c| !allowed.contains(&c.as_str()))
            .collect();
        assert!(
            extra.is_empty(),
            "scan of {table} read unused columns {extra:?} (read {columns:?}): {sql}"
        );
    }
}

const SUB_B: &str = "SELECT count(*) nb FROM f, d WHERE fk = dk AND dname = 'b'";
const SUB_C: &str = "SELECT count(*) nc FROM f, d WHERE fk = dk AND dname = 'c'";

#[tokio::test]
async fn one_subquery_with_a_join_reads_only_used_columns() {
    // Control: the same join without a cross join around it.
    let (rows, scans) = run(SUB_B).await;
    assert_eq!(rows, vec![vec!["2".to_string()]]);
    assert_only_used_columns(SUB_B, &scans);
}

#[tokio::test]
async fn cross_joined_subqueries_with_joins_read_only_used_columns() {
    let sql = format!("SELECT * FROM ({SUB_B}) x, ({SUB_C}) y");
    let (rows, scans) = run(&sql).await;
    assert_eq!(rows, vec![vec!["2".to_string(), "1".to_string()]]);
    assert_eq!(scans.len(), 4, "two scans per subquery: {scans:?}");
    assert_only_used_columns(&sql, &scans);
}

#[tokio::test]
async fn explicit_cross_join_of_subqueries_with_joins_reads_only_used_columns() {
    let sql = format!("SELECT nb + nc FROM ({SUB_B}) x CROSS JOIN ({SUB_C}) y");
    let (rows, scans) = run(&sql).await;
    assert_eq!(rows, vec![vec!["3".to_string()]]);
    assert_only_used_columns(&sql, &scans);
}
