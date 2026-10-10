//! Scan tests for ORC tables, partitioned tables and the unsupported
//! layouts (ACID, other formats), through [`HiveConnectorFactory`] the way
//! the planner drives it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use arrow::util::display::array_value_to_string;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use arneb_common::stream::collect_stream;
use arneb_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
use arneb_connectors::storage::StorageRegistry;
use arneb_connectors::ConnectorFactory;
use arneb_execution::{DataSource, ScanContext};
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast::BinaryOp;

use crate::catalog::HiveTableProvider;
use crate::datasource::{props, HiveConnectorFactory, HivePartition};
use arneb_catalog::TableProvider;

const ORC_INPUT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
const ORC_SERDE: &str = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
const PARQUET_INPUT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

fn col(name: &str, data_type: DataType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
    }
}

/// Encode batches as one ORC file, one stripe per batch.
fn orc_bytes(batches: &[RecordBatch]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = orc_rust::ArrowWriterBuilder::new(&mut buf, batches[0].schema())
        .try_build()
        .unwrap();
    for b in batches {
        writer.write(b).unwrap();
        writer.flush_stripe().unwrap();
    }
    writer.close().unwrap();
    buf
}

fn parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
    buf
}

/// File columns `ID` (int32), `Name` (string), `day` (date), `ts`
/// (timestamp) — mixed case on purpose.
fn people(ids: &[i32]) -> RecordBatch {
    let names: Vec<Option<String>> = ids
        .iter()
        .map(|i| (i % 3 != 0).then(|| format!("n{i}")))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ID", ArrowDataType::Int32, true),
            Field::new("Name", ArrowDataType::Utf8, true),
            Field::new("day", ArrowDataType::Date32, true),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ])),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names)),
            Arc::new(Date32Array::from(
                ids.iter().map(|&i| Some(19_000 + i)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                ids.iter()
                    .map(|&i| Some(1_700_000_000_000_000 + i as i64 * 1_000))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Table schema for `people` files, with `id` widened to BIGINT.
fn people_columns() -> Vec<ColumnInfo> {
    vec![
        col("id", DataType::Int64),
        col("name", DataType::Utf8),
        col("day", DataType::Date32),
        col(
            "ts",
            DataType::Timestamp {
                unit: arneb_common::types::TimeUnit::Microsecond,
                timezone: None,
            },
        ),
    ]
}

struct Table {
    /// `s3://lake`, where the tables live.
    store: Arc<dyn ObjectStore>,
    /// `s3://other`, for partitions stored outside the table's bucket.
    other: Arc<dyn ObjectStore>,
    factory: HiveConnectorFactory,
}

impl Table {
    fn new() -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let other: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("s3://lake", store.clone());
        registry.register_store("s3://other", other.clone());
        Self {
            store,
            other,
            factory: HiveConnectorFactory::new(registry),
        }
    }

    async fn put(&self, path: &str, bytes: Vec<u8>) {
        put(&self.store, path, bytes).await;
    }

    /// Create the data source the way the planner does: schema and
    /// properties from a `HiveTableProvider`.
    async fn open(&self, name: &str, provider: HiveTableProvider) -> Arc<dyn DataSource> {
        self.try_open(name, provider).await.unwrap()
    }

    async fn try_open(
        &self,
        name: &str,
        provider: HiveTableProvider,
    ) -> Result<Arc<dyn DataSource>, String> {
        self.factory
            .create_data_source(
                &TableReference::table(name),
                &provider.schema(),
                &provider.properties(),
            )
            .await
            .map_err(|e| e.to_string())
    }
}

async fn put(store: &Arc<dyn ObjectStore>, path: &str, bytes: Vec<u8>) {
    store
        .put(&ObjectPath::parse(path).unwrap(), PutPayload::from(bytes))
        .await
        .unwrap();
}

fn partition(values: &[&str], location: &str) -> HivePartition {
    HivePartition {
        values: values.iter().map(|v| v.to_string()).collect(),
        location: location.to_string(),
    }
}

fn orc_table(location: &str, columns: Vec<ColumnInfo>) -> HiveTableProvider {
    HiveTableProvider::new(columns, location.to_string(), ORC_INPUT.to_string())
        .with_serde_lib(ORC_SERDE)
}

/// Every row of every scan partition, cells rendered as text (`NULL` for
/// nulls), sorted.
async fn try_scan_rows(ds: &Arc<dyn DataSource>, ctx: ScanContext) -> Result<Vec<String>, String> {
    let mut rows = Vec::new();
    for p in 0..ds.partition_count() {
        let stream = ds.scan(&ctx, p).await.map_err(|e| e.to_string())?;
        let batches = collect_stream(stream).await.map_err(|e| e.to_string())?;
        for b in &batches {
            for r in 0..b.num_rows() {
                let cells: Vec<String> = b
                    .columns()
                    .iter()
                    .map(|c: &ArrayRef| {
                        if c.is_null(r) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(c, r).unwrap()
                        }
                    })
                    .collect();
                rows.push(cells.join("|"));
            }
        }
    }
    rows.sort();
    Ok(rows)
}

async fn scan_rows(ds: &Arc<dyn DataSource>, ctx: ScanContext) -> Vec<String> {
    try_scan_rows(ds, ctx).await.unwrap()
}

fn eq(index: usize, name: &str, value: ScalarValue) -> PlanExpr {
    PlanExpr::BinaryOp {
        left: Box::new(PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        }),
        op: BinaryOp::Eq,
        right: Box::new(PlanExpr::Literal { value, span: None }),
        span: None,
    }
}

#[tokio::test]
async fn orc_full_scan_maps_columns_case_insensitively_and_widens() {
    let t = Table::new();
    t.put("w/people/f0.orc", orc_bytes(&[people(&[1, 2, 3])]))
        .await;
    let ds = t
        .open("people", orc_table("s3://lake/w/people", people_columns()))
        .await;
    let rows = scan_rows(&ds, ScanContext::default()).await;
    assert_eq!(
        rows,
        vec![
            "1|n1|2022-01-09|2023-11-14T22:13:20.001",
            "2|n2|2022-01-10|2023-11-14T22:13:20.002",
            "3|NULL|2022-01-11|2023-11-14T22:13:20.003",
        ]
    );
    // BIGINT table column read from an int32 file column.
    let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
    let batches = collect_stream(stream).await.unwrap();
    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert!(b.column(0).as_any().downcast_ref::<Int64Array>().is_some());
}

#[tokio::test]
async fn orc_projection_in_requested_order_and_missing_column_is_null() {
    let t = Table::new();
    t.put("w/people/f0.orc", orc_bytes(&[people(&[1, 2])]))
        .await;
    let mut columns = people_columns();
    columns.push(col("added_later", DataType::Int32));
    let ds = t
        .open("people", orc_table("s3://lake/w/people", columns))
        .await;
    let rows = scan_rows(&ds, ScanContext::default().with_projection(vec![4, 1, 0])).await;
    assert_eq!(rows, vec!["NULL|n1|1", "NULL|n2|2"]);
    // Empty projection (count(*)) still yields the row count.
    let mut n = 0;
    for p in 0..ds.partition_count() {
        let s = ds
            .scan(&ScanContext::default().with_projection(vec![]), p)
            .await
            .unwrap();
        n += collect_stream(s)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
    }
    assert_eq!(n, 2);
}

#[tokio::test]
async fn orc_filters_are_left_to_the_filter_operator() {
    // The ORC scan does not evaluate pushed filters; it must not drop or
    // break on them (FilterExec above the scan applies them).
    let t = Table::new();
    t.put("w/people/f0.orc", orc_bytes(&[people(&[1, 2, 3])]))
        .await;
    let ds = t
        .open("people", orc_table("s3://lake/w/people", people_columns()))
        .await;
    let ctx = ScanContext::default()
        .with_projection(vec![0])
        .with_filters(vec![eq(0, "id", ScalarValue::Int64(2))]);
    assert_eq!(scan_rows(&ds, ctx).await, vec!["1", "2", "3"]);
}

#[tokio::test]
async fn orc_multi_stripe_files_split_without_loss_or_duplication() {
    // 7 stripes x 100 rows in one file, plus a 1-stripe file: whatever
    // splits_per_file this machine picks, every row is read exactly once.
    let t = Table::new();
    let stripes: Vec<RecordBatch> = (0..7)
        .map(|s| people(&((s * 100)..(s * 100 + 100)).collect::<Vec<_>>()))
        .collect();
    t.put("w/people/big.orc", orc_bytes(&stripes)).await;
    t.put("w/people/small.orc", orc_bytes(&[people(&[1000, 1001])]))
        .await;
    let ds = t
        .open("people", orc_table("s3://lake/w/people", people_columns()))
        .await;
    let rows = scan_rows(&ds, ScanContext::default().with_projection(vec![0])).await;
    let mut ids: Vec<i64> = rows.iter().map(|r| r.parse().unwrap()).collect();
    ids.sort_unstable();
    let mut expected: Vec<i64> = (0..700).collect();
    expected.extend([1000, 1001]);
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn orc_row_slices_longer_than_a_batch_are_read_exactly() {
    // One stripe of 5000 rows read as 3 row-range splits with 100-row
    // batches: each split must return exactly its slice (orc-rust would
    // otherwise read from the slice start to the end of the stripe).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = ObjectPath::from("f.orc");
    let ids: Vec<i32> = (0..5000).collect();
    store
        .put(&path, PutPayload::from(orc_bytes(&[people(&ids)])))
        .await
        .unwrap();
    let columns = people_columns();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        ArrowDataType::Int64,
        true,
    )]));
    let mut seen = Vec::new();
    for split_idx in 0..3 {
        let stream = crate::orc::scan(crate::orc::OrcScan {
            store: &store,
            path: &path,
            data_columns: &columns,
            projection: &[0],
            partition_values: &[],
            output_schema: schema.clone(),
            batch_size: 100,
            split_idx,
            splits_per_file: 3,
        })
        .await
        .unwrap();
        let mut split_ids = Vec::new();
        for b in collect_stream(stream).await.unwrap() {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            split_ids.extend(col.values().iter().copied());
        }
        assert_eq!(split_ids.len(), [1667, 1667, 1666][split_idx]);
        seen.extend(split_ids);
    }
    assert_eq!(seen, (0..5000).collect::<Vec<i64>>());
}

#[tokio::test]
async fn orc_incompatible_type_is_a_clear_error() {
    let t = Table::new();
    t.put("w/people/f0.orc", orc_bytes(&[people(&[1])])).await;
    let mut columns = people_columns();
    columns[1] = col("name", DataType::Int64);
    let ds = t
        .open("people", orc_table("s3://lake/w/people", columns))
        .await;
    let err = try_scan_rows(&ds, ScanContext::default())
        .await
        .unwrap_err();
    assert!(
        err.contains("column 'name' has ORC type") && err.contains("cannot be read"),
        "{err}"
    );
}

#[tokio::test]
async fn orc_positional_hive_column_names_map_by_position() {
    let t = Table::new();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("_col0", ArrowDataType::Int64, true),
            Field::new("_col1", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    t.put("w/old/f.orc", orc_bytes(&[batch])).await;
    let table = orc_table(
        "s3://lake/w/old",
        vec![col("k", DataType::Int64), col("v", DataType::Utf8)],
    );
    let ds = t.open("old", table).await;
    assert_eq!(scan_rows(&ds, ScanContext::default()).await, vec!["7|x"]);
}

#[tokio::test]
async fn orc_partitioned_table_reads_values_from_paths() {
    let t = Table::new();
    t.put(
        "w/pt/ds=2024-01-01/region=us/f.orc",
        orc_bytes(&[people(&[1])]),
    )
    .await;
    t.put(
        "w/pt/ds=2024-01-02/region=a%2Fb/f.orc",
        orc_bytes(&[people(&[2])]),
    )
    .await;
    t.put(
        "w/pt/ds=__HIVE_DEFAULT_PARTITION__/region=eu/f.orc",
        orc_bytes(&[people(&[3])]),
    )
    .await;
    // Hidden staging output must be ignored.
    t.put("w/pt/.trino-staging/ds=x/region=y/f.orc", vec![1, 2, 3])
        .await;
    let table = orc_table(
        "s3://lake/w/pt",
        people_columns().into_iter().take(2).collect(),
    )
    .with_partition_columns(vec![
        col("ds", DataType::Date32),
        col("REGION", DataType::Utf8),
    ]);
    let ds = t.open("pt", table).await;
    assert_eq!(ds.schema().len(), 4);
    let rows = scan_rows(&ds, ScanContext::default()).await;
    assert_eq!(
        rows,
        vec![
            "1|n1|2024-01-01|us",
            "2|n2|2024-01-02|a/b",
            "3|NULL|NULL|eu"
        ]
    );
    // Partition columns only, in a different order, with a partition filter.
    let ctx = ScanContext::default()
        .with_projection(vec![3, 0])
        .with_filters(vec![eq(3, "region", ScalarValue::Utf8("us".into()))]);
    assert_eq!(scan_rows(&ds, ctx).await, vec!["a/b|2", "eu|3", "us|1"]);
}

#[tokio::test]
async fn parquet_partitioned_table_reads_values_from_paths() {
    let t = Table::new();
    let batch = |id: i64| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "id",
                ArrowDataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![id]))],
        )
        .unwrap()
    };
    t.put("w/pq/p=1/a.parquet", parquet_bytes(&batch(10))).await;
    t.put("w/pq/p=2/b.parquet", parquet_bytes(&batch(20))).await;
    let table = HiveTableProvider::new(
        vec![col("id", DataType::Int64)],
        "s3://lake/w/pq".to_string(),
        PARQUET_INPUT.to_string(),
    )
    .with_partition_columns(vec![col("p", DataType::Int32)]);
    let ds = t.open("pq", table).await;
    assert_eq!(
        scan_rows(&ds, ScanContext::default()).await,
        vec!["10|1", "20|2"]
    );
    // A filter on the partition column is not pushed into Parquet.
    let ctx = ScanContext::default()
        .with_projection(vec![1, 0])
        .with_filters(vec![eq(1, "p", ScalarValue::Int32(2))]);
    assert_eq!(scan_rows(&ds, ctx).await, vec!["1|10", "2|20"]);
}

#[tokio::test]
async fn without_hms_partitions_files_outside_partition_directories_are_skipped() {
    // Directory discovery (no HMS partitions): a stray file at the table
    // root must not make the table unreadable; Trino ignores it.
    let t = Table::new();
    t.put("w/pt/stray.orc", orc_bytes(&[people(&[1])])).await;
    t.put("w/pt/ds=a/f.orc", orc_bytes(&[people(&[2])])).await;
    let table = orc_table(
        "s3://lake/w/pt",
        people_columns().into_iter().take(1).collect(),
    )
    .with_partition_columns(vec![col("ds", DataType::Utf8)]);
    let ds = t.open("pt", table).await;
    assert_eq!(scan_rows(&ds, ScanContext::default()).await, vec!["2|a"]);
}

/// A table partitioned by `ds` (varchar), one ORC file per directory.
async fn ds_partitioned(t: &Table, root: &str, dirs: &[(&str, i32)]) -> HiveTableProvider {
    for (dir, id) in dirs {
        t.put(&format!("{root}/{dir}/f.orc"), orc_bytes(&[people(&[*id])]))
            .await;
    }
    orc_table(
        &format!("s3://lake/{root}"),
        people_columns().into_iter().take(1).collect(),
    )
    .with_partition_columns(vec![col("ds", DataType::Utf8)])
}

#[tokio::test]
async fn hms_partitions_ignore_unregistered_directories_and_stray_files() {
    let t = Table::new();
    let table = ds_partitioned(&t, "w/hp", &[("ds=a", 1), ("ds=b", 2), ("ds=ghost", 3)]).await;
    // Files at the table root belong to no partition.
    t.put("w/hp/strayfile", orc_bytes(&[people(&[4])])).await;
    let table = table.with_partitions(vec![
        partition(&["a"], "s3://lake/w/hp/ds=a"),
        partition(&["b"], "s3://lake/w/hp/ds=b"),
    ]);
    let ds = t.open("hp", table).await;
    assert_eq!(
        scan_rows(&ds, ScanContext::default()).await,
        vec!["1|a", "2|b"]
    );
}

#[tokio::test]
async fn hms_partition_outside_the_table_directory_is_read() {
    let t = Table::new();
    let table = ds_partitioned(&t, "w/hp", &[("ds=a", 1)]).await;
    // `ALTER TABLE ... ADD PARTITION ... LOCATION` in another bucket, under
    // a directory name that says nothing about the partition.
    put(&t.other, "elsewhere/data/f.orc", orc_bytes(&[people(&[5])])).await;
    let table = table.with_partitions(vec![
        partition(&["a"], "s3://lake/w/hp/ds=a"),
        partition(&["z"], "s3://other/elsewhere/data"),
    ]);
    let ds = t.open("hp", table).await;
    assert_eq!(
        scan_rows(&ds, ScanContext::default()).await,
        vec!["1|a", "5|z"]
    );
}

#[tokio::test]
async fn hms_partition_without_location_uses_the_default_layout() {
    let t = Table::new();
    // Hive escapes '/' and ':' in partition directory names.
    t.put("w/dl/ds=x%2Fy%3Az/f.orc", orc_bytes(&[people(&[6])]))
        .await;
    t.put(
        "w/dl/ds=__HIVE_DEFAULT_PARTITION__/f.orc",
        orc_bytes(&[people(&[7])]),
    )
    .await;
    let table = orc_table(
        "s3://lake/w/dl/",
        people_columns().into_iter().take(1).collect(),
    )
    .with_partition_columns(vec![col("ds", DataType::Utf8)])
    .with_partitions(vec![
        partition(&["x/y:z"], ""),
        partition(&["__HIVE_DEFAULT_PARTITION__"], ""),
    ]);
    let ds = t.open("dl", table).await;
    assert_eq!(
        scan_rows(&ds, ScanContext::default()).await,
        vec!["6|x/y:z", "7|NULL"]
    );
}

#[tokio::test]
async fn hms_partitions_empty_or_missing_data_read_as_empty() {
    let t = Table::new();
    let table = ds_partitioned(&t, "w/hp", &[("ds=a", 1)]).await;
    // A table with no registered partitions is empty, whatever its
    // directory holds; a registered partition with no files adds no rows.
    let ds = t.open("hp", table.clone().with_partitions(vec![])).await;
    assert!(scan_rows(&ds, ScanContext::default()).await.is_empty());
    let ds = t
        .open(
            "hp",
            table.with_partitions(vec![partition(&["b"], "s3://lake/w/hp/ds=b")]),
        )
        .await;
    assert!(scan_rows(&ds, ScanContext::default()).await.is_empty());
}

#[tokio::test]
async fn hms_partition_values_are_typed_and_validated() {
    let t = Table::new();
    t.put("w/ty/p=1/f.orc", orc_bytes(&[people(&[1])])).await;
    let table = |values: &[&str]| {
        orc_table(
            "s3://lake/w/ty",
            people_columns().into_iter().take(1).collect(),
        )
        .with_partition_columns(vec![col("p", DataType::Int32)])
        .with_partitions(vec![partition(values, "s3://lake/w/ty/p=1")])
    };
    let ds = t.open("ty", table(&["1"])).await;
    assert_eq!(scan_rows(&ds, ScanContext::default()).await, vec!["1|1"]);
    let err = t.try_open("ty", table(&["x"])).await.unwrap_err();
    assert!(err.contains("not a valid"), "{err}");
    let err = t.try_open("ty", table(&["1", "2"])).await.unwrap_err();
    assert!(err.contains("has 2 values"), "{err}");
}

#[tokio::test]
async fn transactional_tables_are_rejected() {
    let t = Table::new();
    let table = orc_table("s3://lake/w/acid", people_columns()).with_transactional(true);
    let err = t.try_open("acid", table).await.unwrap_err();
    assert!(err.contains("transactional (ACID)"), "{err}");
}

#[tokio::test]
async fn acid_directory_layout_is_rejected() {
    // An ACID table whose HMS flag was lost still must not be misread.
    let t = Table::new();
    t.put(
        "w/acid/delta_0000001_0000001_0000/bucket_00000",
        orc_bytes(&[people(&[1])]),
    )
    .await;
    let err = t
        .try_open("acid", orc_table("s3://lake/w/acid", people_columns()))
        .await
        .unwrap_err();
    assert!(
        err.contains("Hive ACID (transactional) tables are not supported"),
        "{err}"
    );
}

#[tokio::test]
async fn acid_orc_file_layout_is_rejected() {
    let t = Table::new();
    let fields = [
        ("operation", ArrowDataType::Int32),
        ("originalTransaction", ArrowDataType::Int64),
        ("bucket", ArrowDataType::Int32),
        ("rowId", ArrowDataType::Int64),
        ("currentTransaction", ArrowDataType::Int64),
        ("row", ArrowDataType::Int64),
    ];
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, t)| Field::new(*n, t.clone(), true))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = fields
        .iter()
        .map(|(_, t)| arrow::array::new_null_array(t, 1))
        .collect();
    t.put(
        "w/acid/bucket_00000",
        orc_bytes(&[RecordBatch::try_new(schema, columns).unwrap()]),
    )
    .await;
    let ds = t
        .open(
            "acid",
            orc_table("s3://lake/w/acid", vec![col("row", DataType::Int64)]),
        )
        .await;
    let err = try_scan_rows(&ds, ScanContext::default())
        .await
        .unwrap_err();
    assert!(err.contains("ACID"), "{err}");
}

#[tokio::test]
async fn unsupported_storage_format_is_rejected() {
    let t = Table::new();
    let table = HiveTableProvider::new(
        people_columns(),
        "s3://lake/w/text".to_string(),
        "org.apache.hadoop.mapred.TextInputFormat".to_string(),
    )
    .with_serde_lib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    let err = t.try_open("text", table).await.unwrap_err();
    assert!(err.contains("only Parquet and ORC"), "{err}");
}

#[tokio::test]
async fn provider_properties_carry_format_and_partitioning() {
    let table = orc_table("s3://lake/w/pt", people_columns())
        .with_partition_columns(vec![col("ds", DataType::Utf8)]);
    let p: HashMap<String, String> = table.properties();
    assert_eq!(p[props::INPUT_FORMAT], ORC_INPUT);
    assert_eq!(p[props::SERDE_LIB], ORC_SERDE);
    assert_eq!(p[props::PARTITION_COLUMNS], "1");
    assert!(!p.contains_key(props::PARTITIONS));
    assert!(!p.contains_key(props::TRANSACTIONAL));
    assert_eq!(table.schema().last().unwrap().name, "ds");

    // HMS partitions travel as JSON, distinguishing "none registered" from
    // "not fetched".
    let p = table.clone().with_partitions(vec![]).properties();
    assert_eq!(p[props::PARTITIONS], "[]");
    let parts = vec![partition(&["2024-01-01"], "s3://lake/w/pt/ds=2024-01-01")];
    let p = table.with_partitions(parts.clone()).properties();
    let decoded: Vec<HivePartition> = serde_json::from_str(&p[props::PARTITIONS]).unwrap();
    assert_eq!(decoded, parts);
}

/// Trino-written ORC file (Hive connector, default ZLIB), produced by
/// `CREATE TABLE hive.s.types AS SELECT * FROM (VALUES ...)` — see
/// `tests/fixtures/README.md`.
const TRINO_TYPES_ORC: &[u8] = include_bytes!("../tests/fixtures/trino_types.orc");

fn trino_types_columns() -> Vec<ColumnInfo> {
    let dec = |precision, scale| DataType::Decimal128 { precision, scale };
    vec![
        col("tiny", DataType::Int8),
        col("small", DataType::Int16),
        col("i", DataType::Int32),
        col("big", DataType::Int64),
        col("r", DataType::Float32),
        col("d", DataType::Float64),
        col("b", DataType::Boolean),
        col("s", DataType::Utf8),
        col("v", DataType::Utf8),
        col("bin", DataType::Binary),
        col("dec", dec(12, 2)),
        col("bigdec", dec(30, 10)),
        col("day", DataType::Date32),
        col(
            "ts",
            DataType::Timestamp {
                unit: arneb_common::types::TimeUnit::Microsecond,
                timezone: None,
            },
        ),
    ]
}

#[tokio::test]
async fn trino_written_orc_all_types() {
    let t = Table::new();
    t.put("w/types/f", TRINO_TYPES_ORC.to_vec()).await;
    let ds = t
        .open(
            "types",
            orc_table("s3://lake/w/types", trino_types_columns()),
        )
        .await;
    // Expected values are what Trino returns for the same table, including
    // its reading of the pre-1970 fractional timestamp
    // (`1969-12-31 23:59:59.999` comes back as `1970-01-01 00:00:00.999`).
    assert_eq!(
        scan_rows(&ds, ScanContext::default()).await,
        vec![
            "-128|-32768|-2147483648|-9223372036854775808|-1.5|-22500000000.0|false||é漢字|00ff|\
             -9999999999.99|-12345678901234567890.0123456789|1900-01-01|1970-01-01T00:00:00.999",
            "0|0|0|0|0.0|0.0|true|x|y|41|42.00|0.0000000000|1970-01-01|2262-04-11T23:47:16.854",
            "127|32767|2147483647|9223372036854775807|3.25|1e-300|true|hello|v||0.01|\
             1.0000000001|2024-02-29|2024-03-10T02:30:00.123",
            "NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
        ]
    );
}
