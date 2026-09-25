//! End-to-end scan tests over an in-memory object store: metadata JSON →
//! manifest list → manifest → Parquet data files, exercised through the
//! connector factory exactly as the engine calls it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

use arneb_common::stream::collect_stream;
use arneb_common::types::{ScalarValue, TableReference};
use arneb_connectors::storage::StorageRegistry;
use arneb_connectors::ConnectorFactory;
use arneb_execution::{DataSource, ScanContext};
use arneb_hive::catalog::HiveTableMeta;
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast::BinaryOp;

use crate::catalog::IcebergTableProvider;
use crate::datasource::{props, IcebergConnectorFactory};
use crate::manifest::fixtures::*;
use arneb_catalog::TableProvider;

const BASE: &str = "s3://warehouse/db/t";

/// Current schema: `id long` (files store it as `int` — promotion),
/// `name string` (files call it `old_name` — rename), `region long`
/// (identity partition), `note string` (added later — absent in files).
fn metadata_json(manifest_list: Option<&str>) -> String {
    let snapshot = match manifest_list {
        Some(ml) => format!(
            r#""current-snapshot-id": 7,
               "snapshots": [{{"snapshot-id": 7, "sequence-number": 1, "timestamp-ms": 1,
                 "manifest-list": "{ml}",
                 "summary": {{"operation": "append", "total-records": "6", "total-files-size": "2048"}}}}]"#
        ),
        None => r#""current-snapshot-id": null, "snapshots": []"#.to_string(),
    };
    format!(
        r#"{{
      "format-version": 2,
      "table-uuid": "00000000-0000-0000-0000-000000000001",
      "location": "{BASE}",
      "last-sequence-number": 1, "last-updated-ms": 1, "last-column-id": 4,
      "current-schema-id": 1,
      "schemas": [{{"type": "struct", "schema-id": 1, "fields": [
        {{"id": 1, "name": "id", "required": false, "type": "long"}},
        {{"id": 2, "name": "name", "required": false, "type": "string"}},
        {{"id": 3, "name": "region", "required": false, "type": "long"}},
        {{"id": 4, "name": "note", "required": false, "type": "string"}}
      ]}}],
      "default-spec-id": 0,
      "partition-specs": [{{"spec-id": 0, "fields": [
        {{"name": "n_regionkey", "transform": "identity", "source-id": 3, "field-id": 1000}}
      ]}}],
      "last-partition-id": 1000,
      "properties": {{}},
      {snapshot}
    }}"#
    )
}

fn field(name: &str, dt: ArrowDataType, id: i32) -> Field {
    Field::new(name, dt, true).with_metadata(HashMap::from([(
        "PARQUET:field_id".to_string(),
        id.to_string(),
    )]))
}

/// A data file whose physical layout differs from the table schema.
/// `rewritten = false`: an old file — `id` stored as `int` (before the
/// `int -> long` promotion). `rewritten = true`: a newer file — `id` stored
/// as `long` and the columns physically reversed. Both use the old column
/// name `old_name` for field 2.
fn data_file(ids: Vec<i64>, names: Vec<&str>, region: i64, rewritten: bool) -> Vec<u8> {
    let n = ids.len();
    let id_col: (Field, arrow::array::ArrayRef) = if rewritten {
        (
            field("id", ArrowDataType::Int64, 1),
            Arc::new(Int64Array::from(ids)),
        )
    } else {
        (
            field("id", ArrowDataType::Int32, 1),
            Arc::new(Int32Array::from(
                ids.into_iter().map(|v| v as i32).collect::<Vec<_>>(),
            )),
        )
    };
    let name_col: (Field, arrow::array::ArrayRef) = (
        field("old_name", ArrowDataType::Utf8, 2),
        Arc::new(StringArray::from(names)),
    );
    let region_col: (Field, arrow::array::ArrayRef) = (
        field("region", ArrowDataType::Int64, 3),
        Arc::new(Int64Array::from(vec![region; n])),
    );
    let mut cols = vec![id_col, name_col, region_col];
    if rewritten {
        cols.reverse();
    }
    let schema = Arc::new(Schema::new(
        cols.iter().map(|(f, _)| f.clone()).collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), cols.into_iter().map(|(_, a)| a).collect()).unwrap();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    buf
}

struct Fixture {
    storage: Arc<StorageRegistry>,
    store: Arc<dyn ObjectStore>,
}

impl Fixture {
    async fn put(&self, uri: &str, bytes: Vec<u8>) {
        let path = uri.strip_prefix("s3://warehouse/").unwrap();
        self.store
            .put(&ObjectPath::from(path), PutPayload::from(bytes))
            .await
            .unwrap();
    }

    /// Table with two live files (region 1 and 2) plus a tombstoned file
    /// that does not exist in storage (reading it would fail the test).
    async fn new(with_delete_manifest: bool) -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = Arc::new(StorageRegistry::new());
        storage.register_store("s3://warehouse", store.clone());
        let f = Fixture { storage, store };

        let a = format!("{BASE}/data/region=1/a.parquet");
        let b = format!("{BASE}/data/region=2/b.parquet");
        f.put(&a, data_file(vec![1, 2, 3], vec!["a", "b", "c"], 1, false))
            .await;
        f.put(
            &b,
            data_file(vec![10, 11, 12], vec!["x", "y", "z"], 2, true),
        )
        .await;

        let long = |v: i64| v.to_le_bytes().to_vec();
        let int = |v: i32| v.to_le_bytes().to_vec();
        let manifest = manifest_bytes(&[
            Entry {
                status: 1,
                content: 0,
                path: &a,
                region: Some(1),
                rows: 3,
                // Written while `id` was still an `int`: 4-byte bounds.
                lower: vec![(1, int(1)), (3, long(1))],
                upper: vec![(1, int(3)), (3, long(1))],
            },
            Entry {
                status: 0,
                content: 0,
                path: &b,
                region: Some(2),
                rows: 3,
                lower: vec![(1, long(10))],
                upper: vec![(1, long(12))],
            },
            Entry {
                status: 2,
                content: 0,
                path: "s3://warehouse/db/t/data/gone.parquet",
                region: Some(9),
                rows: 100,
                lower: vec![],
                upper: vec![],
            },
        ]);
        let m_path = format!("{BASE}/metadata/m0.avro");
        f.put(&m_path, manifest).await;

        let mut list_rows = vec![manifest_list_row(&m_path, 0, 1, 1)];
        if with_delete_manifest {
            let d_path = format!("{BASE}/metadata/d0.avro");
            let deletes = manifest_bytes(&[Entry {
                status: 1,
                content: 1,
                path: "s3://warehouse/db/t/data/pos-deletes.parquet",
                region: Some(1),
                rows: 1,
                lower: vec![],
                upper: vec![],
            }]);
            f.put(&d_path, deletes).await;
            list_rows.push(manifest_list_row(&d_path, 1, 1, 0));
        }
        let ml_path = format!("{BASE}/metadata/snap-7.avro");
        f.put(&ml_path, avro_file(MANIFEST_LIST_SCHEMA, list_rows))
            .await;
        f.put(
            &format!("{BASE}/metadata/00001.metadata.json"),
            metadata_json(Some(&ml_path)).into_bytes(),
        )
        .await;
        f.put(
            &format!("{BASE}/metadata/00000.metadata.json"),
            metadata_json(None).into_bytes(),
        )
        .await;
        f
    }

    async fn provider(&self, metadata_file: &str) -> IcebergTableProvider {
        let meta = HiveTableMeta {
            columns: vec![],
            location: BASE.into(),
            input_format: String::new(),
            row_count: None,
            size_bytes: None,
            column_stats: HashMap::new(),
            parameters: HashMap::from([
                ("table_type".to_string(), "ICEBERG".to_string()),
                (
                    "metadata_location".to_string(),
                    format!("{BASE}/metadata/{metadata_file}"),
                ),
            ]),
        };
        IcebergTableProvider::resolve(&self.storage, "db.t", meta)
            .await
            .unwrap()
    }

    async fn source(&self, metadata_file: &str) -> Arc<dyn DataSource> {
        let p = self.provider(metadata_file).await;
        IcebergConnectorFactory::new(self.storage.clone())
            .create_data_source(&TableReference::table("t"), &p.schema(), &p.properties())
            .await
            .unwrap()
    }
}

/// Scan every partition and return rows as display strings, sorted.
async fn scan_rows(ds: &Arc<dyn DataSource>, ctx: ScanContext) -> Vec<String> {
    let mut rows = Vec::new();
    for p in 0..ds.partition_count() {
        let batches = collect_stream(ds.scan(&ctx, p).await.unwrap())
            .await
            .unwrap();
        for b in batches {
            for r in 0..b.num_rows() {
                let cells: Vec<String> = b
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(r) {
                            "NULL".to_string()
                        } else {
                            arrow::util::display::array_value_to_string(c, r).unwrap()
                        }
                    })
                    .collect();
                rows.push(cells.join("|"));
            }
        }
    }
    rows.sort();
    rows
}

fn col(index: usize) -> PlanExpr {
    PlanExpr::Column {
        index,
        name: String::new(),
        span: None,
    }
}

fn eq(index: usize, value: ScalarValue) -> PlanExpr {
    PlanExpr::BinaryOp {
        left: Box::new(col(index)),
        op: BinaryOp::Eq,
        right: Box::new(PlanExpr::Literal { value, span: None }),
        span: None,
    }
}

#[tokio::test]
async fn provider_exposes_current_schema_snapshot_and_stats() {
    let f = Fixture::new(false).await;
    let p = f.provider("00001.metadata.json").await;
    let names: Vec<_> = p.schema().into_iter().map(|c| c.name).collect();
    assert_eq!(names, ["id", "name", "region", "note"]);
    assert_eq!(
        p.properties().get(props::SNAPSHOT_ID).map(String::as_str),
        Some("7")
    );
    let stats = p.statistics().unwrap();
    assert_eq!(stats.row_count, Some(6));
    assert_eq!(stats.size_bytes, Some(2048));
}

#[tokio::test]
async fn full_scan_resolves_columns_by_field_id() {
    let f = Fixture::new(false).await;
    let ds = f.source("00001.metadata.json").await;
    // `id` is promoted int→long, `name` is read from `old_name`, file b
    // has reversed column order, `note` is null-filled; the tombstoned
    // file is never opened.
    assert_eq!(
        ds.schema()[0].data_type,
        arneb_common::types::DataType::Int64
    );
    assert_eq!(
        scan_rows(&ds, ScanContext::default()).await,
        [
            "10|x|2|NULL",
            "11|y|2|NULL",
            "12|z|2|NULL",
            "1|a|1|NULL",
            "2|b|1|NULL",
            "3|c|1|NULL",
        ]
    );
}

#[tokio::test]
async fn projection_in_requested_order() {
    let f = Fixture::new(false).await;
    let ds = f.source("00001.metadata.json").await;
    let rows = scan_rows(&ds, ScanContext::default().with_projection(vec![3, 1])).await;
    assert_eq!(
        rows,
        ["NULL|a", "NULL|b", "NULL|c", "NULL|x", "NULL|y", "NULL|z"]
    );
    // Only the null-filled column: row counts must survive.
    let rows = scan_rows(&ds, ScanContext::default().with_projection(vec![3])).await;
    assert_eq!(rows.len(), 6);
}

#[tokio::test]
async fn manifest_bounds_prune_files() {
    let f = Fixture::new(false).await;
    let ds = f.source("00001.metadata.json").await;
    // id = 2 is outside file b's [10, 12] bounds, so only file a is read.
    // File a stores `id` as `int`, so the predicate is not pushed into its
    // Parquet reader (physical type differs) and all its rows come back;
    // the engine's FilterExec above the scan does the rest.
    let rows = scan_rows(
        &ds,
        ScanContext::default().with_filters(vec![eq(0, ScalarValue::Int64(2))]),
    )
    .await;
    assert_eq!(rows, ["1|a|1|NULL", "2|b|1|NULL", "3|c|1|NULL"]);
}

#[tokio::test]
async fn identity_partition_prunes_files() {
    let f = Fixture::new(false).await;
    let ds = f.source("00001.metadata.json").await;
    let rows = scan_rows(
        &ds,
        ScanContext::default().with_filters(vec![eq(2, ScalarValue::Int64(1))]),
    )
    .await;
    // Partition pruning drops file b; the `region` row filter keeps all
    // of file a.
    assert_eq!(rows, ["1|a|1|NULL", "2|b|1|NULL", "3|c|1|NULL"]);
}

#[tokio::test]
async fn row_filter_pushdown_uses_file_column_index() {
    let f = Fixture::new(false).await;
    let ds = f.source("00001.metadata.json").await;
    // id = 11: file a (4-byte bounds [1, 3]) is pruned by manifest stats;
    // file b stores `id` as its *last* physical column, so the pushed
    // Parquet predicate must follow the field ID, not table position 0.
    let rows = scan_rows(
        &ds,
        ScanContext::default().with_filters(vec![eq(0, ScalarValue::Int64(11))]),
    )
    .await;
    assert_eq!(rows, ["11|y|2|NULL"]);
}

#[tokio::test]
async fn mismatched_literal_type_is_not_pushed() {
    let f = Fixture::new(false).await;
    let ds = f.source("00001.metadata.json").await;
    // An Int32 literal against a long column must not reach the Parquet
    // comparison kernel (which would fail on Int64 vs Int32); the scan
    // still succeeds and returns a superset.
    let rows = scan_rows(
        &ds,
        ScanContext::default().with_filters(vec![eq(2, ScalarValue::Int32(2))]),
    )
    .await;
    assert_eq!(rows, ["10|x|2|NULL", "11|y|2|NULL", "12|z|2|NULL"]);
}

#[tokio::test]
async fn delete_files_are_rejected() {
    let f = Fixture::new(true).await;
    let p = f.provider("00001.metadata.json").await;
    let err = IcebergConnectorFactory::new(f.storage.clone())
        .create_data_source(&TableReference::table("t"), &p.schema(), &p.properties())
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("position delete files"), "{msg}");
    assert!(msg.contains("not supported"), "{msg}");
}

#[tokio::test]
async fn empty_table_scans_no_rows() {
    let f = Fixture::new(false).await;
    let p = f.provider("00000.metadata.json").await;
    assert!(!p.properties().contains_key(props::SNAPSHOT_ID));
    assert_eq!(p.statistics().unwrap().row_count, Some(0));
    let ds = f.source("00000.metadata.json").await;
    assert!(scan_rows(&ds, ScanContext::default()).await.is_empty());
}

#[tokio::test]
async fn non_iceberg_table_fails_with_clear_error() {
    let f = Fixture::new(false).await;
    let meta = HiveTableMeta {
        columns: vec![],
        location: BASE.into(),
        input_format: String::new(),
        row_count: None,
        size_bytes: None,
        column_stats: HashMap::new(),
        parameters: HashMap::new(),
    };
    let p = IcebergTableProvider::resolve(&f.storage, "db.plain", meta)
        .await
        .unwrap();
    let err = IcebergConnectorFactory::new(f.storage.clone())
        .create_data_source(&TableReference::table("plain"), &[], &p.properties())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not an Iceberg table"), "{err}");
}
