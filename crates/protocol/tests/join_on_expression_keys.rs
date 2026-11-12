//! Joins whose equality condition compares expressions rather than bare
//! columns, e.g. `substr(a.s, 1, 2) = substr(b.s, 1, 2)` (TPC-DS q08).
//! Results must follow SQL semantics (NULL keys never match, outer joins
//! null-extend), and such a join must not compare every left row with every
//! right row: the at-scale case would not finish in time if it did.

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// A table with a nullable INTEGER column `k` and a nullable VARCHAR column `s`.
fn ks_table(rows: Vec<(Option<i32>, Option<String>)>) -> Arc<MemoryTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", ArrowDataType::Int32, true),
        Field::new("s", ArrowDataType::Utf8, true),
    ]));
    let (ks, ss): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
    let k: ArrayRef = Arc::new(Int32Array::from(ks));
    let s: ArrayRef = Arc::new(StringArray::from(ss));
    let batch = RecordBatch::try_new(schema, vec![k, s]).unwrap();
    Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            ColumnInfo {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ],
        vec![batch],
    ))
}

fn row(k: i32, s: Option<&str>) -> (Option<i32>, Option<String>) {
    (Some(k), s.map(str::to_string))
}

const BIG_ROWS: i32 = 200_000;
const SMALL_ROWS: i32 = 1_000;

/// - `a(k, s)`: (1,'ab1') (2,'ab2') (3,'cd3') (4,NULL) (5,'zz5')
/// - `b(k, s)`: (10,'ab9') (20,'cd8') (30,NULL) (40,'ee7') (50,'ab0')
/// - `big(k, s)`: k in 0..200000, s = k as six digits
/// - `small(k, s)`: k in 0..1000, s = (k * 200) as six digits
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "a",
        ks_table(vec![
            row(1, Some("ab1")),
            row(2, Some("ab2")),
            row(3, Some("cd3")),
            row(4, None),
            row(5, Some("zz5")),
        ]),
    );
    schema.register_table(
        "b",
        ks_table(vec![
            row(10, Some("ab9")),
            row(20, Some("cd8")),
            row(30, None),
            row(40, Some("ee7")),
            row(50, Some("ab0")),
        ]),
    );
    schema.register_table(
        "big",
        ks_table(
            (0..BIG_ROWS)
                .map(|k| (Some(k), Some(format!("{k:06}"))))
                .collect(),
        ),
    );
    schema.register_table(
        "small",
        ks_table(
            (0..SMALL_ROWS)
                .map(|k| (Some(k), Some(format!("{:06}", k * 200))))
                .collect(),
        ),
    );
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run `sql` on its own thread, giving up after 10 seconds; return every cell
/// as text (`NULL` for nulls).
fn rows_within_time_limit(sql: &str) -> Vec<Vec<String>> {
    let (tx, rx) = mpsc::channel();
    let sql_owned = sql.to_string();
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let (cm, cr) = setup();
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
            let (_plan, batches) =
                arneb_protocol::__private::execute_query(&sql_owned, &cm, &cr, None, &pool)
                    .await
                    .unwrap_or_else(|e| panic!("query failed: {sql_owned}\n{e}"));
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
                            .collect::<Vec<String>>(),
                    );
                }
            }
            out
        });
        let _ = tx.send(result);
    });
    rx.recv_timeout(Duration::from_secs(10))
        .unwrap_or_else(|_| panic!("query did not finish within 10 seconds: {sql}"))
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

const PREFIX_PAIRS: &[&[&str]] = &[
    &["1", "10"],
    &["1", "50"],
    &["2", "10"],
    &["2", "50"],
    &["3", "20"],
];

fn with_extra(base: &[&[&str]], extra: &[&[&str]]) -> Vec<Vec<String>> {
    let mut all = table(base);
    all.extend(table(extra));
    all.sort();
    all
}

#[test]
fn inner_join_on_function_keys_matches_equal_values_only() {
    assert_eq!(
        rows_within_time_limit(
            "SELECT a.k, b.k FROM a JOIN b ON substr(a.s, 1, 2) = substr(b.s, 1, 2) \
             ORDER BY a.k, b.k"
        ),
        table(PREFIX_PAIRS)
    );
}

#[test]
fn comma_join_with_function_keys_in_where() {
    assert_eq!(
        rows_within_time_limit(
            "SELECT a.k, b.k FROM a, b WHERE substr(a.s, 1, 2) = substr(b.s, 1, 2) \
             ORDER BY a.k, b.k"
        ),
        table(PREFIX_PAIRS)
    );
}

#[test]
fn function_keys_written_right_side_first() {
    assert_eq!(
        rows_within_time_limit(
            "SELECT a.k, b.k FROM a JOIN b ON substr(b.s, 1, 2) = substr(a.s, 1, 2) \
             ORDER BY a.k, b.k"
        ),
        table(PREFIX_PAIRS)
    );
}

#[test]
fn left_join_on_function_keys_keeps_unmatched_and_null_key_rows() {
    let mut got = rows_within_time_limit(
        "SELECT a.k, b.k FROM a LEFT JOIN b ON substr(a.s, 1, 2) = substr(b.s, 1, 2)",
    );
    got.sort();
    assert_eq!(
        got,
        with_extra(PREFIX_PAIRS, &[&["4", "NULL"], &["5", "NULL"]])
    );
}

#[test]
fn right_join_on_function_keys_keeps_unmatched_and_null_key_rows() {
    let mut got = rows_within_time_limit(
        "SELECT a.k, b.k FROM a RIGHT JOIN b ON substr(a.s, 1, 2) = substr(b.s, 1, 2)",
    );
    got.sort();
    assert_eq!(
        got,
        with_extra(PREFIX_PAIRS, &[&["NULL", "30"], &["NULL", "40"]])
    );
}

#[test]
fn full_join_on_function_keys_keeps_both_sides_unmatched_rows() {
    let mut got = rows_within_time_limit(
        "SELECT a.k, b.k FROM a FULL JOIN b ON substr(a.s, 1, 2) = substr(b.s, 1, 2)",
    );
    got.sort();
    assert_eq!(
        got,
        with_extra(
            PREFIX_PAIRS,
            &[
                &["4", "NULL"],
                &["5", "NULL"],
                &["NULL", "30"],
                &["NULL", "40"],
            ]
        )
    );
}

#[test]
fn arithmetic_key_on_one_side_only() {
    assert_eq!(
        rows_within_time_limit("SELECT a.k, b.k FROM a JOIN b ON a.k * 10 = b.k ORDER BY a.k"),
        table(&[
            &["1", "10"],
            &["2", "20"],
            &["3", "30"],
            &["4", "40"],
            &["5", "50"]
        ])
    );
}

#[test]
fn key_expressions_of_different_integer_widths() {
    assert_eq!(
        rows_within_time_limit(
            "SELECT a.k, b.k FROM a JOIN b ON CAST(a.k AS BIGINT) * 10 = b.k ORDER BY a.k"
        ),
        table(&[
            &["1", "10"],
            &["2", "20"],
            &["3", "30"],
            &["4", "40"],
            &["5", "50"]
        ])
    );
}

#[test]
fn two_expression_keys_together() {
    // a.k * 10 = b.k pairs (1,10) (2,20) (3,30) (4,40) (5,50); of these only
    // (1,10) also agrees on the first character ('a' = 'a'); NULLs never match.
    assert_eq!(
        rows_within_time_limit(
            "SELECT a.k, b.k FROM a JOIN b \
             ON a.k * 10 = b.k AND substr(a.s, 1, 1) = substr(b.s, 1, 1) ORDER BY a.k"
        ),
        table(&[&["1", "10"]])
    );
}

#[test]
fn expression_key_beside_a_non_equi_residual() {
    // k < 10k - 25 holds for k >= 3.
    assert_eq!(
        rows_within_time_limit(
            "SELECT a.k, b.k FROM a JOIN b ON a.k * 10 = b.k AND a.k < b.k - 25 ORDER BY a.k"
        ),
        table(&[&["3", "30"], &["4", "40"], &["5", "50"]])
    );
}

#[test]
fn left_join_on_expression_key_with_residual_null_extends_rejected_rows() {
    let mut got = rows_within_time_limit(
        "SELECT a.k, b.k FROM a LEFT JOIN b ON a.k * 10 = b.k AND a.k < b.k - 25",
    );
    got.sort();
    assert_eq!(
        got,
        table(&[
            &["1", "NULL"],
            &["2", "NULL"],
            &["3", "30"],
            &["4", "40"],
            &["5", "50"]
        ])
    );
}

#[test]
fn equality_mixing_both_sides_in_one_operand_is_not_a_key() {
    assert_eq!(
        rows_within_time_limit("SELECT a.k, b.k FROM a JOIN b ON a.k + b.k = 22 ORDER BY a.k"),
        table(&[&["2", "20"]])
    );
}

#[test]
fn function_key_join_over_many_rows() {
    // big has 1000 rows per three-digit prefix (000..199); small has 5 rows per
    // prefix 000..199. Every small row matches 1000 big rows.
    let expected = (SMALL_ROWS as i64) * 1000;
    assert_eq!(
        rows_within_time_limit(
            "SELECT count(*) FROM big JOIN small ON substr(big.s, 1, 3) = substr(small.s, 1, 3)"
        ),
        table(&[&[expected.to_string().as_str()]])
    );
}
