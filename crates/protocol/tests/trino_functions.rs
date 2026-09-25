//! End-to-end SQL tests for the Trino scalar functions added in
//! `trino-functions-batch1`: SQL text → parser → planner/analyzer →
//! optimizer → physical plan → Arrow batches, through the same
//! `execute_query` pipeline the pgwire handler uses.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Int32Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit as ArrowTimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType, TimeUnit};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// Days since epoch for a civil date.
fn days(y: i32, m: u32, d: u32) -> i32 {
    let date = chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap();
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (date - epoch).num_days() as i32
}

fn micros(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
    chrono::NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, mi, s)
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

/// Table `t`:
///
/// | id | name      | price  | d          | ts                  |
/// |----|-----------|--------|------------|---------------------|
/// | 1  | 'a-b-c'   | 12.50  | 2024-01-31 | 2024-01-31 10:15:30 |
/// | 2  | 'bob'     | 3.25   | 2024-02-29 | 2024-02-29 23:59:59 |
/// | 3  | NULL      | 100.00 | 2023-12-25 | 2023-12-25 00:00:00 |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let ts_type = ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None);
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, true),
        Field::new("price", ArrowDataType::Decimal128(12, 2), false),
        Field::new("d", ArrowDataType::Date32, false),
        Field::new("ts", ts_type, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a-b-c"), Some("bob"), None])),
            Arc::new(
                Decimal128Array::from(vec![1250, 325, 10000])
                    .with_precision_and_scale(12, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![
                days(2024, 1, 31),
                days(2024, 2, 29),
                days(2023, 12, 25),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                micros(2024, 1, 31, 10, 15, 30),
                micros(2024, 2, 29, 23, 59, 59),
                micros(2023, 12, 25, 0, 0, 0),
            ])),
        ],
    )
    .unwrap();

    let col = |name: &str, data_type: DataType, nullable: bool| ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable,
    };
    let table = Arc::new(MemoryTable::new(
        vec![
            col("id", DataType::Int32, false),
            col("name", DataType::Utf8, true),
            col(
                "price",
                DataType::Decimal128 {
                    precision: 12,
                    scale: 2,
                },
                false,
            ),
            col("d", DataType::Date32, false),
            col(
                "ts",
                DataType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: None,
                },
                false,
            ),
        ],
        vec![batch],
    ));

    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", table);
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run `sql` and render every cell as text (`NULL` for nulls).
async fn query(sql: &str) -> Vec<Vec<String>> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c: &ArrayRef| {
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
    rows
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn string_functions_in_select() {
    let rows = query(
        "SELECT id, split_part(name, '-', 2), strpos(name, 'b'), lpad(name, 6, '*'), \
         upper(reverse(name)), name || '!', concat_ws('/', name, 'x'), \
         regexp_extract(name, '([a-z])-([a-z])', 2), regexp_replace(name, '-', '_') \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            row(&["1", "b", "3", "*a-b-c", "C-B-A", "a-b-c!", "a-b-c/x", "b", "a_b_c"]),
            row(&["2", "NULL", "1", "***bob", "BOB", "bob!", "bob/x", "NULL", "bob"]),
            row(&["3", "NULL", "NULL", "NULL", "NULL", "NULL", "x", "NULL", "NULL"]),
        ]
    );
}

#[tokio::test]
async fn functions_in_where_clause() {
    let rows = query("SELECT id FROM t WHERE regexp_like(name, '^a') ORDER BY id").await;
    assert_eq!(rows, vec![row(&["1"])]);

    // COALESCE guards the NULL name: arneb's OR is not yet Kleene
    // (NULL OR TRUE evaluates to NULL), which is a pre-existing gap
    // unrelated to these functions.
    let rows = query(
        "SELECT id FROM t WHERE COALESCE(starts_with(name, 'bo'), false) OR day_of_week(d) = 1 \
         ORDER BY id",
    )
    .await;
    // 2023-12-25 is a Monday (ISO day 1).
    assert_eq!(rows, vec![row(&["2"]), row(&["3"])]);

    let rows =
        query("SELECT id FROM t WHERE date_diff('day', d, DATE '2024-03-01') < 5 ORDER BY id")
            .await;
    assert_eq!(rows, vec![row(&["2"])]);

    let rows = query("SELECT id FROM t WHERE greatest(id, 2) = 2 ORDER BY id").await;
    assert_eq!(rows, vec![row(&["1"]), row(&["2"])]);
}

#[tokio::test]
async fn math_functions() {
    let rows = query(
        "SELECT id, greatest(id, 2), least(price, 10), sqrt(id * id), log(2, 8), \
         sign(0 - id), truncate(price), CEIL(price), FLOOR(price), abs(id - 2) \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            row(&["1", "2", "10.00", "1.0", "3.0", "-1", "12.0", "13.0", "12.0", "1"]),
            row(&["2", "2", "3.25", "2.0", "3.0", "-1", "3.0", "4.0", "3.0", "0"]),
            row(&["3", "3", "10.00", "3.0", "3.0", "-1", "100.0", "100.0", "100.0", "1"]),
        ]
    );
}

#[tokio::test]
async fn date_time_functions() {
    let rows = query(
        "SELECT id, year(d), month(d), day(d), quarter(d), day_of_week(d), \
         date_add('month', 1, d), last_day_of_month(d), \
         date_diff('month', d, DATE '2024-03-31'), \
         date_format(ts, '%Y-%m-%d %H:%i'), date_trunc('hour', ts), hour(ts), \
         format_datetime(ts, 'yyyy/MM/dd HH:mm:ss') \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            row(&[
                "1",
                "2024",
                "1",
                "31",
                "1",
                "3",
                "2024-02-29",
                "2024-01-31",
                "2",
                "2024-01-31 10:15",
                "2024-01-31T10:00:00",
                "10",
                "2024/01/31 10:15:30",
            ]),
            row(&[
                "2",
                "2024",
                "2",
                "29",
                "1",
                "4",
                "2024-03-29",
                "2024-02-29",
                "1",
                "2024-02-29 23:59",
                "2024-02-29T23:00:00",
                "23",
                "2024/02/29 23:59:59",
            ]),
            row(&[
                "3",
                "2023",
                "12",
                "25",
                "4",
                "1",
                "2024-01-25",
                "2023-12-31",
                "3",
                "2023-12-25 00:00",
                "2023-12-25T00:00:00",
                "0",
                "2023/12/25 00:00:00",
            ]),
        ]
    );
}

#[tokio::test]
async fn date_parse_and_unixtime_roundtrip() {
    let rows = query(
        "SELECT date_parse('2024-03-15 08:30', '%Y-%m-%d %H:%i'), \
         to_unixtime(from_unixtime(1700000000)), \
         from_unixtime(0), date('2024-07-04'), \
         extract(DOW FROM DATE '2024-07-04'), extract(QUARTER FROM DATE '2024-07-04')",
    )
    .await;
    assert_eq!(
        rows,
        vec![row(&[
            "2024-03-15T08:30:00",
            "1700000000.0",
            "1970-01-01T00:00:00",
            "2024-07-04",
            "4",
            "3",
        ])]
    );
}

#[tokio::test]
async fn group_by_date_part() {
    let rows = query("SELECT year(d), count(*) FROM t GROUP BY year(d) ORDER BY year(d)").await;
    assert_eq!(rows, vec![row(&["2023", "1"]), row(&["2024", "2"])]);
}

#[tokio::test]
async fn if_try_and_special_syntax() {
    let rows = query(
        "SELECT id, IF(id > 1, 'big', 'small'), IF(id > 2, 'x'), TRY(chr(id + 63)), \
         TRY(split_part('a,b', ',', id - 1)), TRIM(BOTH '*' FROM '**x**'), \
         POSITION('b' IN name), TRIM(LEADING '-' FROM '--y') \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            row(&["1", "small", "NULL", "@", "NULL", "x", "3", "y"]),
            row(&["2", "big", "NULL", "A", "a", "x", "1", "y"]),
            row(&["3", "big", "x", "B", "b", "x", "NULL", "y"]),
        ]
    );
}

#[tokio::test]
async fn nullary_functions_produce_a_value_per_row() {
    let rows = query(
        "SELECT round(pi() * 100), now() IS NOT NULL, current_timestamp IS NOT NULL, \
         random() < 1.0, current_date IS NOT NULL FROM t",
    )
    .await;
    assert_eq!(rows.len(), 3);
    for r in rows {
        assert_eq!(r, row(&["314.0", "true", "true", "true", "true"]));
    }
}
