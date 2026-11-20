//! Correlated scalar subqueries in SELECT lists over pgwire (G6 stage 2b-1).
//!
//! The shapes mirror SQLAlchemy's PostgreSQL column-reflection query: a
//! correlated subquery inside a `CASE` condition and branch, an outer-only
//! guard conjunct, an expression correlation key, a join in the subquery's
//! `FROM`, and an outer reference in the subquery's value.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

fn mem(cols: Vec<(&str, ArrowDataType, ArrayRef)>) -> Arc<MemoryTable> {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, t, _)| Field::new(*n, t.clone(), true))
            .collect::<Vec<_>>(),
    ));
    let infos = cols
        .iter()
        .map(|(n, t, _)| ColumnInfo {
            name: n.to_string(),
            data_type: DataType::try_from(t.clone()).unwrap(),
            nullable: true,
        })
        .collect();
    let batch =
        RecordBatch::try_new(schema, cols.into_iter().map(|(_, _, a)| a).collect()).unwrap();
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

fn i64s(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v)) as ArrayRef
}

/// Fixtures:
/// - `o(id, k, flag)`: (1,1,t) (2,2,f) (3,3,f) (4,NULL,t)
/// - `i(k, v, b)`: (1,10,t) (3,30,f) (4,40,NULL) (5,50,t) — unique keys
/// - `idup(k, v)`: (1,10) (1,11) (2,20) — key 1 has two rows
/// - `j(v, w)`: (10,'ten') (30,'thirty')
/// - `n32(k INT, s)`: (1,'one') (2,'two')
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table(
        "o",
        mem(vec![
            (
                "id",
                ArrowDataType::Int64,
                i64s(vec![Some(1), Some(2), Some(3), Some(4)]),
            ),
            (
                "k",
                ArrowDataType::Int64,
                i64s(vec![Some(1), Some(2), Some(3), None]),
            ),
            (
                "flag",
                ArrowDataType::Boolean,
                Arc::new(BooleanArray::from(vec![true, false, false, true])) as ArrayRef,
            ),
        ]),
    );
    default_schema.register_table(
        "i",
        mem(vec![
            (
                "k",
                ArrowDataType::Int64,
                i64s(vec![Some(1), Some(3), Some(4), Some(5)]),
            ),
            (
                "v",
                ArrowDataType::Int64,
                i64s(vec![Some(10), Some(30), Some(40), Some(50)]),
            ),
            (
                "b",
                ArrowDataType::Boolean,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                ])) as ArrayRef,
            ),
        ]),
    );
    default_schema.register_table(
        "idup",
        mem(vec![
            (
                "k",
                ArrowDataType::Int64,
                i64s(vec![Some(1), Some(1), Some(2)]),
            ),
            (
                "v",
                ArrowDataType::Int64,
                i64s(vec![Some(10), Some(11), Some(20)]),
            ),
        ]),
    );
    default_schema.register_table(
        "j",
        mem(vec![
            ("v", ArrowDataType::Int64, i64s(vec![Some(10), Some(30)])),
            (
                "w",
                ArrowDataType::Utf8,
                Arc::new(StringArray::from(vec!["ten", "thirty"])) as ArrayRef,
            ),
        ]),
    );
    default_schema.register_table(
        "n32",
        mem(vec![
            (
                "k",
                ArrowDataType::Int32,
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                "s",
                ArrowDataType::Utf8,
                Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
            ),
        ]),
    );

    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", default_schema);
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );
    (Arc::new(cm), Arc::new(registry))
}

async fn pg_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    );
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=smoke dbname=file"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    client
}

async fn client() -> tokio_postgres::Client {
    connect(pg_server().await).await
}

/// All rows of a simple query as text cells.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).map(str::to_string))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect()
}

async fn error(client: &tokio_postgres::Client, sql: &str) -> String {
    let err = client.simple_query(sql).await.expect_err("query must fail");
    err.as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string())
}

/// `[(id, value)]` rows, as strings, for the expectations below.
fn expect(pairs: &[(i64, Option<&str>)]) -> Vec<Vec<Option<String>>> {
    pairs
        .iter()
        .map(|(id, v)| vec![Some(id.to_string()), v.map(str::to_string)])
        .collect()
}

#[tokio::test]
async fn select_list_correlated_scalar() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT v FROM i WHERE i.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("10")), (2, None), (3, Some("30")), (4, None)])
    );
}

#[tokio::test]
async fn select_list_correlated_aggregate() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT count(*) FROM idup WHERE idup.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[
            (1, Some("2")),
            (2, Some("1")),
            (3, Some("0")),
            (4, Some("0"))
        ])
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT sum(v) FROM idup WHERE idup.k = o.k) AS s FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("21")), (2, Some("20")), (3, None), (4, None)])
    );
}

#[tokio::test]
async fn correlated_inside_case_condition_and_branch() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, CASE WHEN (SELECT v FROM i WHERE i.k = o.k) > 10 \
             THEN (SELECT v FROM i WHERE i.k = o.k) END FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, None), (2, None), (3, Some("30")), (4, None)])
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, CASE WHEN (SELECT v FROM i WHERE i.k = o.k) > 1 \
             THEN (SELECT v FROM i WHERE i.k = o.k) END FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("10")), (2, None), (3, Some("30")), (4, None)])
    );
}

#[tokio::test]
async fn correlated_with_outer_only_guard() {
    let c = client().await;
    // Row 3 matches `i` but its flag is false.
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT v FROM i WHERE o.flag AND i.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("10")), (2, None), (3, None), (4, None)])
    );
    // An aggregate over the rows a false guard leaves is the empty-set value.
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT count(*) FROM i WHERE o.flag AND i.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[
            (1, Some("1")),
            (2, Some("0")),
            (3, Some("0")),
            (4, Some("0"))
        ])
    );
    // The SQLAlchemy shape: the guard repeated as the CASE condition.
    assert_eq!(
        rows(
            &c,
            "SELECT id, CASE WHEN o.flag THEN (SELECT v FROM i WHERE o.flag AND i.k = o.k) \
             ELSE NULL END AS d FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("10")), (2, None), (3, None), (4, None)])
    );
}

#[tokio::test]
async fn correlated_with_expression_key() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT v FROM i WHERE i.k = o.k + 1) FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, None), (2, Some("30")), (3, Some("40")), (4, None)])
    );
    // Expression on both sides, mirrored, with casts.
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT v FROM i WHERE CAST(CAST(o.k AS VARCHAR) AS BIGINT) * 10 = i.v) \
             FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("10")), (2, None), (3, Some("30")), (4, None)])
    );
}

#[tokio::test]
async fn correlated_inner_join_in_subquery_from() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT j.w FROM i JOIN j ON j.v = i.v WHERE i.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("ten")), (2, None), (3, Some("thirty")), (4, None)])
    );
}

#[tokio::test]
async fn correlated_inner_table_shadows_outer_table_of_same_name() {
    let c = client().await;
    // `i` is in both FROM clauses; inside the subquery `i.k` is the inner one.
    assert_eq!(
        rows(
            &c,
            "SELECT o.id, (SELECT i.v FROM i WHERE i.k = o.k + 1) FROM o \
             LEFT JOIN i ON i.k = o.k ORDER BY o.id"
        )
        .await,
        expect(&[(1, None), (2, Some("30")), (3, Some("40")), (4, None)])
    );
}

#[tokio::test]
async fn correlated_value_with_outer_reference_and_boolean() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT i.v + o.id FROM i WHERE i.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("11")), (2, None), (3, Some("33")), (4, None)])
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT b FROM i WHERE i.k = o.k + 3) FROM o ORDER BY id"
        )
        .await,
        // k+3: 4 → NULL b, 5 → true, 6 → no row.
        expect(&[(1, None), (2, Some("t")), (3, None), (4, None)])
    );
}

#[tokio::test]
async fn correlated_in_aggregating_outer_query() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT o.k, count(*), (SELECT v FROM i WHERE i.k = o.k) FROM o \
             WHERE o.k IS NOT NULL GROUP BY o.k ORDER BY o.k"
        )
        .await,
        vec![
            vec![Some("1".into()), Some("1".into()), Some("10".into())],
            vec![Some("2".into()), Some("1".into()), None],
            vec![Some("3".into()), Some("1".into()), Some("30".into())],
        ]
    );
}

#[tokio::test]
async fn correlated_subquery_multirow_only_errors_when_evaluated() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, CASE WHEN o.id < 0 THEN (SELECT v FROM idup WHERE idup.k = o.k) END \
             FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, None), (2, None), (3, None), (4, None)])
    );
    // Rows that do not reach key 1 are fine even without a CASE.
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT v FROM idup WHERE idup.k = o.k) FROM o WHERE o.id > 1 ORDER BY id"
        )
        .await,
        expect(&[(2, Some("20")), (3, None), (4, None)])
    );
    let message = error(
        &c,
        "SELECT id, (SELECT v FROM idup WHERE idup.k = o.k) FROM o ORDER BY id",
    )
    .await;
    assert!(
        message.contains("more than one row returned by a subquery used as an expression"),
        "{message}"
    );
}

#[tokio::test]
async fn unsupported_correlation_is_clear() {
    let c = client().await;
    for sql in [
        "SELECT id, (SELECT v FROM i WHERE i.k > o.k) FROM o",
        "SELECT id, (SELECT v FROM i WHERE i.k = o.k ORDER BY v LIMIT 1) FROM o",
        "SELECT id, (SELECT max(v) FROM i WHERE i.k = o.k GROUP BY i.k) FROM o",
        "SELECT id, (SELECT (SELECT max(j.w) FROM j WHERE j.v = o.id) FROM i WHERE i.k = o.k) \
         FROM o",
    ] {
        let message = error(&c, sql).await;
        assert!(message.contains("correlated subquery"), "{sql}: {message}");
        assert!(!message.contains("column not found"), "{sql}: {message}");
    }
}

#[tokio::test]
async fn uncorrelated_select_subquery_unchanged() {
    let c = client().await;
    assert_eq!(
        rows(&c, "SELECT id, (SELECT max(v) FROM i) FROM o ORDER BY id").await,
        expect(&[
            (1, Some("50")),
            (2, Some("50")),
            (3, Some("50")),
            (4, Some("50"))
        ])
    );
    let plan = rows(&c, "EXPLAIN SELECT id, (SELECT max(v) FROM i) FROM o")
        .await
        .into_iter()
        .flatten()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!plan.contains("Join"), "{plan}");
    // And the correlated form does plan a join.
    let plan = rows(
        &c,
        "EXPLAIN SELECT id, (SELECT v FROM i WHERE i.k = o.k) FROM o",
    )
    .await
    .into_iter()
    .flatten()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n");
    assert!(plan.contains("Join"), "{plan}");
}

#[tokio::test]
async fn reflection_like_shape() {
    let c = client().await;
    // Outer FROM is a join; ORDER BY reads a column that is not selected;
    // one subquery sits in a CASE condition next to an outer-only test, one
    // in a branch with a guard; the subquery FROM is a join.
    assert_eq!(
        rows(
            &c,
            "SELECT o.k AS key, \
             CASE WHEN o.flag THEN (SELECT i.v FROM i WHERE o.flag AND i.k = o.k) ELSE NULL END AS d, \
             CASE WHEN (o.k != 0 AND (SELECT i.v FROM i WHERE i.k = o.k) != o.k) \
             THEN (SELECT j.w FROM i JOIN j ON j.v = i.v WHERE i.k = o.k) ELSE NULL END AS w \
             FROM o LEFT JOIN idup ON idup.k = o.k AND idup.v = 20 \
             ORDER BY o.id"
        )
        .await,
        vec![
            vec![Some("1".into()), Some("10".into()), Some("ten".into())],
            vec![Some("2".into()), None, None],
            vec![Some("3".into()), None, Some("thirty".into())],
            vec![None, None, None],
        ]
    );
}

#[tokio::test]
async fn several_subqueries_in_one_item_and_wildcard() {
    let c = client().await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, coalesce((SELECT v FROM i WHERE i.k = o.k), 0) \
             + (SELECT count(*) FROM idup WHERE idup.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, None), (2, Some("1")), (3, Some("30")), (4, Some("0"))])
            .into_iter()
            .map(|mut r| {
                if r[0].as_deref() == Some("1") {
                    // idup has two rows for key 1, but count(*) is an aggregate.
                    r[1] = Some("12".into());
                }
                r
            })
            .collect::<Vec<_>>()
    );
    // `*` does not expand to the join's helper columns.
    let all = rows(
        &c,
        "SELECT *, (SELECT v FROM i WHERE i.k = o.k) FROM o ORDER BY id",
    )
    .await;
    assert_eq!(all[0].len(), 4, "{all:?}");
}

#[tokio::test]
async fn key_types_differ() {
    let c = client().await;
    // n32.k is INT, o.k is BIGINT.
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT s FROM n32 WHERE n32.k = o.k) FROM o ORDER BY id"
        )
        .await,
        expect(&[(1, Some("one")), (2, Some("two")), (3, None), (4, None)])
    );
}

#[tokio::test]
async fn extended_protocol_plans_the_same() {
    let c = client().await;
    let got: Vec<(String, Option<String>)> = c
        .query(
            "SELECT CAST(id AS VARCHAR), \
             CAST((SELECT v FROM i WHERE i.k = o.k) AS VARCHAR) AS v FROM o ORDER BY id",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".to_string(), Some("10".to_string())),
            ("2".to_string(), None),
            ("3".to_string(), Some("30".to_string())),
            ("4".to_string(), None),
        ]
    );
}
