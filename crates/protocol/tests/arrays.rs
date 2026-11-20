//! Array support and aggregate call modifiers over pgwire (G6 stage 2b-1).

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// `file.default.t(x BIGINT)` holding the rows 1, 2, 3.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "x",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let t = Arc::new(MemoryTable::new(
        vec![ColumnInfo {
            name: "x".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }],
        vec![batch],
    ));
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table("t", t);
    // `dt(d DATE)` holding 2024-01-01.
    let dschema = Arc::new(Schema::new(vec![Field::new(
        "d",
        ArrowDataType::Date32,
        false,
    )]));
    let dbatch = RecordBatch::try_new(
        dschema,
        vec![Arc::new(Date32Array::from(vec![19723])) as ArrayRef],
    )
    .unwrap();
    default_schema.register_table(
        "dt",
        Arc::new(MemoryTable::new(
            vec![ColumnInfo {
                name: "d".to_string(),
                data_type: DataType::Date32,
                nullable: false,
            }],
            vec![dbatch],
        )),
    );
    // Aggregation fixtures.
    let mem = |cols: Vec<(&str, ArrowDataType, ArrayRef)>| {
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
    };
    let i64s = |v: Vec<Option<i64>>| Arc::new(Int64Array::from(v)) as ArrayRef;
    let strs = |v: Vec<Option<&str>>| Arc::new(StringArray::from(v)) as ArrayRef;
    default_schema.register_table(
        "nation",
        mem(vec![
            (
                "n_nationkey",
                ArrowDataType::Int64,
                i64s((1..=6).map(Some).collect()),
            ),
            (
                "n_regionkey",
                ArrowDataType::Int64,
                i64s(vec![Some(0), Some(1), Some(0), Some(1), Some(0), Some(1)]),
            ),
            (
                "n_name",
                ArrowDataType::Utf8,
                strs(vec![
                    Some("A"),
                    Some("D"),
                    Some("B"),
                    Some("F"),
                    Some("C"),
                    Some("E"),
                ]),
            ),
        ]),
    );
    default_schema.register_table(
        "multi",
        mem(vec![
            (
                "v",
                ArrowDataType::Utf8,
                strs(vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]),
            ),
            (
                "k1",
                ArrowDataType::Int64,
                i64s(vec![Some(2), Some(1), Some(2), Some(1), Some(2)]),
            ),
            (
                "k2",
                ArrowDataType::Int64,
                i64s(vec![Some(5), None, Some(7), Some(3), None]),
            ),
        ]),
    );
    default_schema.register_table(
        "nulls",
        mem(vec![("x", ArrowDataType::Int64, i64s(vec![None, None]))]),
    );
    default_schema.register_table(
        "bools",
        mem(vec![
            (
                "g",
                ArrowDataType::Int64,
                i64s(vec![Some(1), Some(1), Some(2), Some(2), Some(3), Some(3)]),
            ),
            (
                "b",
                ArrowDataType::Boolean,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    None,
                    None,
                ])) as ArrayRef,
            ),
        ]),
    );
    // `arrs(a BIGINT[])` holding a NULL array and an empty array (ruling R1:
    // stands in for `FROM (VALUES ...) v(a)`, which Arneb does not parse).
    let item = Arc::new(Field::new("item", ArrowDataType::Int64, true));
    let mut lists =
        arrow::array::ListBuilder::new(arrow::array::Int64Builder::new()).with_field(item.clone());
    lists.append_null();
    lists.append(true);
    default_schema.register_table(
        "arrs",
        mem(vec![(
            "a",
            ArrowDataType::List(item),
            Arc::new(lists.finish()) as ArrayRef,
        )]),
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

#[tokio::test]
async fn order_by_in_other_aggregate_is_refused() {
    let client = connect(pg_server().await).await;
    let err = client
        .simple_query("SELECT sum(x ORDER BY x) FROM t")
        .await
        .expect_err("ORDER BY inside sum() must be refused");
    let message = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string());
    assert!(message.contains("ORDER BY inside"), "{message}");
}

/// Text of the first column of the first data row of a simple query.
async fn first_text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    for m in client.simple_query(sql).await.unwrap() {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            return r.get(0).map(str::to_string);
        }
    }
    panic!("no rows for {sql}");
}

#[tokio::test]
async fn array_literal_text() {
    let client = connect(pg_server().await).await;
    assert_eq!(
        first_text(&client, "SELECT ARRAY[1,2,3]").await.unwrap(),
        "{1,2,3}"
    );
    let stmt = client.prepare("SELECT ARRAY[1,2,3]").await.unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 1016);
}

#[tokio::test]
async fn array_literal_common_supertype() {
    let client = connect(pg_server().await).await;
    // Arneb types `2.5` as DECIMAL(2,1), so the common supertype of (1, 2.5)
    // is DECIMAL(..,1) and the integer element renders with the decimal
    // scale ("1.0"), where PostgreSQL's numeric prints `{1,2.5}`.
    assert_eq!(
        first_text(&client, "SELECT ARRAY[1, 2.5]").await.unwrap(),
        "{1.0,2.5}"
    );
}

#[tokio::test]
async fn empty_array_without_cast_is_refused() {
    let client = connect(pg_server().await).await;
    client
        .simple_query("SELECT ARRAY[]")
        .await
        .expect_err("ARRAY[] without a cast has no element type");
}

#[tokio::test]
async fn array_text_encoding_round_trips() {
    let client = connect(pg_server().await).await;
    // PostgreSQL's array_out double-quotes an element that is empty, equals
    // NULL (any case), or contains `,` `"` `\` `{` `}` or whitespace, and
    // backslash-escapes only `"` and `\` inside the quotes. Braces are
    // quoted, not escaped. (Derived from the documented rules: no PostgreSQL
    // was reachable on 5433 when this was written.)
    let got = first_text(
        &client,
        r#"SELECT ARRAY['', 'a,b', 'q"x', 'b\s', '{c}', 'sp ace', 'NULL', 'plain', NULL]"#,
    )
    .await
    .unwrap();
    assert_eq!(
        got,
        r#"{"","a,b","q\"x","b\\s","{c}","sp ace","NULL",plain,NULL}"#
    );
}

#[tokio::test]
async fn array_of_columns_is_evaluated_per_row() {
    let client = connect(pg_server().await).await;
    let mut got = Vec::new();
    for m in client
        .simple_query("SELECT ARRAY[x, x + 1] FROM t ORDER BY x")
        .await
        .unwrap()
    {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            got.push(r.get(0).unwrap().to_string());
        }
    }
    assert_eq!(got, ["{1,2}", "{2,3}", "{3,4}"]);
}

#[tokio::test]
async fn array_null_and_text_types() {
    let client = connect(pg_server().await).await;
    // `ARRAY[NULL]` is text[] (1009 would be PostgreSQL's; Arneb's text is
    // varchar, so varchar[] 1015).
    assert_eq!(
        first_text(&client, "SELECT ARRAY[NULL, 'a']")
            .await
            .unwrap(),
        "{NULL,a}"
    );
    let stmt = client.prepare("SELECT ARRAY['a', 'b']").await.unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 1015);
}

#[tokio::test]
async fn array_over_date_column_plans_and_evaluates_the_same_type() {
    let client = connect(pg_server().await).await;
    let sql = "SELECT ARRAY[d, '2024-01-01'] FROM dt";
    let stmt = client.prepare(sql).await.unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 1182, "date[]");
    assert_eq!(
        first_text(&client, sql).await.unwrap(),
        "{2024-01-01,2024-01-01}"
    );
}

/// Reads one backend message: (tag, body).
async fn read_msg(s: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    let tag = s.read_u8().await.unwrap();
    let len = s.read_i32().await.unwrap() as usize;
    let mut body = vec![0; len - 4];
    s.read_exact(&mut body).await.unwrap();
    (tag, body)
}

#[tokio::test]
async fn simple_query_row_description_carries_the_array_oid() {
    use tokio::io::AsyncWriteExt;
    let port = pg_server().await;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    // Startup message: protocol 3.0, user and database.
    let mut params = Vec::new();
    for (k, v) in [("user", "smoke"), ("database", "file")] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0);
    let mut startup = Vec::new();
    startup.extend_from_slice(&((8 + params.len()) as i32).to_be_bytes());
    startup.extend_from_slice(&196608i32.to_be_bytes());
    startup.extend_from_slice(&params);
    s.write_all(&startup).await.unwrap();
    while read_msg(&mut s).await.0 != b'Z' {}
    // Simple Query.
    let sql = b"SELECT ARRAY[1,2]\0";
    let mut q = vec![b'Q'];
    q.extend_from_slice(&((4 + sql.len()) as i32).to_be_bytes());
    q.extend_from_slice(sql);
    s.write_all(&q).await.unwrap();
    loop {
        let (tag, body) = read_msg(&mut s).await;
        assert_ne!(tag, b'Z', "no RowDescription before ReadyForQuery");
        if tag == b'T' {
            // i16 field count, then name\0, i32 table oid, i16 attr, i32 type oid.
            assert_eq!(i16::from_be_bytes([body[0], body[1]]), 1);
            let name_end = body[2..].iter().position(|b| *b == 0).unwrap() + 2;
            let o = name_end + 1 + 4 + 2;
            let oid = i32::from_be_bytes(body[o..o + 4].try_into().unwrap());
            assert_eq!(oid, 1016, "int8[]");
            break;
        }
    }
}

/// First column of every row of a simple query, in order.
async fn column_text(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).map(str::to_string)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn any_over_array_expression() {
    let client = connect(pg_server().await).await;
    // An `ARRAY[...]` literal on the right is desugared to an IN list; the
    // cast makes it an array-valued expression, which takes the new path.
    let got = column_text(
        &client,
        "SELECT x FROM t WHERE x = ANY (ARRAY[4 - x, 3]::bigint[]) ORDER BY x",
    )
    .await;
    assert_eq!(got, [Some("2".to_string()), Some("3".to_string())]);
    let got = column_text(
        &client,
        "SELECT x FROM t WHERE x <> ALL (ARRAY[4 - x, 3]::bigint[]) ORDER BY x",
    )
    .await;
    assert_eq!(got, [Some("1".to_string())]);
}

#[tokio::test]
async fn any_array_param_binary() {
    let client = connect(pg_server().await).await;
    let stmt = client
        .prepare("SELECT CAST(x AS VARCHAR) FROM t WHERE x = ANY($1) ORDER BY x")
        .await
        .unwrap();
    assert_eq!(stmt.params()[0].oid(), 1016, "$1 is described as int8[]");
    let rows = client.query(&stmt, &[&vec![1i64, 3]]).await.unwrap();
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, ["1", "3"]);
    let rows = client
        .query(
            "SELECT CAST(x AS VARCHAR) FROM t WHERE x = ANY($1) ORDER BY x",
            &[&Vec::<i64>::new()],
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// A parameter sent in text format.
#[derive(Debug)]
struct TextParam(&'static str);

impl tokio_postgres::types::ToSql for TextParam {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(tokio_postgres::types::IsNull::No)
    }
    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }
    fn encode_format(&self, _ty: &tokio_postgres::types::Type) -> tokio_postgres::types::Format {
        tokio_postgres::types::Format::Text
    }
    tokio_postgres::types::to_sql_checked!();
}

#[tokio::test]
async fn any_array_param_text_format() {
    let client = connect(pg_server().await).await;
    let sql = "SELECT CAST(x AS VARCHAR) FROM t WHERE x = ANY($1) ORDER BY x";
    let rows = client.query(sql, &[&TextParam("{1,3}")]).await.unwrap();
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, ["1", "3"]);
    let rows = client.query(sql, &[&TextParam("{}")]).await.unwrap();
    assert!(rows.is_empty());
    let rows = client
        .query(sql, &[&TextParam(r#"{ 2 , NULL }"#)])
        .await
        .unwrap();
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, ["2"]);
}

#[tokio::test]
async fn any_array_text_param_with_quoted_elements() {
    let client = connect(pg_server().await).await;
    let rows = client
        .query(
            "SELECT $1::text = ANY($2)",
            &[&"a b", &TextParam(r#"{"a b",c,"d\"e",NULL}"#)],
        )
        .await;
    // `$2` has no int8[] hint from the left side here; it is inferred from
    // the text operand, so the array is text[].
    let rows = rows.unwrap();
    assert!(rows[0].get::<_, bool>(0));
}

#[tokio::test]
async fn any_array_param_null_semantics() {
    let client = connect(pg_server().await).await;
    for (sql, want) in [
        ("SELECT 2 = ANY('{}'::int[])", Some("f")),
        ("SELECT 2 = ANY(ARRAY[1,NULL])", None),
        ("SELECT 2 <> ALL(ARRAY[1,NULL])", None),
        ("SELECT 1 = ANY(ARRAY[1,NULL])", Some("t")),
        ("SELECT 1 <> ALL(ARRAY[1,NULL])", Some("f")),
        ("SELECT 3 <> ALL('{}'::int[])", Some("t")),
        ("SELECT NULL = ANY('{1}'::int[])", None),
        ("SELECT NULL = ANY('{}'::int[])", Some("f")),
        ("SELECT 2 = ANY(NULL::int[])", None),
        ("SELECT 2 = ANY('{1,NULL}'::int[])", None),
        (
            "SELECT 2 = ANY(ARRAY[x, 5]::bigint[]) FROM t WHERE x = 2",
            Some("t"),
        ),
        ("SELECT 2 = ANY('{1,2}')", Some("t")),
        (
            "SELECT 5 = ANY(ARRAY[x, NULL]::bigint[]) FROM t WHERE x = 2",
            None,
        ),
    ] {
        assert_eq!(first_text(&client, sql).await.as_deref(), want, "{sql}");
    }
}

#[tokio::test]
async fn array_text_cast_parses_postgres_syntax() {
    let client = connect(pg_server().await).await;
    assert_eq!(
        first_text(&client, r#"SELECT '{1,NULL, 3}'::int[]"#)
            .await
            .unwrap(),
        "{1,NULL,3}"
    );
    assert_eq!(
        first_text(&client, r#"SELECT '{"a b",NULL,"NULL",c\,d}'::text[]"#)
            .await
            .unwrap(),
        r#"{"a b",NULL,"NULL","c,d"}"#
    );
    assert!(client.simple_query("SELECT '{1,x}'::int[]").await.is_err());
    assert!(client.simple_query("SELECT '{1,2'::int[]").await.is_err());
}

#[tokio::test]
async fn binary_scalar_param_from_tokio_postgres() {
    let client = connect(pg_server().await).await;
    let rows = client
        .query("SELECT CAST(x AS VARCHAR) FROM t WHERE x = $1", &[&2i64])
        .await
        .unwrap();
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, ["2"]);
}

#[tokio::test]
async fn constant_array_any_explains_as_an_in_list() {
    let client = connect(pg_server().await).await;
    let plan = |sql: &'static str| {
        let client = &client;
        async move {
            column_text(client, sql)
                .await
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("\n")
        }
    };
    let text = plan("EXPLAIN SELECT x FROM t WHERE x = ANY('{1,3}'::bigint[])").await;
    assert!(text.contains(" IN ("), "{text}");
    assert!(!text.contains("ANY"), "{text}");
    let text = plan("EXPLAIN SELECT x FROM t WHERE x <> ALL('{1,3}'::bigint[])").await;
    assert!(text.contains("NOT IN ("), "{text}");
    // Semantics survive the rewrite.
    for (sql, want) in [
        ("SELECT 2 = ANY('{1,NULL}'::bigint[])", None),
        ("SELECT 1 = ANY('{1,NULL}'::bigint[])", Some("t")),
        ("SELECT 2 <> ALL('{1,NULL}'::bigint[])", None),
        ("SELECT NULL = ANY('{}'::bigint[])", Some("f")),
        ("SELECT NULL <> ALL('{}'::bigint[])", Some("t")),
    ] {
        assert_eq!(first_text(&client, sql).await.as_deref(), want, "{sql}");
    }
    // An empty constant array filters every row (= ANY) or none (<> ALL).
    assert!(
        column_text(&client, "SELECT x FROM t WHERE x = ANY('{}'::bigint[])")
            .await
            .is_empty()
    );
    assert_eq!(
        column_text(&client, "SELECT x FROM t WHERE x <> ALL('{}'::bigint[])")
            .await
            .len(),
        3
    );
}

/// Every row of a simple query as text columns.
async fn all_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn array_agg_order_by() {
    let client = connect(pg_server().await).await;
    let got = all_rows(
        &client,
        "SELECT n_regionkey, array_agg(n_name ORDER BY n_name DESC) FROM nation GROUP BY 1 ORDER BY 1",
    )
    .await;
    let want = |a: &str, b: &str| vec![Some(a.to_string()), Some(b.to_string())];
    assert_eq!(got, [want("0", "{C,B,A}"), want("1", "{F,E,D}")]);
    let stmt = client
        .prepare("SELECT array_agg(n_name ORDER BY n_name) FROM nation")
        .await
        .unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 1015, "varchar[]");
}

#[tokio::test]
async fn array_agg_order_by_column_not_otherwise_selected() {
    let client = connect(pg_server().await).await;
    // `n_nationkey` is used only as a sort key; column pruning must keep it.
    assert_eq!(
        first_text(
            &client,
            "SELECT array_agg(n_name ORDER BY n_nationkey DESC) FROM nation"
        )
        .await
        .unwrap(),
        "{E,C,F,B,D,A}"
    );
    // Grouped, with the key also absent from the GROUP BY output.
    let got = column_text(
        &client,
        "SELECT array_agg(n_name ORDER BY n_nationkey DESC) FROM nation GROUP BY n_regionkey ORDER BY n_regionkey",
    )
    .await;
    assert_eq!(
        got,
        [Some("{C,B,A}".to_string()), Some("{E,F,D}".to_string())]
    );
}

#[tokio::test]
async fn array_agg_multi_key_order_with_nulls_last() {
    let client = connect(pg_server().await).await;
    // k1 asc: (b,d) then (a,c,e). Within k1=1: k2 DESC NULLS LAST: d(3), b(NULL).
    // Within k1=2: c(7), a(5), e(NULL).
    assert_eq!(
        first_text(
            &client,
            "SELECT array_agg(v ORDER BY k1, k2 DESC NULLS LAST) FROM multi"
        )
        .await
        .unwrap(),
        "{d,b,c,a,e}"
    );
    // Default for ASC is NULLS LAST; explicit NULLS FIRST flips it.
    assert_eq!(
        first_text(&client, "SELECT array_agg(v ORDER BY k1, k2) FROM multi")
            .await
            .unwrap(),
        "{d,b,a,c,e}"
    );
    assert_eq!(
        first_text(
            &client,
            "SELECT array_agg(v ORDER BY k1, k2 NULLS FIRST) FROM multi"
        )
        .await
        .unwrap(),
        "{b,d,e,a,c}"
    );
}

#[tokio::test]
async fn array_agg_nulls_and_empty_input() {
    let client = connect(pg_server().await).await;
    assert_eq!(
        first_text(&client, "SELECT array_agg(x) FROM nulls")
            .await
            .unwrap(),
        "{NULL,NULL}"
    );
    let got = column_text(&client, "SELECT array_agg(x) FROM t WHERE false").await;
    assert_eq!(got, [None], "one row, NULL");
    let got = column_text(&client, "SELECT array_agg(x ORDER BY x) FROM t WHERE false").await;
    assert_eq!(got, [None]);
}

#[tokio::test]
async fn array_agg_distinct_is_refused() {
    let client = connect(pg_server().await).await;
    let err = client
        .simple_query("SELECT array_agg(DISTINCT x) FROM t")
        .await
        .expect_err("array_agg(DISTINCT) must be refused");
    let message = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string());
    assert!(message.contains("DISTINCT"), "{message}");
}

#[tokio::test]
async fn bool_and_basic() {
    let client = connect(pg_server().await).await;
    let got = all_rows(
        &client,
        "SELECT g, bool_and(b) FROM bools GROUP BY g ORDER BY g",
    )
    .await;
    let row = |g: &str, v: Option<&str>| vec![Some(g.to_string()), v.map(str::to_string)];
    assert_eq!(
        got,
        [row("1", Some("t")), row("2", Some("f")), row("3", None)]
    );
    assert_eq!(
        column_text(&client, "SELECT bool_and(b) FROM bools WHERE g = 1").await,
        [Some("t".to_string())]
    );
    assert_eq!(
        column_text(&client, "SELECT bool_and(b) FROM bools WHERE g = 2").await,
        [Some("f".to_string())]
    );
    assert_eq!(
        column_text(&client, "SELECT bool_and(b) FROM bools WHERE g = 3").await,
        [None]
    );
}

/// Message of a query that must fail.
async fn error_message(client: &tokio_postgres::Client, sql: &str) -> String {
    let err = client
        .simple_query(sql)
        .await
        .expect_err(&format!("{sql} must be refused"));
    err.as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string())
}

fn texts(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

#[tokio::test]
async fn unnest_literal() {
    let client = connect(pg_server().await).await;
    assert_eq!(
        column_text(&client, "SELECT unnest(ARRAY[1,2,3])").await,
        texts(&["1", "2", "3"])
    );
    let stmt = client.prepare("SELECT unnest(ARRAY[1,2,3])").await.unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 20, "int8");
}

#[tokio::test]
async fn generate_subscripts_basic() {
    let client = connect(pg_server().await).await;
    assert_eq!(
        column_text(&client, "SELECT generate_subscripts(ARRAY['a','b'], 1)").await,
        texts(&["1", "2"])
    );
    let stmt = client
        .prepare("SELECT generate_subscripts(ARRAY['a','b'], 1)")
        .await
        .unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 23, "int4");
}

#[tokio::test]
async fn srf_zip_pads_with_null() {
    let client = connect(pg_server().await).await;
    let got = all_rows(
        &client,
        "SELECT 'x' AS c, unnest(ARRAY[1,2,3]), unnest(ARRAY['a'])",
    )
    .await;
    let row = |n: &str, s: Option<&str>| {
        vec![
            Some("x".to_string()),
            Some(n.to_string()),
            s.map(str::to_string),
        ]
    };
    assert_eq!(got, [row("1", Some("a")), row("2", None), row("3", None)]);
}

#[tokio::test]
async fn srf_zip_with_input_rows_and_aliases() {
    let client = connect(pg_server().await).await;
    // Each input row repeats once per element; the SRF's alias names it,
    // and SQLAlchemy's shape (unnest + generate_subscripts side by side)
    // lines elements up with their positions.
    let got = all_rows(
        &client,
        "SELECT x, unnest(ARRAY[x, x * 10]) AS v, generate_subscripts(ARRAY[x, x * 10], 1) AS i \
         FROM t WHERE x < 3",
    )
    .await;
    let row = |a: &str, b: &str, c: &str| texts(&[a, b, c]);
    assert_eq!(
        got,
        [
            row("1", "1", "1"),
            row("1", "10", "2"),
            row("2", "2", "1"),
            row("2", "20", "2")
        ]
    );
    let stmt = client
        .prepare("SELECT unnest(ARRAY[x]) AS v FROM t")
        .await
        .unwrap();
    assert_eq!(stmt.columns()[0].name(), "v");
}

#[tokio::test]
async fn srf_null_and_empty_yield_no_rows() {
    let client = connect(pg_server().await).await;
    assert!(column_text(&client, "SELECT unnest(a) FROM arrs")
        .await
        .is_empty());
    assert!(
        column_text(&client, "SELECT generate_subscripts(a, 1) FROM arrs")
            .await
            .is_empty()
    );
    assert!(column_text(&client, "SELECT unnest(CAST('{}' AS int[]))")
        .await
        .is_empty());
}

#[tokio::test]
async fn srf_over_array_agg_subquery() {
    let client = connect(pg_server().await).await;
    assert_eq!(
        column_text(
            &client,
            "SELECT unnest(arr) FROM (SELECT array_agg(n_name ORDER BY n_name) arr FROM nation) s"
        )
        .await,
        texts(&["A", "B", "C", "D", "E", "F"])
    );
}

#[tokio::test]
async fn srf_in_where_is_refused() {
    let client = connect(pg_server().await).await;
    let message = error_message(&client, "SELECT x FROM t WHERE unnest(ARRAY[x]) = 1").await;
    assert!(
        message.contains("set-returning function") && message.contains("not supported here"),
        "{message}"
    );
}

#[tokio::test]
async fn srf_nested_in_expression_is_refused() {
    let client = connect(pg_server().await).await;
    let message = error_message(&client, "SELECT unnest(ARRAY[1]) + 1").await;
    assert!(
        message.contains("set-returning function") && message.contains("not supported here"),
        "{message}"
    );
}

#[tokio::test]
async fn generate_subscripts_dim_other_than_1_is_refused() {
    let client = connect(pg_server().await).await;
    let message = error_message(&client, "SELECT generate_subscripts(ARRAY[1], 2)").await;
    assert!(
        message.contains("generate_subscripts") && message.contains("dimension 1"),
        "{message}"
    );
}

#[tokio::test]
async fn srf_shapes_used_by_reflection() {
    let client = connect(pg_server().await).await;
    // Over an aggregate in the same SELECT.
    assert_eq!(
        column_text(
            &client,
            "SELECT unnest(array_agg(x ORDER BY x DESC)) FROM t"
        )
        .await,
        texts(&["3", "2", "1"])
    );
    // In a derived table filtered, joined and ordered outside.
    let got = all_rows(
        &client,
        "SELECT s.k, s.v, t.x FROM \
         (SELECT x AS k, unnest(ARRAY[x, x * 10]) AS v, generate_subscripts(ARRAY[x, x * 10], 1) AS i \
          FROM t) s JOIN t ON t.x = s.k \
         WHERE s.i = 2 AND s.v > 10 ORDER BY s.v",
    )
    .await;
    assert_eq!(got, [texts(&["2", "20", "2"]), texts(&["3", "30", "3"])]);
    // The same SRF twice, and next to `*`.
    let got = all_rows(
        &client,
        "SELECT *, unnest(ARRAY[x]), unnest(ARRAY[x]) FROM t WHERE x = 1",
    )
    .await;
    assert_eq!(got, [texts(&["1", "1", "1"])]);
    let stmt = client
        .prepare("SELECT unnest(ARRAY[x]), generate_subscripts(ARRAY[x], 1) FROM t")
        .await
        .unwrap();
    let names: Vec<&str> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["unnest", "generate_subscripts"]);
}
