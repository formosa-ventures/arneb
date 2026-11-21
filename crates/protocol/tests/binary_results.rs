//! Result columns honor the format codes the client sent in Bind (G6 stage
//! 2b-2, Task 10): text, binary, or one code per column. The simple query
//! protocol stays text.
//!
//! tokio-postgres `query()` asks for every result column in binary, as
//! pgJDBC does from a server-prepared statement's 5th execution on.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int64Builder, ListBuilder, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::{Buf, BytesMut};
use chrono::{NaiveDate, NaiveDateTime};
use postgres_protocol::message::{backend, frontend};
use rust_decimal::Decimal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::NoTls;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// A 38-digit value at scale 2: past `rust_decimal`'s 28 digits.
const BIG: i128 = 12_345_678_901_234_567_890_123_456_789_012_345_678;

/// `file.default.typed`: one row of known values, then one row of NULLs.
fn typed() -> Arc<MemoryTable> {
    let mut la = ListBuilder::new(Int64Builder::new());
    la.append_value([Some(1), None, Some(-3)]);
    la.append_null();
    let mut ls = ListBuilder::new(StringBuilder::new());
    ls.append_value([Some("a"), Some("b c"), None]);
    ls.append_null();
    let ts = NaiveDate::from_ymd_opt(2024, 3, 9)
        .unwrap()
        .and_hms_micro_opt(13, 45, 7, 123_456)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    let cols: Vec<(&str, ArrayRef)> = vec![
        ("s", Arc::new(Int16Array::from(vec![Some(-7), None]))),
        ("i", Arc::new(Int32Array::from(vec![Some(70_000), None]))),
        ("l", Arc::new(Int64Array::from(vec![Some(1 << 40), None]))),
        ("r", Arc::new(Float32Array::from(vec![Some(1.5), None]))),
        ("d", Arc::new(Float64Array::from(vec![Some(-2.25), None]))),
        ("b", Arc::new(BooleanArray::from(vec![Some(true), None]))),
        ("t", Arc::new(StringArray::from(vec![Some("héllo"), None]))),
        (
            "n",
            Arc::new(
                Decimal128Array::from(vec![Some(-12_345), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ),
        (
            "nb",
            Arc::new(
                Decimal128Array::from(vec![Some(BIG), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ),
        ("dt", Arc::new(Date32Array::from(vec![Some(19_791), None]))),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![Some(ts), None])),
        ),
        (
            "by",
            Arc::new(BinaryArray::from(vec![Some(&b"\x00\xffab"[..]), None])),
        ),
        ("la", Arc::new(la.finish())),
        ("ls", Arc::new(ls.finish())),
    ];
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let infos = cols
        .iter()
        .map(|(n, a)| ColumnInfo {
            name: n.to_string(),
            data_type: DataType::try_from(a.data_type().clone()).unwrap(),
            nullable: true,
        })
        .collect();
    let batch = RecordBatch::try_new(schema, cols.into_iter().map(|(_, a)| a).collect()).unwrap();
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

fn nation() -> Arc<MemoryTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", ArrowDataType::Int32, false),
        Field::new("n_name", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ALGERIA", "ARGENTINA"])),
        ],
    )
    .unwrap();
    Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "n_nationkey".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "n_name".into(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ],
        vec![batch],
    ))
}

async fn pg_server() -> u16 {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("typed", typed());
    schema.register_table("nation", nation());
    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", schema);
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        Arc::new(cm),
        Arc::new(registry),
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

/// The raw binary bytes of a `numeric` value, for a value past
/// `rust_decimal`'s range: decoding succeeds and yields the base-10000
/// digits.
#[derive(Debug, PartialEq)]
struct RawNumeric {
    weight: i16,
    negative: bool,
    dscale: u16,
    digits: Vec<i16>,
}

impl<'a> FromSql<'a> for RawNumeric {
    fn from_sql(_: &Type, mut raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let ndigits = raw.get_i16();
        let weight = raw.get_i16();
        let sign = raw.get_u16();
        let dscale = raw.get_u16();
        let digits = (0..ndigits).map(|_| raw.get_i16()).collect();
        if !raw.is_empty() {
            return Err("trailing bytes after numeric digits".into());
        }
        Ok(Self {
            weight,
            negative: sign == 0x4000,
            dscale,
            digits,
        })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

const TYPED_SQL: &str = "SELECT s, i, l, r, d, b, t, n, nb, CAST(nb AS VARCHAR) AS nbs, dt, ts, \
     by, la, ls, CAST(t AS CHAR(7)) AS c FROM typed";

#[tokio::test]
async fn tokio_postgres_query_reads_binary_results() {
    let client = connect(pg_server().await).await;
    let rows = client.query(TYPED_SQL, &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let (v, nulls) = (&rows[0], &rows[1]);

    assert_eq!(v.get::<_, i16>("s"), -7);
    assert_eq!(v.get::<_, i32>("i"), 70_000);
    assert_eq!(v.get::<_, i64>("l"), 1 << 40);
    assert_eq!(v.get::<_, f32>("r"), 1.5);
    assert_eq!(v.get::<_, f64>("d"), -2.25);
    assert!(v.get::<_, bool>("b"));
    assert_eq!(v.get::<_, String>("t"), "héllo");
    assert_eq!(v.get::<_, String>("c"), "héllo  ");
    assert_eq!(v.get::<_, Decimal>("n"), Decimal::new(-12_345, 2));
    // 38 digits: decode the raw form and compare it to the text sibling.
    let big: RawNumeric = v.get("nb");
    assert_eq!(
        v.get::<_, String>("nbs"),
        "123456789012345678901234567890123456.78"
    );
    assert_eq!(
        big,
        RawNumeric {
            weight: 8,
            negative: false,
            dscale: 2,
            digits: vec![1234, 5678, 9012, 3456, 7890, 1234, 5678, 9012, 3456, 7800],
        }
    );
    assert_eq!(
        v.get::<_, NaiveDate>("dt"),
        NaiveDate::from_ymd_opt(2024, 3, 9).unwrap()
    );
    assert_eq!(
        v.get::<_, NaiveDateTime>("ts"),
        NaiveDate::from_ymd_opt(2024, 3, 9)
            .unwrap()
            .and_hms_micro_opt(13, 45, 7, 123_456)
            .unwrap()
    );
    assert_eq!(v.get::<_, Vec<u8>>("by"), b"\x00\xffab".to_vec());
    assert_eq!(
        v.get::<_, Vec<Option<i64>>>("la"),
        vec![Some(1), None, Some(-3)]
    );
    assert_eq!(
        v.get::<_, Vec<Option<String>>>("ls"),
        vec![Some("a".to_string()), Some("b c".to_string()), None]
    );

    for col in 0..nulls.len() {
        assert!(
            nulls.try_get::<_, Option<RawAny>>(col).unwrap().is_none(),
            "column {col} of the NULL row is not NULL"
        );
    }
}

/// Any value, for the NULL checks.
struct RawAny;

impl<'a> FromSql<'a> for RawAny {
    fn from_sql(_: &Type, _: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(RawAny)
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

#[tokio::test]
async fn small_numerics_in_binary() {
    let client = connect(pg_server().await).await;
    let row = client
        .query_one(
            "SELECT CAST(0.05 AS DECIMAL(10, 2)) AS small, CAST(0 AS DECIMAL(10, 2)) AS zero \
             FROM typed WHERE s = -7",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Decimal>("small"), Decimal::new(5, 2));
    assert_eq!(row.get::<_, Decimal>("zero"), Decimal::new(0, 2));
}

/// Writes `messages` and reads backend messages until ReadyForQuery,
/// returning them in order.
async fn exchange(stream: &mut TcpStream, out: BytesMut) -> Vec<backend::Message> {
    stream.write_all(&out).await.unwrap();
    let mut buf = BytesMut::new();
    let mut messages = Vec::new();
    loop {
        while let Some(message) = backend::Message::parse(&mut buf).unwrap() {
            let done = matches!(message, backend::Message::ReadyForQuery(_));
            messages.push(message);
            if done {
                return messages;
            }
        }
        if stream.read_buf(&mut buf).await.unwrap() == 0 {
            panic!("connection closed");
        }
    }
}

async fn raw_connect(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut out = BytesMut::new();
    frontend::startup_message([("user", "smoke"), ("database", "file")], &mut out).unwrap();
    let messages = exchange(&mut stream, out).await;
    assert!(messages
        .iter()
        .any(|m| matches!(m, backend::Message::AuthenticationOk)));
    stream
}

/// Parse + Bind (`result_formats`) + Describe portal + Execute + Sync of
/// `sql`: the RowDescription format codes and the first DataRow's cells.
async fn bound_query(
    stream: &mut TcpStream,
    sql: &str,
    result_formats: &[i16],
) -> Result<(Vec<i16>, Vec<Option<Vec<u8>>>), String> {
    use fallible_iterator::FallibleIterator;

    let mut out = BytesMut::new();
    frontend::parse("", sql, std::iter::empty(), &mut out).unwrap();
    frontend::bind(
        "",
        "",
        std::iter::empty::<i16>(),
        std::iter::empty::<()>(),
        |_: (),
         _: &mut BytesMut|
         -> Result<postgres_protocol::IsNull, Box<dyn Error + Sync + Send>> {
            unreachable!()
        },
        result_formats.iter().copied(),
        &mut out,
    )
    .map_err(|_| "bind")
    .unwrap();
    frontend::describe(b'P', "", &mut out).unwrap();
    frontend::execute("", 0, &mut out).unwrap();
    frontend::sync(&mut out);
    let mut formats = None;
    let mut cells = None;
    for message in exchange(stream, out).await {
        match message {
            backend::Message::ErrorResponse(body) => {
                let fields: Vec<_> = body.fields().collect().unwrap();
                let code = fields.iter().find(|f| f.type_() == b'C').unwrap();
                let msg = fields.iter().find(|f| f.type_() == b'M').unwrap();
                return Err(format!(
                    "{} {}",
                    String::from_utf8_lossy(code.value_bytes()),
                    String::from_utf8_lossy(msg.value_bytes())
                ));
            }
            backend::Message::RowDescription(body) => {
                formats = Some(
                    body.fields()
                        .map(|f| Ok(f.format()))
                        .collect::<Vec<_>>()
                        .unwrap(),
                );
            }
            backend::Message::DataRow(body) if cells.is_none() => {
                let buf = body.buffer();
                cells = Some(
                    body.ranges()
                        .map(|r| Ok(r.map(|r| buf[r].to_vec())))
                        .collect::<Vec<_>>()
                        .unwrap(),
                );
            }
            _ => {}
        }
    }
    Ok((formats.unwrap(), cells.unwrap_or_default()))
}

#[tokio::test]
async fn mixed_formats_per_column() {
    let mut stream = raw_connect(pg_server().await).await;
    let (formats, cells) = bound_query(&mut stream, "SELECT l, l FROM typed WHERE s = -7", &[0, 1])
        .await
        .unwrap();
    assert_eq!(formats, vec![0, 1]);
    assert_eq!(cells[0].as_deref(), Some(&b"1099511627776"[..]));
    assert_eq!(cells[1].as_deref(), Some(&(1i64 << 40).to_be_bytes()[..]));

    // One code applies to every column; none means text.
    let (formats, cells) = bound_query(&mut stream, "SELECT i, t FROM typed WHERE s = -7", &[1])
        .await
        .unwrap();
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(cells[0].as_deref(), Some(&70_000i32.to_be_bytes()[..]));
    assert_eq!(cells[1].as_deref(), Some("héllo".as_bytes()));
    let (formats, cells) = bound_query(&mut stream, "SELECT i, t FROM typed WHERE s = -7", &[])
        .await
        .unwrap();
    assert_eq!(formats, vec![0, 0]);
    assert_eq!(cells[0].as_deref(), Some(&b"70000"[..]));

    // A format count that is neither 0, 1 nor the column count is refused.
    let err = bound_query(&mut stream, "SELECT i, t, l FROM typed", &[0, 1])
        .await
        .unwrap_err();
    assert!(err.starts_with("08P01"), "{err}");
}

#[tokio::test]
async fn simple_query_stays_text() {
    let client = connect(pg_server().await).await;
    let messages = client
        .simple_query("SELECT l, n, dt, ts, la, b FROM typed WHERE s = -7")
        .await
        .unwrap();
    let row = messages
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .unwrap();
    let cells: Vec<_> = (0..row.len()).map(|i| row.get(i).unwrap()).collect();
    assert_eq!(
        cells,
        vec![
            "1099511627776",
            "-123.45",
            "2024-03-09",
            "2024-03-09 13:45:07.123456",
            "{1,NULL,-3}",
            "t"
        ]
    );
}

/// pgJDBC's `getPrimaryKeys` statement (the `result.TABLE_CAT` query in
/// `metabase-captured.sql`): Metabase's sync runs it once per table, and
/// pgJDBC asks for binary results from the 5th execution on.
#[tokio::test]
async fn metabase_primary_key_query_survives_binary_executions() {
    let sql = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/superpowers/specs/2026-11-21-g6-stage-2b2/metabase-captured.sql"
    ))
    .unwrap()
    .lines()
    .find(|l| l.contains("result.TABLE_CAT") && l.contains("ct.relname = 'nation'"))
    .unwrap()
    .to_string();
    let client = connect(pg_server().await).await;
    let stmt = client.prepare(&sql).await.unwrap();
    for _ in 0..6 {
        let rows = client.query(&stmt, &[]).await.unwrap();
        for row in rows {
            for col in 0..row.len() {
                row.try_get::<_, Option<RawAny>>(col).unwrap();
            }
        }
    }
    // A statement with rows, executed 6 times in binary, decodes each time.
    let stmt = client
        .prepare("SELECT n_nationkey, n_name FROM nation ORDER BY n_nationkey")
        .await
        .unwrap();
    for _ in 0..6 {
        let rows = client.query(&stmt, &[]).await.unwrap();
        assert_eq!(rows[1].get::<_, i32>(0), 1);
        assert_eq!(rows[1].get::<_, String>(1), "ARGENTINA");
    }
}

/// A statement with placeholders is described before its parameters are
/// bound: `$1 + 1` is described as `text` but executes as `int8`. Binary
/// results follow the described types, which the client decodes by.
#[tokio::test]
async fn parameterized_binary_results_follow_the_described_types() {
    let client = connect(pg_server().await).await;
    let stmt = client
        .prepare("SELECT $1 + 1 AS v, l + $1 AS w, t FROM typed WHERE s = -7")
        .await
        .unwrap();
    let described: Vec<Type> = stmt.columns().iter().map(|c| c.type_().clone()).collect();
    assert_eq!(described, vec![Type::TEXT, Type::TEXT, Type::VARCHAR]);
    for _ in 0..2 {
        let row = client.query_one(&stmt, &[&5i64]).await.unwrap();
        assert_eq!(row.get::<_, String>("v"), "6");
        assert_eq!(row.get::<_, String>("w"), "1099511627781");
        assert_eq!(row.get::<_, String>("t"), "héllo");
    }
    let row = client
        .query_one("SELECT i FROM typed WHERE i = $1", &[&70_000i32])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 70_000);
}
