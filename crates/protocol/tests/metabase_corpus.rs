//! The Metabase v0.64.8.3 corpus (G6 stage 2b-2, Tier 1): every statement
//! Metabase and pgJDBC sent during a sync and 25 query-builder questions
//! against PostgreSQL 17, replayed in order on one Arneb connection.
//!
//! Inputs are read in place from the spec directory
//! (`docs/superpowers/specs/2026-11-21-g6-stage-2b2/`, via
//! `CARGO_MANIFEST_DIR/../../docs/...`), not copied:
//! - `metabase-captured.sql`: 79 statements separated by `\n;;;;\n`;
//! - `metabase-pg17-expected.json`: PostgreSQL 17's rows for the 35
//!   metadata statements (`index` is the 0-based statement).
//!
//! The memory tables are `file.default.{nation, region, part, supplier,
//! partsupp, customer, orders, lineitem}` with the capture's column types
//! (PostgreSQL's unconstrained `numeric` is `DECIMAL(15,2)` here) and rows:
//! the 25 TPC-H nations, 2 orders, 1 customer, the rest empty. An empty
//! `public` schema stands for the one every PostgreSQL database has.
//!
//! Golden rows are compared with oid-valued columns masked, as multisets
//! unless the statement has `ORDER BY`. Every column or column name that
//! legitimately differs is listed, with its reason, in
//! [`KNOWN_DIFFERENCES`] or [`KNOWN_HEADER_DIFFERENCES`]; an entry that no
//! longer differs fails the test.
//!
//! Some statements inline the oids PostgreSQL gave the tables. The test maps
//! each captured oid to its table through the golden rows (they carry the
//! oid beside `relname`) and substitutes Arneb's oid for that table.
//!
//! Both tests assert the process-global `pg_catalog` fallback counter, so
//! they live in their own binary.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// Columns that legitimately differ from PostgreSQL 17, by statement index.
const KNOWN_DIFFERENCES: &[(usize, &str, &str)] = &[
    (
        3,
        "current_catalog",
        "the capture's PostgreSQL database was named `arneb`; Arneb reports its session catalog (`file`)",
    ),
    (
        7,
        "TABLE_CATALOG",
        "current_database(): the capture's database was `arneb`, Arneb's session catalog is `file`",
    ),
    (
        9,
        "estimated_row_count",
        "Arneb has no table statistics: pg_stat_user_tables.n_live_tup is 0, so NULLIF yields NULL (spec non-goal)",
    ),
];

/// Column names that legitimately differ, values still compared:
/// `(statements, PostgreSQL's name, Arneb's name, reason)`.
const KNOWN_HEADER_DIFFERENCES: &[(&[usize], &str, &str, &str)] = &[
    (
        &[1],
        "?column?",
        "1",
        "Arneb names an unaliased expression after its text; PostgreSQL names a constant ?column?",
    ),
    (
        PGJDBC_COLUMN_FLAGS,
        "?column?",
        "attnotnull OR typtype = 'd' AND typnotnull",
        "Arneb names an unaliased expression after its text; PostgreSQL names an operator expression ?column?",
    ),
    (
        PGJDBC_COLUMN_FLAGS,
        "?column?",
        "attidentity != '' OR pg_get_expr(adbin, adrelid) LIKE '%nextval(%'",
        "Arneb names an unaliased expression after its text; PostgreSQL names an operator expression ?column?",
    ),
];

/// pgJDBC's result-set metadata query (nullability and auto-increment of
/// the columns of a result), sent once per previewed table. It reads the
/// result by position.
const PGJDBC_COLUMN_FLAGS: &[usize] = &[32, 34, 36, 38, 40, 42, 44, 46, 49, 52, 54];

/// `numeric` in the capture DDL.
const NUMERIC: DataType = DataType::Decimal128 {
    precision: 15,
    scale: 2,
};

type Columns = &'static [(&'static str, DataType)];

const NATION: Columns = &[
    ("n_nationkey", DataType::Int32),
    ("n_name", DataType::Utf8),
    ("n_regionkey", DataType::Int32),
    ("n_comment", DataType::Utf8),
];
const REGION: Columns = &[
    ("r_regionkey", DataType::Int32),
    ("r_name", DataType::Utf8),
    ("r_comment", DataType::Utf8),
];
const PARTSUPP: Columns = &[
    ("ps_partkey", DataType::Int64),
    ("ps_suppkey", DataType::Int64),
    ("ps_availqty", DataType::Int64),
    ("ps_supplycost", NUMERIC),
    ("ps_comment", DataType::Utf8),
];
const SUPPLIER: Columns = &[
    ("s_suppkey", DataType::Int64),
    ("s_name", DataType::Utf8),
    ("s_address", DataType::Utf8),
    ("s_nationkey", DataType::Int32),
    ("s_phone", DataType::Utf8),
    ("s_acctbal", NUMERIC),
    ("s_comment", DataType::Utf8),
];
const LINEITEM: Columns = &[
    ("l_orderkey", DataType::Int64),
    ("l_partkey", DataType::Int64),
    ("l_suppkey", DataType::Int64),
    ("l_linenumber", DataType::Int64),
    ("l_quantity", NUMERIC),
    ("l_extendedprice", NUMERIC),
    ("l_discount", NUMERIC),
    ("l_tax", NUMERIC),
    ("l_returnflag", DataType::Utf8),
    ("l_linestatus", DataType::Utf8),
    ("l_shipdate", DataType::Date32),
    ("l_commitdate", DataType::Date32),
    ("l_receiptdate", DataType::Date32),
    ("l_shipinstruct", DataType::Utf8),
    ("l_shipmode", DataType::Utf8),
    ("l_comment", DataType::Utf8),
];
const PART: Columns = &[
    ("p_partkey", DataType::Int64),
    ("p_name", DataType::Utf8),
    ("p_mfgr", DataType::Utf8),
    ("p_brand", DataType::Utf8),
    ("p_type", DataType::Utf8),
    ("p_size", DataType::Int32),
    ("p_container", DataType::Utf8),
    ("p_retailprice", NUMERIC),
    ("p_comment", DataType::Utf8),
];
const ORDERS: Columns = &[
    ("o_orderkey", DataType::Int64),
    ("o_custkey", DataType::Int64),
    ("o_orderstatus", DataType::Utf8),
    ("o_totalprice", NUMERIC),
    ("o_orderdate", DataType::Date32),
    ("o_orderpriority", DataType::Utf8),
    ("o_clerk", DataType::Utf8),
    ("o_shippriority", DataType::Int32),
    ("o_comment", DataType::Utf8),
];
const CUSTOMER: Columns = &[
    ("c_custkey", DataType::Int64),
    ("c_name", DataType::Utf8),
    ("c_address", DataType::Utf8),
    ("c_nationkey", DataType::Int32),
    ("c_phone", DataType::Utf8),
    ("c_acctbal", NUMERIC),
    ("c_mktsegment", DataType::Utf8),
    ("c_comment", DataType::Utf8),
];

/// The 25 TPC-H nations, as loaded into the capture instance.
const NATION_ROWS: &[[&str; 4]] = &[
    ["0", "ALGERIA", "0", " haggle. carefully final deposits detect slyly agai"],
    ["1", "ARGENTINA", "1", "al foxes promise slyly according to the regular accounts. bold requests alon"],
    ["2", "BRAZIL", "1", "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special "],
    ["3", "CANADA", "1", "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold"],
    ["4", "EGYPT", "4", "y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d"],
    ["5", "ETHIOPIA", "0", "ven packages wake quickly. regu"],
    ["6", "FRANCE", "3", "refully final requests. regular, ironi"],
    ["7", "GERMANY", "3", "l platelets. regular accounts x-ray: unusual, regular acco"],
    ["8", "INDIA", "2", "ss excuses cajole slyly across the packages. deposits print aroun"],
    ["9", "INDONESIA", "2", " slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull"],
    ["10", "IRAN", "4", "efully alongside of the slyly final dependencies. "],
    ["11", "IRAQ", "4", "nic deposits boost atop the quickly final requests? quickly regula"],
    ["12", "JAPAN", "2", "ously. final, express gifts cajole a"],
    ["13", "JORDAN", "4", "ic deposits are blithely about the carefully regular pa"],
    ["14", "KENYA", "0", " pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t"],
    ["15", "MOROCCO", "0", "rns. blithely bold courts among the closely regular packages use furiously bold platelets?"],
    ["16", "MOZAMBIQUE", "0", "s. ironic, unusual asymptotes wake blithely r"],
    ["17", "PERU", "1", "platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun"],
    ["18", "CHINA", "2", "c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos"],
    ["19", "ROMANIA", "3", "ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account"],
    ["20", "SAUDI ARABIA", "4", "ts. silent requests haggle. closely express packages sleep across the blithely"],
    ["21", "VIETNAM", "2", "hely enticingly express accounts. even, final "],
    ["22", "RUSSIA", "3", " requests against the platelets use never according to the quickly regular pint"],
    ["23", "UNITED KINGDOM", "3", "eans boost carefully special requests. accounts are. carefull"],
    ["24", "UNITED STATES", "1", "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be"],
];
const ORDERS_ROWS: &[[&str; 9]] = &[
    [
        "1",
        "1",
        "O",
        "100.5",
        "1996-01-02",
        "1-URGENT",
        "Clerk#1",
        "0",
        "x",
    ],
    [
        "2",
        "2",
        "F",
        "200000",
        "1995-03-04",
        "2-HIGH",
        "Clerk#2",
        "0",
        "y",
    ],
];
const CUSTOMER_ROWS: &[[&str; 8]] = &[[
    "1",
    "Customer#000000001",
    "a",
    "1",
    "p",
    "1.0",
    "BUILDING",
    "c",
]];

fn days_since_epoch(date: &str) -> i32 {
    let d = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
    (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
}

/// `"100.5"` at scale 2 is 10050.
fn decimal_units(value: &str, scale: i8) -> i128 {
    let (int, frac) = value.split_once('.').unwrap_or((value, ""));
    let mut frac = frac.to_string();
    frac.extend(std::iter::repeat_n('0', scale as usize - frac.len()));
    format!("{int}{frac}").parse().unwrap()
}

fn column_array(data_type: &DataType, cells: Vec<&str>) -> ArrayRef {
    match data_type {
        DataType::Int32 => Arc::new(Int32Array::from_iter_values(
            cells.iter().map(|c| c.parse::<i32>().unwrap()),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter_values(
            cells.iter().map(|c| c.parse::<i64>().unwrap()),
        )),
        DataType::Utf8 => Arc::new(StringArray::from(cells)),
        DataType::Date32 => Arc::new(Date32Array::from_iter_values(
            cells.iter().map(|c| days_since_epoch(c)),
        )),
        DataType::Decimal128 { precision, scale } => Arc::new(
            Decimal128Array::from_iter_values(cells.iter().map(|c| decimal_units(c, *scale)))
                .with_precision_and_scale(*precision, *scale)
                .unwrap(),
        ),
        other => unreachable!("{other:?}"),
    }
}

/// A NOT NULL memory table with `rows` (text cells, one per column).
fn table<const N: usize>(columns: Columns, rows: &[[&str; N]]) -> Arc<MemoryTable> {
    assert!(rows.is_empty() || N == columns.len());
    let infos: Vec<ColumnInfo> = columns
        .iter()
        .map(|(name, data_type)| ColumnInfo {
            name: name.to_string(),
            data_type: data_type.clone(),
            nullable: false,
        })
        .collect();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, data_type)| {
                Field::new(*name, ArrowDataType::from(data_type.clone()), false)
            })
            .collect::<Vec<_>>(),
    ));
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(i, (_, data_type))| column_array(data_type, rows.iter().map(|r| r[i]).collect()))
        .collect();
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

fn empty(columns: Columns) -> Arc<MemoryTable> {
    table::<0>(columns, &[])
}

fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("nation", table(NATION, NATION_ROWS));
    schema.register_table("region", empty(REGION));
    schema.register_table("part", empty(PART));
    schema.register_table("supplier", empty(SUPPLIER));
    schema.register_table("partsupp", empty(PARTSUPP));
    schema.register_table("customer", table(CUSTOMER, CUSTOMER_ROWS));
    schema.register_table("orders", table(ORDERS, ORDERS_ROWS));
    schema.register_table("lineitem", empty(LINEITEM));
    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", schema);
    // The capture database also had PostgreSQL's built-in, empty `public`.
    file.register_schema("public", Arc::new(MemorySchema::new()));
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );
    (Arc::new(cm), Arc::new(registry))
}

async fn connect() -> tokio_postgres::Client {
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
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=metabase dbname=file"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    client
}

fn spec_file(name: &str) -> String {
    let path = format!(
        "{}/../../docs/superpowers/specs/2026-11-21-g6-stage-2b2/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn corpus() -> Vec<String> {
    spec_file("metabase-captured.sql")
        .split("\n;;;;\n")
        .map(str::to_owned)
        .collect()
}

/// One golden statement: PostgreSQL's column names and rows, as text cells.
struct Expected {
    index: usize,
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

/// A JSON cell as PostgreSQL's text format sends it.
fn text_cell(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_owned()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        other => panic!("unexpected golden cell {other}"),
    }
}

fn expected() -> Vec<Expected> {
    let doc: Value = serde_json::from_str(&spec_file("metabase-pg17-expected.json")).unwrap();
    doc["statements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| Expected {
            index: s["index"].as_u64().unwrap() as usize,
            columns: s["columns"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c.as_str().unwrap().to_owned())
                .collect(),
            rows: s["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r.as_array().unwrap().iter().map(text_cell).collect())
                .collect(),
        })
        .collect()
}

/// The capture's table oids, from golden rows that have both `oid` and
/// `relname`.
fn captured_oids(expected: &[Expected]) -> BTreeMap<String, String> {
    let mut oids = BTreeMap::new();
    for e in expected {
        let (Some(oid), Some(relname)) = (
            e.columns.iter().position(|c| c == "oid"),
            e.columns.iter().position(|c| c == "relname"),
        ) else {
            continue;
        };
        for row in &e.rows {
            oids.insert(row[oid].clone().unwrap(), row[relname].clone().unwrap());
        }
    }
    oids
}

/// A statement's result: the column names from RowDescription and the rows.
struct Answer {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

async fn run(client: &tokio_postgres::Client, sql: &str) -> Result<Answer, String> {
    let messages = client.simple_query(sql).await.map_err(|e| {
        e.as_db_error()
            .map(|d| d.message().to_owned())
            .unwrap_or_else(|| e.to_string())
    })?;
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for m in &messages {
        match m {
            SimpleQueryMessage::RowDescription(cols) => {
                columns = cols.iter().map(|c| c.name().to_owned()).collect();
            }
            SimpleQueryMessage::Row(r) => {
                rows.push((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
            }
            _ => {}
        }
    }
    Ok(Answer { columns, rows })
}

/// The corpus with each captured oid replaced by Arneb's oid for the same
/// table in `default`.
async fn corpus_with_arneb_oids(
    client: &tokio_postgres::Client,
    expected: &[Expected],
) -> Vec<String> {
    let mut substitutions = Vec::new();
    for (captured, relname) in captured_oids(expected) {
        let sql = format!(
            "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
             WHERE n.nspname = 'default' AND c.relname = '{relname}'"
        );
        let answer = run(client, &sql).await.unwrap();
        assert_eq!(answer.rows.len(), 1, "{relname}: {:?}", answer.rows);
        substitutions.push((captured, answer.rows[0][0].clone().unwrap()));
    }
    corpus()
        .into_iter()
        .map(|mut sql| {
            for (captured, arneb) in &substitutions {
                sql = sql.replace(captured.as_str(), arneb);
            }
            sql
        })
        .collect()
}

fn fallbacks() -> u64 {
    arneb_common::metrics::pg_catalog_fallbacks()
}

/// The fallback counter is process-global: the two tests must not overlap.
static SERIAL: Mutex<()> = Mutex::const_new(());

fn head(sql: &str) -> String {
    sql.chars().take(200).collect()
}

#[tokio::test]
async fn every_corpus_statement_runs_without_fallback() {
    let _serial = SERIAL.lock().await;
    let client = connect().await;
    let statements = corpus_with_arneb_oids(&client, &expected()).await;
    assert_eq!(statements.len(), 79);
    let before = fallbacks();
    let mut failures = Vec::new();
    for (i, sql) in statements.iter().enumerate() {
        let start = fallbacks();
        if let Err(e) = run(&client, sql).await {
            failures.push(format!("[{i}] {e}\n    {}", head(sql)));
        } else if fallbacks() != start {
            failures.push(format!(
                "[{i}] ran through the pg_catalog fallback\n    {}",
                head(sql)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert_eq!(fallbacks(), before);
}

/// Masks oid-valued cells: they are PostgreSQL's, not Arneb's.
fn masked(name: &str) -> bool {
    name.ends_with("oid")
        || name.ends_with("relid")
        || name.ends_with("attrelid")
        || name == "objoid"
}

#[tokio::test]
async fn metadata_statements_match_postgres() {
    let _serial = SERIAL.lock().await;
    let client = connect().await;
    let expected = expected();
    assert_eq!(expected.len(), 35);
    let statements = corpus_with_arneb_oids(&client, &expected).await;
    let known: HashMap<(usize, &str), &str> = KNOWN_DIFFERENCES
        .iter()
        .map(|(i, c, reason)| ((*i, *c), *reason))
        .collect();
    let mut used = HashSet::new();
    let mut used_headers = HashSet::new();
    let mut failures = Vec::new();
    for e in &expected {
        let sql = &statements[e.index];
        let answer = match run(&client, sql).await {
            Ok(a) => a,
            Err(err) => {
                failures.push(format!("[{}] {err}\n    {}", e.index, head(sql)));
                continue;
            }
        };
        let header_matches = answer.columns.len() == e.columns.len()
            && answer.columns.iter().zip(&e.columns).all(|(arneb, pg)| {
                if arneb == pg {
                    return true;
                }
                let known = KNOWN_HEADER_DIFFERENCES
                    .iter()
                    .position(|(at, p, a, _)| at.contains(&e.index) && p == pg && a == arneb);
                if let Some(k) = known {
                    used_headers.insert((k, e.index));
                }
                known.is_some()
            });
        if !header_matches {
            failures.push(format!(
                "[{}] columns {:?}, PostgreSQL {:?}",
                e.index, answer.columns, e.columns
            ));
            continue;
        }
        // Cells compared: not oid-valued and not a known difference. A known
        // difference counts as used only if that column really differs.
        let mut skip = vec![false; e.columns.len()];
        for (c, name) in e.columns.iter().enumerate() {
            if masked(name) {
                skip[c] = true;
            } else if known.contains_key(&(e.index, name.as_str())) {
                skip[c] = true;
                let differs = answer.rows.len() != e.rows.len()
                    || answer.rows.iter().zip(&e.rows).any(|(a, x)| a[c] != x[c]);
                if differs {
                    used.insert((e.index, name.clone()));
                }
            }
        }
        let project = |rows: &[Vec<Option<String>>]| -> Vec<Vec<Option<String>>> {
            rows.iter()
                .map(|r| {
                    r.iter()
                        .zip(&skip)
                        .filter(|(_, s)| !**s)
                        .map(|(v, _)| v.clone())
                        .collect()
                })
                .collect()
        };
        let mut got = project(&answer.rows);
        let mut want = project(&e.rows);
        if !sql.to_ascii_uppercase().contains("ORDER BY") {
            got.sort();
            want.sort();
        }
        if got != want {
            failures.push(format!(
                "[{}] rows differ\n    arneb:      {got:?}\n    postgresql: {want:?}\n    {}",
                e.index,
                head(sql)
            ));
        }
    }
    for (i, c, reason) in KNOWN_DIFFERENCES {
        if !used.contains(&(*i, c.to_string())) {
            failures.push(format!(
                "unused KNOWN_DIFFERENCES entry [{i}] {c}: {reason}"
            ));
        }
    }
    for (k, (at, pg, arneb, reason)) in KNOWN_HEADER_DIFFERENCES.iter().enumerate() {
        for i in *at {
            if !used_headers.contains(&(k, *i)) {
                failures.push(format!(
                    "unused KNOWN_HEADER_DIFFERENCES entry [{i}] {pg} / {arneb}: {reason}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
