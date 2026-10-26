//! `pg_catalog` over pgwire, run in the engine (pg-catalog-tables). The
//! statements are the ones dbt-postgres 1.11 and SQLAlchemy 2.1 send,
//! recorded from PostgreSQL with `log_statement=all`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::{AccessRules, CatalogManager};
use arneb_common::pg_oid;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

fn table(columns: &[(&str, DataType, bool)]) -> Arc<MemoryTable> {
    let infos: Vec<ColumnInfo> = columns
        .iter()
        .map(|(name, data_type, nullable)| ColumnInfo {
            name: name.to_string(),
            data_type: data_type.clone(),
            nullable: *nullable,
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    // Only the catalog is read here; the batch is never scanned.
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

/// `file.default.{nation, region}` and `file.staging.raw`, with `file` the
/// default catalog.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table(
        "nation",
        table(&[
            ("n_nationkey", DataType::Int64, false),
            ("n_name", DataType::Utf8, true),
            (
                "n_price",
                DataType::Decimal128 {
                    precision: 12,
                    scale: 2,
                },
                true,
            ),
        ]),
    );
    default_schema.register_table("region", table(&[("r_regionkey", DataType::Int32, false)]));
    let staging = Arc::new(MemorySchema::new());
    staging.register_table("raw", table(&[("payload", DataType::Utf8, true)]));
    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", default_schema);
    file.register_schema("staging", staging);
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let mut registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );
    (Arc::new(cm), Arc::new(registry))
}

async fn pg_server(rules: Option<&str>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    )
    .with_access_rules(rules.map(|r| Arc::new(AccessRules::from_json(r).unwrap())));
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

async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n{e:?}"))
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
            }
            _ => None,
        })
        .collect()
}

fn column(rows: &[Vec<Option<String>>], i: usize) -> Vec<String> {
    rows.iter()
        .map(|r| r[i].clone().unwrap_or_default())
        .collect()
}

/// dbt-postgres: the relations query `dbt test`, `show` and `docs generate`
/// start with.
const DBT_RELATIONS: &str = r#"/* {"app": "dbt"} */
    select distinct
        dependent_namespace.nspname as dependent_schema,
        dependent_class.relname as dependent_name,
        referenced_namespace.nspname as referenced_schema,
        referenced_class.relname as referenced_name
    from pg_class as dependent_class
    join pg_namespace as dependent_namespace on dependent_namespace.oid = dependent_class.relnamespace
    join pg_depend as dependent_depend on dependent_depend.refobjid = dependent_class.oid
        and dependent_depend.classid = 'pg_rewrite'::regclass
        and dependent_depend.refclassid = 'pg_class'::regclass
        and dependent_depend.deptype = 'i'
    join pg_depend as joining_depend on joining_depend.objid = dependent_depend.objid
        and joining_depend.classid = 'pg_rewrite'::regclass
        and joining_depend.refclassid = 'pg_class'::regclass
        and joining_depend.refobjid != dependent_depend.refobjid
    join pg_class as referenced_class on referenced_class.oid = joining_depend.refobjid
    join pg_namespace as referenced_namespace on referenced_namespace.oid = referenced_class.relnamespace
        and referenced_namespace.nspname != 'information_schema'
        and referenced_namespace.nspname not like 'pg\_%'
    order by
        dependent_schema, dependent_name, referenced_schema, referenced_name;"#;

/// dbt-postgres `docs generate`: the catalog query for two sources.
const DBT_CATALOG: &str = r#"
    select
        'file' as table_database,
        sch.nspname as table_schema,
        tbl.relname as table_name,
        case tbl.relkind
            when 'v' then 'VIEW'
            when 'm' then 'MATERIALIZED VIEW'
            else 'BASE TABLE'
        end as table_type,
        tbl_desc.description as table_comment,
        col.attname as column_name,
        col.attnum as column_index,
        pg_catalog.format_type(col.atttypid, col.atttypmod) as column_type,
        col_desc.description as column_comment,
        pg_get_userbyid(tbl.relowner) as table_owner
    from pg_catalog.pg_namespace sch
    join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
    join pg_catalog.pg_attribute col on col.attrelid = tbl.oid
    left outer join pg_catalog.pg_description tbl_desc on (tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0)
    left outer join pg_catalog.pg_description col_desc on (col_desc.objoid = tbl.oid and col_desc.objsubid = col.attnum)
    where ((upper(sch.nspname) = upper('default') and
           upper(tbl.relname) = upper('region')) or (upper(sch.nspname) = upper('default') and
           upper(tbl.relname) = upper('nation')))
      and not pg_is_other_temp_schema(sch.oid) -- not a temporary schema belonging to another session
      and tbl.relpersistence in ('p', 'u') -- [p]ermanent table or [u]nlogged table
      and tbl.relkind in ('r', 'v', 'f', 'p', 'm')
      and col.attnum > 0
      and not col.attisdropped
    order by
        sch.nspname,
        tbl.relname,
        col.attnum"#;

#[tokio::test]
async fn dbt_postgres_catalog_queries() {
    let client = connect(pg_server(None).await).await;

    assert!(rows(&client, DBT_RELATIONS).await.is_empty());

    let catalog = rows(&client, DBT_CATALOG).await;
    let described: Vec<(String, String, String, String)> = catalog
        .iter()
        .map(|r| {
            (
                r[2].clone().unwrap(),
                r[5].clone().unwrap(),
                r[7].clone().unwrap(),
                r[9].clone().unwrap(),
            )
        })
        .collect();
    let want = [
        ("nation", "n_nationkey", "bigint"),
        ("nation", "n_name", "character varying"),
        ("nation", "n_price", "numeric(12,2)"),
        ("region", "r_regionkey", "integer"),
    ];
    assert_eq!(described.len(), want.len(), "{catalog:?}");
    for ((table, col, ty, owner), (t, c, y)) in described.iter().zip(want) {
        assert_eq!((table.as_str(), col.as_str(), ty.as_str()), (t, c, y));
        assert_eq!(owner, "arneb");
    }
    assert!(catalog.iter().all(|r| r[0].as_deref() == Some("file")
        && r[3].as_deref() == Some("BASE TABLE")
        && r[4].is_none()));
}

#[tokio::test]
async fn sqlalchemy_listing_queries() {
    let client = connect(pg_server(None).await).await;

    let schemas = rows(
        &client,
        "SELECT pg_catalog.pg_namespace.nspname FROM pg_catalog.pg_namespace \
         WHERE (pg_catalog.pg_namespace.nspname NOT LIKE 'pg/_' || '%' ESCAPE '/') \
         ORDER BY pg_catalog.pg_namespace.nspname",
    )
    .await;
    assert_eq!(
        column(&schemas, 0),
        ["default", "information_schema", "staging"]
    );

    let tables = rows(
        &client,
        "SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class \
         JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace \
         WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p']) \
         AND pg_catalog.pg_class.relpersistence != 't' \
         AND pg_catalog.pg_namespace.nspname = 'default' ORDER BY 1",
    )
    .await;
    assert_eq!(column(&tables, 0), ["nation", "region"]);

    let views = rows(
        &client,
        "SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class \
         JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace \
         WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['v']) \
         AND pg_catalog.pg_class.relpersistence != 't' \
         AND pg_catalog.pg_namespace.nspname = 'default'",
    )
    .await;
    assert!(views.is_empty());

    // psycopg2's hstore probe: no such type.
    let hstore = rows(
        &client,
        "SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns \
         ON typnamespace = ns.oid WHERE typname = 'hstore'",
    )
    .await;
    assert!(hstore.is_empty());

    // psycopg 3's type lookup (`TypeInfo.fetch`), with a bound name.
    for (name, want) in [("hstore", None), ("int4", Some("integer"))] {
        let found = client
            .query(
                "SELECT typname AS name, oid, typarray AS array_oid, \
                 oid::regtype::text AS regtype, typdelim AS delimiter \
                 FROM pg_type t WHERE t.oid = to_regtype($1) ORDER BY t.oid",
                &[&name],
            )
            .await
            .unwrap();
        assert_eq!(
            found.first().map(|r| r.get::<_, String>("regtype")),
            want.map(str::to_owned),
            "{name}"
        );
    }

    // Extended protocol, with a bound parameter.
    let found = client
        .query(
            "SELECT relname FROM pg_catalog.pg_class WHERE relname = $1",
            &[&"nation"],
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].get::<_, String>(0), "nation");
}

#[tokio::test]
async fn regclass_matches_pg_class_oids() {
    let client = connect(pg_server(None).await).await;

    let oid = |r: Vec<Vec<Option<String>>>| r[0][0].clone().unwrap();
    assert_eq!(
        oid(rows(&client, "SELECT 'pg_rewrite'::regclass").await),
        "2618"
    );
    assert_eq!(
        oid(rows(&client, "SELECT CAST('pg_catalog.pg_class' AS REGCLASS)").await),
        "1259"
    );
    let nation = pg_oid::relation_oid("file", "default", "nation").to_string();
    assert_eq!(
        oid(rows(&client, "SELECT oid FROM pg_class WHERE relname = 'nation'").await),
        nation
    );
    assert_eq!(
        oid(rows(&client, "SELECT 'nation'::regclass").await),
        nation
    );
    assert_eq!(
        oid(rows(
            &client,
            "SELECT relname FROM pg_class WHERE oid = '\"default\".nation'::regclass"
        )
        .await),
        "nation"
    );
}

#[tokio::test]
async fn hidden_objects_are_absent() {
    // `region` and everything in `staging` are hidden.
    let rules = r#"{
        "catalogs": [{"allow": "read-only"}],
        "schemas": [{"owner": false}],
        "tables": [
            {"table": "region", "privileges": []},
            {"schema": "staging", "privileges": []},
            {"privileges": ["SELECT"]}
        ]
    }"#;
    let client = connect(pg_server(Some(rules)).await).await;
    let tables = rows(
        &client,
        "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n \
         ON n.oid = c.relnamespace WHERE n.nspname <> 'pg_catalog' ORDER BY 1, 2",
    )
    .await;
    assert_eq!(column(&tables, 1), ["nation"], "{tables:?}");
    let columns = rows(
        &client,
        "SELECT a.attname, a.attnum FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
         WHERE c.relname IN ('nation', 'region', 'raw') ORDER BY a.attnum",
    )
    .await;
    assert_eq!(column(&columns, 0), ["n_nationkey", "n_name", "n_price"]);
}

#[tokio::test]
async fn statements_the_engine_cannot_run_keep_the_fixed_result() {
    let client = connect(pg_server(None).await).await;
    // SQLAlchemy's primary-key reflection reads arrays (stage 2b).
    let fixed = rows(
        &client,
        "SELECT unnest(pg_catalog.pg_class.relname) FROM pg_catalog.pg_class",
    )
    .await;
    assert!(!fixed.is_empty());
}
