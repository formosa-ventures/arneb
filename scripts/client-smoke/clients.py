"""Client compatibility smoke test against a running Arneb (client-smoke).

Usage: python clients.py

Connects the way BI and ELT tools do and checks what they need:
  - psycopg2 in its default mode (statements inside BEGIN ... COMMIT) and
    psycopg 3, over pgwire
  - SQLAlchemy's postgresql dialect (psycopg2 and psycopg 3) listing
    schemas and tables from pg_catalog (pg-catalog-tables)
  - the Trino DB-API client, and SQLAlchemy's Trino dialect reflecting a
    table and running a query built from it (what Superset does when a
    Trino database is added and a table is browsed)
  - SQLAlchemy's postgresql dialect reflecting columns, keys and indexes,
    with no pg_catalog fallback answer counted
  - array, set-returning-function and correlated-subquery queries on the
    cluster, compared with DuckDB on the same Parquet files (DATA_DIR)

pgwire is at PGHOST/PGPORT (default 127.0.0.1:15432), the Trino protocol at
TRINO_HOST/TRINO_PORT (default 127.0.0.1:18080); the TPC-H tables are in
catalog ARNEB_CATALOG (default "file"), schema "default". Prints one line per
check and exits non-zero if any fails.
"""

import os
import sys
import traceback
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "k8s-smoke"))

PG = dict(
    host=os.environ.get("PGHOST", "127.0.0.1"),
    port=int(os.environ.get("PGPORT", "15432")),
    user="smoke",
    dbname="arneb",
)
TRINO_HOST = os.environ.get("TRINO_HOST", "127.0.0.1")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "18080"))
CATALOG = os.environ.get("ARNEB_CATALOG", "file")
SCHEMA = "default"
TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

failed = []


def check(name):
    def wrap(fn):
        try:
            detail = fn()
            print(f"PASS  {name}" + (f": {detail}" if detail else ""))
        except Exception as exc:  # noqa: BLE001 - every failure is reported
            failed.append(name)
            print(f"FAIL  {name}: {type(exc).__name__}: {exc}".splitlines()[0][:400])
            traceback.print_exc(limit=3, file=sys.stdout)
        return fn

    return wrap


@check("psycopg2, default transaction mode")
def _():
    import psycopg2

    with psycopg2.connect(**PG) as conn:
        assert not conn.autocommit
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM nation WHERE n_regionkey = %s", (1,))
            assert cur.fetchone()[0] == 5
            cur.execute("SELECT version()")
            assert cur.fetchone()[0].startswith("PostgreSQL ")
        conn.rollback()
    return f"server_version {conn.server_version}"


@check("psycopg 3, server-side parameters")
def _():
    import psycopg

    with psycopg.connect(**PG) as conn:
        rows = conn.execute(
            "SELECT n_name FROM nation WHERE n_nationkey < %s ORDER BY n_nationkey", (3,)
        ).fetchall()
        assert [r[0] for r in rows] == ["ALGERIA", "ARGENTINA", "BRAZIL"], rows


@check("SQLAlchemy postgresql dialect: list schemas and tables")
def _():
    from sqlalchemy import create_engine, inspect, text

    url = f"{PG['user']}@{PG['host']}:{PG['port']}/{PG['dbname']}"
    for driver in ["psycopg2", "psycopg"]:
        engine = create_engine(f"postgresql+{driver}://{url}")
        with engine.connect() as conn:
            insp = inspect(conn)
            assert SCHEMA in insp.get_schema_names(), driver
            assert sorted(insp.get_table_names(schema=SCHEMA)) == TABLES, driver
            assert insp.has_table("nation", schema=SCHEMA), driver
            assert not insp.has_table("no_such_table", schema=SCHEMA), driver
            assert insp.get_view_names(schema=SCHEMA) == [], driver
            count = conn.execute(text('SELECT count(*) FROM "default".nation')).scalar()
            assert count == 25, (driver, count)


@check("trino DB-API: catalogs, schemas, current session")
def _():
    import trino

    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="smoke", catalog=CATALOG, schema=SCHEMA
    )
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    assert [CATALOG] in cur.fetchall()
    cur.execute("SELECT current_catalog, current_schema")
    assert cur.fetchall() == [[CATALOG, SCHEMA]]
    cur.execute("SELECT count(*) FROM NATION")
    assert cur.fetchall() == [[25]]


@check("SQLAlchemy trino dialect: inspect and query (Superset)")
def _():
    from sqlalchemy import MetaData, Table, create_engine, func, inspect, select

    engine = create_engine(f"trino://smoke@{TRINO_HOST}:{TRINO_PORT}/{CATALOG}/{SCHEMA}")
    insp = inspect(engine)
    assert SCHEMA in insp.get_schema_names()
    assert sorted(insp.get_table_names(schema=SCHEMA)) == TABLES
    columns = [c["name"] for c in insp.get_columns("nation", schema=SCHEMA)]
    assert columns == ["n_nationkey", "n_name", "n_regionkey", "n_comment"], columns
    insp.get_pk_constraint("nation", schema=SCHEMA)
    insp.get_foreign_keys("nation", schema=SCHEMA)
    insp.get_indexes("nation", schema=SCHEMA)
    insp.get_table_comment("nation", schema=SCHEMA)
    nation = Table("nation", MetaData(), autoload_with=engine, schema=SCHEMA)
    query = (
        select(nation.c.n_regionkey, func.count().label("n"))
        .group_by(nation.c.n_regionkey)
        .order_by(nation.c.n_regionkey)
        .limit(100)
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    assert [tuple(r) for r in rows] == [(k, 5) for k in range(5)], rows


def pg_url(driver):
    return f"postgresql+{driver}://{PG['user']}@{PG['host']}:{PG['port']}/{PG['dbname']}"


@check("SQLAlchemy postgresql dialect: reflect columns, keys and indexes")
def _():
    from sqlalchemy import MetaData, Table, create_engine, func, inspect, select

    for driver in ["psycopg2", "psycopg"]:
        engine = create_engine(pg_url(driver))
        with engine.connect() as conn:
            insp = inspect(conn)
            names = [c["name"] for c in insp.get_columns("nation", schema=SCHEMA)]
            assert names == ["n_nationkey", "n_name", "n_regionkey", "n_comment"], (driver, names)
            pk = insp.get_pk_constraint("nation", schema=SCHEMA)
            assert pk["constrained_columns"] == [] and pk["name"] is None, (driver, pk)
            for method in [
                "get_foreign_keys",
                "get_indexes",
                "get_unique_constraints",
                "get_check_constraints",
            ]:
                got = getattr(insp, method)("nation", schema=SCHEMA)
                assert got == [], (driver, method, got)
        nation = Table("nation", MetaData(), autoload_with=engine, schema=SCHEMA)
        query = (
            select(nation.c.n_regionkey, func.count().label("n"))
            .group_by(nation.c.n_regionkey)
            .order_by(nation.c.n_regionkey)
            .limit(3)
        )
        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()
        assert [tuple(r) for r in rows] == [(k, 5) for k in range(3)], (driver, rows)


@check("pg_catalog fallback counter is 0 after reflection")
def _():
    url = f"http://{PG['host']}:{PG['port'] + 1000}/metrics"
    with urllib.request.urlopen(url, timeout=10) as resp:
        text = resp.read().decode()
    values = [
        float(line.rsplit(" ", 1)[1])
        for line in text.splitlines()
        if line.startswith("arneb_pg_catalog_fallbacks_total")
    ]
    # A counter that was never incremented may not be exported at all.
    assert all(v == 0 for v in values), values
    return "metric present" if values else "metric not exported (never incremented)"


# Strings that need quoting inside a PostgreSQL array literal.
QUOTING_SQL = r"""SELECT ARRAY['', 'a,b', 'q"x', 'b\s', '{c}', 'sp ace', 'NULL', 'plain', NULL]"""
QUOTING_LIST = ["", "a,b", 'q"x', "b\\s", "{c}", "sp ace", "NULL", "plain", None]


@check("psycopg 3 parses arrays that need quoting back to the same list")
def _():
    import psycopg

    with psycopg.connect(**PG) as conn:
        got = conn.execute(QUOTING_SQL).fetchone()[0]
        assert got == QUOTING_LIST, got
        got = conn.execute("SELECT ARRAY[1, 2, NULL], ARRAY['a', 'b']").fetchone()
        assert list(got) == [[1, 2, None], ["a", "b"]], got


def norm_row(row, norm):
    return tuple([norm(x) for x in v] if isinstance(v, list) else norm(v) for v in row)


def compare_with_duckdb(arneb_sql, duckdb_sql=None):
    import duckdb
    import psycopg
    from tpch_check import norm, rows_match

    ref = duckdb.connect(os.path.join(os.environ["DATA_DIR"], "tpch.duckdb"), read_only=True)
    want = [norm_row(r, norm) for r in ref.execute(duckdb_sql or arneb_sql).fetchall()]
    with psycopg.connect(**PG, autocommit=True) as conn:
        got = [norm_row(r, norm) for r in conn.execute(arneb_sql).fetchall()]
    assert rows_match(got, want), f"got {got[:3]}..., expected {want[:3]}... ({len(got)} vs {len(want)} rows)"
    return f"{len(got)} rows"


@check("distributed: array_agg ORDER BY matches DuckDB")
def _():
    return compare_with_duckdb(
        "SELECT n_regionkey, array_agg(n_name ORDER BY n_name) FROM nation GROUP BY 1 ORDER BY 1"
    )


@check("distributed: unnest and generate_subscripts over array_agg match DuckDB")
def _():
    return compare_with_duckdb(
        "SELECT r, unnest(a), generate_subscripts(a, 1) FROM "
        "(SELECT n_regionkey r, array_agg(n_nationkey ORDER BY n_nationkey) a FROM nation GROUP BY 1) s "
        "ORDER BY 1, 3",
        # DuckDB has no generate_subscripts: dimension 1 of a list is 1..len(a)
        "SELECT r, unnest(a), unnest(range(1, len(a) + 1)) FROM "
        "(SELECT n_regionkey r, array_agg(n_nationkey ORDER BY n_nationkey) a FROM nation GROUP BY 1) s "
        "ORDER BY 1, 3",
    )


@check("distributed: ARRAY[...] constructor matches DuckDB")
def _():
    return compare_with_duckdb(
        "SELECT o_orderkey, ARRAY[o_orderkey, o_custkey] FROM orders ORDER BY 1 LIMIT 5"
    )


@check("distributed: correlated scalar subqueries in SELECT and CASE match DuckDB")
def _():
    return compare_with_duckdb(
        "SELECT c_custkey, "
        "(SELECT count(*) FROM orders WHERE o_custkey = c_custkey), "
        "(SELECT n_name FROM nation WHERE n_nationkey = c_nationkey), "
        "CASE WHEN c_acctbal < 0 THEN (SELECT r_name FROM region WHERE r_regionkey = c_nationkey % 5) END "
        "FROM customer ORDER BY 1 LIMIT 50"
    )


@check("trino DB-API: array(varchar) equals the pgwire result")
def _():
    import psycopg
    import trino

    sql = "SELECT n_regionkey, array_agg(n_name ORDER BY n_name) FROM nation GROUP BY 1 ORDER BY 1"
    with psycopg.connect(**PG, autocommit=True) as conn:
        want = conn.execute(sql).fetchall()
    cur = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="smoke", catalog=CATALOG, schema=SCHEMA
    ).cursor()
    cur.execute(sql)
    got = cur.fetchall()
    assert len(got) == len(want) == 5, got
    for g, w in zip(got, want):
        assert isinstance(g[1], list), g
        assert g[0] == w[0] and g[1] == w[1], (g, w)


print(f"{len(failed)} failed" if failed else "all client checks passed")
sys.exit(1 if failed else 0)
