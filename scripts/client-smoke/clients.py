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

pgwire is at PGHOST/PGPORT (default 127.0.0.1:15432), the Trino protocol at
TRINO_HOST/TRINO_PORT (default 127.0.0.1:18080); the TPC-H tables are in
catalog ARNEB_CATALOG (default "file"), schema "default". Prints one line per
check and exits non-zero if any fails.
"""

import os
import sys
import traceback

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


print(f"{len(failed)} failed" if failed else "all client checks passed")
sys.exit(1 if failed else 0)
