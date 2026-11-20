"""Probe SQLAlchemy postgresql-dialect reflection over pgwire (G6 stage 2b-1).

Usage: python pg_reflect_probe.py [PORT]  (default 25432; schema "default", table "nation")
Prints OK/FAIL per Inspector call for psycopg2 and psycopg 3.
"""
import logging, sys
from sqlalchemy import create_engine, inspect, Table, MetaData
logging.basicConfig(level=logging.WARNING)
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 25432
for drv in ("psycopg2", "psycopg"):
    eng = create_engine(f"postgresql+{drv}://arneb@127.0.0.1:{PORT}/arneb", echo=False)
    insp = inspect(eng)
    for name, fn in [
        ("schemas", lambda: insp.get_schema_names()),
        ("tables", lambda: insp.get_table_names(schema="default")),
        ("columns", lambda: insp.get_columns("nation", schema="default")),
        ("pk", lambda: insp.get_pk_constraint("nation", schema="default")),
        ("fks", lambda: insp.get_foreign_keys("nation", schema="default")),
        ("indexes", lambda: insp.get_indexes("nation", schema="default")),
        ("unique", lambda: insp.get_unique_constraints("nation", schema="default")),
        ("check", lambda: insp.get_check_constraints("nation", schema="default")),
        ("comment", lambda: insp.get_table_comment("nation", schema="default")),
        ("autoload", lambda: list(Table("nation", MetaData(), autoload_with=eng, schema="default").c.keys())),
    ]:
        try:
            r = fn(); print(drv, name, "OK", str(r)[:200])
        except Exception as e:
            print(drv, name, "FAIL", type(e).__name__, str(e).splitlines()[0][:200])
