"""Generate TPC-H Parquet files with DuckDB's dbgen for the kind smoke test.

Usage: python gen_tpch.py OUT_DIR [SCALE_FACTOR]

Writes one <table>.parquet per TPC-H table to OUT_DIR and keeps the DuckDB
database next to it (tpch.duckdb), which tpch_check.py uses for the
reference answers.
"""

import os
import sys

import duckdb

TABLES = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]


def main() -> None:
    out = sys.argv[1]
    sf = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    os.makedirs(out, exist_ok=True)
    db = os.path.join(out, "tpch.duckdb")
    # dbgen appends to existing tables, so a rerun would double every row.
    if os.path.exists(db):
        os.remove(db)
    con = duckdb.connect(db)
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf = {sf})")
    for table in TABLES:
        path = os.path.join(out, f"{table}.parquet")
        con.execute(f"COPY {table} TO '{path}' (FORMAT parquet)")
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"{table}: {rows} rows")


if __name__ == "__main__":
    main()
