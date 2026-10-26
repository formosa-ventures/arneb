"""Run the 22 TPC-H queries on Arneb and compare each with DuckDB's answer.

Usage: python tpch_check.py DATA_DIR [QUERY ...]

DATA_DIR is the directory gen_tpch.py wrote. The reference answer is
DuckDB running the same query file on tpch.duckdb there; `PRAGMA tpch(N)`
is not used because some of the repo's query files differ from the spec's
text in columns or parameters. Arneb is reached over pgwire at
PGHOST/PGPORT (default 127.0.0.1:5432). QUERY narrows the run, e.g. q01.

A result matches when it has the same number of rows and columns and every
value matches: strings after trimming CHAR padding, dates as ISO strings,
numbers within a relative 1e-6 or half a cent apart (Arneb keeps a DECIMAL
average at the column's scale of 2; DuckDB returns a double). Rows are
first compared in order, then sorted, so ties in ORDER BY do not count as
a mismatch. Exits non-zero unless every query matches.
"""

import datetime
import decimal
import math
import os
import sys
import time

import duckdb
import psycopg

QUERIES = os.path.join(os.path.dirname(__file__), "..", "..", "benchmarks", "tpch", "queries")


def norm(value):
    if isinstance(value, (decimal.Decimal, int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, str):
        return value.rstrip()
    return value


def same(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-6) or abs(a - b) <= 0.005 + 1e-9
    return a == b


def rows_match(got, want) -> bool:
    return len(got) == len(want) and all(
        len(g) == len(w) and all(same(x, y) for x, y in zip(g, w)) for g, w in zip(got, want)
    )


def sort_key(row):
    return tuple((0, round(v, 2)) if isinstance(v, float) else (1, str(v)) for v in row)


def first_difference(got, want) -> str:
    if len(got) != len(want):
        return f"{len(got)} rows, expected {len(want)}"
    for i, (g, w) in enumerate(zip(sorted(got, key=sort_key), sorted(want, key=sort_key))):
        if not rows_match([g], [w]):
            return f"row {i}: got {g}, expected {w}"
    return "rows differ"


def main() -> int:
    data_dir = sys.argv[1]
    names = sys.argv[2:] or [f"q{n:02d}" for n in range(1, 23)]
    reference = duckdb.connect(os.path.join(data_dir, "tpch.duckdb"), read_only=True)
    conn = psycopg.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "arneb"),
        dbname=os.environ.get("PGDATABASE", "arneb"),
        autocommit=True,
    )
    failed = []
    for name in names:
        with open(os.path.join(QUERIES, f"{name}.sql")) as f:
            sql = f.read()
        want = [tuple(map(norm, r)) for r in reference.execute(sql).fetchall()]
        start = time.monotonic()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                got = [tuple(map(norm, r)) for r in cur.fetchall()]
        except psycopg.Error as e:
            failed.append(name)
            print(f"{name}: ERROR {e}".rstrip())
            continue
        elapsed = time.monotonic() - start
        if rows_match(got, want) or rows_match(sorted(got, key=sort_key), sorted(want, key=sort_key)):
            print(f"{name}: ok ({len(got)} rows, {elapsed:.2f} s)")
        else:
            failed.append(name)
            print(f"{name}: MISMATCH {first_difference(got, want)}")
    print(f"{len(names) - len(failed)}/{len(names)} match DuckDB")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
