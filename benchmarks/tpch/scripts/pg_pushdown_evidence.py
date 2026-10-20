#!/usr/bin/env python3
"""Show, per mixed-layout TPC-H query, the SQL PostgreSQL received from Arneb.

Remote-side evidence for filter/projection/dynamic-filter pushdown: the
docker `postgres` service logs every statement with its application_name
(`log_statement=all`, `%m [%a] ` prefix), and Arneb connects as `arneb`.

Each query is bracketed by marker statements sent straight to PostgreSQL
(`SELECT 'arneb-mark <q> begin'` / `... end'`), and only the log between the
two markers is read. No time windows are involved, so slow clocks or log lag
cannot shift statements into the wrong query.

Usage:
    python3 benchmarks/tpch/scripts/pg_pushdown_evidence.py --arneb-port 5434 \
        --queries q05,q11 [--out report.md]
"""
import argparse
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries-mixed"
PG_CONTAINER = "arneb-postgres-1"
STATEMENT = re.compile(r"\[arneb\] LOG:\s+(?:statement|execute [^:]*): (COPY \((SELECT .*)\) TO STDOUT.*)$")
FROM_WHERE = re.compile(r'SELECT (?P<cols>.*?) FROM (?P<table>"[^"]+"\."[^"]+")(?: WHERE (?P<where>.*))?$')


def psql_pg(sql: str) -> None:
    subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", "arneb", "-d", "arneb", "-qAt", "-c", sql],
        check=True, capture_output=True,
    )


def run_arneb(port: str, sql: str) -> str | None:
    out = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", port, "-U", "arneb", "-d", "arneb", "-qAt", "-c", sql],
        capture_output=True, text=True, env={**os.environ, "PGPASSWORD": "x"},
    )
    return None if out.returncode == 0 else (out.stderr.strip() or out.stdout.strip())[:300]


def strip_sql(path: Path) -> str:
    lines = [ln for ln in path.read_text().splitlines() if not ln.lstrip().startswith("--")]
    return " ".join(lines).strip().rstrip(";")


def statements_between(begin: str, end: str) -> list[str]:
    log = subprocess.run(["docker", "logs", PG_CONTAINER], capture_output=True, text=True)
    lines = (log.stdout + log.stderr).splitlines()
    try:
        b = max(i for i, ln in enumerate(lines) if begin in ln)
        e = max(i for i, ln in enumerate(lines) if end in ln)
    except ValueError:
        raise RuntimeError("marker statements not found in the postgres log "
                           "(is log_statement=all set on the service?)")
    return [m.group(2) for ln in lines[b:e] if (m := STATEMENT.search(ln))]


def summarize(statements: list[str]) -> list[tuple[str, str, str, int]]:
    """(table, columns, where, splits) — one row per distinct (table, cols, where)."""
    seen: dict[tuple[str, str, str], int] = {}
    for sql in statements:
        m = FROM_WHERE.match(sql)
        if not m:
            continue
        where = m.group("where") or ""
        # Split ranges differ per partition; fold them out of the key. The
        # builder emits them bare (`"k" >= 10`), translated filters are always
        # parenthesized (`("k" >= 10)`), so only bare ones are dropped.
        where_key = re.sub(r'(?:^| AND )(?<!\()"\w+" (?:>=|<) -?\d+(?!\))', "", where)
        where_key = re.sub(r"^ AND ", "", where_key).strip()
        key = (m.group("table"), m.group("cols"), where_key)
        seen[key] = seen.get(key, 0) + 1
    return [(t, c, w, n) for (t, c, w), n in seen.items()]


def shorten(where: str, limit: int) -> str:
    where = re.sub(r"IN \(([^)]{%d,})\)" % limit,
                   lambda m: f"IN (<{m.group(1).count(',') + 1} values>)", where)
    return where


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--arneb-port", default="5432")
    ap.add_argument("--queries", default=",".join(f"q{i:02d}" for i in range(1, 23)))
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    report = ["| Query | PostgreSQL table | Columns | Remote WHERE | Statements |",
              "|---|---|---|---|---|"]
    for q in args.queries.split(","):
        tag = uuid.uuid4().hex[:8]
        begin, end = f"arneb-mark {q} {tag} begin", f"arneb-mark {q} {tag} end"
        psql_pg(f"SELECT '{begin}'")
        err = run_arneb(args.arneb_port, strip_sql(QUERIES_DIR / f"{q}.sql"))
        psql_pg(f"SELECT '{end}'")
        if err:
            print(f"{q}: arneb error: {err}", file=sys.stderr)
            report.append(f"| {q} | — | — | ERROR: {err} | — |")
            continue
        rows = summarize(statements_between(begin, end))
        print(f"### {q}: {len(rows)} distinct remote scan(s)")
        for table, cols, where, n in rows:
            print(f"  {table} [{n}x] cols={cols}\n    WHERE {shorten(where, 120) or '—'}")
            report.append(f"| {q} | {table} | {cols} | {shorten(where, 120) or '—'} | {n} |")
    if args.out:
        args.out.write_text("\n".join(report) + "\n")
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
