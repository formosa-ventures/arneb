#!/usr/bin/env python3
"""Forced-N>2 nested-join correctness oracle (dist-mxn-nested-joins).

Runs nested multi-way join queries through arneb and Trino against the local
SF1 distributed stack and cell-diffs them numerically (float-tolerant, sorted).
The hash fan-out N is controlled OUT OF BAND by `ARNEB_HASH_PARTITION_TARGET_ROWS`
on the arneb coordinator (set a low value + restart arneb to force N>2); this
script does not manage the stack — it runs the queries, diffs, and reports the
observed N (from the coordinator's task-submission log).

This is the safety belt for the M×N nested-join fix: it reproduces the N>2
row-drop in minutes instead of a 90-minute SF30 run.

Usage:
    python3 nway_oracle.py [--queries q05,q07,q08,q09]

Exit code 0 = all queries cell-identical to Trino; non-zero = at least one FAIL.

Invariants checked per query:
  - same row count as Trino (no undercount / duplication)
  - every cell matches Trino within 1e-9 relative (numeric) / exact (string)
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"
REL_TOL = 1e-9
ARNEB_COORD = "arneb-arneb-1"
TRINO = "arneb-trino-1"


def strip_sql(path: Path) -> str:
    # Drop `-- comment` lines BEFORE flattening — otherwise a leading comment
    # swallows the whole one-line query (the bug that made Trino return 0 rows).
    lines = [ln for ln in path.read_text().splitlines() if not ln.lstrip().startswith("--")]
    return " ".join(lines).strip().rstrip(";")


def run_arneb(sql: str) -> list[list[str]]:
    out = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", "5432", "-U", "arneb", "-d", "arneb",
         "-tA", "-F", ",", "-q", "-c", "SET search_path TO datalake.tpch", "-c", sql],
        capture_output=True, text=True, env={**os.environ, "PGPASSWORD": "x"},
    )
    if out.returncode != 0 or out.stderr.strip():
        raise RuntimeError(f"arneb error: {out.stderr.strip() or out.stdout.strip()[:300]}")
    return [ln.split(",") for ln in out.stdout.splitlines() if ln and ln != "SET"]


def run_trino(sql: str) -> list[list[str]]:
    out = subprocess.run(
        ["docker", "exec", TRINO, "trino", "--catalog", "hive", "--schema", "tpch",
         "--output-format", "CSV_UNQUOTED", "--execute", sql],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        raise RuntimeError(f"trino error: {out.stderr.strip()[:300]}")
    return [ln.split(",") for ln in out.stdout.splitlines() if ln]


def cells_equal(a: str, b: str) -> bool:
    if a == b:
        return True
    try:
        fa, fb = float(a), float(b)
    except ValueError:
        return False
    scale = max(abs(fa), abs(fb), 1.0)
    return abs(fa - fb) <= REL_TOL * scale


def diff(arneb: list[list[str]], trino: list[list[str]]) -> str | None:
    a = sorted(arneb)
    t = sorted(trino)
    if len(a) != len(t):
        return f"row count {len(a)} != Trino {len(t)}"
    for i, (ra, rt) in enumerate(zip(a, t)):
        if len(ra) != len(rt):
            return f"row {i} arity {len(ra)} != {len(rt)}"
        for ca, ct in zip(ra, rt):
            if not cells_equal(ca, ct):
                return f"row {i}: {ra} != Trino {rt}"
    return None


def observed_n() -> str:
    try:
        logs = subprocess.run(["docker", "logs", ARNEB_COORD], capture_output=True, text=True).stdout
        parts = [int(p.split("=")[1]) for p in logs.split() if p.startswith("partition=")]
        return str(max(parts) + 1) if parts else "?"
    except Exception:
        return "?"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default="q05,q07,q08,q09")
    args = ap.parse_args()

    target = subprocess.run(
        ["docker", "exec", ARNEB_COORD, "printenv", "ARNEB_HASH_PARTITION_TARGET_ROWS"],
        capture_output=True, text=True).stdout.strip() or "(unset/default)"
    print(f"nway_oracle: ARNEB_HASH_PARTITION_TARGET_ROWS={target}")

    failures = 0
    for q in args.queries.split(","):
        sql = strip_sql(QUERIES_DIR / f"{q}.sql")
        try:
            ar, tr = run_arneb(sql), run_trino(sql)
        except RuntimeError as e:
            print(f"  {q}: ERROR {e}")
            failures += 1
            continue
        problem = diff(ar, tr)
        status = "PASS" if problem is None else f"FAIL ({problem})"
        if problem:
            failures += 1
        print(f"  {q}: {status}  (arneb {len(ar)} rows, trino {len(tr)} rows)")

    print(f"observed fan-out N (max submitted partition+1): {observed_n()}")
    print(f"{'ALL PASS' if failures == 0 else f'{failures} FAIL'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
