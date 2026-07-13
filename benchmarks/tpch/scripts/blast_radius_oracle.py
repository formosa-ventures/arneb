#!/usr/bin/env python3
"""SF30 correctness gate — determinism + cell-diff vs Trino (all 22 TPC-H queries).

The memory bench (`run_memory_bench.sh` + `verify_memory.py`) measures MEMORY and
only checks ROW COUNT — it never compares cell VALUES. The q21 finding
([[q21-sf30-nondeterministic-rowdrop]]) proved that at SF30 the distributed
deep-join pipeline can SILENTLY and NON-DETERMINISTICALLY drop rows: q21 @ N=2
returns 100 rows both times (the row-count gate "passes") yet ~26/100 rows differ
run-to-run and ~60/100 suppliers differ from Trino. So "rows match" is NOT a
correctness guarantee — this gate is. Blast radius measured 2026-06-11: of the
deep-join queries only q21 is affected; q07/q08/q09/q18 are clean
([[sf30-blast-radius-q21-only]]). Covers all 22 by DEFAULT (no silent
under-coverage — trino-diff's 17-query set omits exactly the heavies q17/q18/q20/
q21/q22 where this class of bug lives).

Two signals per query:

  1. DETERMINISM (`--runs` ≥ 2) — run arneb N times, compare result SETS
     run-to-run. Any run-to-run difference is a DEFINITIVE row-drop bug (same
     query/data/engine → must be identical). Immune to legitimate LIMIT
     tie-breaking differences vs Trino — this is what cleanly caught q21.

  2. CORRECTNESS — compare each arneb run's result SET against Trino's
     (deterministic baseline), tolerance-aware, with magnitude.

Comparison is an order-independent, float-tolerant MULTISET symmetric
difference: each numeric cell is canonicalized to `--sigfig` significant figures
(default 6) so decimal-vs-E-notation and last-digit accumulation noise don't
register, while a whole dropped/changed row shows up as an unmatched tuple.

Run on the bench host with the arneb + Trino stack up (see CLAUDE.md / the
[[reference-remote-bench-host]] recipe). Default is the FULL gate (all 22 × 2
runs) — at SF30 that is ~60-90 min (the deep-join heavies dominate). Faster
modes:
    python3 blast_radius_oracle.py                       # full gate, all 22, 2 runs
    python3 blast_radius_oracle.py --runs 1              # vs-Trino only, no determinism (~half the time)
    python3 blast_radius_oracle.py --queries q07,q09,q18,q21  # the deep-join subset

Exit 0 = every query deterministic AND cell-identical to Trino. Non-zero = at
least one query is non-deterministic, wrong, or unverifiable.
"""
import argparse
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"
# All 22 by default: a correctness gate must not silently under-cover. The
# trino-diff skill's 17-query set omits exactly the deep-join heavies
# (q17/q18/q20/q21/q22) where the q21-class silent row-drop lives.
DEFAULT_QUERIES = ",".join(f"q{i:02d}" for i in range(1, 23))
ARNEB_COORD = "arneb-arneb-1"
TRINO = "arneb-trino-1"

# Deterministic secondary ORDER BY key for queries whose top-N has BOUNDARY
# TIES. q10 is `ORDER BY SUM(l_extendedprice) DESC LIMIT 20` with no secondary
# key, so when the 20th and 21st customers tie on revenue each engine keeps a
# DIFFERENT one — a benign tie-break ambiguity that nonetheless makes the
# cell-diff flip 🟢/🟠 vs Trino run-to-run (a WEAK, noisy signal). Appending a
# stable secondary key (the GROUP BY / SELECT column `c_custkey`) before LIMIT
# makes BOTH engines choose the same boundary rows, so only a real divergence
# can still register. Applied to arneb AND Trino identically.
TIEBREAK = {"q10": "c_custkey"}


def apply_tiebreak(q: str, sql: str) -> str:
    """Insert `TIEBREAK[q]` as a trailing ORDER BY key, just before LIMIT, for a
    tie-prone query. No-op (with a loud warning) if the query has no
    `ORDER BY ... LIMIT` to augment — a misconfigured TIEBREAK entry."""
    tb = TIEBREAK.get(q)
    if not tb:
        return sql
    new_sql, n = re.subn(
        r"(?is)(\border\s+by\b.*?)(\s+limit\s+\d+)", rf"\1, {tb}\2", sql, count=1
    )
    if n == 0:
        print(f"  [tiebreak] WARNING: {q} has no 'ORDER BY ... LIMIT' to augment with {tb}")
        return sql
    return new_sql


def strip_sql(path: Path) -> str:
    # Drop `-- comment` lines BEFORE flattening — a leading comment otherwise
    # swallows the whole one-line query (Trino would return 0 rows).
    lines = [ln for ln in path.read_text().splitlines() if not ln.lstrip().startswith("--")]
    return " ".join(lines).strip().rstrip(";")


def run_arneb(sql: str) -> tuple[list[list[str]] | None, str | None]:
    """(rows, err). rows=None means the query errored (err set)."""
    out = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", "5432", "-U", "arneb", "-d", "arneb",
         "-tA", "-F", ",", "-q", "-c", "SET search_path TO datalake.tpch", "-c", sql],
        capture_output=True, text=True, env={**os.environ, "PGPASSWORD": "x"},
    )
    if out.returncode != 0:
        return None, (out.stderr.strip() or out.stdout.strip())[:300]
    rows = [ln.split(",") for ln in out.stdout.splitlines() if ln and ln != "SET"]
    return rows, None


def run_trino(sql: str) -> tuple[list[list[str]] | None, str | None]:
    out = subprocess.run(
        ["docker", "exec", TRINO, "trino", "--catalog", "hive", "--schema", "tpch",
         "--output-format", "CSV_UNQUOTED", "--execute", sql],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        return None, out.stderr.strip()[:300]
    rows = [ln.split(",") for ln in out.stdout.splitlines() if ln]
    return rows, None


def canon_cell(s: str, sig: int) -> str:
    try:
        f = float(s)
    except (ValueError, TypeError):
        return s
    if f == 0.0:
        return "0"
    return f"{f:.{sig}e}"


def canon_rows(rows: list[list[str]], sig: int) -> Counter:
    return Counter(tuple(canon_cell(c, sig) for c in r) for r in rows)


def symdiff(a: Counter, b: Counter) -> tuple[int, int]:
    """(rows only in a, rows only in b) — exact multiset symmetric difference."""
    return sum((a - b).values()), sum((b - a).values())


def _cell_close(x: str, y: str, rel_tol: float, abs_tol: float) -> bool:
    if x == y:
        return True
    try:
        return abs(float(x) - float(y)) <= max(rel_tol * max(abs(float(x)), abs(float(y))), abs_tol)
    except (ValueError, TypeError):
        return False


def _tuples_close(ta: tuple, tb: tuple, rel_tol: float, abs_tol: float) -> bool:
    return len(ta) == len(tb) and all(
        _cell_close(x, y, rel_tol, abs_tol) for x, y in zip(ta, tb)
    )


def symdiff_fuzzy(a: Counter, b: Counter, rel_tol: float, abs_tol: float) -> tuple[int, int]:
    """Exact multiset symdiff, then a RELATIVE-TOLERANCE reconcile of the
    residual: an a-only tuple cancels a b-only tuple when every cell matches
    (numeric within rel/abs tol, text exact). Absorbs last-bit float-accumulation
    noise — e.g. arneb's distributed SUM(l_extendedprice) summed in a different
    order than Trino's, which can straddle the canon rounding boundary (q10's
    765143.05 vs 765143.0499999999). Does NOT mask a real row drop: a dropped or
    genuinely-wrong row has no close counterpart to cancel against, so it still
    counts. rel_tol=1e-6 sits ~10x above the canon granularity and far below any
    meaningful logic difference."""
    a_only = list((a - b).elements())
    b_only = list((b - a).elements())
    if a_only and b_only:
        b_rem = list(b_only)
        unmatched = []
        for ta in a_only:
            j = next(
                (k for k, tb in enumerate(b_rem) if _tuples_close(ta, tb, rel_tol, abs_tol)),
                None,
            )
            if j is None:
                unmatched.append(ta)
            else:
                b_rem.pop(j)
        a_only, b_only = unmatched, b_rem
    return len(a_only), len(b_only)


def main() -> int:
    global ARNEB_COORD, TRINO
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default=DEFAULT_QUERIES)
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--sigfig", type=int, default=6)
    ap.add_argument("--rel-tol", type=float, default=1e-6,
                    help="relative tolerance for the residual fuzzy-reconcile "
                         "(absorbs float-accumulation/SUM-order noise)")
    ap.add_argument("--abs-tol", type=float, default=1e-6,
                    help="absolute tolerance for cells near zero")
    ap.add_argument("--arneb", default=ARNEB_COORD)
    ap.add_argument("--trino", default=TRINO)
    args = ap.parse_args()
    ARNEB_COORD, TRINO = args.arneb, args.trino

    # Footgun guard: ARNEB_MUST_DRAIN=0 disables the worker-side silent-
    # truncation guard (task_manager.rs) — exactly the q21-class row-drop this
    # gate exists to catch. Refuse to run a correctness gate against a cluster
    # that has it off (it would "pass" while silently dropping rows).
    must_drain = subprocess.run(
        ["docker", "exec", ARNEB_COORD, "printenv", "ARNEB_MUST_DRAIN"],
        capture_output=True, text=True).stdout.strip()
    if must_drain == "0":
        print(
            "REFUSING TO RUN: the arneb cluster has ARNEB_MUST_DRAIN=0, which DISABLES "
            "the silent-truncation guard this gate exists to catch — results could be "
            "silently truncated yet 'pass'. Unset it and recreate the cluster.",
            file=sys.stderr,
        )
        return 2

    target = subprocess.run(
        ["docker", "exec", ARNEB_COORD, "printenv", "ARNEB_MAX_HASH_PARTITIONS"],
        capture_output=True, text=True).stdout.strip() or "(unset → default 2)"
    print(f"blast_radius_oracle: ARNEB_MAX_HASH_PARTITIONS={target}  runs={args.runs}  "
          f"sigfig={args.sigfig}  rel_tol={args.rel_tol}")
    print("=" * 78)

    verdicts = {}
    for q in args.queries.split(","):
        sql = apply_tiebreak(q, strip_sql(QUERIES_DIR / f"{q}.sql"))
        print(f"\n### {q}")

        arneb_results = []  # list of (rows|None, err)
        for i in range(args.runs):
            rows, err = run_arneb(sql)
            arneb_results.append((rows, err))
            n = "ERR" if rows is None else len(rows)
            print(f"  arneb run{i + 1}: {n} rows" + (f"  ERROR: {err}" if err else ""))

        trino_rows, terr = run_trino(sql)
        print(f"  trino:     {'ERR' if trino_rows is None else len(trino_rows)} rows"
              + (f"  ERROR: {terr}" if terr else ""))

        ok_runs = [(i, r) for i, (r, _) in enumerate(arneb_results) if r is not None]
        if not ok_runs:
            verdicts[q] = "🔴 ERROR (all arneb runs failed)"
            print(f"  → {verdicts[q]}")
            continue

        # --- determinism: pairwise multiset symdiff across successful arneb runs
        canons = {i: canon_rows(r, args.sigfig) for i, r in ok_runs}
        base_i = ok_runs[0][0]
        nondet_max = 0
        for i, _ in ok_runs[1:]:
            oa, ob = symdiff_fuzzy(canons[base_i], canons[i], args.rel_tol, args.abs_tol)
            nondet_max = max(nondet_max, oa + ob)
            print(f"  determinism run{base_i + 1} vs run{i + 1}: "
                  f"{oa} only-in-run{base_i + 1} / {ob} only-in-run{i + 1}"
                  + ("  ✅ identical" if oa + ob == 0 else "  ❌ DIFFER"))

        # --- correctness vs Trino (each successful run)
        trino_mismatch = None
        if trino_rows is not None:
            ct = canon_rows(trino_rows, args.sigfig)
            for i, _ in ok_runs:
                oa, ot = symdiff_fuzzy(canons[i], ct, args.rel_tol, args.abs_tol)
                tag = "✅ match" if oa + ot == 0 else "❌ MISMATCH"
                print(f"  vs-Trino run{i + 1}: {oa} arneb-only / {ot} trino-only  {tag}")
                if trino_mismatch is None:
                    trino_mismatch = oa + ot
                else:
                    trino_mismatch = max(trino_mismatch, oa + ot)

        any_err = any(r is None for r, _ in arneb_results)
        if nondet_max > 0:
            verdicts[q] = f"🔴 NON-DETERMINISTIC ROW-DROP (≤{nondet_max} rows churn run-to-run)"
        elif trino_rows is None:
            verdicts[q] = "🟡 deterministic, but Trino baseline errored (cannot verify)"
        elif trino_mismatch and trino_mismatch > 0:
            verdicts[q] = f"🟠 DETERMINISTIC but ≠ Trino ({trino_mismatch} rows differ — logic bug or LIMIT tie)"
        elif any_err:
            verdicts[q] = "🟡 some arneb runs errored (non-deterministic failure)"
        else:
            verdicts[q] = "🟢 PASS (deterministic + cell-identical to Trino)"
        print(f"  → {verdicts[q]}")

    print("\n" + "=" * 78)
    print("BLAST-RADIUS SUMMARY")
    for q, v in verdicts.items():
        print(f"  {q}: {v}")
    bad = sum(1 for v in verdicts.values() if v[0] != "🟢")
    print(f"\n{len(verdicts) - bad}/{len(verdicts)} clean (🟢);  {bad} not clean")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
