#!/usr/bin/env python3
"""Pass/fail gate for per-query latency + total-cluster-memory bench.

Reads the CSV emitted by run_memory_bench.sh and asserts for each query:
  1. arneb.status == OK AND trino.status == OK
  2. arneb.rows == trino.rows (correctness — required as of B-fix-2 2026-05-22)
  3. arneb.latency_ms < 0.6 * trino.latency_ms
  4. arneb mem < trino mem, where "mem" is MEMORY_METRIC (default sim_peak —
     the true SIMULTANEOUS cluster peak; see the metric note below)

Returns non-zero if any query fails any criterion.

B-fix-2 (2026-05-22): the row-count gate exists because Z.4 (commit 6c41e30)
revealed that partial-result silent failures had been passing the OK-status
gate for multiple commits — coord was swallowing worker `ResourceExhausted`
errors and returning whatever rows the surviving tasks produced. Without
this gate, any future memory-tuning win can be fake.

Cell-correctness caveat (2026-06-11): criterion 2 compares only the ROW COUNT
against Trino — it does NOT compare cell values. q21 @ SF30 proved a query can
match row count (100==100) yet silently and NON-deterministically drop/mis-route
rows (~60/100 cells differ from Trino, ~26 rows churn run-to-run). The SF30
cell-correctness gate is benchmarks/tpch/scripts/blast_radius_oracle.py
(determinism + tolerance-aware cell-diff vs Trino); run it alongside this memory
gate. "Rows match" is necessary, NOT sufficient.

Total-peak switch (2026-05-26): the bench previously gated on `delta_kib`
(per-query growth across workers). That hid Trino's ~2 GB committed JVM
baseline — a real production cost when allocating cluster resources —
and made arneb (which lazy-allocates) look 2.4x worse on memory while
in absolute cluster terms it uses HALF Trino's peak RSS. The gate now
uses total cluster peak (coord + 2 workers) so the metric matches what
`kubectl top pods` or any container memory limit would report.

Usage:
    python3 verify_memory.py path/to/memory_total_*.csv

Optional thresholds (env vars):
    LATENCY_RATIO    target ratio (default 0.6)
    MEMORY_RATIO     target ratio (default 1.0; i.e. strictly less than Trino)
    MEMORY_METRIC    peak | sim_peak | sim_anon  (default sim_peak)
"""

import csv
import os
import sys
from collections import defaultdict


LATENCY_RATIO = float(os.environ.get("LATENCY_RATIO", "0.6"))
MEMORY_RATIO = float(os.environ.get("MEMORY_RATIO", "1.0"))

# Which cgroup memory column the gate compares (run_memory_bench.sh emits
# all three). Default `sim_peak`: the true SIMULTANEOUS cluster peak — the
# max over the query's lifetime of the summed per-node memory.current. This
# fixes the legacy `peak`'s flaw of summing per-node LIFETIME peaks that
# never co-occurred (it over-counts the more-pipelined engine). Options:
#   peak     — legacy: sum of per-node memory.peak (non-simultaneous + cache)
#   sim_peak — simultaneous total resident RSS  (the fair default)
#   sim_anon — simultaneous UN-reclaimable anon (excludes evictable spill
#              page cache; a DIAGNOSTIC, not the default — it is a definitional
#              choice that favours engines which spill to disk over those that
#              hold anon, so it is reported but not gated by default)
MEMORY_METRIC = os.environ.get("MEMORY_METRIC", "sim_peak")
_METRIC_COL = {
    "peak": "peak_kib",
    "sim_peak": "sim_peak_kib",
    "sim_anon": "sim_anon_kib",
}.get(MEMORY_METRIC, "sim_peak_kib")


def mem_kib(row):
    """Gated memory value (per MEMORY_METRIC), falling back to peak_kib for
    legacy CSVs that predate the simultaneous-sampling columns."""
    v = row.get(_METRIC_COL, 0)
    return v if v > 0 else row["peak_kib"]


def load(csv_path):
    """Return dict[query] -> dict[engine] -> row."""
    by_q = defaultdict(dict)
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            by_q[row["query"]][row["engine"]] = {
                "status": row["status"],
                "latency_ms": float(row["latency_ms"] or 0),
                "baseline_kib": int(row["baseline_kib"] or 0),
                "peak_kib": int(row["peak_kib"] or 0),
                # Simultaneous-sampling columns (max-over-time of the summed
                # per-node cgroup value during the query). 0 on legacy CSVs.
                "sim_peak_kib": int(row.get("sim_peak_kib") or 0),
                "sim_anon_kib": int(row.get("sim_anon_kib") or 0),
                # Kept as a secondary column for inspection / regression
                # reports; the gate no longer uses it (see module docstring
                # for the 2026-05-26 switch to total cluster peak).
                "delta_kib": int(row.get("delta_kib") or 0),
                # B-fix-2 (2026-05-22): older CSVs predate the rows
                # column; default to -1 (unknown, can't gate on
                # correctness). Newer CSVs always populate this.
                "rows": int(row.get("rows") or -1),
            }
    return by_q


def fmt_ratio(num, den):
    if den <= 0:
        return "n/a"
    return f"{num / den:.2f}x"


def check(by_q):
    passed = []
    failed = []
    rows = []

    for q in sorted(by_q.keys()):
        engines = by_q[q]
        arneb = engines.get("arneb")
        trino = engines.get("trino")

        if arneb is None or trino is None:
            failed.append((q, "missing engine row"))
            rows.append((q, arneb, trino, False, False, None, "missing"))
            continue

        a_ok = arneb["status"] == "OK"
        t_ok = trino["status"] == "OK"
        if not a_ok:
            failed.append((q, f"arneb status={arneb['status']}"))
            rows.append((q, arneb, trino, False, False, None, f"arneb {arneb['status']}"))
            continue
        if not t_ok:
            # Trino failure: can't compare. Treat as ambiguous (skip but report).
            failed.append((q, f"trino status={trino['status']}"))
            rows.append((q, arneb, trino, False, False, None, f"trino {trino['status']}"))
            continue

        # B-fix-2 correctness gate: row count must match Trino.
        # `rows == -1` means a legacy CSV that pre-dates this column;
        # don't gate, just report (don't trust the rest either).
        a_rows = arneb.get("rows", -1)
        t_rows = trino.get("rows", -1)
        if a_rows >= 0 and t_rows >= 0:
            rows_pass = a_rows == t_rows
        else:
            rows_pass = None  # ambiguous

        lat_ratio = arneb["latency_ms"] / max(trino["latency_ms"], 1e-9)
        # Memory gate uses the SIMULTANEOUS total cluster peak by default
        # (MEMORY_METRIC=sim_peak) — see module docstring / the metric note.
        mem_ratio = mem_kib(arneb) / max(mem_kib(trino), 1)

        lat_pass = lat_ratio < LATENCY_RATIO
        mem_pass = mem_ratio < MEMORY_RATIO
        # Correctness mismatch fails the whole gate independently of
        # latency / memory: a fast wrong answer is not a win.
        all_pass = lat_pass and mem_pass and (rows_pass is True)

        rows.append((q, arneb, trino, lat_pass, mem_pass, rows_pass, ""))
        if all_pass:
            passed.append(q)
        else:
            reasons = []
            if rows_pass is False:
                reasons.append(f"rows arneb={a_rows} trino={t_rows}")
            elif rows_pass is None:
                reasons.append("rows unknown (legacy CSV)")
            if not lat_pass:
                reasons.append(f"latency {lat_ratio:.2f}x >= {LATENCY_RATIO}")
            if not mem_pass:
                reasons.append(f"memory {mem_ratio:.2f}x >= {MEMORY_RATIO}")
            failed.append((q, "; ".join(reasons)))

    return rows, passed, failed


def print_table(rows):
    print()
    # `arneb_kib`/`trino_kib`/`mem_x` reflect the GATED metric (MEMORY_METRIC,
    # default sim_peak). `sa_x` is the sim_anon ratio — a DIAGNOSTIC showing
    # how much of the gap is reclaimable spill cache (not gated).
    print(f"(memory gate metric = {MEMORY_METRIC}; sa_x = sim_anon ratio, diagnostic)")
    print(
        f"{'Q':<4} {'arneb_ms':>10} {'trino_ms':>10} {'lat_x':>7} "
        f"{'arneb_kib':>11} {'trino_kib':>11} {'mem_x':>7} {'sa_x':>6} "
        f"{'rows_a':>7} {'rows_t':>7}  result"
    )
    print("-" * 112)
    for q, a, t, lat_pass, mem_pass, rows_pass, note in rows:
        if a is None or t is None:
            print(
                f"{q:<4} {'-':>10} {'-':>10} {'-':>7} {'-':>11} {'-':>11} "
                f"{'-':>7} {'-':>6} {'-':>7} {'-':>7}  {note}"
            )
            continue
        a_ms = a["latency_ms"]
        t_ms = t["latency_ms"]
        a_kib = mem_kib(a)
        t_kib = mem_kib(t)
        a_rows = a.get("rows", -1)
        t_rows = t.get("rows", -1)
        if t_ms > 0:
            lat_x = f"{a_ms / t_ms:.2f}"
        else:
            lat_x = "n/a"
        if t_kib > 0:
            mem_x = f"{a_kib / t_kib:.2f}"
        else:
            mem_x = "n/a"
        t_sa = t.get("sim_anon_kib", 0)
        sa_x = f"{a.get('sim_anon_kib', 0) / t_sa:.2f}" if t_sa > 0 else "n/a"
        marks = []
        if rows_pass is True:
            marks.append("C✓")
        elif rows_pass is False:
            marks.append("C✗")
        else:
            marks.append("C?")
        marks.append("L✓" if lat_pass else "L✗")
        marks.append("M✓" if mem_pass else "M✗")
        if note:
            marks.append(note)
        print(
            f"{q:<4} {a_ms:>10.1f} {t_ms:>10.1f} {lat_x:>7} "
            f"{a_kib:>11} {t_kib:>11} {mem_x:>7} {sa_x:>6} "
            f"{a_rows:>7} {t_rows:>7}  {' '.join(marks)}"
        )


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    by_q = load(csv_path)
    rows, passed, failed = check(by_q)

    print(
        f"Gate: rows = trino AND latency < {LATENCY_RATIO} x trino "
        f"AND {MEMORY_METRIC} < {MEMORY_RATIO} x trino"
    )
    print(f"CSV: {csv_path}")
    print_table(rows)
    print()
    print(f"Summary: {len(passed)} pass, {len(failed)} fail")
    if passed:
        print("PASS:", " ".join(passed))
    if failed:
        print("FAIL:")
        for q, reason in failed:
            print(f"  {q}: {reason}")

    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
