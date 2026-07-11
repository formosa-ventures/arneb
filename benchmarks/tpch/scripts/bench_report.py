#!/usr/bin/env python3
"""Neutral TPC-H benchmark report: arneb vs Trino, latency + peak memory.

Reads a `memory_total_*.csv` produced by `run_memory_bench.sh` and prints a
Markdown comparison table — per-query latency and peak memory for each engine,
the latency speedup (Trino / arneb), and arneb's peak memory as a fraction of
Trino's — plus a geometric-mean speedup and mean memory ratio. There are no
pass/fail thresholds; the numbers speak for themselves.

Usage:
    python3 bench_report.py <memory_total_*.csv>

The memory metric is `sim_peak` (simultaneous peak resident set summed across
the cluster), the same value `run_memory_bench.sh` records; it falls back to
`peak_kib` when `sim_peak_kib` is absent.
"""
import csv
import sys


def geomean(values: list[float]) -> float:
    if not values:
        return float("nan")
    prod = 1.0
    for v in values:
        prod *= v
    return prod ** (1.0 / len(values))


def main(path: str) -> int:
    arneb: dict[str, dict] = {}
    trino: dict[str, dict] = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            row = {
                "lat_ms": float(r["latency_ms"]),
                "mem_kib": float(r.get("sim_peak_kib") or r.get("peak_kib") or 0),
                "status": r["status"],
            }
            (arneb if r["engine"] == "arneb" else trino)[r["query"]] = row

    print("| Query | arneb (ms) | Trino (ms) | Speedup | arneb (MB) | Trino (MB) | Mem vs Trino |")
    print("|-------|-----------:|-----------:|--------:|-----------:|-----------:|-------------:|")

    speedups: list[float] = []
    mem_ratios: list[float] = []
    for q in sorted(set(arneb) | set(trino)):
        a, t = arneb.get(q), trino.get(q)
        if not a or not t or t["lat_ms"] <= 0 or t["mem_kib"] <= 0:
            print(f"| {q:<5} | {'n/a':>10} | {'n/a':>10} | {'n/a':>7} | {'n/a':>10} | {'n/a':>10} | {'n/a':>12} |")
            continue
        a_mb, t_mb = a["mem_kib"] / 1024, t["mem_kib"] / 1024
        speedup = t["lat_ms"] / a["lat_ms"]
        mem_ratio = a_mb / t_mb
        speedups.append(speedup)
        mem_ratios.append(mem_ratio)
        print(
            f"| {q:<5} | {a['lat_ms']:>10.0f} | {t['lat_ms']:>10.0f} | "
            f"{speedup:>6.1f}x | {a_mb:>10.0f} | {t_mb:>10.0f} | {mem_ratio:>11.2f}x |"
        )

    if speedups:
        gm = geomean(speedups)
        mm = sum(mem_ratios) / len(mem_ratios)
        print()
        print(f"Geomean speedup: {gm:.1f}x   ·   Mean peak-mem vs Trino: {mm:.2f}x")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
