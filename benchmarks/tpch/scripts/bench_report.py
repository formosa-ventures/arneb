#!/usr/bin/env python3
"""Neutral TPC-H benchmark report: arneb vs Trino, latency + peak memory.

Reads a `memory_total_*.csv` produced by `run_memory_bench.sh` and prints a
Markdown comparison table — per-query latency and peak memory for each engine,
the latency speedup (Trino / arneb), and arneb's peak memory as a fraction of
Trino's — plus a geometric-mean speedup and mean memory ratio. There are no
pass/fail thresholds; the numbers speak for themselves.

Usage:
    python3 bench_report.py <memory_total_*.csv>

A CSV carrying both `cold` and `warm` rows (the `mode` column, written by
`BENCH_MODE=both`) renders a side-by-side latency table — cold arneb, cold
Trino, cold speedup, then the same three warm — with one geomean per mode,
followed by the peak-memory table from the warm rows. A single-mode CSV, or one
from before the `mode` column existed, renders the single table as before.

The memory metric is `sim_peak` (simultaneous peak resident set summed across
the cluster), the same value `run_memory_bench.sh` records; it falls back to
`peak_kib` when `sim_peak_kib` is absent.

If a sidecar `<csv-stem>.provenance.json` is present — written by
`run_benchmark.sh` — its scale factor and scenario are printed above the table,
so a number cannot be read without the scale it was measured at. A CSV with no
sidecar still renders; the scale prints as `unrecorded` rather than being
assumed.
"""
import csv
import json
import sys
from pathlib import Path


def geomean(values: list[float]) -> float:
    if not values:
        return float("nan")
    prod = 1.0
    for v in values:
        prod *= v
    return prod ** (1.0 / len(values))


def load_provenance(csv_path: str) -> dict:
    """Read the sidecar provenance record, or {} when there is none.

    Absence is a legitimate state — a CSV produced by running
    `run_memory_bench.sh` directly has no sidecar — so this never raises. A
    malformed sidecar is treated the same as an absent one rather than
    aborting a report whose numbers are perfectly readable.
    """
    sidecar = Path(csv_path).with_suffix(".provenance.json")
    try:
        with open(sidecar) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def print_header(prov: dict) -> None:
    scale = prov.get("scale_factor") or "unrecorded"
    scenario = prov.get("scenario") or "unrecorded"
    print(f"**Scale factor:** {scale}   ·   **Scenario:** {scenario}", end="")
    mode = prov.get("mode")
    if mode:
        print(f"   ·   **Mode:** {mode}", end="")
    elif prov.get("bench_warmup") in (1, "1", True):
        print("   ·   **Warm-up:** on", end="")
    elif prov:
        print("   ·   **Warm-up:** off", end="")
    print()
    if prov.get("preflight_bypassed"):
        # A bypassed run must not be indistinguishable from a clean one.
        print()
        print("> **Preflight bypassed** (`SKIP_PREFLIGHT=1`) — the host did not "
              "meet this scenario's declared floors.")
    print()


def load_rows(path: str) -> dict[str, dict[str, dict[str, dict]]]:
    """{mode: {engine: {query: row}}}. Rows without a `mode` column (CSVs
    written before it existed) are filed under "unrecorded"."""
    by_mode: dict[str, dict[str, dict[str, dict]]] = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            row = {
                "lat_ms": float(r["latency_ms"]),
                "mem_kib": float(r.get("sim_peak_kib") or r.get("peak_kib") or 0),
                "status": r["status"],
            }
            mode = r.get("mode") or "unrecorded"
            engine = "arneb" if r["engine"] == "arneb" else "trino"
            by_mode.setdefault(mode, {"arneb": {}, "trino": {}})[engine][r["query"]] = row
    return by_mode


def usable(a: dict | None, t: dict | None) -> bool:
    return bool(a and t and a["lat_ms"] > 0 and t["lat_ms"] > 0 and t["mem_kib"] > 0)


def print_single(arneb: dict[str, dict], trino: dict[str, dict]) -> None:
    print("| Query | arneb (ms) | Trino (ms) | Speedup | arneb (MB) | Trino (MB) | Mem vs Trino |")
    print("|-------|-----------:|-----------:|--------:|-----------:|-----------:|-------------:|")

    speedups: list[float] = []
    mem_ratios: list[float] = []
    for q in sorted(set(arneb) | set(trino)):
        a, t = arneb.get(q), trino.get(q)
        if not usable(a, t):
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


def print_cold_warm(cold: dict, warm: dict) -> None:
    print("Latency (ms) — cold: fresh cluster, RustFS restarted, page cache dropped; "
          "warm: fresh cluster plus one untimed run of the same query.")
    print()
    print("| Query | cold arneb | cold Trino | cold | warm arneb | warm Trino | warm |")
    print("|-------|-----------:|-----------:|-----:|-----------:|-----------:|-----:|")

    speedups: dict[str, list[float]] = {"cold": [], "warm": []}
    queries = set()
    for m in (cold, warm):
        queries |= set(m["arneb"]) | set(m["trino"])
    for q in sorted(queries):
        cells = [q]
        for name, m in (("cold", cold), ("warm", warm)):
            a, t = m["arneb"].get(q), m["trino"].get(q)
            if a and t and a["lat_ms"] > 0 and t["lat_ms"] > 0:
                sp = t["lat_ms"] / a["lat_ms"]
                speedups[name].append(sp)
                cells += [f"{a['lat_ms']:.0f}", f"{t['lat_ms']:.0f}", f"{sp:.1f}x"]
            else:
                cells += ["n/a", "n/a", "n/a"]
        print("| " + " | ".join(cells) + " |")

    print()
    print(f"Geomean speedup: cold {geomean(speedups['cold']):.1f}x, "
          f"warm {geomean(speedups['warm']):.1f}x")
    print()
    print("Peak memory (warm run):")
    print()
    print_single(warm["arneb"], warm["trino"])


def main(path: str) -> int:
    print_header(load_provenance(path))
    by_mode = load_rows(path)
    if "cold" in by_mode and "warm" in by_mode:
        print_cold_warm(by_mode["cold"], by_mode["warm"])
    else:
        for mode, engines in by_mode.items():
            if len(by_mode) > 1:
                print(f"**{mode}**")
                print()
            print_single(engines["arneb"], engines["trino"])
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
