#!/usr/bin/env python3
"""Render a run_memory_bench CSV as a dual-axis pass/fail table.

Gate (the /goal): for each query, arneb WINS iff
    latency_ms  < 0.6 * trino.latency_ms   AND   sim_peak_kib < 0.8 * trino.sim_peak_kib

sim_peak_kib is the headline memory metric (simultaneous cluster peak). Falls
back to peak_kib if sim_peak is absent. Prints per-query ratios + a summary
(how many pass both, which fail and on which axis).

Usage: python3 dual_axis_table.py <memory_total_*.csv>
"""
import csv
import sys
from collections import defaultdict

LAT_GATE = 0.6
MEM_GATE = 0.8


def main(path: str) -> int:
    rows = defaultdict(dict)
    with open(path) as f:
        for r in csv.DictReader(f):
            rows[r["query"]][r["engine"]] = r

    def memkib(r):
        v = r.get("sim_peak_kib") or r.get("peak_kib") or "0"
        try:
            return float(v)
        except ValueError:
            return 0.0

    print(f"{'q':<5}{'a_lat_s':>9}{'t_lat_s':>9}{'lat_x':>7}{'a_mem_G':>9}{'t_mem_G':>9}{'mem_x':>7}  verdict")
    print("-" * 78)
    win = lat_only = mem_only = fail = broken = 0
    losers = []
    for q in sorted(rows):
        a = rows[q].get("arneb")
        t = rows[q].get("trino")
        if not a or not t or a["status"] != "OK" or t["status"] != "OK":
            print(f"{q:<5}  status: arneb={a['status'] if a else 'NA'} trino={t['status'] if t else 'NA'}")
            broken += 1
            continue
        al, tl = float(a["latency_ms"]), float(t["latency_ms"])
        am, tm = memkib(a), memkib(t)
        lx = al / tl if tl else float("inf")
        mx = am / tm if tm else float("inf")
        lat_ok = lx < LAT_GATE
        mem_ok = mx < MEM_GATE
        if lat_ok and mem_ok:
            v = "WIN BOTH"; win += 1
        elif lat_ok:
            v = "lat-only (mem FAIL)"; lat_only += 1; losers.append((q, "mem", mx))
        elif mem_ok:
            v = "mem-only (lat FAIL)"; mem_only += 1; losers.append((q, "lat", lx))
        else:
            v = "FAIL BOTH"; fail += 1; losers.append((q, "both", max(lx, mx)))
        mark = "" if (lat_ok and mem_ok) else "  <<<"
        print(f"{q:<5}{al/1000:>9.1f}{tl/1000:>9.1f}{lx:>7.2f}{am/1e6:>9.2f}{tm/1e6:>9.2f}{mx:>7.2f}  {v}{mark}")
    print("-" * 78)
    total = win + lat_only + mem_only + fail
    print(f"WIN BOTH: {win}/{total}   lat-only: {lat_only}   mem-only: {mem_only}   FAIL BOTH: {fail}   broken: {broken}")
    if losers:
        print("\nLosers (query, failing-axis, worst-ratio):")
        for q, axis, ratio in sorted(losers, key=lambda x: -x[2]):
            print(f"  {q}: {axis}  ratio={ratio:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
