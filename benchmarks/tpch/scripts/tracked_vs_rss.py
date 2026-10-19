#!/usr/bin/env python3
"""Tracked memory vs resident memory, per query and worker.

Reads a run_memory_bench.sh CSV and the `<query>_<mode>.mem` event logs that
sit next to it, then reports how much of each worker's peak the memory pool
saw (exec-memory-accounting tasks 1.1 / 2.4 / 4.3: "tracked fraction").

Per worker and query:
  tracked     max `tracked_peak_bytes` of the `arneb::mem` task events
              (the pool's high-water; the process was fresh for this query)
  jemalloc    max `jemalloc_resident_peak_bytes` of the `arneb::memprofile`
              events (only with the memprofile overlay; sampled on each
              reservation grow, so it can miss a peak no grow coincided with)
  cgroup      the node's cgroup memory.peak from the CSV
  trk/cg      tracked / cgroup, the headline ratio
  trk/je      tracked / jemalloc, the ratio without page cache or allocator
              overhead outside jemalloc

Usage:
  tracked_vs_rss.py RESULTS.csv [--mode cold|warm]
"""

import argparse
import csv
import re
import statistics
import sys
from pathlib import Path

WORKERS = [("arneb-worker-1", "node1_peak_kib"), ("arneb-worker-2", "node2_peak_kib")]
ANSI = re.compile(r"\x1b\[[0-9;]*m")
FIELD = re.compile(r"\b(tracked_peak_bytes|jemalloc_resident_peak_bytes)=(\d+)")


def parse_mem_log(path):
    """{container: {field: max value}} from one query's event log."""
    peaks = {}
    if not path.exists():
        return peaks
    for line in path.read_text(errors="replace").splitlines():
        line = ANSI.sub("", line)
        container, _, rest = line.partition(" ")
        node = peaks.setdefault(container, {})
        for name, value in FIELD.findall(rest):
            node[name] = max(node.get(name, 0), int(value))
    return peaks


def gib(n):
    return f"{n / 2**30:.2f}" if n else "-"


def ratio(num, den):
    return num / den if num and den else None


def fmt_ratio(r):
    return f"{r:.2f}" if r is not None else "-"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", type=Path)
    ap.add_argument("--mode", default=None, help="only rows of this mode")
    args = ap.parse_args()

    log_dir = args.csv.with_name(args.csv.stem + "_logs")
    rows = [
        r
        for r in csv.DictReader(args.csv.open())
        if r["engine"] == "arneb" and (args.mode is None or r["mode"] == args.mode)
    ]
    if not rows:
        sys.exit(f"no arneb rows in {args.csv}")

    print("| query | mode | status | worker | tracked GiB | jemalloc GiB | cgroup GiB | trk/cg | trk/je |")
    print("|---|---|---|---|---:|---:|---:|---:|---:|")
    trk_cg, trk_je, missing = [], [], 0
    for r in rows:
        peaks = parse_mem_log(log_dir / f"arneb_{r['query']}_{r['mode']}.mem")
        for container, col in WORKERS:
            node = peaks.get(container, {})
            tracked = node.get("tracked_peak_bytes", 0)
            jemalloc = node.get("jemalloc_resident_peak_bytes", 0)
            cgroup = int(r[col] or 0) * 1024
            if not tracked:
                missing += 1
            rc, rj = ratio(tracked, cgroup), ratio(tracked, jemalloc)
            if r["status"] == "OK":
                if rc is not None:
                    trk_cg.append(rc)
                if rj is not None:
                    trk_je.append(rj)
            print(
                f"| {r['query']} | {r['mode']} | {r['status']} | {container} | {gib(tracked)} "
                f"| {gib(jemalloc)} | {gib(cgroup)} | {fmt_ratio(rc)} | {fmt_ratio(rj)} |"
            )

    print()
    for name, values in (("tracked / cgroup peak", trk_cg), ("tracked / jemalloc resident", trk_je)):
        if values:
            print(
                f"{name}: median {statistics.median(values):.2f}, "
                f"min {min(values):.2f}, max {max(values):.2f} (n={len(values)})"
            )
        else:
            print(f"{name}: no data")
    if missing:
        print(f"worker/query pairs with no arneb::mem event: {missing}")


if __name__ == "__main__":
    main()
