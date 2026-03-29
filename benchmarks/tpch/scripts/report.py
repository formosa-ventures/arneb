#!/usr/bin/env python3
"""
Generate a comparison report from TPC-H benchmark results.

Usage:
    python3 benchmarks/tpch/scripts/report.py \
        benchmarks/tpch/results/trino-alt_*.json \
        benchmarks/tpch/results/trino_*.json
"""

import json
import math
import sys
from pathlib import Path


def load_results(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    return s[n // 2]


def geometric_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    log_sum = sum(math.log(v) for v in values if v > 0)
    return math.exp(log_sum / len(values))


def extract_medians(data: dict) -> dict[str, float]:
    result = {}
    for q in data.get("queries", []):
        qid = q["query_id"]
        if q.get("status") == "ok" and q.get("median_ms") is not None:
            result[qid] = q["median_ms"]
        else:
            # Calculate from runs if median_ms not present.
            timings = [
                r["wall_clock_ms"]
                for r in q.get("runs", [])
                if not r.get("is_warmup", False)
            ]
            if timings:
                result[qid] = median(timings)
    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: report.py <trino-alt-results.json> [trino-results.json]")
        sys.exit(1)

    alt_path = sys.argv[1]
    trino_path = sys.argv[2] if len(sys.argv) > 2 else None

    alt_data = load_results(alt_path)
    alt_medians = extract_medians(alt_data)

    trino_medians = {}
    if trino_path:
        trino_data = load_results(trino_path)
        trino_medians = extract_medians(trino_data)

    # All query IDs.
    all_ids = sorted(set(list(alt_medians.keys()) + list(trino_medians.keys())))

    # Print header.
    print("# TPC-H Benchmark Comparison")
    print()
    print(f"**trino-alt**: {alt_data.get('timestamp', 'N/A')}")
    if trino_path:
        print(f"**Trino**: {trino_data.get('timestamp', 'N/A')}")
    print()

    if trino_medians:
        print(
            f"| {'Query':<8} | {'trino-alt (ms)':>15} | {'Trino (ms)':>12} | {'Speedup':>8} |"
        )
        print(f"|{'-'*10}|{'-'*17}|{'-'*14}|{'-'*10}|")
    else:
        print(f"| {'Query':<8} | {'Median (ms)':>12} | {'Status':<8} |")
        print(f"|{'-'*10}|{'-'*14}|{'-'*10}|")

    speedups = []

    for qid in all_ids:
        alt_ms = alt_medians.get(qid)
        trino_ms = trino_medians.get(qid)

        if trino_medians:
            alt_str = f"{alt_ms:.1f}" if alt_ms else "-"
            trino_str = f"{trino_ms:.1f}" if trino_ms else "-"

            if alt_ms and trino_ms and alt_ms > 0:
                speedup = trino_ms / alt_ms
                speedups.append(speedup)
                speedup_str = f"{speedup:.2f}x"
            else:
                speedup_str = "-"

            print(
                f"| {qid:<8} | {alt_str:>15} | {trino_str:>12} | {speedup_str:>8} |"
            )
        else:
            alt_str = f"{alt_ms:.1f}" if alt_ms else "-"
            status = "ok" if alt_ms else "fail"
            print(f"| {qid:<8} | {alt_str:>12} | {status:<8} |")

    # Summary.
    print()
    if speedups:
        print(f"**Geometric mean speedup**: {geometric_mean(speedups):.2f}x")
        print(f"**Median speedup**: {median(speedups):.2f}x")
    print(f"**Queries tested**: {len(all_ids)}")
    print(
        f"**Queries passed (trino-alt)**: {len(alt_medians)}/{len(all_ids)}"
    )


if __name__ == "__main__":
    main()
