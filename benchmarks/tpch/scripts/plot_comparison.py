#!/usr/bin/env python3
"""Render a run_memory_bench CSV as an Arneb-vs-Trino comparison chart (SVG).

Two panels over the 22 queries: latency (seconds) and simultaneous cluster
peak memory (GB), one bar per engine, with the Trino / Arneb speedup above
each latency pair and Arneb / Trino above each memory pair. Standard library only, so it runs on the bench host.

Usage:
    python3 plot_comparison.py <memory_total_*.csv> -o chart.svg \
        [--mode cold|warm] [--title "..."] [--note "..."]

The CSV needs `query`, `engine`, `latency_ms` and `sim_peak_kib` (falls back
to `peak_kib`), plus `mode` when it holds both protocols. Memory is shown as
KiB / 1e6, the same "GB" `dual_axis_table.py` prints.
"""
import argparse
import csv
import math
import sys
from collections import defaultdict
from xml.sax.saxutils import escape

ARNEB = "#e8590c"
TRINO = "#8a94a6"
INK = "#1f2933"
MUTED = "#616e7c"
GRID = "#e4e7eb"

WIDTH = 1000
LEFT = 64
RIGHT = 24
PANEL_H = 220
PANEL_GAP = 64


def load(path, mode):
    rows = defaultdict(dict)
    with open(path) as f:
        for r in csv.DictReader(f):
            if mode and r.get("mode") and r["mode"] != mode:
                continue
            if r.get("status", "OK") != "OK":
                continue
            mem = r.get("sim_peak_kib") or r.get("peak_kib")
            rows[r["query"]][r["engine"]] = (
                float(r["latency_ms"]) / 1000.0,
                float(mem) / 1e6,
            )
    queries = sorted(q for q in rows if {"arneb", "trino"} <= rows[q].keys())
    if not queries:
        sys.exit(f"{path}: no query has both an arneb and a trino row")
    return queries, rows


def nice_max(v):
    """Round an axis maximum up to a value whose quarters are round."""
    exp = 10 ** math.floor(math.log10(v))
    for m in (1, 1.2, 1.6, 2, 2.4, 3, 4, 5, 6, 8, 10):
        if v <= m * exp:
            return m * exp
    return 10 * exp


def fmt(v):
    return f"{v:g}"


def panel(out, top, queries, values, title, unit, labels=None):
    """One grouped-bar panel; values[q] = (arneb, trino)."""
    plot_w = WIDTH - LEFT - RIGHT
    ymax = nice_max(max(max(v) for v in values.values()))
    out.append(
        f'<text x="{LEFT}" y="{top - 14}" class="h">{escape(title)}'
        f'<tspan class="m"> {escape(unit)}, lower is better</tspan></text>'
    )
    for i in range(5):
        tick = ymax * i / 4
        y = top + PANEL_H - PANEL_H * i / 4
        out.append(
            f'<line x1="{LEFT}" x2="{WIDTH - RIGHT}" y1="{y:.1f}" y2="{y:.1f}" '
            f'stroke="{GRID}"/>'
        )
        out.append(
            f'<text x="{LEFT - 8}" y="{y + 4:.1f}" class="t" text-anchor="end">'
            f"{fmt(tick)}</text>"
        )
    slot = plot_w / len(queries)
    bar = slot * 0.34
    for i, q in enumerate(queries):
        x0 = LEFT + i * slot + (slot - 2 * bar) / 2
        for j, (v, color) in enumerate(zip(values[q], (ARNEB, TRINO))):
            h = PANEL_H * v / ymax
            out.append(
                f'<rect x="{x0 + j * bar:.1f}" y="{top + PANEL_H - h:.1f}" '
                f'width="{bar - 1:.1f}" height="{h:.1f}" rx="1.5" fill="{color}">'
                f"<title>{q} {('Arneb', 'Trino')[j]}: {v:.2f} {unit}</title></rect>"
            )
        cx = LEFT + i * slot + slot / 2
        out.append(
            f'<text x="{cx:.1f}" y="{top + PANEL_H + 16}" class="t" '
            f'text-anchor="middle">{q}</text>'
        )
        if labels:
            ty = top + PANEL_H - PANEL_H * max(values[q]) / ymax - 5
            out.append(
                f'<text x="{cx:.1f}" y="{ty:.1f}" class="r" text-anchor="middle">'
                f"{labels[q]}</text>"
            )


def render(queries, rows, title, note):
    lat = {q: (rows[q]["arneb"][0], rows[q]["trino"][0]) for q in queries}
    mem = {q: (rows[q]["arneb"][1], rows[q]["trino"][1]) for q in queries}
    speed = {q: t / a for q, (a, t) in lat.items()}
    geo = math.exp(sum(math.log(s) for s in speed.values()) / len(speed))
    mem_ratio = sum(a / t for a, t in mem.values()) / len(mem)
    faster = sum(1 for s in speed.values() if s > 1)

    top1 = 118
    top2 = top1 + PANEL_H + PANEL_GAP
    height = top2 + PANEL_H + 60 + (18 if note else 0)
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" '
        f'width="{WIDTH}" height="{height}" role="img" aria-label="{escape(title)}">',
        "<style>"
        f"text{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;fill:{INK}}}"
        ".title{font-size:20px;font-weight:600}"
        f".sub{{font-size:13px;fill:{MUTED}}}"
        ".h{font-size:14px;font-weight:600}"
        f".m{{font-weight:400;fill:{MUTED}}}"
        f".t{{font-size:11px;fill:{MUTED}}}"
        f".r{{font-size:10px;fill:{ARNEB};font-weight:600}}"
        ".k{font-size:12px}"
        "</style>",
        f'<rect width="{WIDTH}" height="{height}" rx="10" fill="#ffffff"/>',
        f'<text x="{LEFT}" y="38" class="title">{escape(title)}</text>',
        f'<text x="{LEFT}" y="62" class="sub">Arneb is {geo:.1f}× faster (geomean) '
        f"and uses {mem_ratio:.2f}× Trino's peak memory (mean); faster on "
        f"{faster} / {len(queries)} queries.</text>",
    ]
    lx = WIDTH - RIGHT - 170
    for k, (name, color) in enumerate((("Arneb", ARNEB), ("Trino", TRINO))):
        x = lx + k * 86
        out.append(f'<rect x="{x}" y="27" width="12" height="12" rx="2" fill="{color}"/>')
        out.append(f'<text x="{x + 18}" y="38" class="k">{name}</text>')
    panel(out, top1, queries, lat, "Latency", "seconds",
          labels={q: f"{s:.1f}×" for q, s in speed.items()})
    panel(out, top2, queries, mem, "Peak memory", "GB, whole cluster",
          labels={q: f"{a / t:.2f}×" for q, (a, t) in mem.items()})
    if note:
        out.append(
            f'<text x="{LEFT}" y="{height - 18}" class="t">{escape(note)}</text>'
        )
    out.append("</svg>")
    return "\n".join(out) + "\n", geo, mem_ratio, faster


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("csv")
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("--mode", default=None, help="cold or warm (default: all rows)")
    ap.add_argument("--title", default="Arneb vs Trino — TPC-H")
    ap.add_argument("--note", default="")
    args = ap.parse_args()
    queries, rows = load(args.csv, args.mode)
    svg, geo, mem_ratio, faster = render(queries, rows, args.title, args.note)
    with open(args.output, "w") as f:
        f.write(svg)
    print(
        f"{args.output}: {len(queries)} queries, {geo:.2f}x faster (geomean), "
        f"{mem_ratio:.3f}x memory (mean), faster on {faster}"
    )


if __name__ == "__main__":
    main()
