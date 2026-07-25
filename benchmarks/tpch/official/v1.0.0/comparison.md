# TPC-H Comparison Report

**Engines:** datafusion, trino, arneb
**Generated:** 2026-07-25T20:07:41.829800224+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 301.7 | 404.6 | 165.6 | 1.34x | 0.55x | 0.41x |
| q02 | ok | 262.0 | 385.6 | 100.6 | 1.47x | 0.38x | 0.26x |
| q03 | ok | 227.1 | 468.9 | 139.0 | 2.07x | 0.61x | 0.30x |
| q04 | ok | 198.1 | 358.4 | 94.6 | 1.81x | 0.48x | 0.26x |
| q05 | ok | 451.4 | 612.4 | 161.1 | 1.36x | 0.36x | 0.26x |
| q06 | ok | 113.0 | 271.2 | 87.0 | 2.40x | 0.77x | 0.32x |
| q07 | ok | 314.8 | 540.0 | 213.8 | 1.72x | 0.68x | 0.40x |
| q08 | ok | 526.4 | 734.5 | 210.1 | 1.40x | 0.40x | 0.29x |
| q09 | ok | 670.6 | 717.2 | 251.0 | 1.07x | 0.37x | 0.35x |
| q10 | ok | 262.9 | 559.1 | 192.4 | 2.13x | 0.73x | 0.34x |
| q11 | ok | 156.7 | 273.8 | 76.9 | 1.75x | 0.49x | 0.28x |
| q12 | ok | 202.8 | 315.0 | 146.3 | 1.55x | 0.72x | 0.46x |
| q13 | ok | 389.8 | 457.2 | 160.4 | 1.17x | 0.41x | 0.35x |
| q14 | ok | 160.8 | 345.4 | 107.3 | 2.15x | 0.67x | 0.31x |
| q15 | ok | 281.2 | 422.0 | 165.0 | 1.50x | 0.59x | 0.39x |
| q16 | ok | 135.4 | 295.1 | 41.2 | 2.18x | 0.30x | 0.14x |
| q17 | ok | 402.6 | 514.1 | 251.3 | 1.28x | 0.62x | 0.49x |
| q18 | ok | 562.6 | 611.4 | 270.1 | 1.09x | 0.48x | 0.44x |
| q19 | ok | 247.8 | 427.9 | 136.5 | 1.73x | 0.55x | 0.32x |
| q20 | ok | 317.4 | 398.8 | 141.0 | 1.26x | 0.44x | 0.35x |
| q21 | ok | 778.4 | 980.6 | 303.9 | 1.26x | 0.39x | 0.31x |
| q22 | ok | 102.4 | 266.4 | 46.2 | 2.60x | 0.45x | 0.17x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 277.6 |
| trino | 22 | 0 | 0 | 442.8 |
| datafusion | 22 | 0 | 0 | 139.9 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 1.59x |
| arneb → datafusion | 0.50x |
| trino → datafusion | 0.32x |

## Floating-point boundary notes

These queries agree across engines but sit on a rounding boundary in the last retained digit, so their strict hashes differ while the coarser comparison matches. Summation order differs whenever two engines partition an aggregate differently; this is that, not a correctness difference: q09

