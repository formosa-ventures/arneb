# TPC-H Comparison Report

**Engines:** arneb, trino, datafusion
**Generated:** 2026-07-25T21:29:24.423840056+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 308.8 | 558.1 | 210.8 | 1.81x | 0.68x | 0.38x |
| q02 | ok | 248.0 | 694.7 | 89.7 | 2.80x | 0.36x | 0.13x |
| q03 | ok | 285.0 | 691.4 | 139.0 | 2.43x | 0.49x | 0.20x |
| q04 | ok | 221.1 | 513.5 | 96.7 | 2.32x | 0.44x | 0.19x |
| q05 | ok | 520.9 | 770.9 | 163.7 | 1.48x | 0.31x | 0.21x |
| q06 | ok | 120.7 | 282.7 | 92.5 | 2.34x | 0.77x | 0.33x |
| q07 | ok | 318.2 | 681.9 | 230.3 | 2.14x | 0.72x | 0.34x |
| q08 | ok | 645.2 | 820.3 | 214.0 | 1.27x | 0.33x | 0.26x |
| q09 | ok | 775.5 | 792.0 | 254.3 | 1.02x | 0.33x | 0.32x |
| q10 | ok | 287.4 | 603.7 | 181.6 | 2.10x | 0.63x | 0.30x |
| q11 | ok | 151.5 | 301.2 | 77.0 | 1.99x | 0.51x | 0.26x |
| q12 | ok | 235.5 | 369.6 | 148.0 | 1.57x | 0.63x | 0.40x |
| q13 | ok | 387.9 | 496.5 | 158.5 | 1.28x | 0.41x | 0.32x |
| q14 | ok | 171.3 | 361.5 | 108.8 | 2.11x | 0.64x | 0.30x |
| q15 | ok | 273.4 | 434.9 | 170.2 | 1.59x | 0.62x | 0.39x |
| q16 | ok | 135.1 | 357.3 | 40.3 | 2.64x | 0.30x | 0.11x |
| q17 | ok | 427.9 | 519.9 | 238.8 | 1.21x | 0.56x | 0.46x |
| q18 | ok | 619.5 | 617.8 | 281.7 | 1.00x | 0.45x | 0.46x |
| q19 | ok | 277.0 | 468.9 | 136.6 | 1.69x | 0.49x | 0.29x |
| q20 | ok | 309.0 | 452.7 | 140.5 | 1.47x | 0.45x | 0.31x |
| q21 | ok | 758.1 | 998.6 | 302.5 | 1.32x | 0.40x | 0.30x |
| q22 | ok | 90.2 | 273.3 | 44.8 | 3.03x | 0.50x | 0.16x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 293.1 |
| trino | 22 | 0 | 0 | 515.3 |
| datafusion | 22 | 0 | 0 | 141.5 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 1.76x |
| arneb → datafusion | 0.48x |
| trino → datafusion | 0.27x |

## Floating-point boundary notes

These queries agree across engines but sit on a rounding boundary in the last retained digit, so their strict hashes differ while the coarser comparison matches. Summation order differs whenever two engines partition an aggregate differently; this is that, not a correctness difference: q09

