# TPC-H Comparison Report

**Engines:** trino, arneb, datafusion
**Scale factor:** sf1
**Scenario:** sf1
**Generated:** 2026-07-26T12:10:27.392573541+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 304.5 | 589.9 | 175.9 | 1.94x | 0.58x | 0.30x |
| q02 | ok | 236.4 | 732.1 | 96.6 | 3.10x | 0.41x | 0.13x |
| q03 | ok | 250.2 | 613.4 | 145.4 | 2.45x | 0.58x | 0.24x |
| q04 | ok | 196.0 | 520.5 | 94.7 | 2.65x | 0.48x | 0.18x |
| q05 | ok | 487.0 | 756.4 | 161.5 | 1.55x | 0.33x | 0.21x |
| q06 | ok | 116.7 | 278.8 | 94.8 | 2.39x | 0.81x | 0.34x |
| q07 | ok | 318.5 | 683.6 | 212.3 | 2.15x | 0.67x | 0.31x |
| q08 | ok | 603.9 | 878.2 | 208.8 | 1.45x | 0.35x | 0.24x |
| q09 | ok | 721.0 | 852.4 | 249.1 | 1.18x | 0.35x | 0.29x |
| q10 | ok | 263.3 | 646.8 | 180.8 | 2.46x | 0.69x | 0.28x |
| q11 | ok | 158.3 | 289.5 | 75.6 | 1.83x | 0.48x | 0.26x |
| q12 | ok | 212.7 | 358.5 | 147.5 | 1.69x | 0.69x | 0.41x |
| q13 | ok | 344.6 | 502.9 | 160.9 | 1.46x | 0.47x | 0.32x |
| q14 | ok | 165.1 | 359.8 | 111.3 | 2.18x | 0.67x | 0.31x |
| q15 | ok | 268.6 | 426.1 | 171.6 | 1.59x | 0.64x | 0.40x |
| q16 | ok | 127.1 | 324.4 | 42.7 | 2.55x | 0.34x | 0.13x |
| q17 | ok | 439.8 | 494.1 | 231.9 | 1.12x | 0.53x | 0.47x |
| q18 | ok | 585.4 | 599.5 | 278.1 | 1.02x | 0.48x | 0.46x |
| q19 | ok | 258.3 | 448.1 | 139.8 | 1.73x | 0.54x | 0.31x |
| q20 | ok | 330.3 | 458.0 | 138.8 | 1.39x | 0.42x | 0.30x |
| q21 | ok | 751.3 | 957.1 | 312.2 | 1.27x | 0.42x | 0.33x |
| q22 | ok | 90.6 | 286.3 | 45.8 | 3.16x | 0.51x | 0.16x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 280.5 |
| trino | 22 | 0 | 0 | 512.8 |
| datafusion | 22 | 0 | 0 | 140.8 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 1.83x |
| arneb → datafusion | 0.50x |
| trino → datafusion | 0.27x |

## Floating-point boundary notes

These queries agree across engines but sit on a rounding boundary in the last retained digit, so their strict hashes differ while the coarser comparison matches. Summation order differs whenever two engines partition an aggregate differently; this is that, not a correctness difference: q09

