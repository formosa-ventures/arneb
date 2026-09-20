# TPC-H Comparison Report

**Engines:** trino, arneb, datafusion
**Scale factor:** sf10
**Scenario:** sf10
**Generated:** 2026-07-26T12:39:38.837298510+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 1783.2 | 2061.5 | 1218.7 | 1.16x | 0.68x | 0.59x |
| q02 | ok | 7919.7 | 1145.7 | 379.6 | 0.14x | 0.05x | 0.33x |
| q03 | ok | 2439.5 | 2575.6 | 844.5 | 1.06x | 0.35x | 0.33x |
| q04 | ok | 2077.9 | 1798.4 | 538.6 | 0.87x | 0.26x | 0.30x |
| q05 | ok | 4615.3 | 2503.2 | 1176.3 | 0.54x | 0.25x | 0.47x |
| q06 | ok | 652.4 | 968.7 | 629.6 | 1.48x | 0.97x | 0.65x |
| q07 | ok | 2508.1 | 2265.2 | 1589.1 | 0.90x | 0.63x | 0.70x |
| q08 | ok | 4866.0 | 2944.7 | 1232.2 | 0.61x | 0.25x | 0.42x |
| q09 | ok | 6765.7 | 3786.6 | 1657.0 | 0.56x | 0.24x | 0.44x |
| q10 | ok | 2103.3 | 2785.2 | 1069.7 | 1.32x | 0.51x | 0.38x |
| q11 | ok | 1214.9 | 527.7 | 284.3 | 0.43x | 0.23x | 0.54x |
| q12 | ok | 1581.4 | 1103.1 | 948.7 | 0.70x | 0.60x | 0.86x |
| q13 | ok | 3603.7 | 2685.2 | 656.2 | 0.75x | 0.18x | 0.24x |
| q14 | ok | 1209.5 | 1681.8 | 783.3 | 1.39x | 0.65x | 0.47x |
| q15 | ok | 1670.8 | 1835.5 | 1069.6 | 1.10x | 0.64x | 0.58x |
| q16 | ok | 990.2 | 885.0 | 147.6 | 0.89x | 0.15x | 0.17x |
| q17 | ok | 5451.3 | 3680.8 | 1926.4 | 0.68x | 0.35x | 0.52x |
| q18 | ok | 7571.9 | 3700.8 | 2422.7 | 0.49x | 0.32x | 0.65x |
| q19 | ok | 1759.4 | 1918.3 | 991.8 | 1.09x | 0.56x | 0.52x |
| q20 | ok | 3605.4 | 1831.3 | 858.7 | 0.51x | 0.24x | 0.47x |
| q21 | ok | 10501.4 | 4859.9 | 2356.7 | 0.46x | 0.22x | 0.48x |
| q22 | ok | 1005.3 | 837.2 | 292.3 | 0.83x | 0.29x | 0.35x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 2607.4 |
| trino | 22 | 0 | 0 | 1911.6 |
| datafusion | 22 | 0 | 0 | 855.6 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 0.73x |
| arneb → datafusion | 0.33x |
| trino → datafusion | 0.45x |

## Floating-point boundary notes

These queries agree across engines but sit on a rounding boundary in the last retained digit, so their strict hashes differ while the coarser comparison matches. Summation order differs whenever two engines partition an aggregate differently; this is that, not a correctness difference: q01

