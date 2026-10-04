# TPC-H Comparison Report

**Engines:** arneb, datafusion, trino
**Generated:** 2026-07-25T21:15:36.027614857+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 1679.0 | 2119.5 | 1196.1 | 1.26x | 0.71x | 0.56x |
| q02 | ok | 7974.5 | 1156.7 | 389.7 | 0.15x | 0.05x | 0.34x |
| q03 | ok | 2375.9 | 2587.0 | 900.3 | 1.09x | 0.38x | 0.35x |
| q04 | ok | 2141.6 | 1950.0 | 565.4 | 0.91x | 0.26x | 0.29x |
| q05 | ok | 4611.7 | 2546.0 | 1248.7 | 0.55x | 0.27x | 0.49x |
| q06 | ok | 622.9 | 969.9 | 614.4 | 1.56x | 0.99x | 0.63x |
| q07 | ok | 2500.9 | 2395.5 | 1666.8 | 0.96x | 0.67x | 0.70x |
| q08 | ok | 4967.8 | 3003.0 | 1289.9 | 0.60x | 0.26x | 0.43x |
| q09 | ok | 6784.1 | 3820.6 | 1685.1 | 0.56x | 0.25x | 0.44x |
| q10 | ok | 2184.2 | 2954.2 | 1121.1 | 1.35x | 0.51x | 0.38x |
| q11 | ok | 1247.5 | 542.8 | 289.7 | 0.44x | 0.23x | 0.53x |
| q12 | ok | 1565.5 | 1183.5 | 976.3 | 0.76x | 0.62x | 0.82x |
| q13 | ok | 3506.2 | 2986.6 | 784.3 | 0.85x | 0.22x | 0.26x |
| q14 | ok | 1155.1 | 1692.7 | 759.0 | 1.47x | 0.66x | 0.45x |
| q15 | ok | 1552.1 | 1862.9 | 1060.9 | 1.20x | 0.68x | 0.57x |
| q16 | ok | 1046.8 | 899.1 | 150.8 | 0.86x | 0.14x | 0.17x |
| q17 | ok | 5338.5 | 3596.6 | 1973.5 | 0.67x | 0.37x | 0.55x |
| q18 | ok | 7117.1 | 3577.6 | 2457.9 | 0.50x | 0.35x | 0.69x |
| q19 | ok | 1667.0 | 1900.2 | 977.0 | 1.14x | 0.59x | 0.51x |
| q20 | ok | 3292.6 | 1786.1 | 864.2 | 0.54x | 0.26x | 0.48x |
| q21 | ok | 10498.2 | 4854.3 | 2407.0 | 0.46x | 0.23x | 0.50x |
| q22 | ok | 1000.3 | 886.9 | 310.0 | 0.89x | 0.31x | 0.35x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 2566.6 |
| trino | 22 | 0 | 0 | 1954.8 |
| datafusion | 22 | 0 | 0 | 880.5 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 0.76x |
| arneb → datafusion | 0.34x |
| trino → datafusion | 0.45x |

## Floating-point boundary notes

These queries agree across engines but sit on a rounding boundary in the last retained digit, so their strict hashes differ while the coarser comparison matches. Summation order differs whenever two engines partition an aggregate differently; this is that, not a correctness difference: q01

