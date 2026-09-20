# TPC-H Comparison Report

**Engines:** arneb, datafusion, trino
**Scale factor:** sf10
**Scenario:** sf10
**Generated:** 2026-07-26T11:49:51.932011702+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 1709.1 | 2281.7 | 1229.7 | 1.34x | 0.72x | 0.54x |
| q02 | ok | 8175.8 | 1193.8 | 387.6 | 0.15x | 0.05x | 0.32x |
| q03 | ok | 2280.5 | 2726.2 | 877.4 | 1.20x | 0.38x | 0.32x |
| q04 | ok | 2111.9 | 2075.6 | 555.7 | 0.98x | 0.26x | 0.27x |
| q05 | ok | 4444.9 | 2617.5 | 1196.3 | 0.59x | 0.27x | 0.46x |
| q06 | ok | 632.6 | 961.0 | 643.7 | 1.52x | 1.02x | 0.67x |
| q07 | ok | 2516.1 | 2401.7 | 1631.9 | 0.95x | 0.65x | 0.68x |
| q08 | ok | 4740.7 | 2888.7 | 1276.9 | 0.61x | 0.27x | 0.44x |
| q09 | ok | 6697.2 | 4095.0 | 1687.6 | 0.61x | 0.25x | 0.41x |
| q10 | ok | 2115.2 | 2907.2 | 1082.0 | 1.37x | 0.51x | 0.37x |
| q11 | ok | 1286.7 | 531.3 | 283.3 | 0.41x | 0.22x | 0.53x |
| q12 | ok | 1508.8 | 1143.8 | 973.5 | 0.76x | 0.65x | 0.85x |
| q13 | ok | 3706.9 | 2950.2 | 640.1 | 0.80x | 0.17x | 0.22x |
| q14 | ok | 1142.1 | 1684.4 | 821.2 | 1.47x | 0.72x | 0.49x |
| q15 | ok | 1546.4 | 1952.2 | 1131.0 | 1.26x | 0.73x | 0.58x |
| q16 | ok | 1009.9 | 885.7 | 148.6 | 0.88x | 0.15x | 0.17x |
| q17 | ok | 5183.8 | 4041.8 | 1975.5 | 0.78x | 0.38x | 0.49x |
| q18 | ok | 7114.9 | 3736.9 | 2600.2 | 0.53x | 0.37x | 0.70x |
| q19 | ok | 1645.7 | 1986.8 | 1030.8 | 1.21x | 0.63x | 0.52x |
| q20 | ok/failed/ok | 3339.0 | - | 921.8 | - | 0.28x | - |
| q21 | ok | 10354.4 | 5088.1 | 2426.3 | 0.49x | 0.23x | 0.48x |
| q22 | ok | 995.0 | 846.7 | 287.7 | 0.85x | 0.29x | 0.34x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 2547.1 |
| trino | 21 | 1 | 0 | 2002.9 |
| datafusion | 22 | 0 | 0 | 877.6 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 0.80x |
| arneb → datafusion | 0.34x |
| trino → datafusion | 0.44x |

## Floating-point boundary notes

These queries agree across engines but sit on a rounding boundary in the last retained digit, so their strict hashes differ while the coarser comparison matches. Summation order differs whenever two engines partition an aggregate differently; this is that, not a correctness difference: q01

