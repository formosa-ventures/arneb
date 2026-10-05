# TPC-H Comparison Report

**Engines:** trino, datafusion, arneb
**Scale factor:** sf1
**Scenario:** sf1
**Generated:** 2026-07-26T12:05:57.914663957+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 323.0 | 530.9 | 180.0 | 1.64x | 0.56x | 0.34x |
| q02 | ok | 253.5 | 664.8 | 86.9 | 2.62x | 0.34x | 0.13x |
| q03 | ok | 245.6 | 651.9 | 141.8 | 2.65x | 0.58x | 0.22x |
| q04 | ok | 198.8 | 535.0 | 94.5 | 2.69x | 0.48x | 0.18x |
| q05 | ok | 498.5 | 791.9 | 156.8 | 1.59x | 0.31x | 0.20x |
| q06 | ok | 97.4 | 278.4 | 116.3 | 2.86x | 1.19x | 0.42x |
| q07 | ok | 333.4 | 672.7 | 209.2 | 2.02x | 0.63x | 0.31x |
| q08 | ok | 589.8 | 917.5 | 209.1 | 1.56x | 0.35x | 0.23x |
| q09 | ok | 717.1 | 843.6 | 244.5 | 1.18x | 0.34x | 0.29x |
| q10 | ok | 264.0 | 631.8 | 179.5 | 2.39x | 0.68x | 0.28x |
| q11 | ok | 151.2 | 292.9 | 76.3 | 1.94x | 0.50x | 0.26x |
| q12 | ok | 206.9 | 358.6 | 142.3 | 1.73x | 0.69x | 0.40x |
| q13 | ok | 344.1 | 508.4 | 157.4 | 1.48x | 0.46x | 0.31x |
| q14 | ok | 174.3 | 364.8 | 109.3 | 2.09x | 0.63x | 0.30x |
| q15 | ok | 243.3 | 484.9 | 170.6 | 1.99x | 0.70x | 0.35x |
| q16 | ok | 129.3 | 333.6 | 39.8 | 2.58x | 0.31x | 0.12x |
| q17 | ok | 433.2 | 509.6 | 224.1 | 1.18x | 0.52x | 0.44x |
| q18 | ok | 595.4 | 621.3 | 278.1 | 1.04x | 0.47x | 0.45x |
| q19 | ok | 259.0 | 442.9 | 138.1 | 1.71x | 0.53x | 0.31x |
| q20 | ok | 324.0 | 492.8 | 138.1 | 1.52x | 0.43x | 0.28x |
| q21 | ok | 745.7 | 1026.3 | 308.4 | 1.38x | 0.41x | 0.30x |
| q22 | ok | 88.8 | 274.7 | 45.4 | 3.09x | 0.51x | 0.17x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 278.5 |
| trino | 22 | 0 | 0 | 519.3 |
| datafusion | 22 | 0 | 0 | 139.6 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 1.86x |
| arneb → datafusion | 0.50x |
| trino → datafusion | 0.27x |

## Floating-point boundary notes

These queries agree across engines but sit on a rounding boundary in the last retained digit, so their strict hashes differ while the coarser comparison matches. Summation order differs whenever two engines partition an aggregate differently; this is that, not a correctness difference: q09

## Correctness divergences

Queries below produced different result sets across engines, at a difference larger than floating-point rounding accounts for.

### q02

| Engine | Hash (first 8) | Rows |
|---|---|---:|
| arneb | e3b0c442 | 0 |
| trino | 9b3e44ae | 100 |
| datafusion | 9b3e44ae | 100 |

