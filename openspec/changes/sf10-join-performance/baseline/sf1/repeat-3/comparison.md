# TPC-H Comparison Report

**Engines:** arneb, datafusion, trino
**Scale factor:** sf1
**Scenario:** sf1
**Generated:** 2026-07-26T12:14:56.855101841+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

`A→B` columns are A's speedup over B: above 1.00x means A is that many times faster.

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 359.4 | 553.7 | 169.6 | 1.54x | 0.47x | 0.31x |
| q02 | ok | 150.3 | 661.6 | 89.1 | 4.40x | 0.59x | 0.13x |
| q03 | ok | 249.2 | 645.4 | 143.2 | 2.59x | 0.57x | 0.22x |
| q04 | ok | 199.4 | 530.6 | 94.5 | 2.66x | 0.47x | 0.18x |
| q05 | ok | 491.6 | 758.0 | 164.2 | 1.54x | 0.33x | 0.22x |
| q06 | ok | 94.6 | 278.1 | 94.6 | 2.94x | 1.00x | 0.34x |
| q07 | ok | 337.5 | 678.1 | 223.2 | 2.01x | 0.66x | 0.33x |
| q08 | ok | 610.9 | 855.3 | 206.7 | 1.40x | 0.34x | 0.24x |
| q09 | ok | 715.4 | 815.3 | 258.7 | 1.14x | 0.36x | 0.32x |
| q10 | ok | 262.8 | 632.4 | 182.1 | 2.41x | 0.69x | 0.29x |
| q11 | ok | 155.2 | 304.2 | 78.2 | 1.96x | 0.50x | 0.26x |
| q12 | ok | 212.0 | 397.6 | 147.3 | 1.88x | 0.69x | 0.37x |
| q13 | ok | 345.9 | 479.7 | 157.6 | 1.39x | 0.46x | 0.33x |
| q14 | ok | 164.0 | 363.8 | 108.5 | 2.22x | 0.66x | 0.30x |
| q15 | ok | 252.1 | 430.1 | 169.0 | 1.71x | 0.67x | 0.39x |
| q16 | ok | 129.7 | 326.4 | 43.1 | 2.52x | 0.33x | 0.13x |
| q17 | ok | 432.1 | 484.1 | 236.0 | 1.12x | 0.55x | 0.49x |
| q18 | ok | 603.2 | 589.9 | 286.8 | 0.98x | 0.48x | 0.49x |
| q19 | ok | 262.2 | 458.5 | 138.3 | 1.75x | 0.53x | 0.30x |
| q20 | ok | 293.4 | 459.3 | 140.5 | 1.57x | 0.48x | 0.31x |
| q21 | ok | 742.3 | 995.9 | 305.3 | 1.34x | 0.41x | 0.31x |
| q22 | ok | 86.6 | 275.4 | 46.5 | 3.18x | 0.54x | 0.17x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 22 | 0 | 0 | 272.5 |
| trino | 22 | 0 | 0 | 511.1 |
| datafusion | 22 | 0 | 0 | 140.8 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 1.88x |
| arneb → datafusion | 0.52x |
| trino → datafusion | 0.28x |

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

