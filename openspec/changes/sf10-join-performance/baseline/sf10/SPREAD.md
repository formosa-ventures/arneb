# SF10 run-to-run spread

Three executions of the `sf10` scenario on one host with identical engine code
(`crates/` and `Cargo.*` show no diff against `c4efe5b`):

| Run | Date | Seed |
|---|---|---|
| `repeat-1/` | 2026-07-26 | fresh |
| `repeat-2/` | 2026-07-26 | fresh |
| `comparison.md` (the withheld run, `b288bd6`) | 2026-07-25 | fresh |

**Three points, and two of them share a session.** This is a spread, not a
distribution — enough to bound the noise, not enough to characterise its shape.
Task 7.3 asked for at least two repeats and said to say so if only two were
affordable; the withheld run makes a third, from a different day.

## Suite aggregate

Measured over the **21 queries every run compared**. q20 is excluded because
Trino failed it in `repeat-1` — including a query one run could not measure would
compare different suites.

| Run | `arneb→trino` |
|---|---:|
| repeat-1 | 0.798x |
| repeat-2 | 0.745x |
| withheld | 0.775x |

**Range 0.745–0.798x, mean 0.773x, spread 6.8%.** The two same-day runs alone
span 6.9% — so this is not a cross-session artifact; SF10 is simply noisier than
SF1, which spans 2.7%.

## Most of the noise is Trino's, not Arneb's

| Engine | Geomean p50 range | Spread |
|---|---|---:|
| arneb | 2547.1 – 2607.4 ms | **2.3%** |
| trino | 1911.6 – 2002.9 ms | **4.7%** |
| datafusion | 855.6 – 880.5 ms | 2.9% |

Arneb is the *steadiest* engine of the three at SF10, at 2.3% — comparable to its
own 2.9% at SF1. The pairwise ratio is noisy because the denominator is: Trino
varies twice as much, and one run of three lost a query outright.

## The threshold

**A change that moves the SF10 suite aggregate by less than 7% is not a result.**

For scale: closing the gap from 0.773x to parity is a **+29%** move, comfortably
outside this band. The join work can be evaluated against this scenario. What
cannot be evaluated against it is a single-digit-percent improvement.

## Trino fails q20 at SF10 roughly one run in three

`repeat-1` lost q20 to `Unexpected response from .../dynamicfilters`. No container
was OOM-killed (`oom=false` on all three Trino containers; exit 143 is the
harness's own SIGTERM). One failure in three runs is not a rate worth quoting,
but it is worth knowing before a run is discarded as anomalous: the harness
records it as a failure, and the comparison silently drops the query from the
pairwise columns.

Per the release checklist, a run with a failure is not publishable as an official
result. Both of today's runs were floor-confirmation runs, not publication
candidates.

## Consequences for the attribution

`../attribution.md` lists 15 queries below 1.00x. Against a 6.8% suite band and
per-query spreads that reach 15%:

- **q07 (0.95x) and q04 (0.98x) are inside the noise.** They are not regressions.
- **q22 (0.85x) and q16 (0.88x) are within roughly two noise-widths** and rest on
  one measurement each. Treat them as weak.
- The deep losses — q02, q11, q21, q18, q05, q08, q09, all at 0.61x or worse —
  are far outside any spread measured here. Those are real.

The noisiest queries across these three runs are q17 (15.5%), q01 (14.4%),
q15 (13.5%), q03 (12.5%) — note that three of those four are queries Arneb
*wins*, so the win margins are softer than a single run suggests too.
