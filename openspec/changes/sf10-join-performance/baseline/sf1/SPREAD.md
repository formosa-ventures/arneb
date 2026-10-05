# SF1 run-to-run spread

Three executions of the `sf1` scenario on one unchanged host, against one
unchanged dataset, on 2026-07-26. `SKIP_SEED=1` throughout: re-seeding between
repeats would vary the data as well as the run, and the question is how far a
repeat moves when nothing else does. Engine code was identical to the published
run's tree (`crates/` and `Cargo.*` show no diff against `c4efe5b`).

All three repeats compared all 22 queries — no engine failed one.

## Within-session spread

| Measurement | Range | Mean | Spread |
|---|---|---:|---:|
| arneb geomean p50 | 272.5 – 280.5 ms | 277.2 ms | **2.9%** |
| trino geomean p50 | 511.1 – 519.3 ms | 514.4 ms | 1.6% |
| datafusion geomean p50 | 139.6 – 140.8 ms | 140.4 ms | 0.9% |
| `arneb→trino` pairwise | 1.830 – 1.880x | 1.857x | **2.7%** |

Per repeat: 1.86x, 1.83x, 1.88x.

## The threshold

**A change that moves the SF1 suite aggregate by less than 3% is not a result.**
It is inside what this scenario returns when nothing has changed at all.

## Between sessions the spread is larger, and this matters more

The published SF1 run — same host, same engine code, 2026-07-25 — reports
`arneb→trino` **1.76x** with an arneb geomean of 293.1 ms. Today's three repeats
sit at 1.83–1.88x and 272.5–280.5 ms.

That gap is about **5.7%**, twice the within-session spread, and the published run
sits **outside** the band today's repeats occupy. Nothing in the engine changed
between them. The difference is the session: a different day, a fresh seed rather
than a resident one, and whatever else the machine was doing.

So the 3% threshold above governs comparisons **within one sitting**. Comparing a
number measured today against one measured last week needs a wider allowance than
this figure — closer to 6% on the evidence available, from a single pair of
sessions. Treating the within-session band as though it covered cross-session
comparison would let a 5% "improvement" be reported that is only a different
afternoon.

## q02 is not a usable signal at all

| Query | Repeat 1 | Repeat 2 | Repeat 3 | Spread |
|---|---:|---:|---:|---:|
| q02 | 2.62 – 4.40x across the three repeats | | | **52.8%** |

q02's `arneb→trino` ratio ranges from 2.62x to 4.40x across three repeats of an
unchanged system — and the published run recorded 2.80x. No other query comes
close to this.

This bears directly on the SF10 attribution, where **q02 is the worst regression
at 0.15x** and is the headline example of the multi-table-join cluster. A query
whose ratio varies by half its own value between identical repeats cannot carry
that weight on a single measurement. The 0.15x figure needs its own repeats
before any claim rests on its magnitude — the *sign* of the regression is not in
doubt, but its size is not established.

See `../attribution.md`, which cites this file for that caveat.
