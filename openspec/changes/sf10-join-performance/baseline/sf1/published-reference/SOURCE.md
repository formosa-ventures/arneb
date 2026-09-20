# Source of these documents

Copied verbatim from `benchmarks/tpch/official/v1.0.0/sf1/` at commit `c6c2dea`.

- `comparison.md` — the published SF1 comparison report, unedited.
- `provenance.json` — the published SF1 provenance record.

## What is deliberately not here

The per-engine result documents (`arneb.json`, `trino.json`, `datafusion.json`,
~1,500 lines each) are **not copied**. They are already tracked at
`benchmarks/tpch/official/v1.0.0/sf1/` and duplicating them into a change
directory that will be archived is cost with no reader.

Recover them with:

```bash
git show c6c2dea:benchmarks/tpch/official/v1.0.0/sf1/arneb.json
```

## Why this baseline exists

SF1 is the contrast the SF10 attribution rests on. Arneb leads Trino by **1.76x**
geomean here and trails by **0.76x** at SF10; the reversal, not either figure
alone, is what the join work has to explain. Nothing about this directory implies
SF1 needs work.
