## Why

Arneb's positioning is "a Trino alternative built in Rust," but today there is no end-to-end experience that lets a user prove that claim on their own laptop. The existing `benchmarks/tpch` harness runs Arneb and Trino, dumps JSON, and produces a terse Markdown table — there is no DataFusion baseline (the most direct Arrow-native peer), no correctness check, no tutorial walking a newcomer from `git clone` to a chart, and no real per-query statistics beyond the median. The result is that when a prospective user asks "how does Arneb actually compare?", the answer is a paragraph of caveats instead of a reproducible run with numbers.

This change turns the harness into a complete, reproducible TPC-H SF1 comparison experience across **Arneb, Trino, and Apache DataFusion** — the three reference points that matter for our positioning — with a guided tutorial and real per-query statistics (p50/p95/p99, stddev, geomean) plus correctness verification.

## What Changes

- Add a third engine adapter: **DataFusion** (in-process, Arrow-native, reading the same Parquet from the local filesystem or via `object_store` against MinIO so it sees identical bytes to Arneb and Trino).
- Promote the existing single-engine runner into a **multi-engine runner** that can execute the full TPC-H suite against any subset of `{arneb, trino, datafusion}` in one invocation, with a shared run plan (same queries, same warmup, same timing methodology).
- Replace the median-only result struct with **real per-query statistics**: min, p50, p95, p99, stddev, plus rows-returned and a query hash for correctness comparison.
- Add a **correctness check** step: each engine's first non-warmup result set is hashed (sorted-rows hash) and the runner flags any divergence between engines on the same query.
- Add a **three-way comparison report** generator (Markdown + JSON) showing per-query latencies side-by-side with relative speedups vs. each baseline and a geomean across the suite. Drop the Python `report.py` in favor of a Rust subcommand so the whole harness builds with `cargo`.
- Add a **tutorial document** at `benchmarks/tpch/TUTORIAL.md` that walks a new user end-to-end: prerequisites → start stack → seed data → run all three engines → read the report → interpret divergences. Linked from the project root README so it is discoverable.
- Fill the **6 missing TPC-H queries** (q15, q17, q18, q20, q21, q22) so the harness covers the full 22-query suite, or document the specific reason any query is excluded for one engine (e.g., unsupported SQL feature on Arneb today).
- Refresh `scripts/run_benchmark.sh` to drive the new multi-engine runner and to start a DataFusion-CLI-equivalent in-process step (no separate service needed).

Non-goals (explicitly out of scope, to keep this change shippable):
- Larger scale factors beyond SF1 (the existing `TPCH_SF` env var still works for power users; defaults stay SF1).
- TPC-DS or ClickBench.
- A web dashboard or charts UI — reports stay textual (Markdown tables); visualization is a follow-up.
- Resource accounting (memory, CPU) — the harness measures wall-clock latency and result correctness only.

## Capabilities

### New Capabilities

- `tpch-benchmark-engines`: Defines the engine-adapter contract (connect, execute query, return rows + timing + error) and provides adapters for Arneb (pgwire), Trino (REST), and DataFusion (in-process via the `datafusion` crate).
- `tpch-benchmark-runner`: Orchestrates multi-engine runs — query loading, warmup/measurement loop, real per-query statistics (min/p50/p95/p99/stddev), correctness hashing across engines, JSON result persistence.
- `tpch-benchmark-reporting`: Generates the three-way comparison report (Markdown + JSON) from one or more result files, with per-query side-by-side timings, relative speedups, geomean, and a correctness-divergence section.
- `tpch-benchmark-tutorial`: The end-to-end user-facing tutorial (`benchmarks/tpch/TUTORIAL.md`) that walks from prerequisites to interpreted report, plus the linkage from the root README.

### Modified Capabilities

_None._ This change is additive — it introduces the comparison harness as new capabilities. Existing specs (`pg-server`, `file-connector`, etc.) are unchanged because the harness consumes Arneb through its public pgwire interface like any external client.

## Impact

- **New crate dependency** on `datafusion` (44.x — compatible with the workspace's Arrow 58 pin) inside `benchmarks/tpch/Cargo.toml` only — does not enter the main workspace's runtime crates.
- **`benchmarks/tpch/src/`** grows from a single `main.rs` to a module tree (`engines/`, `runner.rs`, `stats.rs`, `report.rs`, `main.rs`). No changes outside `benchmarks/`.
- **`benchmarks/tpch/queries/`** gains 6 new `.sql` files (q15, q17, q18, q20, q21, q22) where Arneb supports them; otherwise a documented skip list lives next to the runner.
- **`benchmarks/tpch/scripts/run_benchmark.sh`** updated; `report.py` removed (or kept as a thin shim that prints a deprecation notice for one release).
- **Top-level `README.md`** gains a one-line pointer to the new tutorial.
- **CI**: the existing benchmark CI (if any) is not in scope; the harness is run manually on a developer machine. The crate must still compile under `cargo build --release` from a clean checkout.
- **No production code paths in `crates/`** are touched. The risk surface is contained to the `benchmarks/tpch` directory.
