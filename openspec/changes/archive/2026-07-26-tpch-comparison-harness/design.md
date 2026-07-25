## Context

Arneb is positioned as "a Trino alternative built in Rust." The current `benchmarks/tpch` crate executes either Arneb (over pgwire) or Trino (over the REST `/v1/statement` API), records median wall-clock per query, and compares pairs via a Python `report.py`. There is no DataFusion baseline (the most direct Arrow-native peer to Arneb), no correctness check between engines, and no narrative tutorial — so a newcomer cannot reproduce a credible comparison without reading the source. The proposal upgrades this into a single-command, three-engine harness with real statistics and a tutorial. This design covers the technical decisions behind that upgrade.

The change is contained inside `benchmarks/tpch/`. No `crates/*` runtime code is touched. The harness consumes Arneb through pgwire exactly as an external user would.

## Goals / Non-Goals

**Goals:**
- A single Rust binary (`tpch-bench`) that can run Arneb, Trino, and DataFusion against TPC-H SF1 in one invocation, sharing identical run methodology (same warmup count, same measurement count, same query SQL).
- Real per-query statistics — min, p50, p95, p99, stddev — plus rows-returned and a correctness hash (sorted-rows hash of the result set).
- A three-way Markdown comparison report with per-query latencies, pairwise speedups, geomean across the suite, and a divergence section if any pair of engines disagree on result-set hashes.
- A tutorial (`benchmarks/tpch/TUTORIAL.md`) that takes a reader from `git clone` to an interpreted report, with copy-pasteable commands.
- Full 22-query coverage where Arneb's SQL surface allows; an explicitly documented skip list for any query an engine cannot run today.
- The whole harness builds with `cargo build --release` from a clean checkout — no Python required.

**Non-Goals:**
- Scale factors above SF1 (the env var still works for power users; defaults stay SF1).
- TPC-DS, ClickBench, or custom federated queries — explicitly deferred.
- Web dashboard, charts, or visualization UI — reports stay textual Markdown.
- Resource accounting (memory, CPU, IO) — wall-clock latency + correctness only.
- CI integration — the harness is a developer tool, run manually.
- Touching production code paths in `crates/`.

## Decisions

### D1 — DataFusion runs in-process, not as a separate service

The DataFusion adapter embeds the `datafusion` crate directly and registers TPC-H tables on a `SessionContext`. We do not stand up a separate process or REST endpoint.

**Rationale.** DataFusion has no canonical RPC server (Ballista is a separate, less-mature project that we excluded from scope). An in-process adapter is the canonical "DataFusion CLI"-equivalent and is what most users compare against. It also keeps the harness self-contained — no extra service to start.

**Alternatives considered.**
- *Ballista server*: rejected — adds operational complexity, and the user's clarification was "Apache DataFusion," not Ballista.
- *Calling `datafusion-cli` via subprocess*: rejected — process startup dominates SF1 query time and pollutes timings; also makes the harness depend on an external binary.

**Consequence.** `tpch-bench`'s `Cargo.toml` gains a `datafusion` dep. This dep stays inside the `benchmarks/` directory; it does not enter the main workspace's runtime crates.

### D2 — All three engines read the same Parquet bytes

Arneb (via Hive catalog), Trino (via Hive catalog), and DataFusion (via `ListingTable` over `object_store`) all point at the same MinIO bucket `s3://warehouse/tpch/<table>/`. DataFusion's `object_store` registry is configured with the same MinIO endpoint Arneb uses.

**Rationale.** Fairness. If one engine reads a different Parquet file (different compression, different row group size, different dictionary encoding), the comparison is meaningless. Sharing the bucket eliminates this.

**Alternatives considered.**
- *Each engine reads its own local copy*: rejected — drift risk, and DataFusion's local-file fast path would unfairly win.
- *DataFusion reads the local SF001 Parquet (the existing `tpch-sf001.toml` data)*: rejected — different scale and different files than the Hive seed; not comparable.

**Consequence.** The DataFusion adapter must wire `AmazonS3Builder` against MinIO using the same env vars Arneb's `[storage.s3]` resolves from. We document this in the tutorial.

### D3 — Engine adapters live behind a small async trait

```rust
#[async_trait]
trait BenchmarkEngine: Send + Sync {
    fn name(&self) -> &'static str;
    async fn connect(&mut self) -> Result<()>;
    async fn execute(&mut self, sql: &str) -> Result<EngineResult>;
}

struct EngineResult {
    rows: Vec<Vec<ScalarBytes>>, // canonicalized for hashing
    elapsed: Duration,
}
```

The runner is generic over `&mut dyn BenchmarkEngine` and does not know about pgwire vs REST vs DataFusion.

**Rationale.** Three concrete engines is the threshold where copy-paste branching becomes obviously wrong. The trait also makes a fourth engine (e.g., Ballista, or a future Arneb-distributed mode) a contained patch later.

**Alternatives considered.**
- *Keep the existing `if engine == "arneb" { ... } else if "trino" { ... }` style and add a third branch*: rejected — three branches is the tipping point.
- *Use a separate binary per engine, orchestrated by a shell script*: rejected — defeats the "single command" goal and makes statistics aggregation awkward.

### D4 — Statistics are computed in Rust, not a Python report

The runner emits a per-query `Stats { min, p50, p95, p99, stddev, geomean_input }` struct; the report subcommand consumes one or more JSON files and prints a Markdown table.

**Rationale.** The proposal calls for a Rust-only build. p95/p99 from 5–10 samples is noisy by definition (proposal default is `--num-runs 5 --warm-up 2` → 3 measurements), but they are the right shape; users can crank `--num-runs` for tighter intervals. Keeping the math in Rust avoids dual-language drift between `report.py` and the runner's JSON shape.

**Trade-off.** With 3 measurements, p95 ≈ max and p99 ≈ max. We document this explicitly in the report header so users do not over-interpret thin samples.

**Alternatives considered.**
- *Keep `report.py`*: rejected — the proposal explicitly removes Python.
- *Compute p95/p99 only when `--num-runs >= 20`*: rejected — surprising conditional output is worse than honest documentation.

### D5 — Correctness check via canonical row hashing, not byte-for-byte

After each engine's first non-warmup run of a query, we collect the rows, normalize each value to a canonical text form (NULL → `\N`, floats → 6-decimal fixed), sort the row list lexicographically, hash the result with SHA-256, and store the digest. The report flags any pair of engines whose hash differs for the same query.

**Rationale.** Different engines emit slightly different byte reps for floats, decimal scales, and timestamps. TPC-H queries with `ORDER BY` on a non-unique key can also yield different row orderings between engines. Canonical normalization + sort is the standard fix and is much simpler than maintaining a per-query expected-result fixture.

**Trade-off.** Two engines can produce numerically equivalent results that differ in our canonical form (e.g., a float computed in different precision rounding to a different last decimal). We pick 6 decimals as a compromise — TPC-H expected results historically use 4 decimals, so 6 is conservative enough to flag real divergence without false positives on rounding noise.

**Alternatives considered.**
- *Compare against a fixed expected-results file*: rejected — generating and maintaining 22 expected result sets at SF1 is a side project we do not need.
- *Compare row counts only*: rejected — too weak; a wrong join can return the right count of wrong rows.

### D6 — Six missing queries: write what Arneb supports, document what it doesn't

For q15, q17, q18, q20, q21, q22 we add the SQL files and run them on every engine that supports them. If Arneb cannot run a query today (e.g., q15 uses a view, q22 uses correlated subqueries that may not be implemented), we record a structured `skipped: { engine, reason }` entry instead of silently dropping the query.

**Rationale.** A full-22 harness with explicit skips is more useful than a 16-query harness whose gaps are unexplained. The skip list itself becomes a TODO that drives Arneb's SQL coverage forward.

**Consequence.** The runner must distinguish *failure* (query ran, errored) from *skip* (declared unsupported up front) — these get different rendering in the report.

### D7 — Tutorial structure: prerequisites → reproduce → interpret

`benchmarks/tpch/TUTORIAL.md` has four sections:

1. **Prerequisites** — what to install (Docker, Rust toolchain, ~4 GB free disk).
2. **Reproduce** — exact commands, in order, with expected output snippets.
3. **Read the report** — annotated example showing how to read latencies, speedups, divergences.
4. **Going further** — pointers to scaling up (`TPCH_SF=sf10`), running a single query, comparing against a custom Trino/DataFusion config.

**Rationale.** This is the "complete experience" the user asked for. The README already covers reference material; the tutorial covers narrative.

**Consequence.** The root `README.md` gets one bullet pointing at the tutorial.

### D8 — Default scale stays SF1, default engine set is all three

Running `cargo run --release -- ` with no flags executes all three engines on SF1 (assuming the user followed the tutorial to start MinIO + Trino + Arneb). A new `--engines arneb,trino` flag selects a subset; `--engine arneb` (singular, existing) still works as a back-compat shorthand for `--engines arneb`.

**Rationale.** The "complete experience" should be one command. Keeping `--engine` as an alias preserves the existing single-engine workflow.

## Risks / Trade-offs

- **DataFusion API churn.** [Risk] DataFusion's public API has historically broken between minor versions. → **Mitigation**: pin a single DataFusion 44.x version (compatible with the workspace's Arrow 58 pin) in `benchmarks/tpch/Cargo.toml`. The benchmark crate is excluded from the workspace's main lockfile (it has its own `Cargo.lock`), so churn is contained.

- **MinIO env var collision.** [Risk] DataFusion's `AmazonS3Builder::from_env()` and Arneb's `[storage.s3]` both look at `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. If a user has real AWS creds in their environment, the harness will silently use them and fail (or worse, hit real AWS). → **Mitigation**: the runner explicitly sets `AWS_ENDPOINT_URL` and `AWS_ALLOW_HTTP=true` in-process before constructing the DataFusion `S3` builder, and the tutorial calls this out.

- **Trino warm cache vs Arneb cold cache.** [Risk] Trino's coordinator caches Hive metadata aggressively; Arneb may not. After warmup the first few measured runs may still be skewed. → **Mitigation**: defaults bumped from `--warm-up 2 --num-runs 5` to `--warm-up 3 --num-runs 8` so we have 5 measured samples per engine. Documented in the tutorial.

- **Result-hash false positives.** [Risk] Float rounding at the 6th decimal may flag legitimate runs as divergent. → **Mitigation**: canonical form documented in design; report shows the actual diff (first ten differing rows) so a human can confirm.

- **Tutorial bit-rot.** [Risk] A tutorial that doesn't run becomes worse than no tutorial. → **Mitigation**: `scripts/run_benchmark.sh` is the canonical implementation of the tutorial's "Reproduce" section; the tutorial cites the script as the authoritative source. A future change can add a CI smoke that runs the script end-to-end on tiny SF.

- **Removing `report.py`.** [Risk] Anyone with muscle memory for `python3 scripts/report.py …` will hit a broken command. → **Mitigation**: keep `report.py` for one release as a thin shim that prints a deprecation message pointing at `tpch-bench report …`, then delete it in a follow-up change.

- **DataFusion can't reach MinIO behind Docker DNS.** [Risk] If the user runs `tpch-bench` on the host but MinIO is `minio:9000` inside Docker's network, DataFusion's S3 client cannot resolve it. → **Mitigation**: tutorial uses the host-published `127.0.0.1:9000` endpoint everywhere; docker-compose already publishes that port.

## Open Questions

- Should `tpch-bench report` accept a directory and auto-pick the latest result per engine, or require explicit file paths? Leaning toward "directory + latest" for tutorial ergonomics; will decide during specs.
- DataFusion can read Parquet via `ListingTable` *or* register a Hive-style external table. The former is simpler and engine-fair (no metastore on the DataFusion path); the latter would let DataFusion benefit from any HMS partition pruning. Leaning toward `ListingTable` for symmetry with how Arneb's Hive connector lists files. To be confirmed during specs.
- Whether to use `comfy-table` or hand-rolled Markdown for the report. Hand-rolled is one fewer dep and Markdown tables are trivial; leaning hand-rolled. Decide during implementation.
