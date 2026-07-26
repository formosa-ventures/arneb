## Why

Arneb's README opens with "a Trino alternative built in Rust" and asserts that all 22 TPC-H queries are cell-identical to Trino while running faster on less peak memory. A reader has no way to check any of that. The repository carries no git tag, `[workspace.package] version` is still `0.1.0`, and neither the README nor the documentation homepage shows a single number, a date, or a machine those claims came from. The strongest asset the project has — it beats the thing it is compared against — is currently a paragraph of adjectives.

Shipping the first official release is the moment to fix that. This change cuts **v1.0.0** and backs the headline claim with one set of official numbers produced by the harness in this repository, at a scale factor a reader can reproduce on a laptop (SF1, via `docker compose`), published in exactly two reader-facing places: the README and the docs homepage.

Preparing this surfaced a gap that blocks the official run, and it is larger than it first appeared. The just-archived `tpch-comparison-harness` change marked 37 of its 41 tasks complete, but the three-engine harness its specs describe **does not exist as a working program**. `benchmarks/tpch/src/main.rs` contains no `mod` declarations and no reference to any type the change introduced; its nine module files — 1,615 lines covering the engine adapters, runner, statistics, correctness hashing, skip list, and reporting — are unreachable from the crate root and have therefore never been compiled. `Cargo.toml` is missing `datafusion`, `object_store`, and `sha2` entirely. All nine files are untracked. `cargo check` passes in seconds because those files are not part of the crate at all.

Consequently `tpch-bench` today offers a single `--engine arneb|trino` with no `--engines` flag, no `report` subcommand, no DataFusion, no per-query statistics, no correctness hashing, and no skip list. Wiring that up is a prerequisite for this release, not adjacent work — there is no way to produce an official three-engine run without it. Two smaller gaps sit on top: `benchmarks/tpch/scripts/report.py` is still tracked and `benchmarks/tpch/scripts/run_benchmark.sh` still calls `python3 report.py` (lines 130 and 132) while driving only Arneb and Trino. The archived `tpch-benchmark-reporting` spec forbids both. The repository also has two competing, mutually inconsistent benchmark front doors — `benchmarks/tpch/README.md` (dual-axis latency + peak memory, Arneb vs Trino, Python reporting) and `benchmarks/tpch/TUTORIAL.md` (three-way Arneb/Trino/DataFusion, Rust reporting) — and the root README links only to the former, violating the archived `tpch-benchmark-tutorial` spec. Closing these is in scope here because the official run has to go through a shell entry point that works.

## What Changes

**Release mechanics**

- Bump the version from `0.1.0` to `1.0.0` in `[workspace.package]` in the root `Cargo.toml`, and separately in `benchmarks/tpch/Cargo.toml` — that crate is `exclude`d from the workspace and carries its own `version` field, so it does not inherit the bump.
- Add a `CHANGELOG.md` with a `1.0.0` entry summarizing the capabilities delivered across Phases 1, 2, and 2.5.
- Tag the release commit `v1.0.0`. The repository has no prior tags; `v1.0.0` is the first.
- Expose the release version through the running server so a reader can confirm which build produced a number (`arneb --version` and the pgwire `server_version` / Web UI footer, whichever already read a version constant).

**Close the harness gaps that block the official run**

- Wire the nine dead module files into the `tpch-bench` crate: add the missing `datafusion`, `object_store`, and `sha2` dependencies, declare the module tree, rewrite `main.rs` as CLI plus dispatch (it currently holds the whole superseded two-engine implementation inline), add the `run`/`report` subcommands and the `--engines` flag, and get 1,615 never-compiled lines to build. Correct the archived change's `tasks.md` so the record no longer overstates what was done.
- Delete `benchmarks/tpch/scripts/report.py`. **BREAKING** for anyone invoking it directly; `tpch-bench report` replaces it.
- Rewrite `benchmarks/tpch/scripts/run_benchmark.sh` to drive all three engines through `--engines arneb,trino,datafusion` and to produce `comparison.md` via `tpch-bench report`, with no Python dependency.
- Run the entire benchmark inside Docker — MinIO, Hive Metastore, Trino, Arneb, **and the `tpch-bench` runner**, which has no container today. This is what makes the comparison fair: DataFusion executes in-process inside the runner binary, so while the runner is native, DataFusion is native no matter what is done to the other engines. `run_benchmark.sh` becomes a compose driver instead of a script that launches host processes.
- Measure Arneb in its **default configuration**. `docker/arneb-bench/docker-compose.bench.yml` enables 16 `ARNEB_*` options that its own comment describes as "gated default-OFF in the engine"; a headline number produced with those on is not the number a reader gets from a stock build, so the official run must not inherit that environment block.
- Reconcile the two benchmark front doors: `TUTORIAL.md` becomes the single entry point for the reproducible three-way comparison, `benchmarks/tpch/README.md` is demoted to a reference for the dual-axis memory harness and cross-links to the tutorial, and the root README links to `TUTORIAL.md`.

**Produce and publish the official numbers**

- Execute one official TPC-H SF1 run covering all 22 queries against Arneb, Trino, and DataFusion on the local `docker compose` stack, using the archived harness. This run also discharges the four live-verification tasks carried over from `tpch-comparison-harness` (5.2, 9.2, 9.4, 9.5).
- Commit the run's artifacts to a tracked path. `benchmarks/tpch/results/` is gitignored (`.gitignore:22`), so official results need a location that is not — proposed `benchmarks/tpch/official/v1.0.0/`, holding the three result JSONs, the generated `comparison.md`, and a provenance record (date, host CPU/RAM/OS, scale factor, engine versions, warmup and measurement counts).
- Establish this directory as the single source of truth: every number quoted in the README or on the docs homepage must be traceable to it, and the release version stamped in the provenance record must match the tagged version.

**Reader-facing surfaces**

- Rewrite the README's `## TPC-H Benchmark` section: replace the unsourced adjectives with the official SF1 headline figures (geomean speedup vs Trino, query counts by status, correctness agreement), state the machine and date they came from, and give the exact command sequence that reproduces them.
- Add a comparison block to the docs homepage `docs/index.md`, below the hero. VitePress's `layout: home` has no field for this, so it needs either a custom section appended after the frontmatter or a theme slot — the mechanism is a design decision, not settled here.

**Non-goals**

- SF10 / SF30 numbers and the remote benchmark host. v1.0.0 publishes the reproducible-on-a-laptop SF1 figure only; larger scale factors remain a follow-up.
- Reworking the dual-axis peak-memory harness (`run_memory_bench.sh`, `bench_report.py`, `verify_memory.py`). It keeps working and keeps its own front door; it is simply not what the homepage cites.
- Expanding Arneb's SQL surface to clear any query currently on the skip list. Whatever the official run skips is published as skipped, with its reason.
- Web UI changes, packaging, or distribution (crates.io, Homebrew, container images).
- Automating the official run in CI.

## Capabilities

### New Capabilities

- `release-versioning`: How the project's release version is declared, kept consistent across the workspace and the excluded `benchmarks/tpch` crate, surfaced at runtime, recorded in a changelog, and tagged in git.
- `release-baseline-numbers`: How the official benchmark numbers for a release are produced, what provenance must accompany them, where they are stored so they survive `.gitignore`, and the rule that every reader-facing performance claim must be traceable to them.

### Modified Capabilities

- `docs-site-scaffold`: Its hero-page requirement currently mandates only a tagline and call-to-action buttons. It changes to also require the homepage to present the official entry-level comparison figures with their provenance and a link to the reproduction tutorial.
- `tpch-benchmark-runner`: Its canonicalization rule mandates formatting floats to six fractional digits. That rule is not scale-invariant and cannot be satisfied at TPC-H magnitudes — six decimals on a value near `5.7e10` demands seventeen significant digits, more than an f64 carries — so one ULP of summation-order noise between engines was reported as a correctness divergence. It changes to a significant-digit rule with explicit scale-invariance and noise-absorption requirements.
- `tpch-benchmark-reporting`: Its speedup requirement gives `1.42x` as an example but never states the orientation, and the implementation computed the reciprocal — rendering a 2x win as `0.49x`. The requirement gains an explicit orientation, a rule that the geomean match the per-query columns, and a requirement that the orientation be covered by a test rather than left to review.
- `tpch-benchmark-tutorial`: Two requirements change once the runner is containerized. The MinIO callout currently mandates telling readers the harness reads `127.0.0.1:9000` from the host — true only of a native invocation — and must instead distinguish the containerized endpoint from the native one. The reproduction-path requirement must mandate that the documented primary path is the fully containerized one, matching how official numbers are produced.

## Impact

- **`Cargo.toml`** (root, `[workspace.package]`) and **`benchmarks/tpch/Cargo.toml`** — version bump in two independent places.
- **`CHANGELOG.md`** — new file at the repository root.
- **`benchmarks/tpch/Cargo.toml`** — gains `datafusion`, `object_store` (`aws` feature), and `sha2`, none of which are present today.
- **`benchmarks/tpch/src/`** — nine untracked module files get wired in and committed; `main.rs` shrinks from a 417-line inline implementation to CLI plus dispatch.
- **`openspec/changes/archive/2026-07-26-tpch-comparison-harness/tasks.md`** — falsely checked tasks corrected, with a pointer to this change.
- **`benchmarks/tpch/scripts/`** — `report.py` deleted; `run_benchmark.sh` rewritten as a compose driver for three containerized engines and Rust reporting.
- **New runner image and compose service** — `benchmarks/tpch` is excluded from the workspace and pulls in `datafusion`, so it needs its own Dockerfile rather than a stage in `docker/arneb-bench/Dockerfile`; plus a compose service with the queries directory and an output volume for result JSONs.
- **A stock Arneb bench service** — reusing `docker/arneb-bench/Dockerfile` and `tpch-hive-container.toml` (both clean of tuning) without `docker-compose.bench.yml`'s `environment:` block.
- **`benchmarks/tpch/src/engines/datafusion.rs:46`** — the MinIO endpoint default (`http://127.0.0.1:9000`) stays host-oriented; the container passes `http://minio:9000` explicitly.
- **`benchmarks/tpch/official/v1.0.0/`** — new tracked directory holding result JSONs, `comparison.md`, and provenance.
- **`.gitignore`** — verify that `benchmarks/tpch/results/` stays ignored while the new `official/` path is not accidentally caught by it.
- **`README.md`** — `## TPC-H Benchmark` section rewritten; link target changes from `benchmarks/tpch/README.md` to `benchmarks/tpch/TUTORIAL.md`.
- **`benchmarks/tpch/README.md`** and **`benchmarks/tpch/TUTORIAL.md`** — scopes separated and cross-linked.
- **`docs/index.md`**, possibly **`docs/.vitepress/config.ts`** and a theme file — homepage comparison block.
- **Version surfacing** — whichever crate holds the version constant read by `--version`, the pgwire handshake, and the Web UI footer.
- **No changes to query execution.** No file under `crates/*/src` changes except version surfacing. Engine behavior at v1.0.0 is exactly what is on `main` today; this change measures and publishes it, it does not alter it.
- **Risk**: the official run is a live-stack dependency (Docker, MinIO, HMS, Trino). If any of the 22 queries fails or diverges on the official run, that result gets published as-is rather than quietly dropped — the numbers are only worth publishing if they are the numbers that actually happened.
