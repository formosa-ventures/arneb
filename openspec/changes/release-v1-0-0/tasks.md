## 1. Resolve open questions before implementation

- [x] 1.1 Host for the official run: the maintainer's MacBook Pro — `MacBookPro18,3`, Apple M1 Pro, 10 physical cores (10 logical), 32 GB RAM, macOS 26.5.2 (25F84), arm64, SSD, `rustc 1.94.1`. These values go into `provenance.json` verbatim.
- [x] 1.2 `git_commit` records the SHA of the commit immediately preceding the provenance commit — the tree that was actually built and measured. Rule recorded in design "Resolved Questions"; task 3.4 writes it into the release checklist.
- [x] 1.3 `CHANGELOG.md` summarizes `1.0.0` per phase (Phase 1 / Phase 2 / Phase 2.5), each a short prose section, with a link to `openspec/changes/archive/` for change-by-change detail.
- [x] 1.4 Configuration for the official run: **default** — no `ARNEB_*` tuning flags — with Arneb and Trino under the same container isolation, so the published figure matches a stock `cargo build --release` build (design D9). Accepted consequences: a likely smaller speedup than the repository's existing tuned-configuration claims, and possible regressions or failures on queries the flags exist to help.
- [x] 1.5 Not applicable — the tuned configuration was not chosen. `docker/arneb-bench/docker-compose.bench.yml` keeps its flag set for tuning work; the official run must not inherit it (task 6.1b).

## 2a. Wire the harness modules the archived change left dead

The archived `tpch-comparison-harness` marked 37/41 tasks complete, but `benchmarks/tpch/src/main.rs` contains zero `mod` declarations and zero references to any new type. Its nine module files (1,615 lines across `canonical.rs`, `correctness.rs`, `runner.rs`, `report.rs`, `skip.rs`, `stats.rs`, `engines/{mod,arneb,trino,datafusion}.rs`) are unreachable from the crate root, so they have never been compiled — `cargo check` passes in seconds precisely because they are not part of the crate. `Cargo.toml` is missing every dependency they need. All nine files are untracked. The three-engine harness described by the `tpch-benchmark-*` specs does not exist as a working program, and nothing in sections 2b, 4, or 6 can proceed until it does.

- [x] 2a.1 Add the missing dependencies to `benchmarks/tpch/Cargo.toml`: `datafusion`, `object_store` (with the `aws` feature), and `sha2`. Pick a `datafusion` version whose Arrow major version does not conflict with what the crate already resolves.
- [x] 2a.2 Declare the module tree from the crate root so all nine files are actually compiled: `canonical`, `correctness`, `runner`, `report`, `skip`, `stats`, and `engines` (with its four submodules).
- [x] 2a.3 Rewrite `main.rs` as CLI plus dispatch. It currently holds the entire old two-engine implementation inline (`BenchClient`, `PgClient`, `TrinoClient`, the old `BenchmarkResult`/`QueryResult`/`RunResult`). Remove what the modules now supersede rather than leaving two parallel implementations in the crate.
- [x] 2a.4 Add the `run` / `report` subcommand split, keeping the existing single-engine flag set working on `run` for backwards compatibility.
- [x] 2a.5 Add the `--engines arneb,trino,datafusion` flag, with `--engine <name>` retained as a single-engine alias and all three engines as the no-flag default.
- [x] 2a.6 Add `--minio-endpoint` and the documented environment-variable precedence the DataFusion adapter needs.
- [x] 2a.7 Get the crate to compile. These 1,615 lines have never been through the compiler; expect real breakage, not just wiring. Resolve it in the modules where the fix belongs rather than by narrowing the interfaces the specs describe.
- [x] 2a.8 Verify the wired binary against the archived specs' observable surface: `tpch-bench --help` shows both subcommands and both engine flags, `tpch-bench report --help` accepts `--dir` and explicit file paths, and a `--engines arneb` run still produces the backwards-compatible JSON shape.
- [x] 2a.9 Correct the archived record: in `openspec/changes/archive/2026-07-26-tpch-comparison-harness/tasks.md`, uncheck the tasks that were marked complete without the code being wired, and add a note pointing at this change as where the work was actually completed. An archive that overstates completion is worse than no archive.
- [ ] 2a.10 Commit the nine untracked module files together with the wiring, so the crate is never again in a state where the code exists but nothing compiles it.
- [ ] 2a.11 Note for section 6: port 5432 on the release host is already held by an unrelated project's `postgres` container, so a native Arneb bench run would collide. Ports 8080, 9000, 9001, 9083, 9090, and 6432 are free. Containerizing the runner (2b) removes the collision entirely, since engines are reached over the compose network rather than published host ports — do not solve this by remapping host ports.

## 2. Close the harness gaps that block the official run (spec: release-baseline-numbers)

- [x] 2.1 Rewrite `benchmarks/tpch/scripts/run_benchmark.sh` to invoke `tpch-bench` for all three engines including DataFusion (which needs no service — only the MinIO endpoint), replacing the current two-engine Arneb/Trino flow.
- [x] 2.2 Replace the `python3 "$SCRIPT_DIR/report.py"` calls at `run_benchmark.sh:130` and `:132` with a single `tpch-bench report --dir "$RESULTS_DIR"` invocation writing `comparison.md`.
- [x] 2.3 Generalize the script's `--skip-trino` flag into an engine selector that forwards to the runner's `--engines`, so a reader can reproduce a subset without editing the script.
- [x] 2.4 Delete `benchmarks/tpch/scripts/report.py` (tracked in git today). This satisfies the archived `tpch-benchmark-reporting` requirement "Removal of `report.py`" and is **BREAKING** for anyone invoking it directly.
- [x] 2.5 Verify `run_benchmark.sh` runs end-to-end with no `python3` on `PATH` (e.g. via a `PATH`-restricted shell), per the archived reporting spec's scenario.

## 2b. Containerize the whole benchmark, runner included (design D10)

- [x] 2b.1 Add a Dockerfile for the `tpch-bench` runner. `benchmarks/tpch` is excluded from the workspace and pulls in `datafusion`, so give it its own build rather than a stage in `docker/arneb-bench/Dockerfile` — that also avoids rebuilding the engine when only the harness changes. Follow the existing Dockerfile's BuildKit cache-mount pattern to keep rebuilds short.
- [x] 2b.2 Add a compose service for the runner: queries directory available, an output volume so result JSONs land on the host for publishing, and `depends_on` conditions for MinIO, Hive Metastore, Trino, and Arneb.
- [x] 2b.3 Add a stock Arneb bench service that uses `docker/arneb-bench/Dockerfile` with `tpch-hive-container.toml` (both verified clean of tuning) and carries **no** `ARNEB_*` environment block, per design D9. Do not reuse `docker-compose.bench.yml`'s service definition — it exists for tuning work and sets 16 flags.
- [x] 2b.4 Point the runner at service-name endpoints on the compose network: `arneb:5432`, `trino:8080`, `http://minio:9000`. Leave `benchmarks/tpch/src/engines/datafusion.rs:46`'s `http://127.0.0.1:9000` default host-oriented for native iteration and pass the service name explicitly from the container, so neither mode is a special case of the other.
- [x] 2b.5 Confirm DataFusion — which runs in-process inside the runner binary — is subject to the runner container's CPU and memory limits, and that those limits match what Arneb and Trino get. This is the whole reason the runner is containerized; verify it rather than assume it.
- [x] 2b.6 Rewrite `run_benchmark.sh` as a compose driver: bring up the stack, seed, run the runner service, collect results and `comparison.md` from the output volume. It must no longer start any native host process.
- [x] 2b.7 Verify no engine's measured latency includes a host port-publishing hop the others avoid — all three are reached over the compose network from inside the runner container.

## 3. Reconcile the two benchmark front doors (spec: release-baseline-numbers)

- [x] 3.1 Rewrite the opening of `benchmarks/tpch/README.md` to state that it documents the dual-axis latency-and-peak-memory harness (`run_memory_bench.sh`, `bench_report.py`, `verify_memory.py`) under container isolation, and to link to `TUTORIAL.md` as the entry point for the reproducible latency comparison.
- [x] 3.2 Update `benchmarks/tpch/TUTORIAL.md` so its "Reproduce the comparison" commands match the rewritten `run_benchmark.sh` exactly, and its "Read the report" section matches the real `tpch-bench report` output headings (`# TPC-H Comparison Report`, `## Per-query latency (p50)`, `## Suite summary`, `### Pairwise geomean speedup`, `## Correctness divergences`).
- [x] 3.2a Make the tutorial's primary reproduction path the fully containerized one, with no step launching a native host process. If a native invocation is kept as a convenience for local iteration, mark it clearly as not the path official numbers come from. _(spec: tpch-benchmark-tutorial, modified)_
- [x] 3.2b Rewrite the tutorial's MinIO callout to distinguish the two modes: `http://minio:9000` on the compose network for the containerized run, `127.0.0.1:9000` for a native run from the host. Keep the `AmazonS3Builder::from_env()` credential warning scoped to the native path, and note that the containerized run is structurally immune because a container sees only what is explicitly passed to it. _(spec: tpch-benchmark-tutorial, modified)_
- [x] 3.3 Change the root `README.md` benchmark link from `benchmarks/tpch/README.md` to `benchmarks/tpch/TUTORIAL.md`, satisfying the archived `tpch-benchmark-tutorial` requirement that the README link resolve to the tutorial.
- [x] 3.4 Add a release checklist (in `TUTORIAL.md` or a sibling document) recording the reusable rules for future releases: `git_commit` names the commit preceding the provenance commit; official results go to `benchmarks/tpch/official/v<version>/`; engine versions are resolved from what ran, not from compose tags; the engine configuration is recorded explicitly.
- [x] 3.5 Correct the stale host comment in `docker/arneb-bench/docker-compose.bench.yml` — it states "Docker Desktop on this host has 3.88 GB total memory", but the container runtime on the release host reports 15.7 GB. A stale memory figure in the file that governs benchmark fairness is a trap for the next person tuning it.

## 4. Live verification of the three-engine path (inherited from `tpch-comparison-harness`)

- [x] 4.1 Start the stack (`docker compose up -d`, `docker compose run --rm tpch-seed`) and run the containerized runner with `--queries 1,6 --engines arneb,trino,datafusion` end-to-end. Confirm three result JSONs land on the host via the output volume. _(inherited task 9.2)_
- [x] 4.2 Run each of `q15`, `q17`, `q18`, `q20`, `q21`, `q22` against Trino and DataFusion and confirm both produce results; record sample row counts for human verification. _(inherited task 5.2)_
- [x] 4.3 Skip list was entirely wrong and is now empty. It declared arneb unable to run q15, q17, q20, q21 and q22, each citing unsupported correlated subqueries; all five execute successfully against a stock arneb build. The entries came from reading the SQL rather than running it (archived task 5.3 pre-populated them from static analysis and deferred verification to a task never done), so the harness silently dropped five queries from every comparison and published a fabricated reason for each. A test now asserts no query is skipped without a verified, quoted failure. _(closes the deferred half of inherited task 5.3)_
- [x] 4.4 Run `./benchmarks/tpch/scripts/run_benchmark.sh` with no arguments and confirm it produces all three engines' JSON files plus `comparison.md` without invoking Python. _(inherited task 9.4)_
- [x] 4.5 Follow `benchmarks/tpch/TUTORIAL.md` end-to-end in a fresh shell with `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY` first, confirming no command has drifted from the runner and the documented report columns match what is emitted. _(inherited task 9.5)_

## 5. Version bump and release metadata (spec: release-versioning)

- [x] 5.1 Set `[workspace.package].version = "1.0.0"` in the root `Cargo.toml`.
- [x] 5.2 Set `version = "1.0.0"` in `benchmarks/tpch/Cargo.toml` (excluded from the workspace, so it does not inherit).
- [x] 5.3 Confirm no crate under `crates/` declares a literal `version` that would drift — all 13 currently use `version.workspace = true`; re-verify after the bump.
- [x] 5.4 Verify the three runtime surfaces report `1.0.0`: `arneb --version` (clap derives it at `crates/server/src/main.rs:124`), `SELECT version()` (`crates/protocol/src/metadata.rs:142`), and the Web UI server-info endpoint (`crates/server/src/web/api.rs:153`).
- [x] 5.5 Confirm `SHOW server_version` still returns `"14.0"` (`crates/protocol/src/metadata.rs:570`) — it is the PostgreSQL compatibility level advertised to clients, not Arneb's version, and must not track the release bump.
- [x] 5.6 Add a regression test asserting that `SELECT version()` reports the crate version while `SHOW server_version` reports the PostgreSQL compatibility level, so a future bump cannot silently conflate them.
- [x] 5.7 Create `CHANGELOG.md` at the repository root with a `1.0.0` section at the chosen granularity (task 1.3), including an explicitly labelled breaking-changes heading naming the `report.py` removal and its replacement.

## 6. Produce and publish the official run (spec: release-baseline-numbers)

- [x] 6.1 On the host recorded in 1.1, execute the full official run: all 22 queries, all three engines, SF1, under the configuration decided in 1.4.
- [x] 6.1a Ensure the run controls for environment parity per design D9 and D10: all three engines and the runner execute inside the container stack, with a deliberately chosen CPU allocation rather than the bench override's default of 2. With the runner containerized there is no native-vs-container asymmetry left to disclose.
- [x] 6.1b Ensure the containerized Arneb for the official run carries **no** `ARNEB_*` tuning variables. `docker/arneb-bench/docker-compose.bench.yml` sets 16 of them; the official run uses the stock service from 2b.3 instead. Verify the running container's environment (`docker inspect`) before measuring, not just the compose file.
- [x] 6.1c Confirm each engine's resource allocation is equal and recorded (CPU count and memory limit per container, including the runner container that hosts DataFusion), so the comparison is not decided by an allocation asymmetry.
- [x] 6.2 Create `benchmarks/tpch/official/v1.0.0/` and confirm it is not matched by `.gitignore` (`benchmarks/tpch/results/` at `.gitignore:22` must keep ignoring scratch runs while leaving `official/` tracked).
- [x] 6.3 Copy the three result JSONs into the directory under stable names `arneb.json`, `trino.json`, `datafusion.json`, and the generated report as `comparison.md`, verbatim and unedited.
- [x] 6.4 Write `provenance.json` with the fields required by the spec: `arneb_version`, `git_commit` (SHA of the commit preceding the provenance commit, per 1.2), `rustc_version` (`1.94.1`), resolved `trino_version` / `datafusion_version` / `minio_version`, `scale_factor`, `queries_total`, `warm_up_runs`, `measurement_runs`, `host` (Apple M1 Pro, 10 physical cores, 32 GB, macOS 26.5.2, arm64, SSD), and `run_date` in RFC 3339.
- [x] 6.4a Record the engine configuration in `provenance.json`: the complete set of `ARNEB_*` environment variables in effect for the measured run (empty set if the default configuration was chosen), and the CPU and memory allocation each engine ran under. Configuration must never be implicit.
- [x] 6.5 Resolve the engine versions from what actually ran — container image digests via `docker inspect` and/or each engine's self-reported version — never transcribed from the `:latest` tags in `docker-compose.yml`.
- [x] 6.6 Record the outcome honestly: if any query failed, was skipped, or diverged across engines, confirm it appears as such in the published `comparison.md`. If the run is unusable, discard it entirely and rerun — do not patch individual query results.

## 7. README rewrite (spec: release-baseline-numbers)

- [x] 7.1 Rewrite the `## TPC-H Benchmark` section (`README.md:146`) to state the SF1 suite-level geomean speedup vs Trino, the per-status query counts, and the correctness-agreement count, each sourced from `benchmarks/tpch/official/v1.0.0/`.
- [x] 7.2 State the run date and the path to the published run next to the figures, and link to `comparison.md` for per-query detail.
- [x] 7.3 Replace the reproduction snippet with the command sequence that actually reproduces the published run through `run_benchmark.sh`, dropping the `python3 bench_report.py` invocation.
- [x] 7.4 Audit the existing unsourced claims for survival: the feature bullet at `README.md:18` and the section text at `README.md:148-151` assert 22/22 cell-identical, faster on every query, at SF10 and SF30. Those figures came from the tuned configuration at scale factors this release does not publish, so under design D9 they cannot stand as written. Narrow them to what the SF1 default-configuration run actually shows, or remove them.
- [x] 7.5 State the configuration explicitly next to the published figures — that they are default-configuration numbers with no tuning flags — and note that a tuned path exists for readers who want it. Readers should not have to open `provenance.json` to learn which Arneb was measured.

## 8. Docs homepage (spec: docs-site-scaffold)

- [x] 8.1 Verify that VitePress's `layout: home` renders trailing Markdown beneath the generated features section — build locally with `pnpm docs:build` and inspect the output before writing the real content. If it does not render, resolve the mechanism before proceeding rather than falling back to a theme by default.
- [x] 8.2 Append the comparison block to `docs/index.md` after the home-layout frontmatter: the suite-level figure vs Trino, the scale factor and run date, and a link to the reproduction tutorial.
- [x] 8.3 Confirm `docs/.vitepress/` still contains only `config.ts` (plus build artifacts) — no theme entry point or component was added.
- [x] 8.4 Run `pnpm docs:build` and `pnpm docs:preview` and confirm the homepage renders the hero, the features, and the comparison block, with the tutorial link resolving.

## 9. Consistency check and release (spec: release-versioning, release-baseline-numbers)

- [x] 9.1 Verify every figure quoted in `README.md` and `docs/index.md` appears in `benchmarks/tpch/official/v1.0.0/comparison.md`, and that both surfaces cite the run date and the published directory.
- [x] 9.2 Verify both reader-facing surfaces link to `benchmarks/tpch/TUTORIAL.md` as the single reproduction entry point, and that `benchmarks/tpch/README.md` links to it too.
- [x] 9.3 Run `cargo build --release`, `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt -- --check` on the release commit.
- [x] 9.4 Run `openspec validate release-v1-0-0` and confirm the change is valid.
- [ ] 9.5 Tag the release commit `v1.0.0` — the repository's first tag — and confirm `[workspace.package].version` at that commit is `1.0.0` and `CHANGELOG.md` has a `1.0.0` section.
