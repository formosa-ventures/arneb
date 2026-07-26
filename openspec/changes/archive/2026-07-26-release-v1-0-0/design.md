## Context

The repository is at `0.1.0` with no git tags. Phases 1, 2, and 2.5 are complete and the TPC-H harness (archived as `tpch-comparison-harness`) can run Arneb, Trino, and DataFusion three ways against identical Parquet in MinIO. What is missing is a release: a version, a tag, and one set of published numbers a reader can reproduce.

Four facts about the current tree shape this design:

1. **Version surfacing is already wired.** `crates/server/src/main.rs:124` declares `#[command(name = "arneb", version, about)]`, so clap derives `--version` from `CARGO_PKG_VERSION`. `crates/protocol/src/metadata.rs:142` renders `format!("Arneb {}", env!("CARGO_PKG_VERSION"))` for SQL `version()`. `crates/server/src/web/api.rs:153` puts the same constant in the Web UI `/info` response. Both crates set `version.workspace = true`, so all three surfaces already track the workspace version. No new version constant or build script is needed.
2. **`benchmarks/tpch` is not in the workspace.** Root `Cargo.toml` has `exclude = ["benchmarks/tpch"]`, and that crate declares a literal `version = "0.1.0"`. It will not inherit a workspace bump.
3. **`benchmarks/tpch/results/` is gitignored** (`.gitignore:22`). Anything published with the release has to live outside that path.
4. **The benchmark stack pins nothing.** `docker-compose.yml` uses `trinodb/trino:latest` (5 services) and `minio/minio:latest`. The comparison baseline therefore drifts silently over time.

The harness gaps found while writing the proposal — `report.py` still tracked, `run_benchmark.sh` still Python-based and two-engine-only, root README linking the wrong benchmark document — are prerequisites, not separate work: the official run has to go through a working entry point.

## Goals / Non-Goals

**Goals:**

- Cut `v1.0.0` with a version that is consistent everywhere it is observable and a changelog that says what is in it.
- Produce one official TPC-H SF1 three-engine run and publish it with enough provenance that a reader can tell whether their own run is comparable.
- Give the README and the docs homepage numbers with a traceable source, replacing unsourced adjectives.
- Leave exactly one front door for the reproducible comparison, so a reader is never choosing between two conflicting sets of instructions.

**Non-Goals:**

- Reproducibility guarantees stronger than "same scale factor, same stack, documented host." Wall-clock numbers from a laptop are not portable and this design does not pretend otherwise.
- Automating the official run, gating it in CI, or regression-testing numbers across releases.
- Pinning the Docker stack to fixed image digests. Recording what was resolved is in scope; changing how the stack pins is not.
- Any change to query planning or execution. v1.0.0 measures what is on `main`.

## Decisions

### D1. Version bump is two file edits; `server_version` deliberately does not move

Set `[workspace.package] version = "1.0.0"` in the root `Cargo.toml` and `version = "1.0.0"` in `benchmarks/tpch/Cargo.toml`. Everything else follows from `CARGO_PKG_VERSION`.

`SHOW server_version` in `crates/protocol/src/metadata.rs:570` returns the hardcoded string `"14.0"`. This is the **PostgreSQL compatibility level advertised to clients**, not Arneb's version, and it must stay `"14.0"`. Clients (JDBC, psycopg2, DBeaver) branch on this value to decide which catalog queries and protocol features to use; reporting `"1.0.0"` would read as PostgreSQL 1.0 and break capability detection.

*Alternative considered:* introduce a distinct `ARNEB_VERSION` constant so the project version is decoupled from crate versions. Rejected — three surfaces already read `CARGO_PKG_VERSION` correctly, and a parallel constant creates a second thing to forget to bump.

### D2. Official numbers live in `benchmarks/tpch/official/v1.0.0/` with stable filenames

```
benchmarks/tpch/official/v1.0.0/
├── provenance.json     # machine-readable run metadata (source of truth)
├── comparison.md       # verbatim `tpch-bench report` output
├── arneb.json          # verbatim runner output, renamed
├── trino.json
└── datafusion.json
```

The runner emits `{engine}_{YYYYMMDD_HHMMSS}.json` into the gitignored `results/`. Publishing copies those three files under stable names — the timestamp is already a field inside each document, so the filename does not need to carry it, and stable names make `git diff` between release directories readable.

Versioning the directory by release (`v1.0.0/`, later `v1.1.0/`) rather than overwriting a single `official/` directory means past releases' claims stay verifiable after new numbers land.

*Alternative considered:* un-ignore `benchmarks/tpch/results/` and commit into it. Rejected — that directory is scratch space for every local run; making it tracked would put every developer's noise into `git status`.

### D3. `provenance.json` records resolved versions, not tag names

Because the stack floats on `:latest`, the provenance record must capture what actually ran, not what the compose file requested. Fields:

- `arneb_version`, `git_commit` (the tagged commit), `rustc_version`
- `trino_version`, `datafusion_version`, `minio_version` — resolved at run time (`docker inspect` digest and/or the engine's own reported version), never copied from the compose file
- `scale_factor`, `queries_total`, `warm_up_runs`, `measurement_runs`
- `host`: CPU model, physical core count, RAM, OS and version, storage type
- `run_date` (RFC 3339)

This is what lets a reader answer "is my number comparable to yours?" — which is the only honest claim a laptop benchmark can make.

### D4. Homepage numbers go in trailing markdown, not a custom theme

`docs/.vitepress/` contains only `config.ts` — there is no theme directory. VitePress's `layout: home` renders any markdown that follows the frontmatter beneath the generated hero and features. The comparison block is therefore plain markdown appended to `docs/index.md`, with no new theme file, no component, and no change to `config.ts`.

*Alternative considered:* a custom theme extending the default with a `home-features-after` slot and a Vue component for the table. Rejected — it introduces a theme entry point, a component, and a build surface, all to render a static five-row table that markdown already renders. If the homepage later needs interactive charts, that is the moment to take on a theme.

This rests on VitePress rendering trailing markdown in home layout; the tasks include verifying it against a local `pnpm docs:build` before the numbers are written, so a wrong assumption surfaces as a layout problem, not a shipped broken page.

### D5. The headline number is the geomean speedup vs Trino

`tpch-bench report` already emits a `### Pairwise geomean speedup` section computed over queries where both engines succeeded. That is the figure the README and homepage lead with, alongside the query counts by status and the correctness-agreement count.

Leading with a geomean rather than a best-case per-query multiple is the difference between a measurement and a marketing number. Per-query detail stays available in `comparison.md`, linked from both surfaces.

DataFusion numbers are published in `comparison.md` but are not part of the headline. DataFusion is in the run as a correctness cross-check and an Arrow-native sanity reference; the positioning claim in the README is about Trino.

### D6. `run_benchmark.sh` becomes the three-engine entry point; `report.py` is deleted

The script currently runs Arneb (`--engine arneb`) and Trino (`--engine trino`) and pipes both JSONs through `python3 report.py` (lines 130, 132). It is rewritten to make one `tpch-bench` invocation per engine including DataFusion — which needs no service, only the MinIO endpoint — and to generate `comparison.md` via `tpch-bench report --dir "$RESULTS_DIR"`. `report.py` is deleted, satisfying the archived `tpch-benchmark-reporting` requirement.

The existing `--skip-trino` flag generalizes to an engine selector that forwards to the runner's `--engines`, so a reader can reproduce a subset without editing the script.

### D7. Two benchmark documents, two distinct jobs

`benchmarks/tpch/TUTORIAL.md` is the single entry point for the reproducible three-engine latency comparison, and is what the root README and the homepage link to. `benchmarks/tpch/README.md` is demoted to reference material for the dual-axis latency-and-peak-memory harness (`run_memory_bench.sh`, `bench_report.py`) and opens with a pointer to the tutorial.

The two are kept rather than merged because they measure different things through different mechanisms — the memory harness runs engines in isolated containers to attribute RSS, which is a heavier setup than a latency comparison needs. Merging would force every reader through container-isolation setup to get a latency number.

### D8. Number consistency is enforced by a rule, not tooling

Three places state the same figures: `provenance.json` + `comparison.md` (generated), the README, and `docs/index.md` (hand-written). The rule is that `benchmarks/tpch/official/v1.0.0/` is the source of truth and both prose surfaces cite the release directory by path, with the run date visible next to the numbers.

No consistency-checking script is built. Automating a check on two hand-written markdown tables that change once per release is more machinery than the drift risk justifies; a release-time verification task covers it instead.

### D9. The official run measures the default configuration, both engines containerized

**Resolved: option 1 below.** The v1.0.0 headline is what a stock build does — no `ARNEB_*` tuning flags — with Arneb and Trino under the same container isolation. The published figure therefore matches what a reader gets from `cargo build --release && ./target/release/arneb`, which is the only way the "reproducible" claim survives contact with a reader who actually tries it.

Two consequences follow and are accepted:

- The speedup will likely be smaller than the repository's existing SF30 claims, which were produced with the tuned flag set. Those claims do not survive into v1.0.0 prose unless a published run backs them (task 7.4).
- Some queries may be slower, regress, or fail without the flags — `ARNEB_GRACE_HJ` in particular exists to bound Q09 memory. At SF1 the volumes are small enough that this may not bite, but whatever happens is published as it happened.

The tuned configuration remains available to anyone who wants it via `docker/arneb-bench/docker-compose.bench.yml`; it is simply not what v1.0.0 advertises. Flipping the validated flags to default-ON is the clean long-term resolution and belongs to a separate change.

The rest of this section records the analysis that led here.

Choosing the host surfaced two ways the current harness would produce a number that flatters Arneb without saying so. Both must be settled before the run.

**Environment parity.** `run_benchmark.sh` starts Arneb natively on the host (`./target/release/arneb`) while Trino runs in a container. On this host that is 10 native cores and 32 GB against a container runtime reporting 10 CPUs and 15.7 GB — plus native versus virtualized filesystem and network paths. DataFusion is worse: it runs in-process inside the harness binary, entirely native. The repository already contains a partial fix — `docker/arneb-bench/docker-compose.bench.yml`, whose own header says it exists so that "the Trino-vs-arneb TPC-H benchmark runs both engines under identical Docker isolation (no native-vs-container bias)" — but `run_benchmark.sh` does not use it. Note that this override also caps Arneb at `BENCH_NODE_CPUS:-2` CPUs, so it is not a drop-in either; the cap has to be chosen deliberately for a published comparison. It also does not address DataFusion; see D10, which resolves environment parity completely by containerizing the runner.

**Configuration parity.** That same override sets 16 `ARNEB_*` environment variables, and its own comment states they are "all gated default-OFF in the engine; enabled here for the TPC-H bench" — including `ARNEB_GRACE_HJ`, `ARNEB_SELECTIVE_DIM_FIRST`, `ARNEB_EAGER_AGG`, `ARNEB_PARALLEL_FINAL_AGG`, `ARNEB_FOLD_SEMIJOIN_HAVING`, and `ARNEB_AGG_PRESIZE`. A number produced with those flags on is not the number a reader gets from `cargo build --release && ./target/release/arneb`. Publishing it as the v1.0.0 headline without disclosure would make the claim unreproducible by exactly the path the tutorial tells readers to follow.

Three ways to resolve, in decreasing order of how well the published figure matches what a user actually gets — **option 1 was chosen**:

1. **Default configuration, both engines containerized.** Publish what an out-of-the-box build does. Most defensible; likely a smaller speedup than previously claimed, and some queries may regress or fail without the flags.
2. **Tuned configuration, both engines containerized, flags disclosed.** Publish the tuned number with the full flag list in `provenance.json` and the exact env vars in the tutorial, so a reader reproduces the same configuration. Defensible only if the tutorial actually sets them.
3. **Publish both.** Default as the headline, tuned as a "with tuning" second row. Most informative, most work, and it invites the question of why the flags are not on by default — which is a fair question to answer in the changelog.

An orthogonal option that resolves configuration parity permanently: flip the validated flags to default-ON in the engine before v1.0.0, so the tuned path *is* the default path. That is an engine change and would contradict this change's "no changes to query execution" non-goal, so it belongs to a separate change if wanted.

Whichever is chosen, `provenance.json` must record the complete `ARNEB_*` environment of the measured run and the container CPU/memory allocation for every engine, so the configuration is never implicit.

### D10. Everything runs in Docker, including the benchmark runner

The official run is executed entirely inside the container stack: MinIO, Hive Metastore, Trino, Arneb, **and the `tpch-bench` runner itself**. The runner has no container today — `docker/arneb-bench/Dockerfile` builds Arneb, but the harness is invoked natively from the host.

Containerizing the runner is what completes D9's environment parity, for a reason specific to this harness: **DataFusion runs in-process inside the runner binary.** As long as the runner is native, DataFusion is native no matter what is done to the other engines, and no compose arrangement can fix it. Move the runner into a container and DataFusion inherits that container's CPU and memory limits, putting all three engines under the same isolation. The caveat this design previously had to accept — "DataFusion cannot be containerized the same way" — disappears.

Two secondary benefits follow. Measurements are taken over the compose network (`arneb:5432`, `trino:8080`) rather than through host port publishing, so no engine's latency includes a port-forwarding hop the others avoid. And the container does not inherit the host shell's environment, which structurally removes the `AWS_ACCESS_KEY_ID` footgun the tutorial currently has to warn about — a container only sees credentials that were explicitly passed to it.

What this requires:

- A runner image. `benchmarks/tpch` is excluded from the workspace and pulls in `datafusion`, so it needs its own Dockerfile rather than a stage added to the Arneb one — keeping the two build graphs separate also avoids rebuilding the engine when only the harness changes.
- A compose service for the runner, with the queries directory available and an output volume so result JSONs land on the host for publishing.
- Service-name endpoints throughout. `benchmarks/tpch/src/engines/datafusion.rs:46` defaults to `http://127.0.0.1:9000`; inside the network the endpoint is `http://minio:9000`. The default stays host-oriented for local development and the container passes the service name explicitly, so neither mode is a special case of the other.
- A stock Arneb service — `docker/arneb-bench/Dockerfile` with `tpch-hive-container.toml`, which is clean (service names only, no tuning) — **without** the bench override's `environment:` block that carries the 16 `ARNEB_*` flags. Per D9 the official run must not inherit those.
- `run_benchmark.sh` becomes a compose driver: bring up the stack, seed, run the runner service, collect results. It no longer starts native processes.

This invalidates a requirement in the archived `tpch-benchmark-tutorial` spec, which mandates a callout stating that the harness reads MinIO at `127.0.0.1:9000` from the host machine. That is true only of the native invocation. The requirement is amended rather than dropped: both paths exist — containerized for the official run, native for quick local iteration — and the tutorial must be explicit about which endpoint applies to which, because a reader who mixes them gets a connection error with no obvious cause.

*Alternative considered:* keep the runner native and disclose DataFusion's advantage as a caveat. Rejected — a caveat that a reader must weigh by hand is a worse answer than an arrangement where the asymmetry does not exist, and this one is cheap to remove.

## Risks / Trade-offs

- **Not every query may succeed or agree on the official run** → Whatever happens is published as-is: failures as failures, skips with their reasons, hash divergences in the divergence section. The README states counts by status rather than implying 22/22. A number that required hiding a result is not worth publishing.
- **The README's current claim may not survive contact with the official run.** It asserts 22/22 cell-identical, faster on every query, at SF10 and SF30 — while this release publishes SF1 only, and the claim predates the DataFusion cross-check → The README is rewritten to say exactly what the SF1 run showed. If the SF10/SF30 claim cannot be sourced to a published artifact, it does not survive into v1.0.0 prose.
- **`:latest` images mean the baseline drifts** → `provenance.json` records resolved versions; every published figure is dated and scoped to that record. Pinning the stack is deliberately out of scope.
- **SF1 on a laptop is noisy and small; some queries finish in milliseconds where startup dominates** → warmup 3 / 5 measurement runs, p50 as the reported statistic, and the report's existing low-sample-count caveat header. The homepage says which machine produced the numbers rather than implying they are universal.
- **Deleting `report.py` is BREAKING for anyone scripting it** → Called out in the changelog under `1.0.0`. The replacement (`tpch-bench report`) covers the same inputs and more engines.
- **A single release directory per version grows the repo over time** → Result JSONs for 22 queries are small (kilobytes); at one directory per release this is negligible for many releases.

## Migration Plan

1. Land the harness fixes (D6, D7) and verify the three-engine path end-to-end at SF1. This also discharges the four live-verification tasks inherited from `tpch-comparison-harness` (5.2, 9.2, 9.4, 9.5).
2. Bump versions (D1), write `CHANGELOG.md`.
3. Execute the official run on the designated host; publish `benchmarks/tpch/official/v1.0.0/` (D2, D3).
4. Rewrite the README section and add the homepage block from the published artifacts (D4, D5, D8); verify `pnpm docs:build` renders the homepage correctly.
5. Tag `v1.0.0` on the resulting commit, and record that commit's SHA in `provenance.json` — note the ordering constraint in Open Questions.

Rollback: the tag is the only irreversible step, and only in the weak sense that a pushed tag should be superseded rather than deleted. Everything before it is ordinary reverts.

## Resolved Questions

- **Host for the official run: the maintainer's MacBook Pro.** `MacBookPro18,3` — Apple M1 Pro, 10 physical cores (10 logical), 32 GB RAM, macOS 26.5.2 (build 25F84), arm64, SSD, `rustc 1.94.1`. This is the published baseline for v1.0.0 and these values go into `provenance.json` verbatim. It satisfies the "reproducible on a laptop" framing; it also means the numbers are arm64 macOS numbers and the published prose must say so rather than implying they generalize to Linux x86 servers.
- **The official run measures the default Arneb configuration, with Arneb and Trino under the same container isolation.** See D9 for the analysis and the accepted consequences.
- **`git_commit` records the commit immediately preceding the provenance commit.** The provenance file cannot contain the SHA of the commit that contains it, so it names its parent — the tree that was actually built and measured. The published directory is written on top of that tree, so the recorded SHA is exactly the code the numbers came from. This rule goes into the release checklist so subsequent releases repeat it.
- **`CHANGELOG.md` summarizes `1.0.0` per phase.** Three sections — Phase 1 (single-node), Phase 2 (distribution + advanced SQL), Phase 2.5 (SQL completeness + client compatibility) — each a short prose summary, with a link to `openspec/changes/archive/` for the change-by-change detail. Enumerating 26 archived changes in a first-release changelog is an index, not a summary.

## Open Questions

- **Does the DataFusion adapter need the same warmup treatment?** It runs in-process with no connection setup, so its first-run penalty is Parquet metadata and object-store client construction rather than a cold JVM. Whether 3 warmups is right for all three engines is worth confirming on the run, not assuming.
