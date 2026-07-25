# Publishing official benchmark numbers for a release

Rules for producing the figures a release quotes in the README and on the
documentation site. They exist so that a reader can tell what was measured, and
so the next release does not have to rediscover them.

## Where results go

`benchmarks/tpch/official/v<version>/<scale-factor>/`, tracked in git — one directory per scale factor, since a release may publish more than one and their figures must not be conflated. Do **not** publish from
`benchmarks/tpch/results/` — that path is gitignored scratch space for local
runs. Each release gets its own directory; never overwrite a previous release's,
because claims made by that release must stay verifiable after new numbers land.

Contents:

| File | What it is |
|---|---|
| `provenance.json` | Machine-readable record of the run (below), including its scale factor. Source of truth. |
| `comparison.md` | Verbatim `tpch-bench report` output. Not hand-edited. |
| `arneb.json` | Verbatim runner output, renamed from `arneb_<timestamp>.json`. |
| `trino.json` | Likewise. |
| `datafusion.json` | Likewise. |

The runner's timestamp is already a field inside each document, so the filename
does not need to carry it — and stable names make `git diff` between release
directories readable.

## How the run is configured

- **Everything in containers, including the runner.** The DataFusion adapter
  executes in-process inside the runner binary, so a native runner means a
  native DataFusion competing against containerized rivals. Use
  `docker/tpch-bench/docker-compose.official.yml`.
- **Arneb in its default configuration.** No `ARNEB_*` tuning options. Do not
  reuse `docker/arneb-bench/docker-compose.bench.yml` for a published run — it
  sets 16 options that are off by default in the engine, so its numbers are not
  what a stock build produces.
- **Equal resources.** Each engine gets a coordinator plus two workers at
  `BENCH_NODE_CPUS`; the runner gets `BENCH_RUNNER_CPUS` (three times
  `BENCH_NODE_CPUS`) because it hosts DataFusion as a single process. Keep these
  in step.
- **One engine at a time.** Only the engine being measured runs; the others are
  stopped, not left idle. An idle engine still holds its heap — three Trino JVMs
  declare `-Xmx8G` each — so an all-up arrangement measures every engine under
  memory pressure from its rivals, and at SF10 the combined ceiling exceeds what
  a 16 GB container runtime has. Use `--no-deps` on the runner: its `depends_on`
  names every engine and will otherwise restart the one you just stopped.
- **Same arrangement at every scale factor.** Figures from different scale
  factors are only comparable if they were produced the same way; do not compare
  an all-up run against a rotated one.

Verify the *running* containers rather than the compose file:

```bash
docker inspect arneb-arneb-1 --format '{{json .Config.Env}}' | tr ',' '\n' | grep ARNEB_ || echo "no tuning vars — good"
```

## What `provenance.json` must record

- `arneb_version`, `git_commit`, `rustc_version`
- `trino_version`, `datafusion_version`, `minio_version` — **resolved from what
  actually ran** (`docker inspect` image digest, or the engine's self-reported
  version). Never transcribed from a compose file: the stack floats on `:latest`
  tags, so a tag name records nothing.
- `scale_factor`, `queries_total`, `warm_up_runs`, `measurement_runs`
- `arneb_env` — the complete set of `ARNEB_*` variables in effect (an empty
  object for a default-configuration run). Configuration must never be implicit.
- `resources` — CPU and memory allocation per engine, including the runner
  container that hosts DataFusion
- `host` — CPU model, physical core count, RAM, OS and version, architecture,
  storage type
- `run_date` — RFC 3339

### The `git_commit` rule

The provenance file is committed, so it cannot contain the SHA of the commit
that contains it. **Record the SHA of the commit immediately preceding the
provenance commit** — that is the tree that was actually built and measured:

```bash
git rev-parse HEAD   # run BEFORE committing the official/ directory
```

## Publishing the numbers

- Publish what happened. Failures stay failures, skips keep their recorded
  reasons, hash divergences appear in the report's divergence section. If a run
  is unusable, discard the whole run and rerun — never patch individual query
  results.
- Every figure in the README or on the docs homepage must appear in that
  release's `comparison.md`, and must be shown with its run date and a reference
  to the published directory.
- Lead with a suite-level aggregate (geomean over queries both engines
  completed), not a best-case single query.
- State the configuration next to the figures. A reader should not have to open
  `provenance.json` to learn which Arneb was measured.
