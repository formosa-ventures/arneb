## ADDED Requirements

### Requirement: Tutorial document exists at a discoverable path

The harness SHALL include a tutorial document at `benchmarks/tpch/TUTORIAL.md`, and the project's root `README.md` MUST include a link to it under a section that mentions benchmarking or comparison.

#### Scenario: A new contributor finds the tutorial from the README

- **WHEN** a contributor reads the project's root `README.md`
- **THEN** they encounter a link whose anchor text mentions a TPC-H comparison or tutorial and which resolves to `benchmarks/tpch/TUTORIAL.md`

### Requirement: Tutorial covers the full reproduction path

The tutorial SHALL contain four sections, in this order:

1. **Prerequisites** — listing the software a reader must install (Docker / Docker Compose, Rust toolchain at the workspace's pinned version) and the approximate disk space needed for SF1.
2. **Reproduce the comparison** — copy-pasteable shell commands, in order, that take the reader from a clean checkout to all three engines having produced a result JSON. Commands MUST reference real files in the repository (`docker compose up -d`, `docker compose run --rm tpch-seed`, the `tpch-hive.toml` config path, the `tpch-bench` invocation with `--engines arneb,trino,datafusion`).
3. **Read the report** — an annotated example of the Markdown report the previous step produces, explaining the per-query table, the geomean line, the divergence section, and the statistics caveat header.
4. **Going further** — pointers to: scaling up via `TPCH_SF=sf10`, narrowing to a single query via `--queries`, swapping in a remote Trino, and rerunning correctness only.

#### Scenario: Reproduce section commands match the script

- **WHEN** a reader copies every command from the "Reproduce the comparison" section in order onto a fresh machine that meets the prerequisites
- **THEN** the same end-state is reached as running `./benchmarks/tpch/scripts/run_benchmark.sh` with default options

#### Scenario: Annotated report example reflects the real schema

- **WHEN** the tutorial's "Read the report" section is compared against the output `tpch-bench report` actually emits
- **THEN** the column names, the speedup formatting, and the divergence section heading match exactly (no drift between tutorial and runner)

### Requirement: Tutorial calls out MinIO endpoint pitfalls

The tutorial SHALL include a callout explaining that the harness reads MinIO at `127.0.0.1:9000` from the host machine and that users must not export real AWS credentials in the same shell session, because DataFusion's S3 client would otherwise pick them up via `AmazonS3Builder::from_env()` and silently bypass MinIO.

#### Scenario: User has real AWS credentials exported

- **WHEN** a reader has `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` exported pointing at real AWS
- **THEN** the tutorial's MinIO callout warns them, in plain language, that they need to either `unset` the variables or run the harness in a fresh shell, and explains why
