## MODIFIED Requirements

### Requirement: Tutorial calls out MinIO endpoint pitfalls

The tutorial SHALL include a callout covering how the harness reaches MinIO in each of its two run modes, because those modes use different addresses and mixing them produces a connection failure with no obvious cause. The two modes are the fully containerized run, which is how official release numbers are produced, and a native run from the host, which is convenient for local iteration.

The callout MUST:
- states which MinIO endpoint applies to each mode: the `minio` service name on the compose network for the containerized run, and `127.0.0.1:9000` for a native run from the host,
- warns that a reader running the harness natively must not have real AWS credentials exported in the same shell session, because DataFusion's S3 client picks them up via `AmazonS3Builder::from_env()` and silently bypasses MinIO,
- notes that the containerized run is not exposed to that failure mode, because a container sees only the environment explicitly passed to it.

#### Scenario: User has real AWS credentials exported

- **WHEN** a reader runs the harness natively from the host with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` exported pointing at real AWS
- **THEN** the tutorial's MinIO callout warns them, in plain language, that they need to either `unset` the variables or run the harness in a fresh shell, and explains why

#### Scenario: Reader picks the wrong endpoint for their mode

- **WHEN** a reader consults the tutorial to determine which MinIO endpoint to configure
- **THEN** the callout tells them the service-name endpoint applies to the containerized run and `127.0.0.1:9000` applies to a native run from the host

### Requirement: Tutorial covers the full reproduction path

The tutorial SHALL contain four sections, in this order:

1. **Prerequisites** — listing the software a reader must install (Docker / Docker Compose, and a Rust toolchain at the workspace's pinned version if they intend to run the harness natively) and the approximate disk space needed for SF1.
2. **Reproduce the comparison** — copy-pasteable shell commands, in order, that take the reader from a clean checkout to all three engines having produced a result JSON. The documented path MUST be the fully containerized one, matching how official release numbers are produced: every engine and the benchmark runner itself execute inside the container stack. Commands MUST reference real files in the repository, and MUST NOT instruct the reader to start any engine or the runner as a native host process for the primary path. A native invocation MAY be documented as a secondary convenience for local iteration, clearly marked as not the path official numbers come from.
3. **Read the report** — an annotated example of the Markdown report the previous step produces, explaining the per-query table, the geomean line, the divergence section, and the statistics caveat header.
4. **Going further** — pointers to: scaling up via `TPCH_SF=sf10`, narrowing to a single query via `--queries`, swapping in a remote Trino, and rerunning correctness only.

#### Scenario: Reproduce section commands match the script

- **WHEN** a reader copies every command from the "Reproduce the comparison" section in order onto a fresh machine that meets the prerequisites
- **THEN** the same end-state is reached as running `./benchmarks/tpch/scripts/run_benchmark.sh` with default options

#### Scenario: Annotated report example reflects the real schema

- **WHEN** the tutorial's "Read the report" section is compared against the output `tpch-bench report` actually emits
- **THEN** the column names, the speedup formatting, and the divergence section heading match exactly (no drift between tutorial and runner)

#### Scenario: Primary path runs entirely in containers

- **WHEN** a reader follows the "Reproduce the comparison" section as written
- **THEN** every engine and the benchmark runner execute inside the container stack, and no step instructs them to launch a native host process
