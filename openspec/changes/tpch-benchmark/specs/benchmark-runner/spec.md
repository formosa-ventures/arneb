## ADDED Requirements

### Requirement: CLI interface
The benchmark runner SHALL accept command-line arguments: `--host` (default 127.0.0.1), `--port` (default 5432), `--scale-factor` (default 1), `--num-runs` (default 5), `--warm-up` (default 2), `--output-dir` (default ./results), `--query` (optional, run a single query by number).

#### Scenario: Default options
- **WHEN** the runner is invoked with no arguments
- **THEN** it connects to 127.0.0.1:5432, runs all 22 queries 5 times each with 2 warm-up runs

#### Scenario: Single query run
- **WHEN** the runner is invoked with `--query 6`
- **THEN** it runs only Q6 for the configured number of runs

#### Scenario: Custom connection
- **WHEN** the runner is invoked with `--host 10.0.0.1 --port 5433`
- **THEN** it connects to 10.0.0.1:5433

### Requirement: Query execution via tokio-postgres
The runner SHALL connect to the server using `tokio-postgres` and execute queries via the PostgreSQL wire protocol. Each query SHALL be executed as a simple query (no prepared statements) to match typical usage.

#### Scenario: Successful query execution
- **WHEN** the runner executes Q1 against a running server with SF1 data loaded
- **THEN** it receives a result set and records the timing and row count

#### Scenario: Connection failure
- **WHEN** the runner cannot connect to the specified host:port
- **THEN** it prints an error message and exits with non-zero code

### Requirement: JSON output format
The runner SHALL write results to a JSON file at `{output_dir}/trino_alt_sf{N}.json`. The JSON SHALL contain: `engine` ("trino-alt"), `scale_factor`, `timestamp` (ISO 8601), `queries` (array of per-query results with `query_id`, `status` (ok/skipped/error), `runs` array, and `summary` statistics).

#### Scenario: JSON output structure
- **WHEN** the runner completes all queries
- **THEN** the output file contains a JSON object with engine metadata and per-query results

#### Scenario: Failed query in output
- **WHEN** query Q15 fails with an error
- **THEN** the output records `"status": "error"` and `"error": "<error message>"` for Q15
- **AND** the runner continues with Q16

### Requirement: Graceful error handling
The runner SHALL continue executing remaining queries when a single query fails. Failed queries SHALL be recorded with their error message. The runner SHALL exit with code 0 if at least one query succeeded, or code 1 if all queries failed.

#### Scenario: Partial success
- **WHEN** 18 queries succeed and 4 fail
- **THEN** the runner exits with code 0 and the output includes results for all 22 queries (18 ok, 4 error)
