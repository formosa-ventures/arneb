#!/usr/bin/env bash
# Scenario 1 acceptance on a local cluster (G6, Tier 2; CI job "Cluster
# smoke"): 1 coordinator + 2 workers on TPC-H Parquet, then
#   1. the 22 TPC-H queries match DuckDB (scripts/k8s-smoke/tpch_check.py)
#   2. ROUNDS rounds of 20 concurrent queries with the admission cap lifted,
#      zero failures (benchmarks/tpch/scripts/concurrency_soak.sh)
#   3. client checks: psycopg2, psycopg 3, the Trino client, SQLAlchemy's
#      Trino dialect (clients.py)
#   4. the dbt project in dbt/: dbt-trino debug, test, show and
#      docs generate; dbt-postgres debug
#
# Usage: scripts/client-smoke/run.sh DATA_DIR
#   DATA_DIR  output of scripts/k8s-smoke/gen_tpch.py
# Env:
#   PYTHON    interpreter with requirements.txt installed (default python3);
#             dbt is taken from the same environment
#   ROUNDS    soak rounds (default 3)
#   RUN_DIR   configs and node logs (default: a temp dir, printed)
#   ARNEB     the binary (default target/release/arneb)
# psql on PATH for the soak.
set -euo pipefail

DATA_DIR=$(cd "$1" && pwd)
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
HERE=$ROOT/scripts/client-smoke
PYTHON=${PYTHON:-python3}
DBT=$(dirname "$(command -v "$PYTHON")")/dbt
ROUNDS=${ROUNDS:-3}
RUN_DIR=${RUN_DIR:-$(mktemp -d)}
export PGHOST=127.0.0.1 PGPORT=${PG_PORT:-15432} PGUSER=smoke PGDATABASE=arneb
export TRINO_HOST=127.0.0.1 TRINO_PORT=${TRINO_PORT:-18080} PG_PORT=$PGPORT
FAILED=()

cleanup() {
    "$HERE/cluster.sh" stop "$RUN_DIR" || true
    if [[ ${#FAILED[@]} -gt 0 ]]; then
        for f in "$RUN_DIR"/*.log; do
            echo "--- $(basename "$f") (last 40 lines)"
            tail -40 "$f"
        done
    fi
}
trap cleanup EXIT

step() { # $1 = name, rest = command
    local name=$1
    shift
    echo "=== $name"
    if "$@"; then echo "--- $name: ok"; else echo "--- $name: FAILED"; FAILED+=("$name"); fi
}

echo "node logs in $RUN_DIR"
ARNEB_MAX_RUNNING_QUERIES=0 "$HERE/cluster.sh" start "$DATA_DIR" "$RUN_DIR"

step "TPC-H 22 vs DuckDB" "$PYTHON" "$ROOT/scripts/k8s-smoke/tpch_check.py" "$DATA_DIR"
step "concurrency soak ($ROUNDS x 20)" env LOG_DIR="$RUN_DIR" \
    "$ROOT/benchmarks/tpch/scripts/concurrency_soak.sh" "$ROUNDS"
step "client checks" "$PYTHON" "$HERE/clients.py"

dbt_run() { # $1 = target, rest = dbt arguments
    local target=$1
    shift
    (cd "$HERE/dbt" && DBT_TARGET_PATH="$RUN_DIR/dbt-target" DBT_LOG_PATH="$RUN_DIR/dbt-logs" \
        "$DBT" "$@" --profiles-dir . --target "$target")
}
# `docs generate`, then check the catalog it wrote has no errors and holds
# the sources' columns.
dbt_docs() {
    dbt_run trino docs generate &&
        "$PYTHON" - "$RUN_DIR/dbt-target/catalog.json" <<'PY'
import json, sys
catalog = json.load(open(sys.argv[1]))
assert not catalog.get("errors"), catalog["errors"]
columns = catalog["sources"]["source.arneb_smoke.tpch.nation"]["columns"]
assert list(columns) == ["n_nationkey", "n_name", "n_regionkey", "n_comment"], columns
print(f"catalog: {len(catalog['sources'])} sources")
PY
}
step "dbt-trino debug" dbt_run trino debug
step "dbt-trino test" dbt_run trino test
step "dbt-trino show" dbt_run trino show --select nations_per_region --limit 10
step "dbt-trino docs generate" dbt_docs
step "dbt-postgres debug" dbt_run postgres debug

if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo "FAILED: ${FAILED[*]}"
    exit 1
fi
echo "ALL STEPS PASSED"
