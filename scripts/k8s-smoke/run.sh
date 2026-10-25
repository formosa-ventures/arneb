#!/usr/bin/env bash
# Chart acceptance test on kind: install charts/arneb (1 coordinator + 2
# workers) on TPC-H SF1 Parquet, check all 22 queries against DuckDB, then
# replace a worker pod and check the cluster recovers.
#
# Usage: scripts/k8s-smoke/run.sh DATA_DIR
#   DATA_DIR  output of gen_tpch.py (Parquet files + tpch.duckdb)
# Env:
#   IMAGE     local image to load into kind (default arneb:ci)
#   PYTHON    interpreter with duckdb + psycopg (default python3)
#   KEEP=1    leave the kind cluster running afterwards
set -euo pipefail

DATA_DIR=$(cd "$1" && pwd)
IMAGE=${IMAGE:-arneb:ci}
PYTHON=${PYTHON:-python3}
CLUSTER=arneb-smoke
RELEASE=arneb
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PG_PORT=15432
WEB_PORT=16432
LOGS=$(mktemp -d)
FORWARD_PID=

cleanup() {
    [[ -n "$FORWARD_PID" ]] && kill "$FORWARD_PID" 2>/dev/null || true
    if [[ "${KEEP:-0}" != "1" ]]; then
        kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

dump() {
    echo "--- pods"
    kubectl get pods -o wide || true
    for pod in $(kubectl get pods -o name 2>/dev/null); do
        echo "--- logs $pod"
        kubectl logs "$pod" --tail=80 || true
    done
}

# Port-forward the coordinator Service; restarted after pod changes.
forward() {
    [[ -n "$FORWARD_PID" ]] && kill "$FORWARD_PID" 2>/dev/null || true
    kubectl port-forward "svc/$RELEASE" "$PG_PORT:5432" "$WEB_PORT:6432" >"$LOGS/forward.log" 2>&1 &
    FORWARD_PID=$!
    for _ in $(seq 1 30); do
        curl -sf "localhost:$WEB_PORT/api/v1/info" >/dev/null && return 0
        sleep 1
    done
    echo "port-forward did not come up"; cat "$LOGS/forward.log"; return 1
}

# Active workers registered with the coordinator, one id per line.
active_workers() {
    curl -sf "localhost:$WEB_PORT/api/v1/cluster/workers" |
        "$PYTHON" -c 'import json,sys; [print(w["worker_id"]) for w in json.load(sys.stdin) if w["state"] == "active" and w["alive"]]'
}

wait_for_workers() {
    local want=$1 exclude=${2:-}
    for _ in $(seq 1 90); do
        local ids
        ids=$(active_workers || true)
        if [[ -n "$exclude" ]]; then ids=$(grep -vx "$exclude" <<<"$ids" || true); fi
        if [[ $(grep -c . <<<"$ids") -ge $want ]]; then
            echo "$ids"
            return 0
        fi
        sleep 2
    done
    echo "fewer than $want active workers"; return 1
}

echo "=== kind cluster"
KIND_DATA_DIR=$DATA_DIR envsubst <"$ROOT/scripts/k8s-smoke/kind-config.yaml" >"$LOGS/kind.yaml"
kind create cluster --name "$CLUSTER" --config "$LOGS/kind.yaml" --wait 120s
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "=== helm install"
repo=${IMAGE%:*}
tag=${IMAGE##*:}
if ! helm install "$RELEASE" "$ROOT/charts/arneb" -f "$ROOT/charts/arneb/ci/kind-values.yaml" \
    --set image.repository="$repo" --set image.tag="$tag" --wait --timeout 5m; then
    dump; exit 1
fi
secret_before=$(kubectl get secret "$RELEASE-cluster" -o jsonpath='{.data.secret}')
forward
echo "workers:"; wait_for_workers 2 || { dump; exit 1; }

echo "=== TPC-H SF1, 22 queries vs DuckDB"
PGPORT=$PG_PORT "$PYTHON" "$ROOT/scripts/k8s-smoke/tpch_check.py" "$DATA_DIR" || { dump; exit 1; }

echo "=== replace a worker pod"
# Delete it while queries run on it: the drain must let them finish.
victim=$(kubectl get pods -l app.kubernetes.io/component=worker -o jsonpath='{.items[0].metadata.name}')
PGPORT=$PG_PORT "$PYTHON" "$ROOT/scripts/k8s-smoke/tpch_check.py" "$DATA_DIR" q18 q21 q09 >"$LOGS/during.log" 2>&1 &
queries=$!
sleep 0.5
kubectl delete pod "$victim" --wait=false
if ! wait "$queries"; then
    echo "queries running while $victim drained failed:"; cat "$LOGS/during.log"; dump; exit 1
fi
cat "$LOGS/during.log"
kubectl wait --for=delete "pod/$victim" --timeout=120s
echo "workers after replacing $victim:"; wait_for_workers 2 "$victim" || { dump; exit 1; }
kubectl logs "deploy/$RELEASE-coordinator" >"$LOGS/coordinator.log"
grep "worker deregistered.*$victim" "$LOGS/coordinator.log" ||
    { echo "coordinator never logged $victim deregistering"; dump; exit 1; }
PGPORT=$PG_PORT "$PYTHON" "$ROOT/scripts/k8s-smoke/tpch_check.py" "$DATA_DIR" q01 q09 || { dump; exit 1; }

echo "=== helm upgrade keeps the cluster secret"
helm upgrade "$RELEASE" "$ROOT/charts/arneb" -f "$ROOT/charts/arneb/ci/kind-values.yaml" \
    --set image.repository="$repo" --set image.tag="$tag" --wait --timeout 5m
secret_after=$(kubectl get secret "$RELEASE-cluster" -o jsonpath='{.data.secret}')
[[ "$secret_before" == "$secret_after" ]] || { echo "cluster secret changed on upgrade"; exit 1; }

echo "=== smoke test passed"
