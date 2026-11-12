#!/usr/bin/env bash
# Headless acceptance scenario on kind: install charts/arneb (1 coordinator +
# 2 workers) with password auth, a hive catalog over TPC-H and OAuth, then
# run scripts/headless-acceptance.sh against it through a port-forward. The
# coordinator restart in the scenario is a `helm upgrade`; the worker restart
# deletes a worker pod.
#
# The services the cluster reads (Hive Metastore, S3, PostgreSQL, Keycloak)
# run outside kind, e.g. from docker-compose.yml. The pods reach them at the
# addresses below. A Keycloak token's issuer must be a loopback URL, so the
# coordinator pod gets a socat sidecar that serves KEYCLOAK_ADDR on
# 127.0.0.1:8180, the address this host uses too.
#
# Usage: scripts/k8s-smoke/acceptance.sh
# Env:
#   IMAGE            local image to load into kind (default arneb:ci)
#   HMS_ADDR         Hive Metastore host:port, from the pods (default hive-metastore:9083)
#   S3_ENDPOINT      S3 endpoint, from the pods (default http://s3:9000)
#   PG_ADDR          PostgreSQL host:port, from the pods (default postgres:5432)
#   PG_HOST_ADDR     the same, from this host (default 127.0.0.1:5433)
#   KEYCLOAK_ADDR    Keycloak host:port, from the pods (default keycloak:8180)
#   KIND_CONNECT     space-separated container=alias pairs to attach to the
#                    kind network, so the defaults above resolve (CI)
#   SOCAT_IMAGE      the sidecar image (default alpine/socat:1.8.0.3)
#   ARNEB_EXPECTED_LINEITEM  count(*) of lineitem (default 6001215, SF1)
#   ARNEB_LONG_SQL   the long statement (default: a lineitem self-join on
#                    l_suppkey, which runs for minutes at SF1)
#   KEEP=1           leave the kind cluster running afterwards
#
# Needs kind, kubectl, helm, docker, curl, jq, openssl and psql. The
# passwords are random per run and never printed.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SELF="$ROOT/scripts/k8s-smoke/acceptance.sh"
CLUSTER=arneb-acceptance
RELEASE=arneb
PG_LOCAL=6432
WEB_LOCAL=7432

# The scenario restarts nodes through this script; ACC_DIR carries the state.
restart_coordinator() {
    helm upgrade "$RELEASE" "$ROOT/charts/arneb" --reuse-values \
        --set-string "coordinator.podAnnotations.restartedAt=$(date +%s)" --wait --timeout 5m
    forward
}

restart_worker() {
    local victim
    victim=$(kubectl get pods -l app.kubernetes.io/component=worker \
        -o jsonpath='{.items[?(@.metadata.name!="'"$ARNEB_WORKER1"'")].metadata.name}' | cut -d' ' -f1)
    echo "deleting $victim"
    kubectl delete pod "$victim" --wait=true --timeout=120s
    kubectl rollout status "deploy/$RELEASE-worker" --timeout=120s
}

# Port-forward the coordinator Service until its health check answers.
forward() {
    if [[ -f "$ACC_DIR/forward.pid" ]]; then
        kill "$(cat "$ACC_DIR/forward.pid")" 2>/dev/null || true
    fi
    nohup kubectl port-forward "svc/$RELEASE" "$PG_LOCAL:5432" "$WEB_LOCAL:6432" \
        >>"$ACC_DIR/forward.log" 2>&1 &
    echo $! >"$ACC_DIR/forward.pid"
    for _ in $(seq 1 60); do
        curl -sf "http://127.0.0.1:$WEB_LOCAL/api/v2/health" >/dev/null && return 0
        sleep 1
    done
    echo "port-forward did not come up"; tail -20 "$ACC_DIR/forward.log"; return 1
}

case "${1:-}" in
    restart-coordinator) restart_coordinator; exit ;;
    restart-worker) restart_worker; exit ;;
    "") ;;
    *) echo "usage: $0" >&2; exit 2 ;;
esac

IMAGE=${IMAGE:-arneb:ci}
HMS_ADDR=${HMS_ADDR:-hive-metastore:9083}
S3_ENDPOINT=${S3_ENDPOINT:-http://s3:9000}
PG_ADDR=${PG_ADDR:-postgres:5432}
KEYCLOAK_ADDR=${KEYCLOAK_ADDR:-keycloak:8180}
SOCAT_IMAGE=${SOCAT_IMAGE:-alpine/socat:1.8.0.3}
ACC_DIR=$(mktemp -d)
export ACC_DIR

cleanup() {
    [[ -f "$ACC_DIR/forward.pid" ]] && kill "$(cat "$ACC_DIR/forward.pid")" 2>/dev/null || true
    if [[ "${KEEP:-0}" != "1" ]]; then
        kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    fi
    rm -rf "$ACC_DIR"
}
trap cleanup EXIT

dump() {
    echo "--- pods"
    kubectl get pods -o wide || true
    for pod in $(kubectl get pods -o name 2>/dev/null); do
        echo "--- logs $pod"
        kubectl logs "$pod" --all-containers --tail=80 || true
    done
}

echo "=== kind cluster"
kind create cluster --name "$CLUSTER" --wait 120s
kind load docker-image "$IMAGE" --name "$CLUSTER"
for pair in ${KIND_CONNECT:-}; do
    docker network connect --alias "${pair#*=}" kind "${pair%%=*}"
done

echo "=== config"
umask 077
ADMIN_PW=$(openssl rand -hex 16)
USER2_PW=$(openssl rand -hex 16)
hash_pw() { printf '%s' "$1" | docker run --rm -i "$IMAGE" hash-password; }
cat >"$ACC_DIR/config.toml" <<EOF
[auth]
type = "password"

[[auth.users]]
name = "alice"
password_hash = "$(hash_pw "$ADMIN_PW")"

[[auth.users]]
name = "dave"
password_hash = "$(hash_pw "$USER2_PW")"

[access_control]
rules_file = "/etc/arneb/rules.json"
admin_users = ["alice"]

[oauth]
issuer = "http://127.0.0.1:8180/realms/arneb"
resource = "http://127.0.0.1:$WEB_LOCAL/mcp"
user_claim = "preferred_username"
# The coordinator can start before its Keycloak sidecar listens; its failed
# startup fetch then blocks a retry for jwks_min_refresh_secs (default 30),
# and the scenario reaches OAuth sooner than that after the restart.
jwks_min_refresh_secs = 1

[storage.s3]
region = "us-east-1"
endpoint = "$S3_ENDPOINT"
allow_http = true
access_key_id = "s3admin"
secret_access_key = "s3adminsecret"

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "$HMS_ADDR"
default_schema = "tpch"
EOF
# dave reads only; everyone else may do everything.
cat >"$ACC_DIR/rules.json" <<'EOF'
{"catalogs":[{"allow":"all"}],
 "tables":[{"user":"dave","privileges":["SELECT"]},
           {"privileges":["SELECT","INSERT","DELETE","UPDATE","OWNERSHIP"]}]}
EOF
kubectl create secret generic "$RELEASE-acceptance-config" \
    --from-file=config.toml="$ACC_DIR/config.toml" --from-file=rules.json="$ACC_DIR/rules.json"
cat >"$ACC_DIR/values.yaml" <<EOF
image:
  pullPolicy: Never
existingConfigSecret: $RELEASE-acceptance-config
auth:
  password: true
worker:
  replicas: 2
  drainTimeoutSecs: 30
coordinator:
  # Password auth needs TLS on the Trino listener; the scenario does not use it.
  trino:
    enabled: false
  extraContainers:
    - name: keycloak-proxy
      image: $SOCAT_IMAGE
      args: ["TCP-LISTEN:8180,bind=127.0.0.1,fork,reuseaddr", "TCP:$KEYCLOAK_ADDR"]
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
EOF

echo "=== helm install"
if ! helm install "$RELEASE" "$ROOT/charts/arneb" -f "$ACC_DIR/values.yaml" \
    --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE##*:}" --wait --timeout 5m; then
    dump; exit 1
fi
forward
read -r -a WORKERS <<<"$(kubectl get pods -l app.kubernetes.io/component=worker \
    -o jsonpath='{.items[*].metadata.name}')"
[[ ${#WORKERS[@]} == 2 ]] || { echo "want 2 worker pods, have ${#WORKERS[@]}"; dump; exit 1; }

echo "=== scenario"
export ARNEB_URL="http://127.0.0.1:$WEB_LOCAL" ARNEB_PG="127.0.0.1:$PG_LOCAL"
export ARNEB_ADMIN_USER=alice ARNEB_ADMIN_PASSWORD="$ADMIN_PW"
export ARNEB_USER2=dave ARNEB_PASSWORD2="$USER2_PW"
export ARNEB_WORKER1="${WORKERS[0]}" ARNEB_WORKER2="${WORKERS[1]}"
export PG_URL="postgresql://arneb:arneb@$PG_ADDR/arneb"
export PG_URL_HOST="postgresql://arneb:arneb@${PG_HOST_ADDR:-127.0.0.1:5433}/arneb"
export ARNEB_RESTART_COORDINATOR="$SELF restart-coordinator"
export ARNEB_RESTART_WORKER="$SELF restart-worker"
export KEYCLOAK_ISSUER=http://127.0.0.1:8180/realms/arneb
export ARNEB_EXPECTED_LINEITEM="${ARNEB_EXPECTED_LINEITEM:-6001215}"
export ARNEB_LONG_SQL="${ARNEB_LONG_SQL:-SELECT count(*) FROM lineitem a JOIN lineitem b ON a.l_suppkey = b.l_suppkey}"
"$ROOT/scripts/headless-acceptance.sh" || { dump; exit 1; }

echo "=== no token or JWT in the pod logs"
kubectl logs "deploy/$RELEASE-coordinator" -c coordinator >"$ACC_DIR/coordinator.log"
for pod in $(kubectl get pods -l app.kubernetes.io/component=worker -o name); do
    kubectl logs "$pod" >>"$ACC_DIR/coordinator.log"
done
if grep -nE 'arneb_[0-9a-f]{16}_[A-Za-z0-9_-]{8,}|eyJ[A-Za-z0-9_-]{10,}\.eyJ' "$ACC_DIR/coordinator.log" |
    sed -E 's/(arneb_[0-9a-f]{16}_)[A-Za-z0-9_-]+/\1<redacted>/g; s/eyJ[A-Za-z0-9_.-]+/eyJ…<redacted>/g'; then
    echo "a token or JWT is in the pod logs"; exit 1
fi
for secret in "$ADMIN_PW" "$USER2_PW"; do
    if grep -qF -- "$secret" "$ACC_DIR/coordinator.log"; then echo "a password is in the pod logs"; exit 1; fi
done

echo "=== headless acceptance on kind passed"
