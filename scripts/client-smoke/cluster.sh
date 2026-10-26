#!/usr/bin/env bash
# Start a local Arneb cluster (1 coordinator + 2 workers) on the TPC-H
# Parquet files gen_tpch.py wrote, and wait until both workers are active.
#
# Usage: scripts/client-smoke/cluster.sh start DATA_DIR RUN_DIR
#        scripts/client-smoke/cluster.sh stop RUN_DIR
#   DATA_DIR  output of scripts/k8s-smoke/gen_tpch.py
#   RUN_DIR   configs, logs (RUN_DIR/*.log) and pid files go here
# Env:
#   ARNEB     the binary (default target/release/arneb)
#   PG_PORT   pgwire port (default 15432; the Web UI is PG_PORT + 1000)
#   TRINO_PORT  Trino protocol port (default 18080)
# Other ARNEB_* variables (e.g. ARNEB_MAX_RUNNING_QUERIES) pass through.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
ARNEB=${ARNEB:-$ROOT/target/release/arneb}
PG_PORT=${PG_PORT:-15432}
TRINO_PORT=${TRINO_PORT:-18080}
WEB_PORT=$((PG_PORT + 1000))
COORD_RPC=19090

tables() { # $1 = data dir
    for t in lineitem orders customer part partsupp supplier nation region; do
        printf '[[tables]]\nname = "%s"\npath = "%s/%s.parquet"\nformat = "parquet"\n\n' "$t" "$1" "$t"
    done
}

start() {
    local data run
    data=$(cd "$1" && pwd)
    run=$2
    mkdir -p "$run"
    run=$(cd "$run" && pwd)
    {
        printf 'bind_address = "127.0.0.1"\nport = %s\n\n[trino]\nport = %s\n\n' "$PG_PORT" "$TRINO_PORT"
        printf '[cluster]\nrole = "coordinator"\nrpc_port = %s\n\n' "$COORD_RPC"
        tables "$data"
    } >"$run/coordinator.toml"
    for n in 1 2; do
        {
            printf 'bind_address = "127.0.0.1"\n\n[cluster]\nrole = "worker"\nrpc_port = %s\n' "$((COORD_RPC + n))"
            printf 'coordinator_address = "127.0.0.1:%s"\nworker_id = "worker-%s"\n\n' "$COORD_RPC" "$n"
            tables "$data"
        } >"$run/worker-$n.toml"
    done

    "$ARNEB" --config "$run/coordinator.toml" --role coordinator >"$run/coordinator.log" 2>&1 &
    echo $! >"$run/coordinator.pid"
    sleep 1
    for n in 1 2; do
        "$ARNEB" --config "$run/worker-$n.toml" --role worker >"$run/worker-$n.log" 2>&1 &
        echo $! >"$run/worker-$n.pid"
    done

    for _ in $(seq 1 60); do
        local active
        active=$(curl -sf "127.0.0.1:$WEB_PORT/api/v1/cluster/workers" |
            python3 -c 'import json,sys; print(sum(1 for w in json.load(sys.stdin) if w["state"] == "active" and w["alive"]))' \
            2>/dev/null || echo 0)
        if [[ $active -ge 2 ]]; then
            echo "cluster up: pgwire 127.0.0.1:$PG_PORT, trino 127.0.0.1:$TRINO_PORT, web 127.0.0.1:$WEB_PORT"
            return 0
        fi
        sleep 1
    done
    echo "workers did not register; logs in $run" >&2
    tail -20 "$run"/*.log >&2
    stop "$run"
    return 1
}

# SIGTERM every node, wait for it to exit (a worker drains first), and
# SIGKILL what is still running after 30 s, so the ports are free again.
stop() {
    local pids=() pid
    for f in "$1"/*.pid; do
        [[ -e $f ]] || continue
        pid=$(cat "$f")
        kill "$pid" 2>/dev/null && pids+=("$pid")
        rm -f "$f"
    done
    [[ ${#pids[@]} -gt 0 ]] || return 0
    for _ in $(seq 1 30); do
        local alive=0
        for pid in "${pids[@]}"; do kill -0 "$pid" 2>/dev/null && alive=1; done
        [[ $alive == 0 ]] && return 0
        sleep 1
    done
    kill -9 "${pids[@]}" 2>/dev/null || true
}

case ${1:-} in
start) start "$2" "$3" ;;
stop) stop "$2" ;;
*) sed -n '2,13p' "$0"; exit 2 ;;
esac
