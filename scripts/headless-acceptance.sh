#!/usr/bin/env bash
# Headless acceptance scenario (docs/superpowers/specs/
# 2026-10-30-headless-agent-interface-design.md, "Acceptance"), as a script.
#
# Runs the per-change smoke tests in the scenario's order against one live
# distributed stack, restarts the coordinator and one worker in between with
# the commands it is given (outside the API, as an operator would), and adds
# the checks that span the smokes: slow queries, the whole audit log after the
# restart, and the identity on every manage action. Prints `PASS <step>` per
# check (the smokes print their own) and exits non-zero at the first failure.
#
# Scenario step -> where it is checked:
#   1-3  discovery, sample, EXPLAIN, truncation, hint  agent-api-smoke 1-7, mcp-smoke 3-6
#   4    long query: poll and cancel                  agent-api-smoke 8, mcp-smoke 7
#   5    cluster, history, slow queries               mcp-smoke 8, A1 below (MCP)
#   6    cordon, steer, drain, uncordon               manage-smoke 4-5
#   7    new catalog on both workers, rule change     runtime-config-smoke 1-3
#   8    user, token, API + pgwire, revoke            runtime-config-smoke 4, mcp-smoke 10
#   9    coordinator restart keeps the changes        manage-smoke 10, runtime-config-smoke 8-9, A2
#   10   every manage action audited with identity    manage-smoke 8, runtime-config-smoke 7, A2
#   negative: no manage, no write                     manage-smoke 2 and 7, runtime-config-smoke 5
#   OAuth: steps 1-2 over an OAuth access token       mcp-oauth-smoke (when KEYCLOAK_ISSUER is set)
#
# The stack: a coordinator and two workers (ids worker-1 and worker-2) with
# password auth, a hive catalog with the TPC-H tables, `admin_users` naming
# the admin user, a persistent `[state] path`, the same `[cluster] secret` on
# every node, and a rules file whose `tables` section grants user2 SELECT
# only (and every other user everything). For the OAuth part, also the
# `[oauth]` section and Keycloak realm that scripts/mcp-oauth-smoke.sh
# describes; the admin user must be `alice`, and user2 must not be a realm
# user (`bob`, `carol`): the OAuth smoke needs bob to have no Arneb user.
#
# Environment:
#   ARNEB_URL             Web port base URL, e.g. http://127.0.0.1:7432 (required)
#   ARNEB_PG              host:port of the pgwire port (required)
#   ARNEB_ADMIN_USER / ARNEB_ADMIN_PASSWORD   a user in admin_users (required)
#   ARNEB_USER2 / ARNEB_PASSWORD2             a user without manage, SELECT only (required)
#   PG_URL                postgresql://... the nodes can reach, for the new catalog (required)
#   ARNEB_RESTART_COORDINATOR   shell command that restarts the coordinator
#                         only and returns once it serves again (required)
#   ARNEB_RESTART_WORKER  shell command that restarts one worker and returns
#                         once it is running again (required)
#   KEYCLOAK_ISSUER       when set, run scripts/mcp-oauth-smoke.sh at the end
#   ARNEB_CATALOG / ARNEB_SCHEMA / ARNEB_LONG_SQL / ARNEB_SMOKE_TIMEOUT_SECS /
#   ARNEB_EXPECTED_LINEITEM / ARNEB_WORKER_LOGS / ARNEB_COORD_LOG /
#   ARNEB_AUDIT_FILE / ARNEB_PG_SCHEMA / ARNEB_PG_TABLE   passed to the smokes
#
# Passwords are passed to the smokes through the environment only.
# scripts/manage-smoke.sh puts them on its curl command lines: run this only
# on a host you trust, with throwaway passwords.
#
# Usage: scripts/headless-acceptance.sh

set -euo pipefail

: "${ARNEB_URL:?set ARNEB_URL to the Web port, e.g. http://127.0.0.1:7432}"
: "${ARNEB_PG:?set ARNEB_PG to host:port of the pgwire port}"
: "${ARNEB_ADMIN_USER:?set ARNEB_ADMIN_USER to a user in admin_users}"
: "${ARNEB_ADMIN_PASSWORD:?set ARNEB_ADMIN_PASSWORD}"
: "${ARNEB_USER2:?set ARNEB_USER2 to a user without manage}"
: "${ARNEB_PASSWORD2:?set ARNEB_PASSWORD2}"
: "${PG_URL:?set PG_URL to postgresql://user:password@host:port/db}"
: "${ARNEB_RESTART_COORDINATOR:?set ARNEB_RESTART_COORDINATOR to a command that restarts the coordinator}"
: "${ARNEB_RESTART_WORKER:?set ARNEB_RESTART_WORKER to a command that restarts one worker}"

if [ -n "${KEYCLOAK_ISSUER:-}" ]; then
    # mcp-oauth-smoke.sh needs the realm users alice (the admin) and an
    # Arneb user list without bob.
    [ "$ARNEB_ADMIN_USER" = alice ] || { echo "FAIL setup: with KEYCLOAK_ISSUER, ARNEB_ADMIN_USER must be alice"; exit 1; }
    case "$ARNEB_USER2" in bob | carol) echo "FAIL setup: with KEYCLOAK_ISSUER, ARNEB_USER2 must not be bob or carol (realm users)"; exit 1 ;; esac
fi
HERE="$(cd "$(dirname "$0")" && pwd)"
API="${ARNEB_URL%/}/api/v2"
ADMIN="$ARNEB_ADMIN_USER"
USER2="$ARNEB_USER2"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
export ARNEB_SMOKE_STATE="$WORK/runtime-config-smoke.json"

STEP=""
LAST=""
CODE=""
pass() { echo "PASS $STEP"; }
fail() {
    echo "FAIL $STEP: $*"
    [ -n "$LAST" ] && echo "  last response (HTTP ${CODE:-?}): $(printf '%s' "$LAST" | head -c 2000)"
    exit 1
}
check() { # $1 = jq filter that must be true on $LAST, $2 = message
    printf '%s' "$LAST" | jq -e "$1" >/dev/null 2>&1 || fail "$2"
}
cfgq() {
    local v="${1//\\/\\\\}"
    v="${v//\"/\\\"}"
    printf '"%s"' "$v"
}
# admin_get <url>: GET as the admin (credentials on stdin, off the argv).
admin_get() {
    local out
    out=$(printf 'user = %s\n' "$(cfgq "$ADMIN:$ARNEB_ADMIN_PASSWORD")" |
        curl -sS -K - -w '\n%{http_code}' "$1") || fail "curl GET $1"
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
    [ "$CODE" = 200 ] || fail "GET $1 answered HTTP $CODE"
}

# mcp_tool <name> <args-json>: tools/call as the admin; $LAST = structuredContent.
mcp_tool() {
    local body out
    body=$(jq -nc --arg n "$1" --argjson a "$2" \
        '{jsonrpc:"2.0", id:1, method:"tools/call", params:{name:$n, arguments:$a}}')
    out=$({ printf 'user = %s\n' "$(cfgq "$ADMIN:$ARNEB_ADMIN_PASSWORD")"
        printf 'data-raw = %s\n' "$(cfgq "$body")"; } |
        curl -sS -K - -w '\n%{http_code}' -X POST "${ARNEB_URL%/}/mcp" \
            -H 'Content-Type: application/json' \
            -H 'Accept: application/json, text/event-stream' \
            -H 'MCP-Protocol-Version: 2025-06-18') || fail "curl POST /mcp"
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
    [ "$CODE" = 200 ] || fail "tools/call $1 answered HTTP $CODE"
    check '.error == null and .result.isError == false' "tools/call $1 failed"
    LAST=$(jq -c '.result.structuredContent' <<<"$LAST")
}

# audit_all: every audit entry, newest first, into $LAST as one JSON array.
audit_all() {
    local all='[]' url="$API/audit?limit=500" cursor
    while :; do
        admin_get "$url"
        all=$(jq -c --argjson acc "$all" '$acc + .entries' <<<"$LAST")
        cursor=$(jq -r 'if .truncated then .next_cursor else empty end' <<<"$LAST")
        [ -n "$cursor" ] || break
        url="$API/audit?limit=500&cursor=$cursor"
    done
    LAST="$all"
}

# The manage actions this run makes as the admin, before the restart.
ACTIONS=(worker.cordon worker.drain worker.uncordon query.cancel admission.set
    catalog.create access_rules.set user.create token.issue token.revoke settings.update)

run() { # <label> <command...>: one smoke, its output indented under a banner.
    local label="$1" rc=0
    shift
    echo "=== $label"
    "$@" 2>&1 | sed 's/^/  /' || rc=$?
    [ "$rc" = 0 ] || { STEP="$label"; LAST=""; fail "exited $rc"; }
}

# Scenario steps 1-4 over the agent API, then over MCP (plus steps 5 and 8).
ARNEB_USER="$ADMIN" ARNEB_PASSWORD="$ARNEB_ADMIN_PASSWORD" \
    ARNEB_PG="host=${ARNEB_PG%:*} port=${ARNEB_PG##*:} user=$ADMIN dbname=arneb" \
    run "agent API (steps 1-4)" "$HERE/agent-api-smoke.sh"
ARNEB_USER="$ADMIN" ARNEB_PASSWORD="$ARNEB_ADMIN_PASSWORD" \
    run "MCP (steps 1-5, 8)" "$HERE/mcp-smoke.sh"

STEP="A1 slow queries"
# The HTTP query list has no elapsed filter; the slow-query view is the MCP
# list_queries tool's min_elapsed_ms. Filter at the slowest query seen so far
# (a long statement the smokes cancelled) and check the filter keeps only it
# and anything slower.
mcp_tool list_queries '{"limit":500}'
ALL="$LAST"
SLOWEST=$(jq '[.queries[].elapsed_ms] | max // 0' <<<"$ALL")
[ "$SLOWEST" -gt 0 ] || fail "no query with a positive elapsed_ms in the history"
mcp_tool list_queries "{\"min_elapsed_ms\":$SLOWEST,\"limit\":500}"
check ".queries | length >= 1 and all(.elapsed_ms >= $SLOWEST)" \
    "list_queries min_elapsed_ms=$SLOWEST kept a faster query, or none"
check "(.queries | length) < ($ALL | .queries | length)" \
    "list_queries min_elapsed_ms=$SLOWEST did not filter anything out"
echo "  $(jq '.queries | length' <<<"$LAST") of $(jq '.queries | length' <<<"$ALL") queries at or over $SLOWEST ms"
pass

# Steps 7, 8 and the negative checks for the runtime config. It needs both
# workers schedulable, so it runs before the manage smoke leaves a cordon.
run "runtime config (steps 7, 8, negative)" "$HERE/runtime-config-smoke.sh"
# Steps 6, 10 and the negative checks; the cordon is kept for the restart.
KEEP_CORDON=1 run "manage (steps 6, 10, negative)" "$HERE/manage-smoke.sh"

STEP="A2 every manage action is audited with its identity"
audit_all
BEFORE="$WORK/audit-before.json"
printf '%s' "$LAST" >"$BEFORE"
for a in "${ACTIONS[@]}"; do
    check "any(.[]; .action == \"$a\" and .user == \"$ADMIN\" and .outcome == \"ok\")" \
        "no successful $a by $ADMIN in the audit log"
done
check 'all(.[]; (.user | type) == "string" and .user != "" and (.via | type) == "string")' \
    "an audit entry has no user or no via"
check "all(.[]; .user != \"$USER2\" or (.action | startswith(\"token.\")))" \
    "the audit log holds a manage action by $USER2, who has no manage privilege"
echo "  $(jq length "$BEFORE") entries, every expected action present"
pass

echo "=== restart the coordinator"
STEP="restart the coordinator"
LAST=""
bash -c "$ARNEB_RESTART_COORDINATOR" 2>&1 | sed 's/^/  /' || fail "the restart command failed"

# Step 9 for the manage state (the cordon, the admission limits, the audit).
run "manage after the restart (step 9)" "$HERE/manage-smoke.sh" --after-restart

STEP="A2 the audit log survives the restart"
audit_all
# Every entry from before the restart is still listed, unchanged.
jq -e --slurpfile b "$BEFORE" '. as $now | all($b[0][]; . as $e | any($now[]; . == $e))' \
    <<<"$LAST" >/dev/null || fail "an audit entry from before the restart is missing or changed"
pass

echo "=== restart one worker"
STEP="restart one worker"
LAST=""
bash -c "$ARNEB_RESTART_WORKER" 2>&1 | sed 's/^/  /' || fail "the restart command failed"

# Step 9 for the runtime config, then the re-seed back to the config file.
run "runtime config after the restart (step 9)" "$HERE/runtime-config-smoke.sh" --after-restart

if [ -n "${KEYCLOAK_ISSUER:-}" ]; then
    run "OAuth (steps 1-2)" "$HERE/mcp-oauth-smoke.sh"
else
    echo "=== OAuth skipped (KEYCLOAK_ISSUER is not set)"
fi

echo "=== headless acceptance passed"
